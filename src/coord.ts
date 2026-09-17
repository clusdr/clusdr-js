import { status } from "@grpc/grpc-js";

import { ClusdrError, wrap } from "./error.js";
import {
  type GrantResponse,
  type LeaseClient,
  type LockClient,
  type LockResponse,
  type TryLockResponse,
  unary,
} from "./proto.js";
import { isServiceError, remainingMs, retry } from "./retry.js";
import { sleep } from "./sleep.js";
import { fromMs, Lease, Lock, renewIntervalMs, ttlMs } from "./types.js";

export interface CoordHost {
  holder: string;
  closed: boolean;
  abortSignal: AbortSignal;
  requestTimeout: number;
  lockClient: LockClient;
  leaseClient: LeaseClient;
  deadline(timeout?: number): number;
}

export class Coord {
  readonly #host: CoordHost;
  readonly #mu = new Mutex();
  #held = new Map<string, Lock>();
  #leased = new Map<string, Lease>();

  constructor(host: CoordHost) {
    this.#host = host;
  }

  async lock(name: string, ttl?: number, timeout?: number): Promise<Lock> {
    const existing = await this.heldLock(name);
    if (existing) {
      return existing;
    }
    const deadline = this.#host.deadline(timeout);
    let resp: LockResponse;
    try {
      resp = await retry(
        () =>
          unary(
            this.#host.lockClient.lock.bind(this.#host.lockClient),
            { name, holder: this.#host.holder, ttlMs: ttlMs(ttl) },
            remainingMs(deadline),
          ),
        deadline,
        undefined,
        this.#host.abortSignal,
      );
    } catch (err) {
      throw wrap(`clusdr: lock ${JSON.stringify(name)}`, err);
    }
    if (!resp.acquired) {
      const msg = resp.message || "not acquired";
      throw new ClusdrError(`clusdr: lock ${JSON.stringify(name)}: ${msg}`);
    }
    return this.adoptLock(resp, name, ttl);
  }

  async tryLock(name: string, ttl?: number, timeout?: number): Promise<Lock | null> {
    const existing = await this.heldLock(name);
    if (existing) {
      return existing;
    }
    const deadline = this.#host.deadline(timeout);
    let resp: TryLockResponse;
    try {
      resp = await retry(
        () =>
          unary(
            this.#host.lockClient.tryLock.bind(this.#host.lockClient),
            { name, holder: this.#host.holder, ttlMs: ttlMs(ttl) },
            remainingMs(deadline),
          ),
        deadline,
        undefined,
        this.#host.abortSignal,
      );
    } catch (err) {
      throw wrap(`clusdr: trylock ${JSON.stringify(name)}`, err);
    }
    if (!resp.acquired) {
      return null;
    }
    return this.adoptLock(resp, name, ttl);
  }

  async unlock(name: string, timeout?: number): Promise<void> {
    const lk = await this.heldLock(name);
    if (!lk) {
      throw new ClusdrError(`clusdr: lock ${JSON.stringify(name)} is not held by this client`);
    }
    await this.releaseLock(lk, timeout);
  }

  async lease(name: string, ttl?: number, timeout?: number): Promise<Lease> {
    const existing = await this.heldLease(name);
    if (existing) {
      return existing;
    }
    const deadline = this.#host.deadline(timeout);
    let resp: GrantResponse;
    try {
      resp = await retry(
        () =>
          unary(
            this.#host.leaseClient.grant.bind(this.#host.leaseClient),
            { name, owner: this.#host.holder, ttlMs: ttlMs(ttl) },
            remainingMs(deadline),
          ),
        deadline,
        undefined,
        this.#host.abortSignal,
      );
    } catch (err) {
      throw wrap(`clusdr: lease ${JSON.stringify(name)}`, err);
    }
    if (!resp.granted) {
      const msg = resp.message || "not granted";
      if (resp.owner) {
        throw new ClusdrError(`clusdr: lease ${JSON.stringify(name)}: ${msg} (owner ${resp.owner})`);
      }
      throw new ClusdrError(`clusdr: lease ${JSON.stringify(name)}: ${msg}`);
    }
    return this.adoptLease(resp, name, ttl);
  }

  async renew(name: string, timeout?: number): Promise<void> {
    const ls = await this.heldLease(name);
    if (!ls) {
      throw new ClusdrError(`clusdr: lease ${JSON.stringify(name)} is not held by this client`);
    }
    const deadline = this.#host.deadline(timeout);
    try {
      const resp = await retry(
        () =>
          unary(
            this.#host.leaseClient.renew.bind(this.#host.leaseClient),
            { name: ls.name, owner: ls.owner, fencingToken: ls.token, ttlMs: 0 },
            remainingMs(deadline),
          ),
        deadline,
        undefined,
        this.#host.abortSignal,
      );
      if (resp && !resp.renewed) {
        throw new ClusdrError(`clusdr: renew ${JSON.stringify(name)}: ${resp.message}`);
      }
      if (resp?.deadlineUnixMs) {
        ls.setDeadline(fromMs(resp.deadlineUnixMs));
      }
    } catch (err) {
      if (err instanceof ClusdrError) {
        throw err;
      }
      throw wrap(`clusdr: renew ${JSON.stringify(name)}`, err);
    }
  }

  async revoke(name: string, timeout?: number): Promise<void> {
    const ls = await this.heldLease(name);
    if (!ls) {
      throw new ClusdrError(`clusdr: lease ${JSON.stringify(name)} is not held by this client`);
    }
    await this.dropLease(ls, timeout);
  }

  async releaseGrants(): Promise<void> {
    const { locks, leases } = await this.#mu.run(() => {
      const locks = [...this.#held.values()];
      const leases = [...this.#leased.values()];
      return { locks, leases };
    });
    for (const lk of locks) {
      try {
        await this.releaseLock(lk);
      } catch {
        // best-effort on close
      }
    }
    for (const ls of leases) {
      try {
        await this.dropLease(ls);
      } catch {
        // best-effort on close
      }
    }
  }

  private async heldLock(name: string): Promise<Lock | undefined> {
    return this.#mu.run(() => this.#held.get(name));
  }

  private async heldLease(name: string): Promise<Lease | undefined> {
    return this.#mu.run(() => this.#leased.get(name));
  }

  private async adoptLock(resp: LockResponse | TryLockResponse, name: string, ttl?: number): Promise<Lock> {
    const lk = new Lock(name, resp.holder || this.#host.holder, resp.fencingToken, fromMs(resp.deadlineUnixMs));
    const existing = await this.#mu.run(() => {
      const cur = this.#held.get(name);
      if (cur && cur.token === lk.token) {
        return cur;
      }
      this.#held.set(name, lk);
      return undefined;
    });
    if (existing) {
      return existing;
    }
    this.startLockRenew(lk, ttl);
    return lk;
  }

  private async adoptLease(resp: GrantResponse, name: string, ttl?: number): Promise<Lease> {
    const ls = new Lease(name, resp.owner || this.#host.holder, resp.fencingToken, fromMs(resp.deadlineUnixMs));
    const existing = await this.#mu.run(() => {
      const cur = this.#leased.get(name);
      if (cur && cur.token === ls.token) {
        return cur;
      }
      this.#leased.set(name, ls);
      return undefined;
    });
    if (existing) {
      return existing;
    }
    this.startLeaseRenew(ls, ttl);
    return ls;
  }

  private async releaseLock(lk: Lock, timeout?: number): Promise<void> {
    lk.stop.abort();
    const deadline = this.#host.deadline(timeout);
    try {
      const resp = await retry(
        () =>
          unary(
            this.#host.lockClient.unlock.bind(this.#host.lockClient),
            { name: lk.name, holder: lk.holder, fencingToken: lk.token },
            remainingMs(deadline),
          ),
        deadline,
        undefined,
        this.#host.abortSignal,
      );
      if (resp && !resp.released) {
        throw new ClusdrError(`clusdr: unlock ${JSON.stringify(lk.name)}: ${resp.message}`);
      }
      await this.forgetLock(lk.name, lk.token);
    } catch (err) {
      if (isServiceError(err) && err.code === status.FAILED_PRECONDITION) {
        await this.forgetLock(lk.name, lk.token);
      }
      if (err instanceof ClusdrError) {
        throw err;
      }
      throw wrap(`clusdr: unlock ${JSON.stringify(lk.name)}`, err);
    }
  }

  private async dropLease(ls: Lease, timeout?: number): Promise<void> {
    ls.stop.abort();
    const deadline = this.#host.deadline(timeout);
    try {
      const resp = await retry(
        () =>
          unary(
            this.#host.leaseClient.revoke.bind(this.#host.leaseClient),
            { name: ls.name, owner: ls.owner, fencingToken: ls.token },
            remainingMs(deadline),
          ),
        deadline,
        undefined,
        this.#host.abortSignal,
      );
      if (resp && !resp.revoked) {
        throw new ClusdrError(`clusdr: revoke ${JSON.stringify(ls.name)}: ${resp.message}`);
      }
      await this.forgetLease(ls.name, ls.token);
    } catch (err) {
      if (isServiceError(err) && err.code === status.FAILED_PRECONDITION) {
        await this.forgetLease(ls.name, ls.token);
      }
      if (err instanceof ClusdrError) {
        throw err;
      }
      throw wrap(`clusdr: revoke ${JSON.stringify(ls.name)}`, err);
    }
  }

  private async forgetLock(name: string, token: number): Promise<void> {
    await this.#mu.run(() => {
      const cur = this.#held.get(name);
      if (cur && cur.token === token) {
        this.#held.delete(name);
      }
    });
  }

  private async forgetLease(name: string, token: number): Promise<void> {
    await this.#mu.run(() => {
      const cur = this.#leased.get(name);
      if (cur && cur.token === token) {
        this.#leased.delete(name);
      }
    });
  }

  private startLockRenew(lk: Lock, ttl?: number): void {
    const interval = renewIntervalMs(ttl, lk.deadline);
    const stop = abortAny(lk.stop.signal, this.#host.abortSignal);
    void (async () => {
      while (await sleep(interval, stop)) {
        if (this.#host.closed) {
          return;
        }
        try {
          const resp = await unary(
            this.#host.lockClient.renew.bind(this.#host.lockClient),
            { name: lk.name, holder: lk.holder, fencingToken: lk.token, ttlMs: 0 },
            this.#host.requestTimeout * 1000,
          );
          if (resp.deadlineUnixMs) {
            lk.setDeadline(fromMs(resp.deadlineUnixMs));
          }
        } catch (err) {
          if (
            isServiceError(err) &&
            (err.code === status.FAILED_PRECONDITION || err.code === status.CANCELLED)
          ) {
            return;
          }
        }
      }
    })();
  }

  private startLeaseRenew(ls: Lease, ttl?: number): void {
    const interval = renewIntervalMs(ttl, ls.deadline);
    const stop = abortAny(ls.stop.signal, this.#host.abortSignal);
    void (async () => {
      while (await sleep(interval, stop)) {
        if (this.#host.closed) {
          return;
        }
        try {
          const resp = await unary(
            this.#host.leaseClient.renew.bind(this.#host.leaseClient),
            { name: ls.name, owner: ls.owner, fencingToken: ls.token, ttlMs: 0 },
            this.#host.requestTimeout * 1000,
          );
          if (resp.deadlineUnixMs) {
            ls.setDeadline(fromMs(resp.deadlineUnixMs));
          }
        } catch (err) {
          if (
            isServiceError(err) &&
            (err.code === status.FAILED_PRECONDITION || err.code === status.CANCELLED)
          ) {
            return;
          }
        }
      }
    })();
  }
}

function abortAny(...signals: AbortSignal[]): AbortSignal {
  const ac = new AbortController();
  for (const s of signals) {
    if (s.aborted) {
      ac.abort();
      return ac.signal;
    }
    s.addEventListener("abort", () => ac.abort(), { once: true });
  }
  return ac.signal;
}

class Mutex {
  #tail: Promise<void> = Promise.resolve();

  run<T>(fn: () => T | Promise<T>): Promise<T> {
    const run = this.#tail.then(fn, fn);
    this.#tail = run.then(
      () => undefined,
      () => undefined,
    );
    return run;
  }
}
