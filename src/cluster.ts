import { Channel, status, type ClientOptions, type ServiceError } from "@grpc/grpc-js";
import { randomUUID } from "node:crypto";

import { Coord } from "./coord.js";
import { ClusdrError, wrap } from "./error.js";
import {
  MAX_PAYLOAD,
  envAddr,
  envInsecure,
  type ConnectOptions,
  type Options,
  type WatchFilter,
  resolveOptions,
} from "./options.js";
import {
  type EventClient,
  type HealthClient,
  type LeaseClient,
  type LockClient,
  type MembershipClient,
  type WatchClient,
  type WatchResponse,
  loadV1,
  unary,
} from "./proto.js";
import { isServiceError, readyTransient, remainingMs, retry } from "./retry.js";
import { sleep } from "./sleep.js";
import { channelSetup } from "./tls.js";
import { encodePayload, fromMs, type Event, type Member, type PublishPayload } from "./types.js";
import { normalizeWatchFilter } from "./watch.js";

export class Cluster {
  readonly #opts: Options;
  readonly #holder: string;
  readonly #channel: Channel;
  readonly #mem: MembershipClient;
  readonly #watch: WatchClient;
  readonly #ev: EventClient;
  readonly #health: HealthClient;
  readonly #lock: LockClient;
  readonly #lease: LeaseClient;
  readonly #closed = new AbortController();
  readonly #coord: Coord;
  readonly #watches = new Set<AbortController>();

  constructor(opts: Options, channel: Channel, clients: Clients, holder: string) {
    this.#opts = opts;
    this.#holder = holder;
    this.#channel = channel;
    this.#mem = clients.mem;
    this.#watch = clients.watch;
    this.#ev = clients.ev;
    this.#health = clients.health;
    this.#lock = clients.lock;
    this.#lease = clients.lease;
    this.#coord = new Coord(this);
  }

  get holder(): string {
    return this.#holder;
  }

  get closed(): boolean {
    return this.#closed.signal.aborted;
  }

  get abortSignal(): AbortSignal {
    return this.#closed.signal;
  }

  get requestTimeout(): number {
    return this.#opts.requestTimeout;
  }

  get lockClient(): LockClient {
    return this.#lock;
  }

  get leaseClient(): LeaseClient {
    return this.#lease;
  }

  async members(timeout?: number): Promise<Member[]> {
    const deadline = this.deadline(timeout);
    try {
      const resp = await retry(
        () => unary(this.#mem.listMembers.bind(this.#mem), {}, remainingMs(deadline)),
        deadline,
        undefined,
        this.#closed.signal,
      );
      return (resp.members ?? []).map((m) => ({
        id: m.id,
        address: m.address,
        status: m.status,
        leader: m.leader,
        role: m.role || "voter",
      }));
    } catch (err) {
      throw wrap("clusdr: members", err);
    }
  }

  async leader(timeout?: number): Promise<Member> {
    const deadline = this.deadline(timeout);
    try {
      const resp = await retry(
        () => unary(this.#mem.getLeader.bind(this.#mem), {}, remainingMs(deadline)),
        deadline,
        undefined,
        this.#closed.signal,
      );
      return {
        id: resp.leaderId,
        address: resp.address,
        status: "alive",
        leader: true,
        role: "voter",
      };
    } catch (err) {
      throw wrap("clusdr: leader", err);
    }
  }

  async publish(topic: string, payload?: PublishPayload, timeout?: number): Promise<void> {
    const body = encodePayload(payload);
    if (body.length > MAX_PAYLOAD) {
      throw new ClusdrError(`clusdr: publish payload exceeds ${MAX_PAYLOAD} bytes`);
    }
    const deadline = this.deadline(timeout);
    try {
      const resp = await retry(
        () =>
          unary(
            this.#ev.publishEvent.bind(this.#ev),
            { topic, payload: body, eventId: "", source: "", relay: false },
            remainingMs(deadline),
          ),
        deadline,
        undefined,
        this.#closed.signal,
      );
      if (resp && !resp.accepted) {
        throw new ClusdrError(`clusdr: publish rejected: ${resp.message}`);
      }
    } catch (err) {
      if (err instanceof ClusdrError) {
        throw err;
      }
      throw wrap("clusdr: publish", err);
    }
  }

  /**
   * Stream events. Empty filter is the full bus.
   *
   * Reconnects with `lastSeq` on drop. Breaking the loop or calling `close`
   * ends it. Several Watch streams on one client are fine.
   */
  watch(filter: WatchFilter = {}): AsyncIterable<Event> {
    const [topics, eventTypes] = normalizeWatchFilter(filter);
    const watchAbort = new AbortController();
    this.#watches.add(watchAbort);
    const stop = abortAny(this.#closed.signal, watchAbort.signal);
    const cluster = this;
    const iter = (async function* () {
      let lastSeq = 0;
      let backoff = 50;
      try {
        while (!stop.aborted) {
          let stream: ReturnType<WatchClient["watch"]>;
          try {
            stream = cluster.#watch.watch({ lastSeq, topics, eventTypes });
          } catch (err) {
            if (stop.aborted || cancelled(err)) {
              return;
            }
            if (isServiceError(err) && err.code === status.INVALID_ARGUMENT) {
              throw wrap("clusdr: watch", err);
            }
            if (!(await sleep(backoff, stop))) {
              return;
            }
            backoff = Math.min(backoff * 2, 2000);
            continue;
          }
          backoff = 50;
          try {
            for await (const resp of readable(stream, stop)) {
              if (resp.seq > lastSeq) {
                lastSeq = resp.seq;
              }
              yield toEvent(resp);
            }
          } catch (err) {
            if (stop.aborted || cancelled(err)) {
              return;
            }
            if (isServiceError(err) && err.code === status.INVALID_ARGUMENT) {
              throw wrap("clusdr: watch", err);
            }
          }
          if (!(await sleep(backoff, stop))) {
            return;
          }
          backoff = Math.min(backoff * 2, 2000);
        }
      } finally {
        watchAbort.abort();
        cluster.#watches.delete(watchAbort);
      }
    })();
    return iter;
  }

  lock(name: string, ttl?: number, timeout?: number) {
    return this.#coord.lock(name, ttl, timeout);
  }

  tryLock(name: string, ttl?: number, timeout?: number) {
    return this.#coord.tryLock(name, ttl, timeout);
  }

  unlock(name: string, timeout?: number) {
    return this.#coord.unlock(name, timeout);
  }

  lease(name: string, ttl?: number, timeout?: number) {
    return this.#coord.lease(name, ttl, timeout);
  }

  renew(name: string, timeout?: number) {
    return this.#coord.renew(name, timeout);
  }

  revoke(name: string, timeout?: number) {
    return this.#coord.revoke(name, timeout);
  }

  async close(): Promise<void> {
    if (this.#closed.signal.aborted) {
      this.#channel.close();
      return;
    }
    await this.#coord.releaseGrants();
    this.#closed.abort();
    for (const w of this.#watches) {
      w.abort();
    }
    this.#watches.clear();
    this.#channel.close();
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.close();
  }

  deadline(timeout?: number): number {
    const seconds = timeout === undefined ? this.#opts.requestTimeout : timeout;
    return Date.now() + seconds * 1000;
  }

  async waitReady(): Promise<void> {
    if (this.#opts.readyTimeout <= 0) {
      return;
    }
    const deadline = Date.now() + this.#opts.readyTimeout * 1000;
    try {
      await retry(
        () => {
          const slice = Math.min(500, Math.max(50, deadline - Date.now()));
          return unary(this.#health.health.bind(this.#health), {}, slice);
        },
        deadline,
        readyTransient,
        this.#closed.signal,
      );
    } catch (err) {
      await this.close();
      throw new ClusdrError(`clusdr: daemon not ready at ${this.#opts.addr}: ${String(err)}`, {
        cause: err,
      });
    }
  }
}

interface Clients {
  mem: MembershipClient;
  watch: WatchClient;
  ev: EventClient;
  health: HealthClient;
  lock: LockClient;
  lease: LeaseClient;
}

/** Connect to the daemon on this host. */
export async function local(opts: ConnectOptions = {}): Promise<Cluster> {
  return connect(resolveOptions(envAddr(), opts));
}

/**
 * Connect to `addr` (Runtime API host:port).
 *
 * Tests and a second daemon on this host. Applications use `local`.
 */
export async function dial(addr: string, opts: ConnectOptions = {}): Promise<Cluster> {
  if (!addr.trim()) {
    throw new ClusdrError("clusdr: empty dial address");
  }
  return connect(resolveOptions(addr, opts));
}

async function connect(opts: Options): Promise<Cluster> {
  if (!opts.insecure && envInsecure() && !opts.dataDir) {
    opts.insecure = true;
  }
  if (!opts.addr) {
    throw new ClusdrError("clusdr: empty dial address");
  }
  const setup = channelSetup(opts);
  const channel = new Channel(opts.addr, setup.credentials, setup.options);
  const clientOpts: ClientOptions = {
    channelOverride: channel,
    ...setup.options,
  };
  const v1 = loadV1();
  const clients: Clients = {
    health: new v1.HealthService(opts.addr, setup.credentials, clientOpts) as unknown as HealthClient,
    mem: new v1.MembershipService(opts.addr, setup.credentials, clientOpts) as unknown as MembershipClient,
    watch: new v1.WatchService(opts.addr, setup.credentials, clientOpts) as unknown as WatchClient,
    ev: new v1.EventService(opts.addr, setup.credentials, clientOpts) as unknown as EventClient,
    lock: new v1.LockService(opts.addr, setup.credentials, clientOpts) as unknown as LockClient,
    lease: new v1.LeaseService(opts.addr, setup.credentials, clientOpts) as unknown as LeaseClient,
  };
  const holder = opts.holder || `sdk-${randomUUID()}`;
  const cluster = new Cluster(opts, channel, clients, holder);
  await cluster.waitReady();
  return cluster;
}

function toEvent(resp: WatchResponse): Event {
  return {
    type: resp.type,
    source: resp.source,
    payload: new Uint8Array(resp.payload ?? Buffer.alloc(0)),
    timestamp: fromMs(resp.timestampUnixMs) ?? new Date(0),
    seq: resp.seq,
  };
}

function cancelled(err: unknown): boolean {
  return isServiceError(err) && err.code === status.CANCELLED;
}

function abortAny(...signals: AbortSignal[]): AbortSignal {
  const ac = new AbortController();
  const onAbort = () => ac.abort();
  for (const s of signals) {
    if (s.aborted) {
      ac.abort();
      return ac.signal;
    }
    s.addEventListener("abort", onAbort, { once: true });
  }
  return ac.signal;
}

async function* readable(
  stream: ReturnType<WatchClient["watch"]>,
  signal: AbortSignal,
): AsyncGenerator<WatchResponse> {
  const queue: WatchResponse[] = [];
  let pending: ((value: IteratorResult<WatchResponse>) => void) | undefined;
  let error: unknown;
  let done = false;

  const onData = (chunk: WatchResponse) => {
    if (pending) {
      const p = pending;
      pending = undefined;
      p({ value: chunk, done: false });
    } else {
      queue.push(chunk);
    }
  };
  const onError = (err: Error) => {
    error = err;
    done = true;
    pending?.({ value: undefined as unknown as WatchResponse, done: true });
    pending = undefined;
  };
  const onEnd = () => {
    done = true;
    pending?.({ value: undefined as unknown as WatchResponse, done: true });
    pending = undefined;
  };
  const onAbort = () => {
    stream.cancel();
    onError(Object.assign(new Error("cancelled"), { code: status.CANCELLED }) as ServiceError);
  };

  stream.on("data", onData);
  stream.on("error", onError);
  stream.on("end", onEnd);
  signal.addEventListener("abort", onAbort, { once: true });

  try {
    for (;;) {
      if (queue.length > 0) {
        yield queue.shift()!;
        continue;
      }
      if (error) {
        throw error;
      }
      if (done) {
        return;
      }
      const next = await new Promise<IteratorResult<WatchResponse>>((resolve) => {
        pending = resolve;
      });
      if (next.done) {
        if (error) {
          throw error;
        }
        return;
      }
      yield next.value;
    }
  } finally {
    signal.removeEventListener("abort", onAbort);
    try {
      stream.cancel();
    } catch {
      // already closed
    }
  }
}
