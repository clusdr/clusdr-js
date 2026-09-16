import {
  Server,
  ServerCredentials,
  status,
  type sendUnaryData,
  type ServerUnaryCall,
  type ServerWritableStream,
} from "@grpc/grpc-js";

import {
  type GrantLeaseRequest,
  type GrantLeaseResponse,
  type LockRequest,
  type LockResponse,
  type PublishEventRequest,
  type PublishEventResponse,
  type RenewLeaseRequest,
  type RenewLeaseResponse,
  type RenewLockRequest,
  type RenewLockResponse,
  type RevokeLeaseRequest,
  type RevokeLeaseResponse,
  type UnlockRequest,
  type UnlockResponse,
  type WatchRequest,
  type WatchResponse,
  loadV1,
} from "../src/proto.ts";

interface Grant {
  name: string;
  holder: string;
  token: number;
  deadline: number;
  ttl: number;
}

export class CoordTable {
  locks = new Map<string, Grant>();
  leases = new Map<string, Grant>();
  nextLock = 0;
  nextLease = 0;
  #waiters: Array<() => void> = [];

  notify(): void {
    const waiters = this.#waiters;
    this.#waiters = [];
    for (const w of waiters) {
      w();
    }
  }

  wait(ms: number): Promise<void> {
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        this.#waiters = this.#waiters.filter((w) => w !== onNotify);
        resolve();
      }, ms);
      const onNotify = () => {
        clearTimeout(timer);
        resolve();
      };
      this.#waiters.push(onNotify);
    });
  }

  acquireLock(name: string, holder: string, ttl: number): [Grant, boolean] {
    this.expireLocked(this.locks);
    const rec = this.locks.get(name);
    if (rec) {
      if (rec.holder === holder) {
        rec.deadline = Date.now() + ttl * 1000;
        rec.ttl = ttl;
        this.notify();
        return [rec, true];
      }
      return [rec, false];
    }
    this.nextLock += 1;
    const created: Grant = {
      name,
      holder,
      token: this.nextLock,
      deadline: Date.now() + ttl * 1000,
      ttl,
    };
    this.locks.set(name, created);
    this.notify();
    return [created, true];
  }

  releaseLock(name: string, holder: string, token: number): void {
    const rec = this.locks.get(name);
    if (!rec || rec.holder !== holder || rec.token !== token) {
      throw new Error("fencing token mismatch");
    }
    this.locks.delete(name);
    this.notify();
  }

  renewLock(name: string, holder: string, token: number, ttl: number): Grant {
    const rec = this.locks.get(name);
    if (!rec || rec.holder !== holder || rec.token !== token) {
      throw new Error("fencing token mismatch");
    }
    rec.deadline = Date.now() + ttl * 1000;
    rec.ttl = ttl;
    return rec;
  }

  grantLease(name: string, owner: string, ttl: number): [Grant, boolean] {
    this.expireLocked(this.leases);
    const rec = this.leases.get(name);
    if (rec) {
      if (rec.holder === owner) {
        rec.deadline = Date.now() + ttl * 1000;
        rec.ttl = ttl;
        return [rec, true];
      }
      return [rec, false];
    }
    this.nextLease += 1;
    const created: Grant = {
      name,
      holder: owner,
      token: this.nextLease,
      deadline: Date.now() + ttl * 1000,
      ttl,
    };
    this.leases.set(name, created);
    return [created, true];
  }

  revokeLease(name: string, owner: string, token: number): void {
    const rec = this.leases.get(name);
    if (!rec || rec.holder !== owner || rec.token !== token) {
      throw new Error("fencing token mismatch");
    }
    this.leases.delete(name);
  }

  renewLease(name: string, owner: string, token: number, ttl: number): Grant {
    const rec = this.leases.get(name);
    if (!rec || rec.holder !== owner || rec.token !== token) {
      throw new Error("fencing token mismatch");
    }
    rec.deadline = Date.now() + ttl * 1000;
    rec.ttl = ttl;
    return rec;
  }

  expireDue(): void {
    this.expireLocked(this.locks);
    this.expireLocked(this.leases);
    this.notify();
  }

  private expireLocked(table: Map<string, Grant>): void {
    const now = Date.now();
    for (const [name, rec] of table) {
      if (rec.deadline <= now) {
        table.delete(name);
      }
    }
  }
}

export class FakeState {
  members = [
    { id: "node-a", address: "127.0.0.1:1", status: "alive", leader: true, role: "" },
  ];
  events: WatchResponse[] = [];
  eventWaiters: Array<() => void> = [];
  published: PublishEventRequest[] = [];
  coord = new CoordTable();

  pushEvent(ev: WatchResponse): void {
    this.events.push(ev);
    const waiters = this.eventWaiters;
    this.eventWaiters = [];
    for (const w of waiters) {
      w();
    }
  }

  takeEvent(timeoutMs: number): Promise<WatchResponse | undefined> {
    if (this.events.length > 0) {
      return Promise.resolve(this.events.shift());
    }
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        this.eventWaiters = this.eventWaiters.filter((w) => w !== onEv);
        resolve(undefined);
      }, timeoutMs);
      const onEv = () => {
        clearTimeout(timer);
        resolve(this.events.shift());
      };
      this.eventWaiters.push(onEv);
    });
  }
}

function ttlS(ttlMs: number, reuse = 15): number {
  if (ttlMs > 0) {
    return ttlMs / 1000;
  }
  return reuse;
}

function deadlineMs(rec: Grant): number {
  return Math.round(rec.deadline);
}

function watchMatch(eventType: string, topics: string[], types: string[]): boolean {
  if (eventType === "watch.sync" || eventType === "watch.gap") {
    return true;
  }
  if (topics.length > 0) {
    if (!eventType.startsWith("custom.") || !topics.includes(eventType.slice(7))) {
      return false;
    }
  }
  if (types.length > 0 && !types.includes(eventType)) {
    return false;
  }
  return true;
}

export function healthImpl() {
  return {
    health(
      _call: ServerUnaryCall<Record<string, never>, unknown>,
      callback: sendUnaryData<{ nodeId: string; clusterId: string; role: string; healthy: boolean }>,
    ) {
      callback(null, { nodeId: "node-a", clusterId: "c1", role: "leader", healthy: true });
    },
  };
}

export class FlakyHealth {
  failures: number;
  calls = 0;

  constructor(failures: number) {
    this.failures = failures;
  }

  health(
    _call: ServerUnaryCall<Record<string, never>, unknown>,
    callback: sendUnaryData<{ nodeId: string; clusterId: string; role: string; healthy: boolean }>,
  ) {
    this.calls += 1;
    if (this.calls <= this.failures) {
      callback({ code: status.UNAVAILABLE, message: "wait" } as never);
      return;
    }
    callback(null, { nodeId: "node-a", clusterId: "c1", role: "leader", healthy: true });
  }
}

function membershipImpl(state: FakeState) {
  return {
    listMembers(
      _call: ServerUnaryCall<Record<string, never>, unknown>,
      callback: sendUnaryData<{ members: FakeState["members"] }>,
    ) {
      callback(null, { members: state.members });
    },
    getLeader(
      _call: ServerUnaryCall<Record<string, never>, unknown>,
      callback: sendUnaryData<{ leaderId: string; address: string }>,
    ) {
      const m = state.members.find((x) => x.leader);
      if (m) {
        callback(null, { leaderId: m.id, address: m.address });
        return;
      }
      callback(null, { leaderId: "", address: "" });
    },
  };
}

function eventsImpl(state: FakeState) {
  return {
    publishEvent(
      call: ServerUnaryCall<PublishEventRequest, PublishEventResponse>,
      callback: sendUnaryData<PublishEventResponse>,
    ) {
      state.published.push(call.request);
      const ev: WatchResponse = {
        type: `custom.${call.request.topic}`,
        source: "node-a",
        payload: call.request.payload,
        timestampUnixMs: 1,
        seq: state.published.length,
      };
      state.pushEvent(ev);
      callback(null, { accepted: true, message: "", eventId: "", type: ev.type });
    },
  };
}

function watchImpl(state: FakeState) {
  return {
    watch(call: ServerWritableStream<WatchRequest, WatchResponse>) {
      const topics = (call.request.topics ?? []).map((t) =>
        t.startsWith("custom.") ? t.slice(7) : t,
      );
      const types = [...(call.request.eventTypes ?? [])];
      const snap: WatchResponse = {
        type: "member.join",
        source: "node-a",
        payload: Buffer.alloc(0),
        timestampUnixMs: 1,
        seq: 0,
      };
      if (watchMatch(snap.type, topics, types)) {
        call.write(snap);
      }
      let stopped = false;
      call.on("cancelled", () => {
        stopped = true;
      });
      call.on("error", () => {
        stopped = true;
      });
      void (async () => {
        while (!stopped && !call.cancelled) {
          const ev = await state.takeEvent(50);
          if (stopped || call.cancelled) {
            return;
          }
          if (!ev) {
            continue;
          }
          if (!watchMatch(ev.type, topics, types)) {
            continue;
          }
          call.write(ev);
        }
      })();
    },
  };
}

function locksImpl(table: CoordTable) {
  return {
    async lock(
      call: ServerUnaryCall<LockRequest, LockResponse>,
      callback: sendUnaryData<LockResponse>,
    ) {
      const ttl = ttlS(call.request.ttlMs);
      while (!call.cancelled) {
        const [rec, ok] = table.acquireLock(call.request.name, call.request.holder, ttl);
        if (ok) {
          callback(null, {
            acquired: true,
            message: "",
            fencingToken: rec.token,
            holder: rec.holder,
            deadlineUnixMs: deadlineMs(rec),
          });
          return;
        }
        await table.wait(50);
      }
      callback({ code: status.CANCELLED, message: "cancelled" } as never);
    },
    tryLock(
      call: ServerUnaryCall<LockRequest, LockResponse>,
      callback: sendUnaryData<LockResponse>,
    ) {
      const [rec, ok] = table.acquireLock(call.request.name, call.request.holder, ttlS(call.request.ttlMs));
      if (!ok) {
        callback(null, {
          acquired: false,
          message: "held",
          fencingToken: rec.token,
          holder: rec.holder,
          deadlineUnixMs: deadlineMs(rec),
        });
        return;
      }
      callback(null, {
        acquired: true,
        message: "",
        fencingToken: rec.token,
        holder: rec.holder,
        deadlineUnixMs: deadlineMs(rec),
      });
    },
    unlock(
      call: ServerUnaryCall<UnlockRequest, UnlockResponse>,
      callback: sendUnaryData<UnlockResponse>,
    ) {
      try {
        table.releaseLock(call.request.name, call.request.holder, call.request.fencingToken);
      } catch (err) {
        callback({ code: status.FAILED_PRECONDITION, message: String(err) } as never);
        return;
      }
      callback(null, { released: true, message: "" });
    },
    renew(
      call: ServerUnaryCall<RenewLockRequest, RenewLockResponse>,
      callback: sendUnaryData<RenewLockResponse>,
    ) {
      try {
        const cur = table.locks.get(call.request.name);
        const reuse = cur?.ttl ?? 15;
        const rec = table.renewLock(
          call.request.name,
          call.request.holder,
          call.request.fencingToken,
          ttlS(call.request.ttlMs, reuse),
        );
        callback(null, {
          renewed: true,
          message: "",
          fencingToken: call.request.fencingToken,
          deadlineUnixMs: deadlineMs(rec),
        });
      } catch (err) {
        callback({ code: status.FAILED_PRECONDITION, message: String(err) } as never);
      }
    },
  };
}

function leasesImpl(table: CoordTable) {
  return {
    grant(
      call: ServerUnaryCall<GrantLeaseRequest, GrantLeaseResponse>,
      callback: sendUnaryData<GrantLeaseResponse>,
    ) {
      const [rec, ok] = table.grantLease(call.request.name, call.request.owner, ttlS(call.request.ttlMs));
      if (!ok) {
        callback(null, {
          granted: false,
          message: "held",
          fencingToken: rec.token,
          owner: rec.holder,
          deadlineUnixMs: deadlineMs(rec),
        });
        return;
      }
      callback(null, {
        granted: true,
        message: "",
        fencingToken: rec.token,
        owner: rec.holder,
        deadlineUnixMs: deadlineMs(rec),
      });
    },
    renew(
      call: ServerUnaryCall<RenewLeaseRequest, RenewLeaseResponse>,
      callback: sendUnaryData<RenewLeaseResponse>,
    ) {
      try {
        const cur = table.leases.get(call.request.name);
        const reuse = cur?.ttl ?? 15;
        const rec = table.renewLease(
          call.request.name,
          call.request.owner,
          call.request.fencingToken,
          ttlS(call.request.ttlMs, reuse),
        );
        callback(null, {
          renewed: true,
          message: "",
          fencingToken: call.request.fencingToken,
          deadlineUnixMs: deadlineMs(rec),
        });
      } catch (err) {
        callback({ code: status.FAILED_PRECONDITION, message: String(err) } as never);
      }
    },
    revoke(
      call: ServerUnaryCall<RevokeLeaseRequest, RevokeLeaseResponse>,
      callback: sendUnaryData<RevokeLeaseResponse>,
    ) {
      try {
        table.revokeLease(call.request.name, call.request.owner, call.request.fencingToken);
      } catch (err) {
        callback({ code: status.FAILED_PRECONDITION, message: String(err) } as never);
        return;
      }
      callback(null, { revoked: true, message: "" });
    },
  };
}

export async function startFakeServer(health: object = healthImpl()): Promise<{
  addr: string;
  state: FakeState;
  server: Server;
}> {
  const v1 = loadV1();
  const state = new FakeState();
  const server = new Server();
  server.addService(v1.HealthService.service, health);
  server.addService(v1.MembershipService.service, membershipImpl(state));
  server.addService(v1.EventService.service, eventsImpl(state));
  server.addService(v1.WatchService.service, watchImpl(state));
  server.addService(v1.LockService.service, locksImpl(state.coord));
  server.addService(v1.LeaseService.service, leasesImpl(state.coord));
  const port = await new Promise<number>((resolve, reject) => {
    server.bindAsync("127.0.0.1:0", ServerCredentials.createInsecure(), (err, bound) => {
      if (err) {
        reject(err);
        return;
      }
      resolve(bound);
    });
  });
  return { addr: `127.0.0.1:${port}`, state, server };
}

export function stopServer(server: Server): Promise<void> {
  return new Promise((resolve) => {
    server.tryShutdown(() => resolve());
    setTimeout(() => {
      server.forceShutdown();
      resolve();
    }, 200).unref();
  });
}
