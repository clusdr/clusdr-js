import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import type { Server } from "@grpc/grpc-js";

import { ClusdrError, dial } from "../src/index.ts";
import { startFakeServer, stopServer, type FakeState } from "./fake.ts";

let addr = "";
let state: FakeState;
let server: Server;

before(async () => {
  const fake = await startFakeServer();
  addr = fake.addr;
  state = fake.state;
  server = fake.server;
});

after(async () => {
  await stopServer(server);
});

test("lock acquire unlock", async () => {
  const a = await dial(addr, { insecure: true, holder: "worker-a" });
  const b = await dial(addr, { insecure: true, holder: "worker-b" });
  try {
    const lk = await a.lock("scheduler", 1);
    assert.ok(lk.token > 0);
    assert.equal(lk.holder, "worker-a");
    assert.ok(lk.deadline);

    assert.equal(await b.tryLock("scheduler", 1), null);

    await a.unlock("scheduler");
    assert.ok(!state.coord.locks.has("scheduler"));

    const won = await b.tryLock("scheduler", 1);
    assert.ok(won);
    assert.ok(won.token > lk.token);
    await b.unlock("scheduler");
  } finally {
    await a.close();
    await b.close();
  }
});

test("lock waits for unlock", async () => {
  const a = await dial(addr, { insecure: true, holder: "worker-a" });
  const b = await dial(addr, { insecure: true, holder: "worker-b" });
  try {
    const first = await a.tryLock("job", 1);
    assert.ok(first);
    const waiter = b.lock("job", 1, 3);
    await new Promise((r) => setTimeout(r, 50));
    await a.unlock("job");
    const got = await waiter;
    assert.equal(got.holder, "worker-b");
  } finally {
    await a.close();
    await b.close();
  }
});

test("unlock requires acquire", async () => {
  const c = await dial(addr, { insecure: true });
  try {
    await assert.rejects(() => c.unlock("missing"), /not held/);
  } finally {
    await c.close();
  }
});

test("close releases lock", async () => {
  const a = await dial(addr, { insecure: true, holder: "worker-a" });
  await a.lock("job", 1);
  await a.close();
  assert.ok(!state.coord.locks.has("job"));
});

test("lock renew keeps grant", async () => {
  const c = await dial(addr, { insecure: true, holder: "worker-a" });
  try {
    const lk = await c.lock("job", 0.15);
    const first = state.coord.locks.get("job")?.deadline ?? 0;
    await new Promise((r) => setTimeout(r, 200));
    state.coord.expireDue();
    assert.ok(state.coord.locks.has("job"));
    assert.ok((state.coord.locks.get("job")?.deadline ?? 0) > first);
    assert.ok(lk.token > 0);
  } finally {
    await c.close();
  }
});

test("lease grant revoke", async () => {
  const a = await dial(addr, { insecure: true, holder: "worker-a" });
  const b = await dial(addr, { insecure: true, holder: "worker-b" });
  try {
    const ls = await a.lease("worker-1", 1);
    assert.ok(ls.token > 0);
    assert.equal(ls.owner, "worker-a");
    await assert.rejects(() => b.lease("worker-1", 1), /held/);
    await a.revoke("worker-1");
    assert.ok(!state.coord.leases.has("worker-1"));
    const won = await b.lease("worker-1", 1);
    assert.ok(won.token > ls.token);
    await b.revoke("worker-1");
  } finally {
    await a.close();
    await b.close();
  }
});

test("lease renew and cancel", async () => {
  const c = await dial(addr, { insecure: true, holder: "worker-a" });
  try {
    const ls = await c.lease("worker-1", 0.08);
    const first = state.coord.leases.get("worker-1")?.deadline ?? 0;
    await c.renew("worker-1");
    assert.ok((state.coord.leases.get("worker-1")?.deadline ?? 0) > first);
    ls.stopRenew();
    const deadline = Date.now() + 2000;
    let expired = false;
    while (Date.now() < deadline) {
      state.coord.expireDue();
      if (!state.coord.leases.has("worker-1")) {
        expired = true;
        break;
      }
      await new Promise((r) => setTimeout(r, 10));
    }
    assert.ok(expired);
    assert.ok(ls.token > 0);
  } finally {
    await c.close();
  }
});

test("close revokes lease", async () => {
  const c = await dial(addr, { insecure: true, holder: "worker-a" });
  await c.lease("worker-1", 1);
  await c.close();
  assert.ok(!state.coord.leases.has("worker-1"));
});

test("revoke requires grant", async () => {
  const c = await dial(addr, { insecure: true });
  try {
    await assert.rejects(() => c.revoke("missing"), /not held/);
    await assert.rejects(() => c.renew("missing"), /not held/);
  } finally {
    await c.close();
  }
});

test("ClusdrError name", () => {
  const err = new ClusdrError("clusdr: x");
  assert.equal(err.name, "ClusdrError");
  assert.ok(err instanceof Error);
});
