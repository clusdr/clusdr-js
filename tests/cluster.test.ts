import assert from "node:assert/strict";
import { after, before, test } from "node:test";

import { ClusdrError, dial, encodePayload, local } from "../src/index.ts";
import { FlakyHealth, startFakeServer, stopServer, type FakeState } from "./fake.ts";
import type { Server } from "@grpc/grpc-js";

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

test("members leader watch publish", async () => {
  const c = await dial(addr, { insecure: true });
  try {
    const members = await c.members();
    assert.equal(members.length, 1);
    assert.equal(members[0]?.id, "node-a");
    assert.equal(members[0]?.leader, true);

    const leader = await c.leader();
    assert.equal(leader.id, "node-a");
    assert.equal(leader.leader, true);

    const events: { type: string; source: string; payload: Uint8Array }[] = [];
    const watching = (async () => {
      for await (const ev of c.watch()) {
        events.push(ev);
        if (ev.type === "custom.deployment") {
          return;
        }
      }
    })();

    const start = Date.now();
    while (Date.now() - start < 3000 && !events.some((e) => e.type === "member.join")) {
      await new Promise((r) => setTimeout(r, 20));
    }
    assert.ok(events.some((e) => e.type === "member.join" && e.source === "node-a"));

    await c.publish("deployment", { sha: "abc" });
    await Promise.race([
      watching,
      new Promise((_, reject) => setTimeout(() => reject(new Error("watch timeout")), 3000)),
    ]);
    const ev = events.find((e) => e.type === "custom.deployment");
    assert.ok(ev);
    assert.equal(Buffer.from(ev.payload).toString(), '{"sha":"abc"}');
    assert.equal(state.published[0]?.topic, "deployment");
  } finally {
    await c.close();
  }
});

test("watch topics", async () => {
  const c = await dial(addr, { insecure: true });
  try {
    const events: string[] = [];
    const watching = (async () => {
      for await (const ev of c.watch({ topics: ["deployment"] })) {
        events.push(ev.type);
        if (ev.type === "custom.deployment") {
          return;
        }
      }
    })();
    await new Promise((r) => setTimeout(r, 50));
    await c.publish("noise", "x");
    await c.publish("deployment", { sha: "abc" });
    await Promise.race([
      watching,
      new Promise((_, reject) => setTimeout(() => reject(new Error("watch timeout")), 3000)),
    ]);
    assert.ok(!events.includes("member.join"));
    assert.ok(!events.includes("custom.noise"));
    assert.ok(events.includes("custom.deployment"));
  } finally {
    await c.close();
  }
});

test("watch event types", async () => {
  const c = await dial(addr, { insecure: true });
  try {
    const events: string[] = [];
    const watching = (async () => {
      for await (const ev of c.watch({ eventTypes: ["custom.deployment"] })) {
        events.push(ev.type);
        if (ev.type === "custom.deployment") {
          return;
        }
      }
    })();
    await new Promise((r) => setTimeout(r, 50));
    await c.publish("noise", "x");
    await c.publish("deployment", { sha: "abc" });
    await Promise.race([
      watching,
      new Promise((_, reject) => setTimeout(() => reject(new Error("watch timeout")), 3000)),
    ]);
    assert.ok(!events.includes("member.join"));
    assert.ok(!events.includes("custom.noise"));
  } finally {
    await c.close();
  }
});

test("watch bad topic", async () => {
  const c = await dial(addr, { insecure: true });
  try {
    assert.throws(() => c.watch({ topics: ["bad topic"] }), ClusdrError);
  } finally {
    await c.close();
  }
});

test("local uses env addr", async () => {
  const prevAddr = process.env.CLUSDR_GRPC_ADDR;
  const prevTls = process.env.CLUSDR_TLS;
  process.env.CLUSDR_GRPC_ADDR = addr;
  process.env.CLUSDR_TLS = "disabled";
  try {
    const c = await local();
    try {
      const members = await c.members();
      assert.equal(members[0]?.id, "node-a");
    } finally {
      await c.close();
    }
  } finally {
    if (prevAddr === undefined) {
      delete process.env.CLUSDR_GRPC_ADDR;
    } else {
      process.env.CLUSDR_GRPC_ADDR = prevAddr;
    }
    if (prevTls === undefined) {
      delete process.env.CLUSDR_TLS;
    } else {
      process.env.CLUSDR_TLS = prevTls;
    }
  }
});

test("retry until ready", async () => {
  const health = new FlakyHealth(2);
  const fake = await startFakeServer(health);
  try {
    const c = await dial(fake.addr, { insecure: true, readyTimeout: 5 });
    try {
      assert.equal((await c.members())[0]?.id, "node-a");
      assert.ok(health.calls >= 3);
    } finally {
      await c.close();
    }
  } finally {
    await stopServer(fake.server);
  }
});

test("empty dial", async () => {
  await assert.rejects(() => dial("  ", { insecure: true }), /empty/);
});

test("publish too large", async () => {
  const c = await dial(addr, { insecure: true });
  try {
    await assert.rejects(
      () => c.publish("deployment", Buffer.alloc(64 * 1024 + 1, 0x78)),
      /exceeds/,
    );
  } finally {
    await c.close();
  }
});

test("encode payload", () => {
  assert.deepEqual(encodePayload(null), Buffer.alloc(0));
  assert.deepEqual(encodePayload(Buffer.from("raw")), Buffer.from("raw"));
  assert.deepEqual(encodePayload("hi"), Buffer.from("hi"));
  assert.deepEqual(encodePayload({ sha: "abc" }), Buffer.from('{"sha":"abc"}'));
  assert.throws(() => encodePayload(1 as never), TypeError);
});
