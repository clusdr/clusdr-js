import assert from "node:assert/strict";
import { status, type ServiceError } from "@grpc/grpc-js";
import { test } from "node:test";

import { retry, transient } from "../src/retry.ts";

function rpc(code: number, details = "x"): ServiceError {
  return Object.assign(new Error(details), { code, details }) as ServiceError;
}

test("transient codes", () => {
  assert.equal(transient(new Error("x")), false);
  assert.equal(transient(rpc(status.UNAVAILABLE)), true);
  assert.equal(transient(rpc(status.ABORTED)), true);
  assert.equal(transient(rpc(status.RESOURCE_EXHAUSTED)), true);
  assert.equal(transient(rpc(status.INVALID_ARGUMENT)), false);
});

test("retry succeeds after transient", async () => {
  let n = 0;
  const out = await retry(async () => {
    n += 1;
    if (n < 3) {
      throw rpc(status.UNAVAILABLE, "wait");
    }
    return "ok";
  }, Date.now() + 2000);
  assert.equal(out, "ok");
  assert.equal(n, 3);
});

test("retry stops on deadline", async () => {
  await assert.rejects(
    () =>
      retry(async () => {
        throw rpc(status.UNAVAILABLE, "down");
      }, Date.now() + 150),
    (err: unknown) => transient(err),
  );
});

test("retry does not retry invalid argument", async () => {
  await assert.rejects(
    () =>
      retry(async () => {
        throw rpc(status.INVALID_ARGUMENT, "bad");
      }, Date.now() + 2000),
    (err: unknown) => (err as ServiceError).code === status.INVALID_ARGUMENT,
  );
});
