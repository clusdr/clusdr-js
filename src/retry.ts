import { status, type ServiceError } from "@grpc/grpc-js";

import { INITIAL_BACKOFF_MS, MAX_BACKOFF_MS } from "./options.js";
import { sleep } from "./sleep.js";

const TRANSIENT = new Set<number>([
  status.UNAVAILABLE,
  status.RESOURCE_EXHAUSTED,
  status.ABORTED,
]);

export function isServiceError(err: unknown): err is ServiceError {
  return (
    typeof err === "object" &&
    err !== null &&
    "code" in err &&
    typeof (err as { code: unknown }).code === "number"
  );
}

export function transient(err: unknown): boolean {
  return isServiceError(err) && TRANSIENT.has(err.code);
}

export function readyTransient(err: unknown): boolean {
  return transient(err) || (isServiceError(err) && err.code === status.DEADLINE_EXCEEDED);
}

export async function retry<T>(
  fn: () => Promise<T>,
  deadlineMs: number,
  isTransient: (err: unknown) => boolean = transient,
  signal?: AbortSignal,
): Promise<T> {
  let backoff = INITIAL_BACKOFF_MS;
  let last: unknown;
  for (;;) {
    if (signal?.aborted) {
      throw last instanceof Error ? last : new Error("clusdr: aborted");
    }
    const remaining = deadlineMs - Date.now();
    if (remaining <= 0) {
      if (last !== undefined) {
        throw last;
      }
      throw new Error("clusdr: request timeout");
    }
    try {
      return await fn();
    } catch (err) {
      if (!isTransient(err)) {
        throw err;
      }
      last = err;
    }
    const wait = Math.min(backoff, Math.max(0, deadlineMs - Date.now()));
    if (wait <= 0) {
      if (last !== undefined) {
        throw last;
      }
      throw new Error("clusdr: request timeout");
    }
    const slept = await sleep(wait, signal);
    if (!slept) {
      throw last instanceof Error ? last : new Error("clusdr: aborted");
    }
    backoff = Math.min(backoff * 2, MAX_BACKOFF_MS);
  }
}

export function remainingMs(deadlineMs: number): number {
  const left = deadlineMs - Date.now();
  if (left <= 0) {
    throw new Error("clusdr: request timeout");
  }
  return left;
}
