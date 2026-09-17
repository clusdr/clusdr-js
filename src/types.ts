export interface Member {
  id: string;
  address: string;
  /** Liveness: `alive` or `dead`. A left id is gone from `members()`. */
  status: string;
  leader: boolean;
  role: string;
}

export interface Event {
  /** `member.join`, `member.dead` (crash, still listed), `member.left` (`clusdr leave`, gone), … */
  type: string;
  source: string;
  payload: Uint8Array;
  timestamp: Date;
  seq: number;
}

export class Lock {
  readonly name: string;
  readonly holder: string;
  readonly token: number;
  #deadline: Date | undefined;
  readonly stop: AbortController = new AbortController();

  constructor(name: string, holder: string, token: number, deadline: Date | undefined) {
    this.name = name;
    this.holder = holder;
    this.token = token;
    this.#deadline = deadline;
  }

  get deadline(): Date | undefined {
    return this.#deadline;
  }

  setDeadline(value: Date | undefined): void {
    this.#deadline = value;
  }
}

export class Lease {
  readonly name: string;
  readonly owner: string;
  readonly token: number;
  #deadline: Date | undefined;
  readonly stop: AbortController = new AbortController();

  constructor(name: string, owner: string, token: number, deadline: Date | undefined) {
    this.name = name;
    this.owner = owner;
    this.token = token;
    this.#deadline = deadline;
  }

  get deadline(): Date | undefined {
    return this.#deadline;
  }

  setDeadline(value: Date | undefined): void {
    this.#deadline = value;
  }

  /** Stop background renewal; the grant then expires at its deadline. */
  stopRenew(): void {
    this.stop.abort();
  }
}

export function fromMs(unixMs: number): Date | undefined {
  if (!unixMs) {
    return undefined;
  }
  return new Date(unixMs);
}

export function ttlMs(ttl: number | undefined): number {
  if (ttl === undefined || ttl <= 0) {
    return 0;
  }
  return Math.max(1, Math.round(ttl * 1000));
}

export function renewIntervalMs(ttl: number | undefined, deadline: Date | undefined): number {
  let d = ttl;
  if (d === undefined || d <= 0) {
    if (deadline) {
      d = (deadline.getTime() - Date.now()) / 1000;
    }
  }
  if (d === undefined || d <= 0) {
    d = 15;
  }
  return Math.max(50, (d / 3) * 1000);
}

export type PublishPayload =
  | Uint8Array
  | Buffer
  | string
  | Record<string, unknown>
  | unknown[]
  | null
  | undefined;

export function encodePayload(payload: PublishPayload): Buffer {
  if (payload === null || payload === undefined) {
    return Buffer.alloc(0);
  }
  if (Buffer.isBuffer(payload)) {
    return payload;
  }
  if (payload instanceof Uint8Array) {
    return Buffer.from(payload);
  }
  if (typeof payload === "string") {
    return Buffer.from(payload, "utf8");
  }
  if (typeof payload === "object") {
    return Buffer.from(JSON.stringify(payload), "utf8");
  }
  throw new TypeError(
    `clusdr: payload must be bytes, string, object, or array; got ${typeof payload}`,
  );
}
