import { homedir } from "node:os";
import { join } from "node:path";

export const DEFAULT_ADDR = "127.0.0.1:7947";
export const DEFAULT_REQUEST_TIMEOUT = 10;
export const DEFAULT_READY_TIMEOUT = 10;
export const MAX_PAYLOAD = 64 * 1024;
export const CA_FILE = "ca.crt";
export const CERT_FILE = "node.crt";
export const KEY_FILE = "node.key";
export const MAX_WATCH_TOPIC = 128;
export const INITIAL_BACKOFF_MS = 50;
export const MAX_BACKOFF_MS = 2000;

export interface ConnectOptions {
  insecure?: boolean;
  dataDir?: string;
  holder?: string;
  /** Unary timeout in seconds (default 10). */
  requestTimeout?: number;
  /** Health wait on connect in seconds (default 10). 0 skips the wait. */
  readyTimeout?: number;
  serverName?: string;
}

export interface Options {
  addr: string;
  insecure: boolean;
  dataDir: string;
  holder: string;
  requestTimeout: number;
  readyTimeout: number;
  serverName: string;
}

export function resolveOptions(addr: string, opts: ConnectOptions = {}): Options {
  return {
    addr: addr.trim(),
    insecure: Boolean(opts.insecure),
    dataDir: opts.dataDir?.trim() ?? "",
    holder: opts.holder?.trim() ?? "",
    requestTimeout:
      opts.requestTimeout && opts.requestTimeout > 0
        ? opts.requestTimeout
        : DEFAULT_REQUEST_TIMEOUT,
    readyTimeout:
      opts.readyTimeout === 0
        ? 0
        : opts.readyTimeout && opts.readyTimeout > 0
          ? opts.readyTimeout
          : DEFAULT_READY_TIMEOUT,
    serverName: opts.serverName?.trim() ?? "",
  };
}

export interface WatchFilter {
  topics?: readonly string[];
  eventTypes?: readonly string[];
}

export function envAddr(): string {
  const v = process.env.CLUSDR_GRPC_ADDR?.trim();
  return v || DEFAULT_ADDR;
}

export function envInsecure(): boolean {
  const v = process.env.CLUSDR_TLS?.trim().toLowerCase() ?? "";
  return v === "disabled" || v === "off" || v === "false" || v === "0";
}

export function envDataDir(): string {
  const v = process.env.CLUSDR_DATA_DIR?.trim();
  if (v) {
    return v;
  }
  return join(homedir(), ".clusdr");
}

export function envServerName(): string {
  return process.env.CLUSDR_TLS_SERVER_NAME?.trim() ?? "";
}
