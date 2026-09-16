/**
 * Application SDK for the local Clusdr daemon.
 *
 * The application is not a cluster member. It dials the daemon on this host
 * the same way a process talks to a local Docker engine. This package never
 * dials other nodes and never joins Raft.
 *
 * ```text
 * your process  ──►  clusdr daemon on this host  ──►  the rest of the cluster
 * ```
 *
 * ```ts
 * import { local } from "clusdr";
 *
 * const c = await local();
 * const members = await c.members();
 * await c.close();
 * ```
 *
 * TLS is on unless `insecure: true` or `CLUSDR_TLS=disabled`. Missing PEMs
 * are an error (no skip-verify fallback). Unary calls retry UNAVAILABLE /
 * ABORTED / RESOURCE_EXHAUSTED.
 *
 * Walkthrough: https://clusdr.io/docs/sdk/typescript
 */
export { Cluster, dial, local } from "./cluster.js";
export { ClusdrError } from "./error.js";
export type { ConnectOptions, WatchFilter } from "./options.js";
export { encodePayload, Lease, Lock, type Event, type Member, type PublishPayload } from "./types.js";
