<h1 align="center">
  <a href="https://clusdr.io/docs/sdk/typescript">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/clusdr/clusdr-js/main/assets/typescript-lettermark-dark.svg">
      <img src="https://raw.githubusercontent.com/clusdr/clusdr-js/main/assets/typescript-lettermark.svg" alt="clusdr TypeScript" width="160" height="164">
    </picture>
  </a>
</h1>

<p align="center">A runtime for the cluster. An SDK for the app.</p>

<p align="center">
  <a href="https://clusdr.io/docs/sdk/typescript"><img src="https://img.shields.io/badge/docs-clusdr.io-0C0C10" alt="docs"></a>
  <a href="https://www.npmjs.com/package/clusdr"><img src="https://img.shields.io/npm/v/clusdr" alt="npm"></a>
  <a href="https://github.com/clusdr/clusdr-js/actions/workflows/ci.yml"><img src="https://github.com/clusdr/clusdr-js/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://github.com/clusdr/clusdr-js/blob/main/package.json"><img src="https://img.shields.io/badge/node-20%2B-green" alt="Node.js 20+"></a>
  <a href="https://github.com/clusdr/clusdr-js/blob/main/LICENSE"><img src="https://img.shields.io/github/license/clusdr/clusdr-js" alt="License"></a>
</p>

Application SDK for the **local** Clusdr daemon. This package does not join the cluster.

```text
your process  ──►  clusdr daemon on this host  ──►  the rest of the cluster
```

Not a database, queue, or Kubernetes. Wire API is **v1alpha1**. TLS is on by default. Async on Node.js 20+.

## Install

```bash
npm install clusdr
```

Same version train as the daemon. A running daemon on this host is required:

```bash
curl -fsSL https://clusdr.io/install.sh | sh
```

Linux amd64/arm64, or [Docker Hub `durguto/clusdr`](https://hub.docker.com/r/durguto/clusdr) (GHCR: `ghcr.io/clusdr/clusdr`). Then `clusdr init` and `clusdr start --bootstrap`. Guide: [first member](https://clusdr.io/docs/guide/first-member).

## Use

`local()` dials `CLUSDR_GRPC_ADDR` or `127.0.0.1:7947`, waits on Health, then you own the connection.

```ts
import { local } from "clusdr";

const c = await local();
const members = await c.members();
const leader = await c.leader();
void members;
void leader;

await c.publish("deployment", { sha: "abc" });

const lk = await c.lock("scheduler", 15);
void lk.token;
await c.unlock("scheduler");

for await (const event of c.watch()) {
  // member.join, leader.changed, custom.deployment, …
  void event.type;
  break;
}

await c.close();
```

`dial` is for tests and operators. Apps use `local()`.

One `Cluster` is safe for concurrent unary calls. Same connection = same holder (`unlock` is process-wide for that name). Several `watch()` loops on one client are fine.

`ttl` is seconds. `close()` stops Watch, unlocks, and revokes what this process still holds. Failures throw `ClusdrError`.

Full surface: [TypeScript SDK](https://clusdr.io/docs/sdk/typescript). Runnable copies (Go, Python, Rust, TypeScript, and Java): [examples](https://github.com/clusdr/clusdr/tree/main/examples).

## TLS

On unless `insecure: true` or `CLUSDR_TLS=disabled`. PEMs (`ca.crt`, `node.crt`, `node.key`) come from `dataDir`, `CLUSDR_DATA_DIR`, or `~/.clusdr`. Missing files are an error; this client does not skip-verify.

## Not in this package

- Join, promote, or configure the cluster (CLI)
- Talking to a remote node's Runtime API as the normal path — put a daemon on that host

## Links

- **Docs:** [clusdr.io](https://clusdr.io) · [TypeScript SDK](https://clusdr.io/docs/sdk/typescript) · [from your app](https://clusdr.io/docs/guide/from-your-app)
- **Daemon:** [github.com/clusdr/clusdr](https://github.com/clusdr/clusdr)
- **This repo:** [github.com/clusdr/clusdr-js](https://github.com/clusdr/clusdr-js)

Apache-2.0. Contributor checkout: [CONTRIBUTING.md](CONTRIBUTING.md).
