# Contributing

This is the TypeScript SDK. The daemon lives in [`clusdr`](https://github.com/clusdr/clusdr). Product docs: [TypeScript SDK](https://clusdr.io/docs/sdk/typescript).

License: [Apache-2.0](LICENSE). Conduct: [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md). Security: [SECURITY.md](SECURITY.md).

## Commits

Every commit and pull-request title uses [Conventional Commits](https://www.conventionalcommits.org/):

```text
<type>(optional-scope): <imperative summary>
```

Types we use: `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `build`, `ci`, `chore`, `revert`.

Examples:

```text
feat: accept a custom Runtime address in local()
fix: surface Unavailable on a dead socket
docs: point install at the current daemon tag
```

A breaking change uses `feat!:` (or another type with `!`) and a `BREAKING CHANGE:` footer. Subject is lowercase after the type, no trailing period.

CI lints PR commits. Prefer squash-merge; the squash title must stay conventional.

## Requirements

Node.js 20+. [Buf](https://buf.build/docs/cli/installation) to refresh vendored proto.

```bash
npm install
make proto    # export buf.build/clusdr/api (sibling ../clusdr/proto/api, else BSR, else GitHub)
npm test
```

`.proto` files live under `proto/`. Do not hand-edit them; export from [`buf.build/clusdr/api`](https://buf.build/clusdr/api) (or the sibling daemon checkout). `join` / `heartbeat` are `buf.build/clusdr/internal` and are not exported. The public API is the TypeScript wrapper, not the generated gRPC shapes.
