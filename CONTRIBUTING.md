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

Node.js 20+.

```bash
npm install
make proto    # copy .proto from ../clusdr/proto
npm test
```

`.proto` files live under `proto/`. Do not hand-edit them; copy from the daemon repo. The public API is the TypeScript wrapper, not the generated gRPC shapes.
