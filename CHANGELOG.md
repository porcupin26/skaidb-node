# Changelog

All notable changes to the skaidb Node.js driver. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[Semantic Versioning](https://semver.org/).

## [1.0.3] - 2026-09-20

### Changed
- Release automation: published from GitHub Actions. Pushing a `vX.Y.Z`
  tag now runs the tests, publishes `@skaidb/client` to npm and creates
  the GitHub Release from this changelog section (no code change; see
  `RELEASING.md`).

## [1.0.2] - 2026-09-20

### Changed
- Published to npm as `@skaidb/client` (package renamed; no code change).
  Install with `npm install @skaidb/client` and require it as
  `@skaidb/client`. Installing from GitHub
  (`npm install github:porcupin26/skaidb-node#v1.0.2`) yields the same
  module name.

## [1.0.1] - 2026-09-19

### Fixed
- `StreamEvent.id` and `SubscribeOptions.after` were declared as `number`,
  but a stream log id is an opaque **string** that sorts in log order
  (`"00000001789857620741-0000000000-0902800000000000000100"`). A consumer
  that followed the types and resumed with `after: 0` issued `WHERE id > 0`
  and never received an event. The declarations now say `id: string` and
  `after?: string | null`; the docs, README and example say so too, and the
  TypeScript consumer compiled in CI asserts that a numeric id or `after`
  no longer type-checks.
- `.return()` on an idle `subscribe()` iterator did not resolve until the
  whole `pollMs` sleep had elapsed (the generator was suspended inside the
  sleep and could not see the request). The sleep is now woken by
  `.return()`, so `break`/`.return()` take effect within a tick while the
  iterator is idle; a stop requested while a page fetch is in flight takes
  effect as soon as that page arrives, without yielding it.
- `?` placeholders were accepted silently and failed on the server with
  `statement expects N parameters, got 0`. With values given, a statement
  containing a bare `?` (outside string literals) and no `$N` now rejects
  before anything is sent: `this driver uses $1, $2 … placeholders; '?' is
  not a placeholder`. `batch()` applies the same rule.

### Added
- `subscribe(name, { signal })`: an `AbortSignal` that ends the iteration
  cleanly (no error), within a tick while idle. A signal that is already
  aborted yields nothing and sends nothing.

### Changed
- Install coordinate: `npm install github:porcupin26/skaidb-node#v1.0.1`.

## [1.0.0] - 2026-09-19

First release as a standalone package (`github.com/porcupin26/skaidb-node`),
carrying its full history over from the skaidb monorepo's `drivers/nodejs`.

### Added
- `Client`: SCRAM-SHA-256 handshake, `query()` with `$1`-style parameters,
  server-side prepared statements with typed bindings, `batch()`,
  `stream()` with the abandon/drain rule, `subscribe()` over stream logs,
  multi-seed failover, transparent reconnect, TLS (`tls`, `tlsCa`,
  `tlsInsecure`, `tlsServerName`), per-connection `database`, and
  per-client or per-statement consistency (`ONE`/`QUORUM`/`ALL`).
- `Pool`: a bounded idle pool with `acquire()`/`release()`/`withConnection()`.
- Multiple result sets (`resultSets`) from a `CALL` whose body `EMIT`s.
- Hello self-identification: the version sent to the server is the
  package version.
- TypeScript declarations covering the whole public surface (`Pool`,
  `seeds`, `stream`, `batch`, `subscribe`, `isUsable`, `resultSets`).
- Unit tests (`node:test`) for the value codec, parameter binding, framing,
  and a full in-process fake server exercising connect/query/stream/batch/
  pool/failover; CI on Node 18, 20 and 22; a TypeScript consumer compiled
  in CI against the declarations.
- Documentation: README plus `docs/` (getting started, API reference,
  types and TypeScript usage), and runnable `examples/`.

### Fixed
- A client with `database` set deadlocked on its first statement after a
  lost connection: the reconnect issued `USE` through the serialized query
  chain, behind the very statement that was reconnecting. `USE` now goes
  straight to the socket.
- A destroyed socket's late `close` event could mark the replacement
  connection broken (and destroy it) when a statement re-dialled right
  after an abandoned stream was dropped. Socket events are now ignored once
  the socket is no longer the client's.
- Binding a `bigint` outside the 64-bit range threw a raw `RangeError`; it
  is now a `SkaidbError`.

### Changed
- Package version series restarts at 1.0.0; `engines.node` is `>=18`.
- The example moved to `examples/basic.js`.

[1.0.2]: https://github.com/porcupin26/skaidb-node/releases/tag/v1.0.2
[1.0.1]: https://github.com/porcupin26/skaidb-node/releases/tag/v1.0.1
[1.0.0]: https://github.com/porcupin26/skaidb-node/releases/tag/v1.0.0
