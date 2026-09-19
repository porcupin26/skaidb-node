# Changelog

All notable changes to the skaidb Node.js driver. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[Semantic Versioning](https://semver.org/).

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

[1.0.0]: https://github.com/porcupin26/skaidb-node/releases/tag/v1.0.0
