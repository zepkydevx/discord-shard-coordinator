# discord-shard-coordinator

[![Tests](https://github.com/zepkydevx/discord-shard-coordinator/actions/workflows/test.yml/badge.svg)](https://github.com/zepkydevx/discord-shard-coordinator/actions/workflows/test.yml)
[![License](https://img.shields.io/github/license/zepkydevx/discord-shard-coordinator)](LICENSE)
![Rust](https://img.shields.io/badge/rust-1.75%2B-orange.svg)

Tracks shard health for a Discord bot running across multiple processes:
which shards are alive, which went silent, and when a dead shard needs to
reconnect.

Doesn't connect to Discord. `main.rs` simulates a small fleet of shards,
kills one on purpose, and shows the coordinator detecting and flagging it.

## Design

`coordinator.rs` and `shard.rs` are pure logic — no I/O, no real clock reads
beyond an `Instant` passed in. That's what keeps the tests fast and
deterministic.

Cross-process propagation lives behind the `EventBus` trait in
`eventbus.rs`:

- `RedisEventBus` — real implementation, Redis pub/sub.
- `FakeEventBus` — in-memory, used by tests and the demo.

## Run

```bash
cargo run
```

## Test

```bash
cargo test
```
