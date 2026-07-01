# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/) and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] — 2026-06-25

### Added

- `WebsocketServer` — bootstraps a `websockets`-based server, auto-registers an ID node, and drives a caller-supplied async connection handler.
- `IDGeneratorMaster` — aiohttp HTTP server exposing `POST /register` and `POST /heartbeat` for distributed node coordination. Supports up to 32 nodes per datacenter and evicts nodes that miss heartbeats for more than 2 minutes via `monitor_nodes`.
- `IDGeneratorNode` — Snowflake-style 64-bit ID generator (41-bit timestamp | 5-bit datacenter | 5-bit node | 12-bit sequence). Registers with the master on startup and maintains liveness via a configurable heartbeat loop.
- `RedisInstance` — thin async Redis client wrapper used by both master and node for node-count tracking and per-millisecond sequence counters.
- Settings resolution via a dotted Python module path or environment variables (`WS_REDIS_HOST`, `WS_REDIS_PORT`, `WS_MASTER_HOST`, `WS_MASTER_PORT`).
- `cleanup()` helper on `IDGeneratorMaster` for clearing stale Redis state on fresh startup.
- Example entry point (`src/example.py`) with CLI flags `--port`, `--datacenter`, and `--generator`.
- MIT license.
