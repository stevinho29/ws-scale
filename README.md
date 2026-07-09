# ws-scale

A Python library for building scalable WebSocket servers with distributed Snowflake-style ID generation. Each WebSocket node registers with a central master, receives a unique node ID, and generates collision-free 64-bit IDs across the cluster. Node state is persisted to a local JSON file on the master — no external datastore required.

## Architecture

```
┌─────────────────────────┐
│   IDGeneratorMaster     │  HTTP :9000
│  (aiohttp, one per DC)  │◄──── register / heartbeat
└────────────┬────────────┘
             │ storage.json
┌────────────▼────────────┐        ┌──────────────────────────┐
│   WebsocketServer #1    │        │   WebsocketServer #2     │
│  + IDGeneratorNode      │        │  + IDGeneratorNode       │
│  (websockets, :8080)    │        │  (websockets, :8081)     │
└─────────────────────────┘        └──────────────────────────┘
```

- **IDGeneratorMaster** — HTTP server that allocates node IDs and evicts nodes that have missed a heartbeat for `MONITOR_NODES_INTERVAL_SECONDS` (120s by default). Supports up to 32 nodes per datacenter. Rejects registrations and heartbeats with an invalid body or whose `datacenter_id` doesn't match the master's own. `bootstrap()` cleans the storage file, starts the HTTP server, and kicks off a periodic `monitor_nodes` loop in one call.
- **IDGeneratorNode** — Registers with the master on startup, sends heartbeats every 60 s via `PUT /heartbeat`, and generates Snowflake IDs (41-bit timestamp | 5-bit datacenter | 5-bit node | 12-bit sequence) using an in-process, lock-guarded sequence counter. Timestamps are relative to a fixed custom epoch (2025-01-01 UTC). Tolerates a temporarily unreachable master — `register()` and `heart_beat()` log and back off instead of raising.
- **WebsocketServer** — Wraps `websockets.serve` and routes connections to a user-supplied async handler. When `WS_CLUSTER` is enabled it also bootstraps an `IDGeneratorNode`, registering it with the master and starting its heartbeat loop; otherwise it just serves WebSocket connections standalone.

## Requirements

- Python 3.13+

## Installation

```bash
pip install ws-scale
```

Or with [uv](https://github.com/astral-sh/uv):

```bash
uv add ws-scale
```

## Configuration

Settings can be provided via a Python module or environment variables.

| Setting | Default | Description |
|---|---|---|
| `WS_MASTER_HOST` | `http://0.0.0.0` | ID master hostname, used by `WebsocketServer` to reach the master |
| `WS_MASTER_PORT` | `9000` | ID master port |
| `WS_DATACENTER_ID` | — | 5-bit datacenter identifier for this node (0–31) |
| `WS_CLUSTER` | — | `"true"`/`"True"` or `"false"`/`"False"` (also accepts a `bool` or `0`/`1` when set programmatically). When true, the node registers with the master and sends heartbeats; when false, `WebsocketServer` runs standalone without contacting any master. Any other value raises `ValueError` |

These settings only configure `WebsocketServer`. `IDGeneratorMaster` takes `datacenter_id` and `port` directly as constructor arguments (see [API Reference](#api-reference)).

**Via settings module** (`settings.py`):

```python
WS_MASTER_HOST = "http://0.0.0.0"
WS_MASTER_PORT = 9000
WS_DATACENTER_ID = 0
WS_CLUSTER = True
```

**Via environment variables** (pass `settings_path=None`):

```bash
export WS_MASTER_HOST=http://0.0.0.0
export WS_MASTER_PORT=9000
export WS_DATACENTER_ID=0
export WS_CLUSTER=true
```

## Quick Start

```python
import asyncio
from wsscale import IDGeneratorMaster, WebsocketServer

async def handle(conn):
    async for message in conn:
        await conn.send(f"echo: {message}")

async def main():
    master = IDGeneratorMaster(datacenter_id=0, port=9000)
    server = WebsocketServer(port=8080, settings_path="settings")

    await master.bootstrap(monitor_interval_seconds=60)  # reset storage, start HTTP master, start node monitor
    await server.bootstrap(handler=handle)                # (if WS_CLUSTER) register node, start heartbeat, serve WS

asyncio.run(main())
```

Run the bundled example from the command line:

```bash
python src/example.py --port 8080 --datacenter 0 --generator True
```

## API Reference

### `WebsocketServer(port, settings_path=None)`

| Parameter | Type | Description |
|---|---|---|
| `port` | `int` | TCP port for the WebSocket server |
| `settings_path` | `str \| None` | Dotted module path to a settings file, or `None` to use env vars |

Reads `WS_MASTER_HOST`, `WS_MASTER_PORT`, `WS_DATACENTER_ID`, and `WS_CLUSTER` from the settings module or environment.

**`await bootstrap(handler)`** — If `WS_CLUSTER` is true, registers the ID node and starts its heartbeat loop; then runs the WebSocket server, dispatching connections to `handler`.  
**`shutdown()`** — Closes the underlying WebSocket server (sync call).

---

### `IDGeneratorMaster(datacenter_id, port)`

| Parameter | Type | Description |
|---|---|---|
| `datacenter_id` | `str` | Logical datacenter identifier |
| `port` | `int \| str` | TCP port for the HTTP master |

**`await bootstrap(monitor_interval_seconds=60)`** — Calls `cleanup()`, then `serve()`, then schedules `monitor_nodes` to run every `monitor_interval_seconds` in the background. The recommended way to start a master.  
**`await serve()`** — Starts the aiohttp HTTP server exposing `POST /register` and `PUT /heartbeat`.  
**`await cleanup()`** — Resets the node storage file (call before `serve()` on a fresh start).  
**`await monitor_nodes()`** — Evicts nodes that have missed heartbeats for more than `MONITOR_NODES_INTERVAL_SECONDS` (class attribute, default 120s) — distinct from `monitor_interval_seconds`, which is how often this check runs.  
**`await shutdown()`** — Stops the HTTP server and cancels the monitor loop started by `bootstrap()`.  
**`get_nodes_data()`** — Returns the raw contents of the node storage file (sync call, mainly useful for debugging/tests).

---

### `IDGeneratorNode(server_host, server_port, data_center_id)`

Typically instantiated automatically by `WebsocketServer` (only used when `WS_CLUSTER` is enabled), but can be used standalone.

**`await generate_id(datacenter_id, node_id) → int`** — Returns a 64-bit Snowflake-style unique ID.  
**`await register()`** — Contacts the master with this node's `datacenter_id` and obtains a `node_id`, setting `registered = True` on success. Leaves `registered = False` (without raising) if the datacenter doesn't match or the master is unreachable.  
**`await heart_beat()`** — Sends a single heartbeat, setting `synced` to reflect whether it succeeded.  
**`await heart_beat_loop(interval_seconds)`** — Calls `heart_beat()` on a fixed interval forever.

## Development

Install dev dependencies and run the test suite with [uv](https://github.com/astral-sh/uv):

```bash
uv sync --group dev
uv run pytest
```

## License

MIT — see [LICENSE](LICENSE).
