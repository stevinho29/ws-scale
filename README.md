# ws-scale

A Python library for building scalable WebSocket servers with distributed Snowflake-style ID generation. Each WebSocket node registers with a central master, receives a unique node ID, and generates collision-free 64-bit IDs across the cluster using Redis for coordination.

## Architecture

```
┌─────────────────────────┐
│   IDGeneratorMaster     │  HTTP :9000
│  (aiohttp, one per DC)  │◄──── register / heartbeat
└────────────┬────────────┘
             │ Redis :6379
┌────────────▼────────────┐        ┌──────────────────────────┐
│   WebsocketServer #1    │        │   WebsocketServer #2     │
│  + IDGeneratorNode      │        │  + IDGeneratorNode       │
│  (websockets, :8080)    │        │  (websockets, :8081)     │
└─────────────────────────┘        └──────────────────────────┘
```

- **IDGeneratorMaster** — HTTP server that allocates node IDs and evicts stale nodes (no heartbeat in 2 min). Supports up to 32 nodes per datacenter.
- **IDGeneratorNode** — Registers with the master on startup, sends heartbeats every 60 s, and generates Snowflake IDs (41-bit timestamp | 5-bit datacenter | 5-bit node | 12-bit sequence).
- **WebsocketServer** — Wraps `websockets.serve`, bootstraps an `IDGeneratorNode`, and routes connections to a user-supplied async handler.

## Requirements

- Python 3.13+
- Redis

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
| `WS_REDIS_HOST` | `localhost` | Redis hostname |
| `WS_REDIS_PORT` | `6379` | Redis port |
| `WS_MASTER_HOST` | `http://0.0.0.0` | ID master hostname |
| `WS_MASTER_PORT` | `9000` | ID master port |

**Via settings module** (`settings.py`):

```python
WS_REDIS_HOST = "localhost"
WS_REDIS_PORT = 6379

WS_MASTER_HOST = "http://0.0.0.0"
WS_MASTER_PORT = 9000
```

**Via environment variables** (pass `settings_path=None`):

```bash
export WS_REDIS_HOST=localhost
export WS_REDIS_PORT=6379
export WS_MASTER_HOST=http://0.0.0.0
export WS_MASTER_PORT=9000
```

## Quick Start

```python
import asyncio
from wsscale import IDGeneratorMaster, WebsocketServer

async def handle(conn):
    async for message in conn:
        await conn.send(f"echo: {message}")

async def main():
    master = IDGeneratorMaster(datacenter_id=0, settings_path="settings")
    server = WebsocketServer(datacenter_id=0, port=8080, settings_path="settings")

    await master.cleanup()   # clear stale Redis state on fresh start
    await master.serve()     # start HTTP master on WS_MASTER_PORT
    await server.bootstrap(handler=handle)  # register node, start heartbeat, serve WS

asyncio.run(main())
```

Run the bundled example from the command line:

```bash
python src/example.py --port 8080 --datacenter 0 --generator True
```

## API Reference

### `WebsocketServer(datacenter_id, port, settings_path=None)`

| Parameter | Type | Description |
|---|---|---|
| `datacenter_id` | `int` | 5-bit datacenter identifier (0–31) |
| `port` | `int` | TCP port for the WebSocket server |
| `settings_path` | `str \| None` | Dotted module path to a settings file, or `None` to use env vars |

**`await bootstrap(handler)`** — Registers the ID node, starts the heartbeat loop, and runs the WebSocket server.

---

### `IDGeneratorMaster(datacenter_id, settings_path=None)`

| Parameter | Type | Description |
|---|---|---|
| `datacenter_id` | `str` | Logical datacenter identifier |
| `settings_path` | `str \| None` | Dotted module path to a settings file, or `None` to use env vars |

**`await serve()`** — Starts the aiohttp HTTP server exposing `POST /register` and `POST /heartbeat`.  
**`await cleanup()`** — Removes all node entries from Redis (call before `serve()` on a fresh start).  
**`await monitor_nodes()`** — Evicts nodes that have missed heartbeats for more than 2 minutes.

---

### `IDGeneratorNode(server_host, server_port, data_center_id, redis_host, redis_port)`

Typically instantiated automatically by `WebsocketServer`, but can be used standalone.

**`await generate_id(datacenter_id, node_id) → int`** — Returns a 64-bit Snowflake-style unique ID.  
**`await register()`** — Contacts the master and obtains a `node_id`.  
**`await heart_beat_loop(interval_seconds)`** — Sends periodic heartbeats to the master forever.

## License

MIT — see [LICENSE](LICENSE).
