from datetime import datetime, timezone
from typing import Optional
from aiohttp import web
from importlib import import_module
import os
import json
import logging

from cache.redis import RedisInstance
logger = logging.getLogger("ws-scale.ids.master")


class IDGeneratorMaster:
    """
    HTTP server that manages node registration and liveness for distributed Snowflake ID generation.

    Nodes register themselves to receive a unique node ID, then send periodic heartbeats.
    Nodes that miss heartbeats for more than two minutes are evicted via ``monitor_nodes``.
    """

    MAX_NODES = 32

    def __init__(self, datacenter_id: str, settings_path:str=None):
        """
        Args:
            datacenter_id: Logical datacenter identifier for this master instance.
            settings_path: Dotted module path to a settings file (e.g. ``"app.settings"``).
                           Falls back to environment variables when omitted.
        """
        self.datacenter_id = datacenter_id
        if settings_path:
            try:
                settings = import_module(name=settings_path)
                redis_host = getattr(settings, "WS_REDIS_HOST")
                redis_port = getattr(settings, "WS_REDIS_PORT")
                id_master_host = getattr(settings, "WS_MASTER_HOST")
                id_master_port = getattr(settings, "WS_MASTER_PORT")
            except ModuleNotFoundError:
                logger.error(f"Settings module not found, invalid path {settings_path}")
                raise
        else:
            redis_host = os.environ.get("WS_REDIS_HOST")
            redis_port = os.environ.get("WS_REDIS_PORT")
            id_master_host = os.environ.get("WS_MASTER_HOST")
            id_master_port = os.environ.get("WS_MASTER_PORT")

        self.port = id_master_port
        self.host = id_master_host
        self.redis = RedisInstance(redis_host=redis_host, redis_port=redis_port).get_instance()
        self.log_header = "[IDGeneratorMaster]"

    def _get_now(self) -> float:
        """Return the current UTC time as a millisecond-precision Unix timestamp."""
        return datetime.now(tz=timezone.utc).timestamp() * 1000

    async def _generate_node_id(self) -> Optional[int]:
        """Atomically allocate the next node ID from Redis.

        Returns:
            The new node ID (1-based), or ``None`` if ``MAX_NODES`` has been reached.
        """
        node_key = "node_count"
        count_bytes = await self.redis.get(node_key)
        count = int(count_bytes.decode()) if count_bytes else 0
        if count < self.MAX_NODES:
            return await self.redis.incr(node_key, 1)
        return None

    async def _cache_node(self, node_id: int) -> None:
        """Store a node's initial last-seen timestamp in Redis.

        Args:
            node_id: The node to cache.
        """
        await self.redis.set(name=f"node_{node_id}", value=self._get_now())

    async def _update_node(self, node_id: int, heart_beat: float) -> None:
        """Overwrite a node's last-seen timestamp with a new heartbeat value.

        Args:
            node_id: The node whose timestamp should be updated.
            heart_beat: New timestamp in milliseconds.
        """
        await self.redis.set(name=f"node_{node_id}", value=heart_beat)

    async def _delete_nodes(self, node_ids: list[int]) -> None:
        """Remove one or more nodes from Redis.

        Args:
            node_ids: List of node IDs to delete. No-op if the list is empty.
        """
        if node_ids:
            await self.redis.delete(*[f"node_{id}" for id in node_ids])

    async def _get_node(self, node_id: int):
        """Fetch a single node's last-seen timestamp from Redis.

        Args:
            node_id: The node to look up.

        Returns:
            The raw bytes value stored in Redis, or ``None`` if the key does not exist.
        """
        return await self.redis.get(name=f"node_{node_id}")

    async def _get_nodes(self, node_ids: list[int]):
        """Fetch last-seen timestamps for multiple nodes in one round-trip.

        Args:
            node_ids: List of node IDs to look up.

        Returns:
            A list of raw bytes values (or ``None`` for missing keys), in the same order as ``node_ids``.
        """
        return await self.redis.mget([f"node_{id}" for id in node_ids])

    async def register(self, request: web.Request) -> web.Response:
        """HTTP POST /register — allocate a node ID and cache it.

        Returns:
            201 with ``{"node_id": <int>}`` on success, or 400 if the node limit is reached.
        """
        node_id = await self._generate_node_id()
        if not node_id:
            return web.Response(
                status=400,
                content_type="application/json",
                body=json.dumps({"message": "maximum number of nodes has been reached"}).encode(),
            )
        await self._cache_node(node_id=node_id)
        return web.Response(
            status=201,
            content_type="application/json",
            body=json.dumps({"message": f"node registered with success: {node_id}", "node_id": node_id}).encode(),
        )

    async def heartbeat(self, request: web.Request) -> web.Response:
        """HTTP POST /heartbeat — refresh a node's last-seen timestamp.

        Expects a JSON body with ``{"node_id": <int>}``.

        Returns:
            200 on success, 400 if the body is invalid or the node is unknown.
        """
        log_header = f"{self.log_header}[heartbeat]"
        try:
            body = await request.json()
        except json.JSONDecodeError:
            return web.Response(
                status=400,
                content_type="application/json",
                body=json.dumps({"message": "invalid or empty body"}).encode()
            )
        node_id = body.get("node_id")

        node = await self._get_node(node_id=node_id)
        if node:
            await self._update_node(node_id=node_id, heart_beat=self._get_now())
            logger.info(f"{log_header} last seen date updated with success for node: {node_id}")
            return web.Response(
                status=200,
                content_type="application/json",
                body=json.dumps({"message": "last seen date updated with success"}).encode(),
            )
        logger.info(f"{log_header} unknown node with id {node_id}")
        return web.Response(
            status=400,
            content_type="application/json",
            body=json.dumps({"message": f"unknown node with id {node_id}"}).encode(),
        )

    async def monitor_nodes(self) -> None:
        """Evict nodes that have not sent a heartbeat in the last two minutes."""
        log_header = f"{self.log_header}[monitor_nodes]"
        two_minutes_ago = self._get_now() - 60 * 2 * 1000
        node_heart_beats = await self._get_nodes(list(range(self.MAX_NODES)))
        to_remove = []
        for i, heart_beat in enumerate(node_heart_beats):
            if heart_beat:
                hb_value = float(heart_beat.decode() if isinstance(heart_beat, bytes) else heart_beat)
                if hb_value < two_minutes_ago:
                    to_remove.append(i)
        await self._delete_nodes(to_remove)
        logger.info(f"{log_header} nodes {to_remove} removed")

    async def serve(self) -> None:
        """Start the aiohttp HTTP server and block until it is stopped."""
        app = web.Application()
        app.router.add_post("/register", self.register)
        app.router.add_post("/heartbeat", self.heartbeat)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, None, self.port)
        await site.start()
        logger.info(f"{self.log_header} id master server listening on localhost:{self.port}")

    async def cleanup(self):
        """Delete all node entries from Redis (intended for graceful shutdown)."""
        log_header = f"{self.log_header}[cleanup]"
        await self._delete_nodes(list(range(self.MAX_NODES)))
        logger.info(f"{log_header} redis database cleaned up with success")