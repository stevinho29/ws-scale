from datetime import datetime, timezone
from typing import Optional
from aiohttp import web
from importlib import import_module
import os
import asyncio
import json
import logging
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
logger = logging.getLogger("ws-scale.ids.master")

#{
#    nodes: {
#        node_1: heart_beat
#    },
#    datacenter_id: "",
#    count: 1
#}


class IDGeneratorMaster:
    """
    HTTP server that manages node registration and liveness for distributed Snowflake ID generation.

    Nodes register themselves to receive a unique node ID, then send periodic heartbeats.
    Nodes that miss heartbeats for more than two minutes are evicted via ``monitor_nodes``.
    """

    MAX_NODES = 32
    MONITOR_NODES_INTERVAL_SECONDS = 120
    def __init__(self, datacenter_id: str, port:int|str):
        """
        Args:
            datacenter_id: Logical datacenter identifier for this master instance.
            settings_path: Dotted module path to a settings file (e.g. ``"app.settings"``).
                           Falls back to environment variables when omitted.
        """
        self.datacenter_id = datacenter_id
        self.port = port

        self._runner = None
        self._monitor_task = None
        self.log_header = "[IDGeneratorMaster]"

    def _get_now(self) -> float:
        """Return the current UTC time as a millisecond-precision Unix timestamp."""
        return datetime.now(tz=timezone.utc).timestamp() * 1000

    def get_nodes_data(self):
        return self._get_json_storage_data()
    
    def _get_json_storage_data(self):
        path = str(BASE_DIR.joinpath("storage.json"))
        with open(path, "r") as f:
            return json.load(f)
    
    def _write_json_storage_data(self, data):
        print("ON WRITE", data)
        with open(BASE_DIR.joinpath("storage.json"), "w") as f:
            json.dump(data, f, indent=4)
            
    
    async def  _generate_node_id(self) -> Optional[int]:
        """Atomically allocate the next node ID from Redis.

        Returns:
            The new node ID (0-based), or ``None`` if ``MAX_NODES`` has been reached.
        """
        data = self._get_json_storage_data()
        count = data.get("count")

        if count < self.MAX_NODES:
            return count
        return None

    async def _cache_node(self, node_id: int) -> None:
        """Store a node's initial last-seen timestamp in Redis.

        Args:
            node_id: The node to cache.
        """
        data = self._get_json_storage_data()
        data["count"] += 1
        data["nodes"][str(node_id)] = self._get_now()
        self._write_json_storage_data(data)

    async def _update_node(self, node_id: int, heart_beat: float) -> None:
        """Overwrite a node's last-seen timestamp with a new heartbeat value.

        Args:
            node_id: The node whose timestamp should be updated.
            heart_beat: New timestamp in milliseconds.
        """
        data = self._get_json_storage_data()
        data["nodes"][str(node_id)] = heart_beat
        self._write_json_storage_data(data)

    async def _delete_nodes(self, node_ids: list[int]) -> None:
        """Remove one or more nodes from Redis.

        Args:
            node_ids: List of node IDs to delete. No-op if the list is empty.
        """
        data = self._get_json_storage_data()
        for node_id in node_ids:
            if str(node_id) in data["nodes"]:
                del data["nodes"][str(node_id)]

        self._write_json_storage_data(data)

    async def _get_node(self, node_id: int):
        """Fetch a single node's last-seen timestamp.

        Args:
            node_id: The node to look up.

        Returns:
            The timestamp, or ``None`` if the key does not exist.
        """
        data = self._get_json_storage_data()
        return data["nodes"].get(str(node_id))

    async def _get_nodes(self, node_ids: list[int]):
        """Fetch last-seen timestamps for multiple nodes in one round-trip.

        Args:
            node_ids: List of node IDs to look up.

        Returns:
            A list of timestamp values (or ``None`` for missing keys), in the same order as ``node_ids``.
        """
        result = []
        data = self._get_json_storage_data()
        for node_id in node_ids:
            result.append(data["nodes"].get(str(node_id)))
        return result

    async def register(self, request: web.Request) -> web.Response:
        """HTTP POST /register — allocate a node ID and cache it.

        Returns:
            201 with ``{"node_id": <int>}`` on success, or 400 if the node limit is reached.
        """
        
        try:
            body = await request.json()
        except json.JSONDecodeError:
            return web.Response(
                status=400,
                content_type="application/json",
                body=json.dumps({"message": "invalid or empty body"}).encode()
            )
        
        node_datacenter_id = body.get("datacenter_id")
        if node_datacenter_id != self.datacenter_id:
            logger.info(f"node datacenter id {node_datacenter_id} is different from the master datacenter id {self.datacenter_id}")
            return web.Response(
                status=400,
                content_type="application/json",
                body=json.dumps({"message": "you are trying to register to the wrong datacenter"}).encode(),
            )
        
        node_id = await self._generate_node_id()
        if node_id is None:
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
        
        node_datacenter_id = body.get("datacenter_id")
        if node_datacenter_id != self.datacenter_id:
            logger.info(f"node datacenter id {node_datacenter_id} is different from the master datacenter id {self.datacenter_id}")
            return web.Response(
                status=400,
                content_type="application/json",
                body=json.dumps({"message": "you are trying to register to the wrong datacenter"}).encode(),
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

    async def monitor_nodes_loop(self, interval_seconds: int) -> None:
        log_header = f"{self.log_header}[monitor_nodes_loop]"
        while True:
            await asyncio.sleep(delay=interval_seconds)
            print(f"monitor node loop called after {interval_seconds}")
            logger.info(f"{log_header} heart beat called after {interval_seconds}")
            await self.monitor_nodes()


    async def monitor_nodes(self) -> None:
        """Evict nodes that have not sent a heartbeat in the last two minutes."""
        log_header = f"{self.log_header}[monitor_nodes]"
        two_minutes_ago = self._get_now() - self.MONITOR_NODES_INTERVAL_SECONDS * 1000
        node_heart_beats = await self._get_nodes(list(range(self.MAX_NODES)))
        to_remove = []
        for i, heart_beat in enumerate(node_heart_beats):
            if heart_beat:
                if heart_beat < two_minutes_ago:
                    to_remove.append(i)

        await self._delete_nodes(to_remove)
        logger.info(f"{log_header} nodes {to_remove} removed")

    async def serve(self) -> None:
        """Start the aiohttp HTTP server and block until it is stopped."""
        app = web.Application()
        app.router.add_post("/register", self.register)
        app.router.add_put("/heartbeat", self.heartbeat)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, None, self.port)
        await site.start()
        logger.info(f"{self.log_header} id master server listening on localhost:{self.port}")

    async def shutdown(self) -> None:
        await self._runner.cleanup()
        self._monitor_task.cancel()

    async def cleanup(self):
        """Delete all node entries from json storage (intended for graceful shutdown)."""
        log_header = f"{self.log_header}[cleanup]"
        data = {
            "nodes": {},
            "datacenter_id": self.datacenter_id,
            "count": 0
        }
        self._write_json_storage_data(data)
        logger.info(f"{log_header} redis database cleaned up with success")
    
    async def bootstrap(self, monitor_interval_seconds:int=60):
        """Start the HTTP server and the node monitor loop.
        Args:
            monitor_interval_seconds: Seconds to wait between each node monitor iteration.
        """
        await self.cleanup()
        await self.serve()
        self._monitor_task = asyncio.create_task(self.monitor_nodes_loop(interval_seconds=monitor_interval_seconds))