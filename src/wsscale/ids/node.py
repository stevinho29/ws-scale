import logging
import asyncio
from aiohttp import ClientSession
from datetime import datetime, timezone

from wsscale.cache.redis import RedisInstance

logger = logging.getLogger("ws-scale.ids.node")
    
    
class IDGeneratorNode:
    """
    Snowflake-style distributed ID generator node.

    Each node registers with the ``IDGeneratorMaster`` to obtain a unique ``node_id``,
    then uses that ID together with a datacenter ID, a millisecond timestamp, and a
    per-millisecond sequence counter to produce 64-bit sortable unique IDs.
    """

    def __init__(self,
                 server_host:str,
                 server_port:int,
                 data_center_id:int,
                 redis_host:str,
                 redis_port:int
                 ):
        """
        Args:
            server_host: Hostname or URL of the ``IDGeneratorMaster`` server.
            server_port: Port the master server is listening on.
            data_center_id: Logical datacenter identifier embedded in generated IDs.
            redis_host: Hostname of the Redis server used for sequence counters.
            redis_port: Port of the Redis server.
        """
        self.epoch = datetime.now(tz=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        self.redis = RedisInstance(redis_host=redis_host, redis_port=redis_port).get_instance()
        self.server_url = f"{server_host}:{server_port}"
        self.data_center_id = data_center_id
        self.node_id = None
        self.registered = False
        self.log_header = "[IDGeneratorNode]"
        
    def get_elapsed_time_since_custom_epoch(self):
        """Return milliseconds elapsed since the custom epoch (midnight UTC of the start day)."""
        now = datetime.now(tz=timezone.utc)
        delta = (now - self.epoch).total_seconds()
        return delta * 1000
    
    async def get_sequence_number(self) -> int:
        """Atomically retrieve and increment the per-millisecond sequence counter from Redis.

        The key expires after 1 ms so the counter resets each millisecond.

        Returns:
            Current sequence number for this millisecond.
        """
        sequence_key = f'{self.node_id}_sequence_number'
        if await self.redis.get(sequence_key):
            sequence = await self.redis.incr(name=sequence_key, amount=1)
        else:
            sequence = await self.redis.set(name=sequence_key, value=0, px=1)
        return sequence
    
    
    async def generate_id(self, datacenter_id:int, node_id:int) -> int:
        """Produce a 64-bit Snowflake-style unique ID.

        Bit layout: 41-bit timestamp | 5-bit datacenter ID | 5-bit node ID | 12-bit sequence.

        Args:
            datacenter_id: 5-bit datacenter identifier (0–31).
            node_id: 5-bit node identifier (0–31).

        Returns:
            A globally unique 64-bit integer.
        """
        timestamp = self.get_elapsed_time_since_custom_epoch()
        sequence_number = await self.get_sequence_number()
        _id = (int(timestamp) << 22) | (datacenter_id << 17) | (node_id << 12) | sequence_number
        return int(_id)
    
    async def register(self):
        """Register this node with the master server to obtain a unique ``node_id``."""
        log_header = f"{self.log_header}[register]"
        async with ClientSession() as client:
            logger.info(f"{log_header} start registering id generator node to master")
            async with client.post(url=self.server_url + '/register', timeout=30) as req:
                if req.status == 201:
                    body = await req.json()
                    self.node_id = body.get("node_id")
                    self.registered = True
                    logger.info(f"{log_header} node registered with success, node_id: {self.node_id}")
                else:
                    logger.error(f"{log_header} request to register ended with status code {req.status}")
                    logger.error(f"{log_header} {await req.text()}")

    async def heart_beat_loop(self, interval_seconds):
        """Run ``heart_beat`` on a fixed interval forever.

        Args:
            interval_seconds: Seconds to wait between each heartbeat.
        """
        log_header = f"{self.log_header}[heart_beat_loop]"
        while True:
            await asyncio.sleep(delay=interval_seconds)
            logger.info(f"{log_header} heart beat called after {interval_seconds}")
            await self.heart_beat()

    async def heart_beat(self):
        """Send a single heartbeat POST to the master server to renew this node's liveness."""
        log_header = f"{self.log_header}[heart_beat]"
        logger.info(f"{log_header} about to send heart beat for node: {self.node_id}")
        async with ClientSession() as client:
            async with client.post(
                url=self.server_url + "/heartbeat",
                json={"node_id": self.node_id},
                timeout=30) as req:
                if req.status == 200:
                    logger.info(f"{log_header} heart beat sent with success")
                else:
                    logger.error(f"{log_header} request to send heart beat ended with status code {req.status}")
                    logger.error(f"{log_header} {await req.text()}")