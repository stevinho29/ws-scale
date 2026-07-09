import logging
import asyncio
from aiohttp import ClientSession, ClientConnectionError
from datetime import datetime, timezone

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
                 ):
        """
        Args:
            server_host: Hostname or URL of the ``IDGeneratorMaster`` server.
            server_port: Port the master server is listening on.
            data_center_id: Logical datacenter identifier embedded in generated IDs.
            redis_host: Hostname of the Redis server used for sequence counters.
            redis_port: Port of the Redis server.
        """
        # custom epoch is fixed 01 / 01 / 2025
        self.epoch = datetime.now(tz=timezone.utc).replace(year=2025, month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        self.server_url = f"{server_host}:{server_port}"
        self.data_center_id = data_center_id
        self.node_id = None
        self.registered = False
        self.synced = False
        self.log_header = "[IDGeneratorNode]"
        self.last_ts = 0
        self.last_sequence = 0
        
    def get_elapsed_time_since_custom_epoch(self):
        """Return microseconds elapsed since the custom epoch (midnight UTC of the start day)."""
        now = datetime.now(tz=timezone.utc)
        delta = (now - self.epoch).total_seconds()
        return delta * 1000
    
    async def get_sequence_number(self) -> int:
        """Atomically retrieve and increment the per-millisecond sequence counter.

        Returns:
            Current sequence number for this millisecond.
        """
        async with asyncio.Lock():
            current_ts = datetime.now(tz=timezone.utc).timestamp() * 1000
            if current_ts == self.last_ts:
                if self.last_sequence & 0xFFF == 0xFFF:
                    # Sequence number has reached its maximum for this millisecond; wait for the next millisecond
                    while current_ts <= self.last_ts:
                        await asyncio.sleep(0.001)  # Sleep for 1 ms
                        current_ts = datetime.now(tz=timezone.utc).timestamp() * 1000
                    self.last_sequence = 0
                else:
                    self.last_sequence += 1
                self.last_ts = current_ts
            else:
                self.last_ts = current_ts
                self.last_sequence = 0

            return self.last_sequence
    
    
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
        return _id
    
    async def register(self):
        """Register this node with the master server to obtain a unique ``node_id``."""
        log_header = f"{self.log_header}[register]"
        async with ClientSession() as client:
            logger.info(f"{log_header} start registering id generator node to master")
            try:
                async with client.post(
                        url=self.server_url + '/register', 
                        timeout=30, 
                        json={"datacenter_id": self.data_center_id}
                    ) as req:
                    if req.status == 201:
                        body = await req.json()
                        self.node_id = body.get("node_id")
                        self.registered = True
                        logger.info(f"{log_header} node registered with success, node_id: {self.node_id}")
                    else:
                        logger.error(f"{log_header} request to register ended with status code {req.status}")
                        logger.error(f"{log_header} {await req.text()}")
            except ClientConnectionError:
                logger.info(f"Unable to contact master at url {self.server_url}")

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
        try:
            async with ClientSession() as client:
                async with client.put(
                    url=self.server_url + "/heartbeat",
                    json={"node_id": self.node_id, "datacenter_id": self.data_center_id},
                    timeout=30) as req:
                    if req.status == 200:
                        logger.info(f"{log_header} heart beat sent with success")
                        self.synced = True
                    else:
                        logger.error(f"{log_header} request to send heart beat ended with status code {req.status}")
                        logger.error(f"{log_header} {await req.text()}")
                        self.synced = False
        except ClientConnectionError:
            logger.info(f"Unable to contact master at url: {self.server_url}")
            self.synced = False