import redis.asyncio as redis
from redis import Redis

class RedisInstance:
    """Manages a single async Redis client connection."""

    def __init__(self, redis_host:str, redis_port:int):
        """
        Args:
            redis_host: Hostname or IP address of the Redis server.
            redis_port: Port number the Redis server is listening on.
        """
        self.client:Redis = redis.Redis(host=redis_host, port=redis_port)

    def get_instance(self):
        """Return the underlying async Redis client."""
        return self.client

    async def __del__(self):
        """Close the Redis connection when the instance is garbage-collected."""
        await self.client.aclose()
