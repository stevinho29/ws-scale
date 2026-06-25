
import asyncio
import logging
import os
from importlib import import_module
from typing import Callable, Awaitable, Optional, Any
from websockets.asyncio.connection import Connection
from websockets.asyncio.server import serve, Server
from wsscale.ids.node import IDGeneratorNode


Handler = Awaitable[Callable[[Connection], Any]]


logger = logging.getLogger("ws-scale.server")

class WebsocketServer:
    """
    Main entry point for the WebSocket server application.

    Initializes an ``IDGeneratorNode``, registers it with the master on startup,
    and drives a ``websockets`` server that routes incoming connections to the
    caller-supplied handler.
    """

    def __init__(self, datacenter_id:int, port:int, settings_path:Optional[str]=None):
        """
        Args:
            datacenter_id: 5-bit datacenter identifier embedded in generated IDs.
            port: Local TCP port the WebSocket server will listen on.
            settings_path: Dotted module path to a settings file (e.g. ``"app.settings"``).
                           Falls back to environment variables when omitted.
        """
        self.port = port
        self.server: Server = None
        self.log_header = "[WebsocketServer]"
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

        try:
            self.node = IDGeneratorNode(
                server_host=id_master_host, 
                server_port=id_master_port, 
                data_center_id=datacenter_id,
                redis_host=redis_host,
                redis_port=redis_port
            )
        except Exception as e:
            logger.warning(e)
            logger.error("Unable to start id generator node, please verify your configuration")

    async def bootstrap(self, handler: Handler):
        """Register the ID node, start the heartbeat task, and run the WebSocket server.

        Args:
            handler: Async callable that handles each incoming WebSocket connection.
        """
        log_header = f"{self.log_header}[bootstrap]"
        await self.node.register()
        asyncio.create_task(self.node.heart_beat_loop(interval_seconds=60))
        logger.info(f"{log_header} websockets server listening on localhost:{self.port}")
        async with serve(handler=handler, host="", port=self.port) as server:
            self.server = server
            await server.serve_forever()
        
        