
import asyncio
import contextlib
import os

from wsscale.ws.server import WebsocketServer
import pytest
import websockets


@pytest.fixture
def setup_env_variables():
    os.environ["WS_DATACENTER_ID"] = "0"
    os.environ["WS_MASTER_HOST"] = "http://0.0.0.0"
    os.environ["WS_MASTER_PORT"] = "8900"
    os.environ["WS_CLUSTER"] = "true"
    yield
    del os.environ["WS_DATACENTER_ID"]
    del os.environ["WS_MASTER_HOST"]
    del os.environ["WS_MASTER_PORT"]
    del os.environ["WS_CLUSTER"]


async def handler(conn):
    async for message in conn:
        await conn.send(f"echo: {message}")


@pytest.fixture
async def setup_server(setup_env_variables):
    server = WebsocketServer(port=8050)
    bootstrap_task = asyncio.create_task(server.bootstrap(handler=handler))

    # wait for the server socket to actually be bound before handing it to the test
    for _ in range(100):
        if server.server is not None:
            break
        await asyncio.sleep(0.01)
    else:
        raise RuntimeError("websocket server did not start in time")

    yield server

    server.shutdown()
    bootstrap_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await bootstrap_task


class TestWebsocketServer:

    async def test_constructor_with_env_variables(self, setup_env_variables):
        server = WebsocketServer(port=8050)
        assert server.cluster is True

    async def test_constructor_with_settings_path(self):
        server = WebsocketServer(port=8050, settings_path="settings")
        assert server.cluster is True
    
    async def test_bootstrap_in_cluster_mode(self, setup_env_variables):
        server = WebsocketServer(port=8050)
        assert server.cluster is True

        asyncio.create_task(server.bootstrap(handler=handler))
    
    async def test_bootstrap_in_non_cluster_mode(self, setup_env_variables):
        os.environ["WS_CLUSTER"] = "false"
        server = WebsocketServer(port=8050)
        assert server.cluster is False
        
        asyncio.create_task(server.bootstrap(handler=handler))

    async def test_connect_to_ws_server(self, setup_server: WebsocketServer):
        uri = f"ws://0.0.0.0:{setup_server.port}"
        async with websockets.connect(uri=uri) as conn:
            await conn.send(message="connected")
            response = await conn.recv()
            assert response == "echo: connected"
