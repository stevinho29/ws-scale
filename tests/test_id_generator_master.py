import asyncio
from typing import AsyncGenerator

from wsscale.ids.master import IDGeneratorMaster
from aiohttp import ClientSession
import pytest

DATACENTER_ID = 1

async def setup_master(interval=60) -> AsyncGenerator[IDGeneratorMaster]:
    master = IDGeneratorMaster(datacenter_id=DATACENTER_ID, port=2890)
    await master.bootstrap(monitor_interval_seconds=interval)
    yield master
    await master.shutdown()

@pytest.fixture(scope="function")
async def master_fixture():
    master = IDGeneratorMaster(datacenter_id=DATACENTER_ID, port=2890)
    await master.bootstrap(monitor_interval_seconds=60)
    yield master
    await master.shutdown()

class TestGeneratorMaster:

    async def test_constructor_sets_datacenter_and_port(self):
        master = IDGeneratorMaster(datacenter_id=DATACENTER_ID, port=2890)
        assert master.datacenter_id == DATACENTER_ID
        assert master.port == 2890

    async def test_api_register_ok(self, master_fixture:IDGeneratorMaster):
        url = f"http://0.0.0.0:{master_fixture.port}/register"
        async with ClientSession() as client:
            result = await client.post(url=url, json={"datacenter_id": DATACENTER_ID})
            assert result.status == 201
            data = await result.json()
            assert data.get("node_id") == 0

        async with ClientSession() as client:
            result = await client.post(url=url, json={"datacenter_id": DATACENTER_ID})
            assert result.status == 201
            data = await result.json()
            assert data.get("node_id") == 1

    async def test_api_register_nok(self, master_fixture:IDGeneratorMaster):
        url = f"http://0.0.0.0:{master_fixture.port}/register"
        # empty body
        async with ClientSession() as client:
            result = await client.post(url=url, json={})
            assert result.status == 400

        # wrong datacenter id
        async with ClientSession() as client:
            result = await client.post(url=url, json={"datacenter_id": 1900})
            assert result.status == 400

    async def test_api_register_cant_handle_more_than_32_nodes(self, master_fixture:IDGeneratorMaster):
        url = f"http://0.0.0.0:{master_fixture.port}/register"
        async with ClientSession() as client:
            result = await client.post(url=url, json={"datacenter_id": DATACENTER_ID})
            assert result.status == 201
            data = await result.json()
            assert data.get("node_id") == 0
        
            result = await client.post(url=url, json={"datacenter_id": DATACENTER_ID})
            assert result.status == 201
            data = await result.json()
            assert data.get("node_id") == 1
        
            result = await client.post(url=url, json={"datacenter_id": DATACENTER_ID})
            assert result.status == 201
            data = await result.json()
            assert data.get("node_id") == 2

            result = await client.post(url=url, json={"datacenter_id": DATACENTER_ID})
            assert result.status == 201
            data = await result.json()
            assert data.get("node_id") == 3

            result = await client.post(url=url, json={"datacenter_id": DATACENTER_ID})
            assert result.status == 201
            data = await result.json()
            assert data.get("node_id") == 4

            result = await client.post(url=url, json={"datacenter_id": DATACENTER_ID})
            assert result.status == 201
            data = await result.json()
            assert data.get("node_id") == 5

            result = await client.post(url=url, json={"datacenter_id": DATACENTER_ID})
            assert result.status == 201
            data = await result.json()
            assert data.get("node_id") == 6
 
            result = await client.post(url=url, json={"datacenter_id": DATACENTER_ID})
            assert result.status == 201
            data = await result.json()
            assert data.get("node_id") == 7

            result = await client.post(url=url, json={"datacenter_id": DATACENTER_ID})
            assert result.status == 201
            data = await result.json()
            assert data.get("node_id") == 8

            result = await client.post(url=url, json={"datacenter_id": DATACENTER_ID})
            assert result.status == 201
            data = await result.json()
            assert data.get("node_id") == 9

            result = await client.post(url=url, json={"datacenter_id": DATACENTER_ID})
            assert result.status == 201
            data = await result.json()
            assert data.get("node_id") == 10

            result = await client.post(url=url, json={"datacenter_id": DATACENTER_ID})
            assert result.status == 201
            data = await result.json()
            assert data.get("node_id") == 11

            result = await client.post(url=url, json={"datacenter_id": DATACENTER_ID})
            assert result.status == 201
            data = await result.json()
            assert data.get("node_id") == 12

            result = await client.post(url=url, json={"datacenter_id": DATACENTER_ID})
            assert result.status == 201
            data = await result.json()
            assert data.get("node_id") == 13

            result = await client.post(url=url, json={"datacenter_id": DATACENTER_ID})
            assert result.status == 201
            data = await result.json()
            assert data.get("node_id") == 14

            result = await client.post(url=url, json={"datacenter_id": DATACENTER_ID})
            assert result.status == 201
            data = await result.json()
            assert data.get("node_id") == 15

            result = await client.post(url=url, json={"datacenter_id": DATACENTER_ID})
            assert result.status == 201
            data = await result.json()
            assert data.get("node_id") == 16

            result = await client.post(url=url, json={"datacenter_id": DATACENTER_ID})
            assert result.status == 201
            data = await result.json()
            assert data.get("node_id") == 17
 
            result = await client.post(url=url, json={"datacenter_id": DATACENTER_ID})
            assert result.status == 201
            data = await result.json()
            assert data.get("node_id") == 18

            result = await client.post(url=url, json={"datacenter_id": DATACENTER_ID})
            assert result.status == 201
            data = await result.json()
            assert data.get("node_id") == 19
 
            result = await client.post(url=url, json={"datacenter_id": DATACENTER_ID})
            assert result.status == 201
            data = await result.json()
            assert data.get("node_id") == 20

            result = await client.post(url=url, json={"datacenter_id": DATACENTER_ID})
            assert result.status == 201
            data = await result.json()
            assert data.get("node_id") == 21

            result = await client.post(url=url, json={"datacenter_id": DATACENTER_ID})
            assert result.status == 201
            data = await result.json()
            assert data.get("node_id") == 22

            result = await client.post(url=url, json={"datacenter_id": DATACENTER_ID})
            assert result.status == 201
            data = await result.json()
            assert data.get("node_id") == 23

            result = await client.post(url=url, json={"datacenter_id": DATACENTER_ID})
            assert result.status == 201
            data = await result.json()
            assert data.get("node_id") == 24

            result = await client.post(url=url, json={"datacenter_id": DATACENTER_ID})
            assert result.status == 201
            data = await result.json()
            assert data.get("node_id") == 25

            result = await client.post(url=url, json={"datacenter_id": DATACENTER_ID})
            assert result.status == 201
            data = await result.json()
            assert data.get("node_id") == 26

            result = await client.post(url=url, json={"datacenter_id": DATACENTER_ID})
            assert result.status == 201
            data = await result.json()
            assert data.get("node_id") == 27
 
            result = await client.post(url=url, json={"datacenter_id": DATACENTER_ID})
            assert result.status == 201
            data = await result.json()
            assert data.get("node_id") == 28
        
 
            result = await client.post(url=url, json={"datacenter_id": DATACENTER_ID})
            assert result.status == 201
            data = await result.json()
            assert data.get("node_id") == 29
        
            result = await client.post(url=url, json={"datacenter_id": DATACENTER_ID})
            assert result.status == 201
            data = await result.json()
            assert data.get("node_id") == 30


            result = await client.post(url=url, json={"datacenter_id": DATACENTER_ID})
            assert result.status == 201
            data = await result.json()
            assert data.get("node_id") == 31

            # can't handle 33 nodes
            result = await client.post(url=url, json={"datacenter_id": DATACENTER_ID})
            assert result.status == 400

    async def test_api_heartbeat_ok(self, master_fixture: IDGeneratorMaster):
        url_heartbeat = f"http://0.0.0.0:{master_fixture.port}/heartbeat"
        url_register = f"http://0.0.0.0:{master_fixture.port}/register"

        async with ClientSession() as client:
            # first register a node
            result = await client.post(url=url_register, json={"datacenter_id": DATACENTER_ID})
            assert result.status == 201

            # send heartbeat
            data = await result.json()
            node_id = data.get("node_id")
            result = await client.put(url=url_heartbeat, json={"datacenter_id": DATACENTER_ID, "node_id": node_id})
            assert result.status == 200

    async def test_api_heartbeat_nok(self, master_fixture: IDGeneratorMaster):
        url_heartbeat = f"http://0.0.0.0:{master_fixture.port}/heartbeat"

        # send heartbeat when node is not registered
        async with ClientSession() as client:
            # send heartbeat
            result = await client.put(url=url_heartbeat, json={"node_id": 1000, "datacenter_id": DATACENTER_ID})
            assert result.status == 400

        # empty body
        async with ClientSession() as client:
            # send heartbeat
            result = await client.put(url=url_heartbeat, json={})
            assert result.status == 400

    async def test_monitor_nodes(self):
        async for master in  setup_master(interval=5):
            # set treshold of liveness lower than monitoring interval
            master.MONITOR_NODES_INTERVAL_SECONDS = 3 
            url = f"http://0.0.0.0:{master.port}/register"
            async with ClientSession() as client:
                result = await client.post(url=url, json={"datacenter_id": DATACENTER_ID})
                data = await result.json()
                node_id = data.get("node_id")
                assert result.status == 201

            await asyncio.sleep(6)
            nodes_data = master.get_nodes_data()
 
            assert node_id not in nodes_data.get("nodes")
            

