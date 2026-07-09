import pytest
from wsscale.ids.master import IDGeneratorMaster
from wsscale.ids.node import IDGeneratorNode

@pytest.fixture
async def setup_master():
    master = IDGeneratorMaster(datacenter_id=0, port=9000)
    await master.bootstrap(monitor_interval_seconds=60)
    yield master
    await master.shutdown()


class TestIDGeneratorNode:


    async def test_register_ok(self, setup_master:IDGeneratorMaster):
        node = IDGeneratorNode(data_center_id=setup_master.datacenter_id, server_host="http://0.0.0.0", server_port=setup_master.port)
        await node.register()
        assert node.node_id == 0
        assert node.registered

    async def test_register_nok(self, setup_master:IDGeneratorMaster):
        # try register to wrong datacenter
        node = IDGeneratorNode(data_center_id=1000, server_host="http://0.0.0.0", server_port=setup_master.port)
        await node.register()
        assert node.node_id is None
        assert not node.registered

    async def test_register_master_unavailable(self):
        node = IDGeneratorNode(data_center_id=1000, server_host="http://0.0.0.0", server_port=9000)
        await node.register()
        assert node.node_id is None
        assert not node.registered
        
    async def test_heartbeat_ok(self, setup_master:IDGeneratorMaster):
        node = IDGeneratorNode(data_center_id=setup_master.datacenter_id, server_host="http://0.0.0.0", server_port=setup_master.port)
        # first register
        await node.register()
        assert node.node_id is not None 
        # send heart beat
        await node.heart_beat()
        assert node.synced is True
        

    async def test_heart_beat_nok(self, setup_master:IDGeneratorMaster):
        # try send heart beat with the wrong datacenter id
        node = IDGeneratorNode(data_center_id=1000, server_host="http://0.0.0.0", server_port=setup_master.port)
        await node.heart_beat()
        assert node.synced is False
    
    async def test_heart_beat_master_unavailable(self):
        node = IDGeneratorNode(data_center_id=1000, server_host="http://0.0.0.0", server_port=9000)
        await node.heart_beat()
        assert node.node_id is None
        assert not node.registered

    def _ensure_id_is_correct(self, _id:int):
        timestamp = _id >> 22 & 0b1111111111111111111111111111111111111111 # shift 22 bits to the rights and take the first 41
        datacenter_id = _id >> 17 & 0b11111 # shift 17 bits to the right and take the first five bits
        node_id = _id >> 12 & 0b11111 # shift 12 bits to the right and take the first five bits
        sequence_number = _id & 0b111111111111 # take the first 12 bits

        assert 0 <= int(sequence_number) <= 4095
        assert 0 <= int(node_id) <= 31
        assert 0 <= int(datacenter_id) <= 31

        # the timestamp counter can't exceed 41 bits
        assert 0 <= int(timestamp) <= 2**41

        return timestamp, datacenter_id, node_id, sequence_number
    async def test_generate_id_ok(self):
        # ensure every part of the generated id respect the bytes length
        node = IDGeneratorNode(data_center_id=0, server_host="", server_port=0)

        _id = await node.generate_id(datacenter_id=10, node_id=10)

        timestamp, datacenter_id, node_id, sequence_number = self._ensure_id_is_correct(_id)

        assert node_id == 10
        assert datacenter_id == 10

        # generate a second id and ensure its bigger than the previous
        _id_2 = await node.generate_id(datacenter_id=10, node_id=10)

        _, datacenter_id, node_id, _ = self._ensure_id_is_correct(_id)

        assert node_id == 10
        assert datacenter_id == 10

        assert _id <= _id_2