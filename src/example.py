
import argparse
import asyncio
from wsscale import IDGeneratorMaster
from wsscale import WebsocketServer
import logging
import sys


formatter = logging.Formatter(fmt="%(asctime)s [%(levelname)s] [<%(name)s>:%(module)s:%(lineno)s] %(message)s")
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(fmt=formatter)
logging.getLogger("ws-scale").setLevel(logging.INFO)
logging.getLogger("ws-scale").addHandler(handler)

async def do_nothing(conn):
    print(conn)

async def start():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8080, help="Port to listen on")
    parser.add_argument("--datacenter", type=int, default=0, help="Port to listen on")
    parser.add_argument("--generator", type=bool, default=True, help="Start generator master or not")
    args = parser.parse_args()
    port = args.port
    id_generator = args.generator
    datacenter = args.datacenter

    id_master_server = None
    if id_generator:
        id_master_server = IDGeneratorMaster(datacenter_id=datacenter, settings_path="settings")
    
    ws_server = WebsocketServer(
        datacenter_id=datacenter,
        port=port,
        settings_path="settings"
    )
    
    if id_master_server:
        # cleanup remaining keys in redis for fresh start
        await id_master_server.cleanup()
        await id_master_server.serve()
        await ws_server.bootstrap(handler=do_nothing)
        
    else:
        await ws_server.bootstrap(handler=do_nothing)

if __name__ == "__main__":
    asyncio.run(start())