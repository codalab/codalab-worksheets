# This is the real ws-server, basically.
# Main entry point for CodaLab cl-ws-server.
import argparse
import json
import asyncio
from collections import defaultdict
import logging
import re
from typing import Any, Dict, List
import websockets
from dataclasses import dataclass
import threading
import time

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())
logging.basicConfig(format='%(asctime)s %(message)s %(pathname)s %(lineno)d')


"""
TODO!!! WE NEED TO ADD A SECRET OR SOME SORT OF AUTH FOR THE WS SERVER ENDPOITNS
Otherwise, people could create a custom local worker build and wreak havoc on the ws server...
Note that this was already an issue, though; people could've just hit the checkpoint endpoint...
"""

@dataclass
class WS:
    """
    Stores websocket object and whether or not the websocket is available.
    TODO: give this a better, less confusing name.
    """
    _ws: Any = None
    _is_available: bool = True
    _lock: threading.Lock = threading.Lock()
    _timeout: float = 5.0
    _last_use: float = None

    @property
    def ws(self):
        return self._ws
    
    @property
    def lock(self):
        return self._lock
    
    @property
    def is_available(self):
        return self._is_available

    @is_available.setter
    def is_available(self, value):
        self._is_available = value
    
    @property
    def timeout(self):
        return self._timeout
    
    @property
    def last_use(self):
        return self._last_use
    
    @last_use.setter
    def last_use(self, value):
        self._last_use = value

worker_to_ws: Dict[str, Dict[str, WS]] = defaultdict(dict)  # Maps worker to list of its websockets (since each worker has a pool of connections)
server_ws_to_worker_ws: Dict[Any, Any] = dict()  # Map the rest-server websocket connection to the corresponding worker socket connection.

async def connection_handler(websocket, worker_id):
    """
    Handles routes of the form: /server/connect/worker_id. This route is called by the rest-server
    (or bundle-manager) when they want a connection to a worker.
    Returns the id of the socket to connect to, which will be used in later requests.
    """
    logger.error(f"Got a message from the rest server, to connect to worker: {worker_id}.")
    socket_id = None
    #logger.error([ws.is_available for _,ws in worker_to_ws[worker_id].items()])
    for s_id, ws in worker_to_ws[worker_id].items():
        logger.error("about to lock")
        with ws.lock:
            logger.error("locked")
            if ws.is_available or time.time() - ws.last_use >= ws.timeout:
                logger.error("available")
                ws.last_use = time.time()
                socket_id = s_id
                worker_to_ws[worker_id][socket_id].is_available = False
                logger.error("breaking")
                break
    
    logger.error(f"SOCKET ID: {socket_id}")
    if not socket_id:
        logger.error(f"No socket ids available for worker {worker_id}")
    #import pdb; pdb.set_trace()
    await websocket.send(json.dumps({'socket_id': socket_id}))
    logger.error("Sent.")
    

async def disconnection_handler(websocket, worker_id, socket_id):
    """
    Handles routes of the form: /server/connect/worker_id/socket_id. This route is called by the rest-server
    (or bundle-manager) when they want a connection to a worker.
    Returns the id of the socket to connect to, which will be used in later requests.
    """
    with worker_to_ws[worker_id][socket_id].lock:
        if worker_to_ws[worker_id][socket_id].is_available:
            # For now, just log the error.
            logging.error("Available socket set for disconnection")
        worker_to_ws[worker_id][socket_id].is_available = True
    

async def exchange(from_ws, to_ws, worker_id, socket_id):
    if worker_id not in worker_to_ws or socket_id not in worker_to_ws[worker_id]:
        logger.error("Invalid request. WorkerID: {worker_id}, SocketID: {socket_id}")
    
    # Send and receive all data until connection closes.
    # (We may be streaming a file, in which case we need to receive and then send
    # lots of chunks)
    async for data in from_ws:
        await to_ws.send(data)

async def send_handler(websocket, worker_id, socket_id):
    """Handles routes of the form: /send/{worker_id}/{socket_id}. This route is called by
    the rest-server or bundle-manager when either wants to send a message/stream to the worker.
    """
    with worker_to_ws[worker_id][socket_id].lock:
        worker_to_ws[worker_id][socket_id].ws.last_use = time.time()
    await exchange(websocket, worker_to_ws[worker_id][socket_id].ws, worker_id, socket_id)

async def recv_handler(websocket, worker_id, socket_id):
    """Handles routes of the form: /recv/{worker_id}/{socket_id}. This route is called by
    the rest-server or bundle-manager when either wants to receive a message/stream from the worker.
    """
    with worker_to_ws[worker_id][socket_id].lock:
        worker_to_ws[worker_id][socket_id].ws.last_use = time.time()
    await exchange(worker_to_ws[worker_id][socket_id].ws, websocket, worker_id, socket_id)

async def worker_handler(websocket, worker_id, socket_id):
    """Handles routes of the form: /worker/{worker_id}/{socket_id}. This route is called when
    a worker first connects to the ws-server, creating a connection that can
    be used to ask the worker to check-in later.
    """
    # runs on worker connect
    worker_to_ws[worker_id][socket_id] = WS(websocket)
    logger.warning(f"Worker {worker_id} connected; has {len(worker_to_ws[worker_id])} connections")

    # keep connection alive.
    while True:
        try:
            await asyncio.sleep(60)
        except asyncio.futures.TimeoutError:
            pass
        except websockets.exceptions.ConnectionClosed:
            logger.error(f"Socket connection closed with worker {worker_id}.")
            break

ROUTES = (
    (r'^.*/send/(.+)/(.+)$', send_handler),
    (r'^.*/recv/(.+)/(.+)$', recv_handler),
    (r'^.*/server/connect/(.+)$', connection_handler),
    (r'^.*/server/disconnect/(.+)/(.+)$', disconnection_handler),
    (r'^.*/worker/(.+)/(.+)$', worker_handler),
)
async def ws_handler(websocket, *args):
    """Handler for websocket connections. Routes websockets to the appropriate
    route handler defined in ROUTES."""
    logger.error("Websocket router")
    logger.error(f"websocket handler, path: {websocket.path}.")
    for (pattern, handler) in ROUTES:
        match = re.match(pattern, websocket.path)
        if match:
            return await handler(websocket, *match.groups())

async def async_main():
    """Main function that runs the websocket server."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', help='Port to run the server on.', type=int, required=False, default=2901)
    args = parser.parse_args()
    logging.debug(f"Running ws-server on 0.0.0.0:{args.port}")
    async with websockets.serve(ws_handler, "0.0.0.0", args.port, logger=logger):
        await asyncio.Future()  # run server forever


def main():
    futures = [async_main()]
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(asyncio.wait(futures))


if __name__ == '__main__':
    main()