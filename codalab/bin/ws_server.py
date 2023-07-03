# This is the real ws-server, basically.
# Main entry point for CodaLab cl-ws-server.
import argparse
import asyncio
from collections import defaultdict
import logging
import re
from typing import Any, Dict
import websockets
from dataclasses import dataclass
import threading

ACK = b'a'

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
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
    _timeout: float = 86400
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


worker_to_ws: Dict[str, Dict[str, WS]] = defaultdict(
    dict
)  # Maps worker to list of its websockets (since each worker has a pool of connections)
server_worker_to_ws: Dict[str, Dict[str, WS]] = defaultdict(
    dict
)  # Map the rest-server websocket connection to the corresponding worker socket connection.


async def send_handler(server_websocket, worker_id):
    """Handles routes of the form: /send/{worker_id}. This route is called by
    the rest-server or bundle-manager when either wants to send a message/stream to the worker.
    """
    data = await server_websocket.recv()
    logger.error("received data")
    for _, worker_websocket in worker_to_ws[worker_id].items():
        logger.error("trying to acquire a websocket")
        if worker_websocket.lock.acquire(blocking=False):
            logger.error("Sending data")
            await worker_websocket.ws.send(data)
            await server_websocket.send(ACK)
            logger.error("sent ACK")
            worker_websocket.lock.release()
            break


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
    (r'^.*/send/(.+)$', send_handler),
    (r'^.*/worker/(.+)/(.+)$', worker_handler),
)


async def ws_handler(websocket, *args):
    """Handler for websocket connections. Routes websockets to the appropriate
    route handler defined in ROUTES."""
    logger.debug(f"websocket handler, path: {websocket.path}.")
    for (pattern, handler) in ROUTES:
        match = re.match(pattern, websocket.path)
        if match:
            return await handler(websocket, *match.groups())


async def async_main():
    """Main function that runs the websocket server."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--port', help='Port to run the server on.', type=int, required=False, default=2901
    )
    args = parser.parse_args()
    logging.debug(f"Running ws-server on 0.0.0.0:{args.port}")
    async with websockets.serve(ws_handler, "0.0.0.0", args.port):
        await asyncio.Future()  # run server forever


def main():
    futures = [async_main()]
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(asyncio.wait(futures))


if __name__ == '__main__':
    main()
