# Main entry point for CodaLab cl-ws-server.
import argparse
import asyncio
from collections import defaultdict
import logging
import re
from typing import Any, Dict, Optional
import websockets
from dataclasses import dataclass
import threading

from codalab.lib.codalab_manager import CodaLabManager

"""
TODO!!! WE NEED TO ADD A SECRET OR SOME SORT OF AUTH FOR THE WS SERVER ENDPOITNS
Otherwise, people could create a custom local worker build and wreak havoc on the ws server...
Note that this was already an issue, though; people could've just hit the checkpoint endpoint...
"""

# class WebsocketRestClient(RestClient):
#     """Allows Websocket Server to make HTTP requests to REST server.

#     Used to authenticate workers.
#     """
#     def __init__(self, base_url: str, access_token: str) -> None:
#         super(WebsocketRestClient, self).__init__(base_url)
#         self._access_token = access_token

#     def _get_access_token(self) -> str:
#         return self._access_token

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
    _last_use: Optional[float] = None

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
ACK = b'a'
logger = logging.getLogger(__name__)
bundle_model = CodalabManager().model
server_secret = os.environ["CODALAB_SERVER_SECRET"]


async def send_handler(server_websocket, worker_id):
    """Handles routes of the form: /send/{worker_id}. This route is called by
    the rest-server or bundle-manager when either wants to send a message/stream to the worker.
    """
    receieved_secret = await server_websocket.recv()
    if not (receieved_secret == server_secret):
        logger.error("Server sent incorrect secret. Aborting")
        return

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


async def authenticate_worker(websocket: Any, user_id: str) -> bool:
    """Helper function to verify worker identity.

    It checks if the Oauth2 token corresponding to the provided user_id is the same
    as the Oauth2 token corresponding to the provided access_token.
    """
    access_token = await websocket.recv()
    oauth2_token_for_access_token = bundle_model.get_oauth2_token(access_token)
    oauth2_token_for_user = bundle_model.find_oauth2_token(
        client_id = 'codalab_worker_client',  # TODO: Don't hard-code client-id.
        user_id = user_id
    )

    return (oauth2_token_for_access_token == oauth2_token_for_user)


async def worker_handler(websocket: Any, user_id: str, worker_id: str, socket_id: str) -> None:
    """Handles routes of the form: /worker/{user_id}/{worker_id}/{socket_id}.
    This route is called when a worker first connects to the ws-server, creating
    a connection that can be used to ask the worker to check-in later.
    """
    authenticated = await authenticate_worker(websocket, user_id)
    if not authenticated:
        logger.error("Thread {socket_id} for worker {worker_id} unable to authenticate.")
        return

    # Otherwise, establish a connection with the worker
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


async def ws_handler(websocket, *args):
    """Handler for websocket connections. Routes websockets to the appropriate
    route handler defined in ROUTES."""
    ROUTES = (
        (r'^.*/send/(.+)$', send_handler),
        (r'^.*/worker/(.+)/(.+)/(.+)$', worker_handler),
    )
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
