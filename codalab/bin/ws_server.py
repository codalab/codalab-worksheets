# Main entry point for CodaLab cl-ws-server.
import argparse
import asyncio
from collections import defaultdict
import logging
import os
import random
import re
import time
from typing import Any, Dict
import websockets
import threading

from codalab.lib.codalab_manager import CodaLabManager


class TimedLock:
    """A lock that gets automatically released after timeout_seconds.
    """

    def __init__(self, timeout_seconds: float = 60):
        self._lock = threading.Lock()
        self._time_since_locked: float
        self._timeout: float = timeout_seconds

    def acquire(self, blocking=True, timeout=-1):
        acquired = self._lock.acquire(blocking, timeout)
        if acquired:
            self._time_since_locked = time.time()
        return acquired

    def locked(self):
        return self._lock.locked()

    def release(self):
        self._lock.release()

    def timeout(self):
        return time.time() - self._time_since_locked > self._timeout

    def release_if_timeout(self):
        if self.locked() and self.timeout():
            self.release()


worker_to_ws: Dict[str, Dict[str, Any]] = defaultdict(dict)  # Maps worker ID to socket ID to websocket
worker_to_lock: Dict[str, Dict[str, TimedLock]] = defaultdict(dict)  # Maps worker ID to socket ID to lock
ACK = b'a'
logger = logging.getLogger(__name__)
manager = CodaLabManager()
bundle_model = manager.model()
worker_model = manager.worker_model()
server_secret = os.getenv("CODALAB_SERVER_SECRET")


async def send_to_worker_handler(server_websocket, worker_id):
    """Handles routes of the form: /send_to_worker/{worker_id}. This route is called by
    the rest-server or bundle-manager when either wants to send a message/stream to the worker.
    """
    # Check if any websockets available
    if worker_id not in worker_to_ws or len(worker_to_ws[worker_id]) == 0:
        logger.warning(f"No websockets currently available for worker {worker_id}")
        await server_websocket.close(
            1013, f"No websockets currently available for worker {worker_id}"
        )
        return

    # Authenticate server.
    received_secret = await server_websocket.recv()
    if received_secret != server_secret:
        logger.warning("Server unable to authenticate.")
        await server_websocket.close(1008, "Server unable to authenticate.")
        return

    # Send message from server to worker.
    for socket_id, worker_websocket in random.sample(
        worker_to_ws[worker_id].items(), len(worker_to_ws[worker_id])
    ):
        if worker_to_lock[worker_id][socket_id].acquire(blocking=False):
            data = await server_websocket.recv()
            await worker_websocket.send(data)
            await server_websocket.send(ACK)
            worker_to_lock[worker_id][socket_id].release()
            return

    logger.warning(f"All websockets for worker {worker_id} are currently busy.")
    await server_websocket.close(1013, f"All websockets for worker {worker_id} are currently busy.")


async def worker_connection_handler(websocket: Any, worker_id: str, socket_id: str) -> None:
    """Handles routes of the form: /worker_connect/{worker_id}/{socket_id}.
    This route is called when a worker first connects to the ws-server, creating
    a connection that can be used to ask the worker to check-in later.
    """
    # Authenticate worker.
    access_token = await websocket.recv()
    user_id = worker_model.get_user_id_for_worker(worker_id=worker_id)
    authenticated = bundle_model.access_token_exists_for_user(
        'codalab_worker_client', user_id, access_token  # TODO: Avoid hard-coding this if possible.
    )
    if not authenticated:
        logger.warning(f"Thread {socket_id} for worker {worker_id} unable to authenticate.")
        await websocket.close(
            1008, f"Thread {socket_id} for worker {worker_id} unable to authenticate."
        )
        return

    # Establish a connection with worker and keep it alive.
    worker_to_ws[worker_id][socket_id] = websocket
    worker_to_lock[worker_id][socket_id] = TimedLock()
    logger.warning(f"Worker {worker_id} connected; has {len(worker_to_ws[worker_id])} connections")
    while True:
        try:
            await asyncio.wait_for(websocket.recv(), timeout=60)
            worker_to_lock[worker_id][socket_id].release_if_timeout()  # Failsafe in case not released
        except asyncio.futures.TimeoutError:
            pass
        except websockets.exceptions.ConnectionClosed:
            logger.warning(f"Socket connection closed with worker {worker_id}.")
            break
    del worker_to_ws[worker_id][socket_id]
    del worker_to_lock[worker_id][socket_id]
    logger.warning(f"Worker {worker_id} now has {len(worker_to_ws[worker_id])} connections")


async def ws_handler(websocket, *args):
    """Handler for websocket connections. Routes websockets to the appropriate
    route handler defined in ROUTES."""
    ROUTES = (
        (r'^.*/send_to_worker/(.+)$', send_to_worker_handler),
        (r'^.*/worker_connect/(.+)/(.+)$', worker_connection_handler),
    )
    logger.info(f"websocket handler, path: {websocket.path}.")
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
