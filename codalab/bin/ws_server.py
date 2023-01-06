# Main entry point for CodaLab cl-ws-server.
import argparse
import asyncio
import logging
import re
from typing import Any, Dict
import websockets

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)
logging.basicConfig(format='%(asctime)s %(message)s %(pathname)s %(lineno)d')

worker_to_ws: Dict[str, Any] = {}


async def rest_server_handler(websocket):
    """Handles routes of the form: /main. This route is called by the rest-server
    whenever a worker needs to be pinged (to ask it to check in). The body of the
    message is the worker id to ping. This function sends a message to the worker
    with that worker id through an appropriate websocket.
    """
    # Got a message from the rest server.
    worker_id = await websocket.recv()
    logger.warning(f"Got a message from the rest server, to ping worker: {worker_id}.")

    try:
        worker_ws = worker_to_ws[worker_id]
        await worker_ws.send(worker_id)
    except KeyError:
        logger.error(f"Websocket not found for worker: {worker_id}")


async def worker_handler(websocket, worker_id):
    """Handles routes of the form: /worker/{id}. This route is called when
    a worker first connects to the ws-server, creating a connection that can
    be used to ask the worker to check-in later.
    """
    # runs on worker connect
    worker_to_ws[worker_id] = websocket
    logger.warning(f"Connected to worker {worker_id}!")

    while True:
        try:
            await asyncio.wait_for(websocket.recv(), timeout=60)
        except asyncio.futures.TimeoutError:
            pass
        except websockets.exceptions.ConnectionClosed:
            logger.error(f"Socket connection closed with worker {worker_id}.")
            break


ROUTES = (
    (r'^.*/main$', rest_server_handler),
    (r'^.*/worker/(.+)$', worker_handler),
)


async def ws_handler(websocket, *args):
    """Handler for websocket connections. Routes websockets to the appropriate
    route handler defined in ROUTES."""
    logger.warning(f"websocket handler, path: {websocket.path}.")
    for (pattern, handler) in ROUTES:
        match = re.match(pattern, websocket.path)
        if match:
            return await handler(websocket, *match.groups())
    assert False


async def async_main():
    """Main function that runs the websocket server."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', help='Port to run the server on.', type=int, required=True)
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
