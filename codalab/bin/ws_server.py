# Main entry point for CodaLab cl-ws-server.
import argparse
import asyncio
import logging
import re
import websockets

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

worker_to_ws = {}


async def rest_server_handler(websocket):
    # Got a message from the rest server.
    logger.warn(f"rest_server_handler")
    print("RSH")
    worker_id = await websocket.recv()
    logger.warn(f"Got a message from the rest server, to ping worker: {worker_id}.")

    try:
        worker_ws = worker_to_ws[worker_id]
        await worker_ws.send(worker_id)
    except KeyError:
        logger.warn(f"Websocket not found for worker: {worker_id}")


async def worker_handler(websocket, worker_id):
    # runs on worker connect
    worker_to_ws[worker_id] = websocket
    logger.warn(f"Connected to worker {worker_id}!")

    while True:
        try:
            await asyncio.wait_for(websocket.recv(), timeout=60)
        except asyncio.futures.TimeoutError:
            pass
        except websockets.exceptions.ConnectionClosed:
            logger.warn(f"Socket connection closed with worker {worker_id}.")
            break


ROUTES = (
    (r'^/main$', rest_server_handler),
    (r'^/worker/(.+)$', worker_handler),
)


async def ws_handler(websocket, *args):
    print("handler")
    logger.warn(f"websocket handler, path: {websocket.path}.")
    for (pattern, handler) in ROUTES:
        match = re.match(pattern, websocket.path)
        if match:
            return await handler(websocket, *match.groups())
    assert False


async def async_main():
    print('test!')
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', help='Port to run the server on.', type=int, required=True)
    args = parser.parse_args()
    print('main 4', args.port)
    async with websockets.serve(ws_handler, "0.0.0.0", 2901):
        await asyncio.Future()  # run forever


def main():
    futures = [async_main()]
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(asyncio.wait(futures))


if __name__ == '__main__':
    main()
