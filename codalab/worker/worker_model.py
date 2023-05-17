import asyncio
from contextlib import closing
import datetime
import json
import logging
import os
import socket
import time
import websockets
from websockets.sync.client import connect

from sqlalchemy import and_, select

logger = logging.getLogger(__name__)
ACK=b'a'


class WorkerModel(object):
    """
    Manages the worker, worker_dependency and worker_socket tables. This class
    serves 2 primary functions:

    1) It is used to add, remove and query information about workers.
    2) It is used for communication with the workers. This communication happens
       through Unix domain sockets stored in a special directory. This class
       provides methods to allocate sockets (i.e. figure out unique paths in the
       socket directory), clean up sockets (i.e. delete the socket files),
       listen on these sockets for messages and send messages to these sockets.
    """

    def __init__(self, engine, socket_dir, ws_server):
        self._engine = engine
        self._socket_dir = socket_dir
        self._ws_server = ws_server
    

    def _connect(self, worker_id, timeout_secs=20):
        with connect(f"{self._ws_server}/server/connect/{worker_id}", open_timeout=timeout_secs, close_timeout=timeout_secs) as websocket:
            socket_id = json.loads(websocket.recv())['socket_id']
        return socket_id

    def connect_to_ws(self, worker_id, timeout_secs=20):
        """
        Loop until connection achieved.
        """
        socket_id = None
        start_time = time.time()
        while time.time() - start_time < timeout_secs:
            try:
                socket_id = self._connect(worker_id, timeout_secs)
            except Exception as e:
                logger.error(f"Error receiving socket_id from _connect: {e}")
            if socket_id:
                break
            else:
                logging.error(f"No sockets available for worker {worker_id}; retrying")
                time.sleep(0.5)
        if not socket_id:
            logging.error("No connection reached")
            raise ValueError("Worker socket ID is None. Worker cannot")
        return socket_id

    def disconnect(self, worker_id, socket_id, timeout_secs=20):
        with connect(f"{self._ws_server}/server/disconnect/{worker_id}/{socket_id}", open_timeout=timeout_secs, close_timeout=timeout_secs) as websocket:
            pass  # Just disconnect it.

    def send(self, data, worker_id, socket_id, timeout_secs=60, is_json=True):
        """
        Send data to the worker.

        :param socket_id: The ID of the socket through which we send data to the worker.
        :param worker_id: The ID of the worker to send data to
        :param data: Data to send to worker. Could be a file or a json message.
        :param timeout_secs: Seconds until timeout. The actual data sending could take
                                2 times this value (although this is quite unlikely) since
                                both open_timeout and close_timeout are set to timeout_secs 
        :param is_json: True if data should be json encoded before being sent.
                        Otherwise, data is sent in
        
        :return True if data was sent properly, False otherwise.
        """
        CHUNK_SIZE = 4096  # TODO: Make this a variable set in Codalab environment.
        logger.error("in send")
        if not socket_id: return False
        try:
            with connect(f"{self._ws_server}/send/{worker_id}/{socket_id}", open_timeout=timeout_secs, close_timeout=timeout_secs) as websocket:
                if is_json:
                    logger.error("in send json")
                    websocket.send(json.dumps(data).encode())
                    logger.error("sent")
                    data = websocket.recv()
                    logger.error(f"Received ack: {data}")
                    return (data == ACK)
                else:
                    while True:
                        chunk = data.read(CHUNK_SIZE)
                        if not chunk: break
                        websocket.send(chunk)
                        data = websocket.recv()
                        if (data != ACK): return False
                    return True
        except Exception as e:
            logger.error(f"Send to worker {worker_id} through socket {socket_id} failed with {e}")
        return False

    def _recv(self, recv_fn, worker_id, socket_id, timeout_secs=5):
        """
        Receive data from the worker.

        :param recv_fn: A Callable which takes a websocket as argument and receives data from the worker socket.
        :param socket_id: The ID of the socket through which we send data to the worker.
        :param worker_id: The ID of the worker to send data to
        :param timeout_secs: Seconds until timeout. The actual data sending could take
                                2 times this value (although this is quite unlikely) since
                                both open_timeout and close_timeout are set to timeout_secs 
        :param is_json: True if received data should be json decoded.
                        Otherwise, we assume we are receiving a stream and the function is used as a generator,
                          yielding bytes chunk by chunk.

        :return A dictionary if is_json is True. A generator otherwise.
        """
        logger.error("in recv")
        if not socket_id:
            logger.error("no socket id")
            return
        try:
            logger.error("about to connect")
            with connect(f"{self._ws_server}/recv/{worker_id}/{socket_id}", open_timeout=timeout_secs, close_timeout=timeout_secs) as websocket:
                logger.error("connected")
                return recv_fn(websocket)
        except Exception as e:
            logger.error(f"Recv from worker {worker_id} through socket {socket_id} failed with {e}")
    
    def recv_json(self, worker_id, socket_id, timeout_secs=60):
        def _recv_json(websocket):
            logger.error("In recv json")
            data = websocket.recv()
            logger.error("received")
            websocket.send(ACK)
            return json.loads(data.decode())
        return self._recv(_recv_json, worker_id, socket_id, timeout_secs)
    def recv_stream(self, worker_id, socket_id, timeout_secs=60):
        def _recv_stream(websocket):
            logger.error("In recv STREAM")
            while True:
                try:
                    chunk = websocket.recv()
                    if not chunk: break
                    websocket.send(ACK)
                    yield chunk
                except websockets.exceptions.ConnectionClosed: break
        return self._recv(_recv_stream, worker_id, socket_id, timeout_secs)

    def connect_and_send(self, data, worker_id, timeout_secs=60, is_json=True):
        """
        Convenience method to connect to worker socket, send, and then disconnect.
        """
        socket_id = self.connect_to_ws(worker_id)
        if not socket_id: return False
        sent = self.send(data, worker_id, socket_id, timeout_secs, is_json)
        self.disconnect(worker_id, socket_id)
        return sent