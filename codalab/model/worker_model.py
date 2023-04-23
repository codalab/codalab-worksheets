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

from codalab.common import precondition
from codalab.model.tables import (
    worker as cl_worker,
    group as cl_group,
    worker_socket as cl_worker_socket,
    worker_run as cl_worker_run,
    worker_dependency as cl_worker_dependency,
)

logger = logging.getLogger(__name__)


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
        self._ws_server = ws_server

    def worker_checkin(
        self,
        user_id,
        worker_id,
        tag,
        group_name,
        cpus,
        gpus,
        memory_bytes,
        free_disk_bytes,
        dependencies,
        shared_file_system,
        tag_exclusive,
        exit_after_num_runs,
        is_terminating,
        preemptible,
    ):
        """
        Adds the worker to the database, if not yet there. Returns the socket ID
        that the worker should listen for messages on.
        """
        with self._engine.begin() as conn:
            worker_row = {
                'tag': tag,
                'cpus': cpus,
                'gpus': gpus,
                'memory_bytes': memory_bytes,
                'free_disk_bytes': free_disk_bytes,
                'checkin_time': datetime.datetime.utcnow(),
                'shared_file_system': shared_file_system,
                'tag_exclusive': tag_exclusive,
                'exit_after_num_runs': exit_after_num_runs,
                'is_terminating': is_terminating,
                'preemptible': preemptible,
            }

            # Populate the group for this worker, if group_name is valid
            group_row = conn.execute(
                cl_group.select().where(cl_group.c.name == group_name)
            ).fetchone()
            if group_row:
                worker_row['group_uuid'] = group_row.uuid

            existing_row = conn.execute(
                cl_worker.select().where(
                    and_(cl_worker.c.user_id == user_id, cl_worker.c.worker_id == worker_id)
                )
            ).fetchone()
            if existing_row:
                conn.execute(
                    cl_worker.update()
                    .where(and_(cl_worker.c.user_id == user_id, cl_worker.c.worker_id == worker_id))
                    .values(worker_row)
                )
            else:
                # TODO: migrate worker table to not have socket id.
                worker_row.update(
                    {'user_id': user_id, 'worker_id': worker_id}
                )
                conn.execute(cl_worker.insert().values(worker_row))

            # Update dependencies
            blob = self._serialize_dependencies(dependencies).encode()
            if existing_row:
                conn.execute(
                    cl_worker_dependency.update()
                    .where(
                        and_(
                            cl_worker_dependency.c.user_id == user_id,
                            cl_worker_dependency.c.worker_id == worker_id,
                        )
                    )
                    .values(dependencies=blob)
                )
            else:
                conn.execute(
                    cl_worker_dependency.insert().values(
                        user_id=user_id, worker_id=worker_id, dependencies=blob
                    )
                )

    @staticmethod
    def _serialize_dependencies(dependencies):
        return json.dumps(dependencies, separators=(',', ':'))

    @staticmethod
    def _deserialize_dependencies(blob):
        return list(map(tuple, json.loads(blob)))

    def worker_cleanup(self, user_id, worker_id):
        """
        Deletes the worker and all associated data from the database as well
        as the socket directory.
        """
        with self._engine.begin() as conn:
            conn.execute(
                cl_worker_run.delete().where(
                    and_(cl_worker_run.c.user_id == user_id, cl_worker_run.c.worker_id == worker_id)
                )
            )
            conn.execute(
                cl_worker_dependency.delete().where(
                    and_(
                        cl_worker_dependency.c.user_id == user_id,
                        cl_worker_dependency.c.worker_id == worker_id,
                    )
                )
            )
            conn.execute(
                cl_worker.delete().where(
                    and_(cl_worker.c.user_id == user_id, cl_worker.c.worker_id == worker_id)
                )
            )

    def get_workers(self):
        """
        Returns information about all the workers in the database. The return
        value is a list of dicts with the structure shown in the code below.
        """
        with self._engine.begin() as conn:
            worker_rows = conn.execute(
                select([cl_worker, cl_worker_dependency.c.dependencies]).select_from(
                    cl_worker.outerjoin(
                        cl_worker_dependency,
                        cl_worker.c.worker_id == cl_worker_dependency.c.worker_id,
                    )
                )
            ).fetchall()
            worker_run_rows = conn.execute(cl_worker_run.select()).fetchall()

        worker_dict = {
            (row.user_id, row.worker_id): {
                'user_id': row.user_id,
                'worker_id': row.worker_id,
                'group_uuid': row.group_uuid,
                'tag': row.tag,
                'cpus': row.cpus,
                'gpus': row.gpus,
                'memory_bytes': row.memory_bytes,
                'free_disk_bytes': row.free_disk_bytes,
                'checkin_time': row.checkin_time,
                # run_uuids will be set later
                'run_uuids': [],
                'dependencies': row.dependencies
                and self._deserialize_dependencies(row.dependencies),
                'shared_file_system': row.shared_file_system,
                'tag_exclusive': row.tag_exclusive,
                'exit_after_num_runs': row.exit_after_num_runs,
                'is_terminating': row.is_terminating,
                'preemptible': row.preemptible,
            }
            for row in worker_rows
        }
        for row in worker_run_rows:
            worker_dict[(row.user_id, row.worker_id)]['run_uuids'].append(row.run_uuid)
        return list(worker_dict.values())

    def update_workers(self, user_id, worker_id, update):
        """
        Update the designated worker with columns and values
        :param user_id: a user_id indicating whom a worker belongs to
        :param worker_id: a worker's identification number
        :param update: a dictionary of (key, value) pairs that specifies the columns and the values to update
        """
        if not update:
            return

        with self._engine.begin() as conn:
            existing_row = conn.execute(
                cl_worker.select().where(
                    and_(cl_worker.c.user_id == user_id, cl_worker.c.worker_id == worker_id)
                )
            ).fetchone()

            if existing_row:
                conn.execute(
                    cl_worker.update()
                    .where(and_(cl_worker.c.user_id == user_id, cl_worker.c.worker_id == worker_id))
                    .values(update)
                )
    
    def _connect(self, worker_id, timeout_secs):
        with connect(f"{self._ws_server}/server/connect/{worker_id}", open_timeout=timeout_secs, close_timeout=timeout_secs) as websocket:
            try:
                socket_id = websocket.recv()
            except:
                socket_id = None
        return socket_id

    def connect_to_ws(self, worker_id, timeout_secs=5):
        """
        Loop until connection achieved.
        """
        socket_id = None
        start_time = time.time()
        while time.time() - start_time < timeout_secs:
            socket_id = self._connect(worker_id, timeout_secs)
            if socket_id: 
                break
            else:
                logging.error(f"No sockets available for worker {worker_id}; retrying")
                time.sleep(0.5)
        if not socket_id: logging.error("No connection reached")
        return socket_id

    def disconnect(self, worker_id, socket_id, timeout_secs=5):
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
        try:
            with connect(f"{self._ws_server}/send/{worker_id}/{socket_id}", open_timeout=timeout_secs, close_timeout=timeout_secs) as websocket:
                if is_json:
                    websocket.send(json.dumps(data).encode())
                    return True
                else:
                    while True:
                        chunk = data.read(CHUNK_SIZE)
                        if not chunk: break
                        websocket.send(chunk)
                    return True
        except Exception as e:
            logger.error(f"Send to worker {worker_id} through socket {socket_id} failed with {e}")
        return False

    def recv(self, worker_id, socket_id, timeout_secs=60, is_json=True):
        """
        Receive data from the worker.

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
        try:
            with connect(f"{self._ws_server}/recv/{worker_id}/{socket_id}", open_timeout=timeout_secs, close_timeout=timeout_secs) as websocket:
                if is_json:
                    data = websocket.recv()
                    return json.loads(data.decode())
                else:
                    while True:
                        try:
                            chunk = websocket.recv()
                            if not chunk: break
                            yield chunk
                        except websockets.exceptions.ConnectionClosed: break
        except Exception as e:
            logger.error(f"Recv from worker {worker_id} through socket {socket_id} failed with {e}")

    def connect_and_send(self, data, worker_id, timeout_secs=60, is_json=True):
        """
        Convenience method to connect to worker socket, send, and then disconnect.
        """
        socket_id = self.connect_to_ws(worker_id)
        sent = self.send(data, worker_id, socket_id, timeout_secs, is_json)
        self.disconnect(worker_id, socket_id)
        return sent