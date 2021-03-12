from contextlib import closing
import datetime
import json
import logging
import os
import socket
import time

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

    def __init__(self, engine, socket_dir):
        self._engine = engine
        self._socket_dir = socket_dir

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
                socket_id = existing_row.socket_id
                conn.execute(
                    cl_worker.update()
                    .where(and_(cl_worker.c.user_id == user_id, cl_worker.c.worker_id == worker_id))
                    .values(worker_row)
                )
            else:
                socket_id = self.allocate_socket(user_id, worker_id, conn)
                worker_row.update(
                    {'user_id': user_id, 'worker_id': worker_id, 'socket_id': socket_id}
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

        return socket_id

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
            socket_rows = conn.execute(
                cl_worker_socket.select().where(
                    and_(
                        cl_worker_socket.c.user_id == user_id,
                        cl_worker_socket.c.worker_id == worker_id,
                    )
                )
            ).fetchall()
            for socket_row in socket_rows:
                self._cleanup_socket(socket_row.socket_id)
            conn.execute(
                cl_worker_socket.delete().where(
                    and_(
                        cl_worker_socket.c.user_id == user_id,
                        cl_worker_socket.c.worker_id == worker_id,
                    )
                )
            )
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
                'socket_id': row.socket_id,
                # run_uuids will be set later
                'run_uuids': [],
                'dependencies': row.dependencies
                and self._deserialize_dependencies(row.dependencies),
                'shared_file_system': row.shared_file_system,
                'tag_exclusive': row.tag_exclusive,
                'exit_after_num_runs': row.exit_after_num_runs,
                'is_terminating': row.is_terminating,
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

    def allocate_socket(self, user_id, worker_id, conn=None):
        """
        Allocates a unique socket ID.
        """

        def do(conn):
            socket_row = {'user_id': user_id, 'worker_id': worker_id}
            return conn.execute(cl_worker_socket.insert().values(socket_row)).inserted_primary_key[
                0
            ]

        if conn is None:
            with self._engine.begin() as conn:
                return do(conn)
        else:
            return do(conn)

    def deallocate_socket(self, socket_id):
        """
        Cleans up the socket, removing the associated file in the socket
        directory.
        """
        self._cleanup_socket(socket_id)
        with self._engine.begin() as conn:
            conn.execute(cl_worker_socket.delete().where(cl_worker_socket.c.socket_id == socket_id))

    def _socket_path(self, socket_id):
        return os.path.join(self._socket_dir, str(socket_id))

    def _cleanup_socket(self, socket_id):
        try:
            os.remove(self._socket_path(socket_id))
        except OSError:
            pass

    def start_listening(self, socket_id):
        """
        Returns a Python socket object that can be used to accept connections on
        the socket with the given ID. This object should be passed to the
        get_ methods below. as in:

            with closing(worker_model.start_listening(socket_id)) as sock:
                message = worker_model.get_json_message(sock, timeout_secs)
        """
        self._cleanup_socket(socket_id)
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.bind(self._socket_path(socket_id))
        sock.listen(0)
        return sock

    ACK = b'a'

    def get_stream(self, sock, timeout_secs):
        """
        Receives a single message on the given socket and returns a file-like
        object that can be used for streaming the message data.

        If no messages are received within timeout_secs seconds, returns None.
        """
        sock.settimeout(timeout_secs)
        try:
            conn, _ = sock.accept()
            # Send Ack. This helps protect from messages to the worker being
            # lost due to spuriously accepted connections when the socket
            # file is deleted.
            conn.sendall(WorkerModel.ACK)
            conn.settimeout(None)  # Need to remove timeout before makefile.
            fileobj = conn.makefile('rb')
            conn.close()
            return fileobj
        except socket.timeout:
            return None

    def get_json_message(self, sock, timeout_secs):
        """
        Receives a single message on the given socket and returns the message
        data parsed as JSON.

        If no messages are received within timeout_secs seconds, returns None.
        """
        fileobj = self.get_stream(sock, timeout_secs)

        if fileobj is None:
            return None

        with closing(fileobj):
            return json.loads(fileobj.read().decode())

    def send_stream(self, socket_id, fileobj, timeout_secs):
        """
        Streams the given file-like object to the given socket.

        If nothing accepts a connection on the socket for more than timeout_secs,
        return False. Otherwise, returns True.
        """
        start_time = time.time()
        while time.time() - start_time < timeout_secs:
            with closing(socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)) as sock:
                sock.settimeout(timeout_secs)

                success = False
                try:
                    sock.connect(self._socket_path(socket_id))
                    success = sock.recv(len(WorkerModel.ACK)) == WorkerModel.ACK
                except socket.error:
                    pass

                if not success:
                    # Shouldn't be too expensive just to keep retrying.
                    time.sleep(0.003)
                    continue

                while True:
                    data = fileobj.read(4096)
                    if not data:
                        return True
                    sock.sendall(data)

        return False

    def send_json_message(self, socket_id, message, timeout_secs, autoretry=True):
        """
        Sends a JSON message to the given socket, retrying until it is received
        correctly.

        If the message is not sent successfully after timeout_secs, return
        False. Otherwise, returns True.

        Note, only the worker should call this method with autoretry set to
        False. See comments below.
        """
        start_time = time.time()
        while time.time() - start_time < timeout_secs:
            with closing(socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)) as sock:
                sock.settimeout(timeout_secs)

                success = False
                try:
                    sock.connect(self._socket_path(socket_id))
                    if autoretry:
                        # This auto retry mechanisms helps ensure that messages
                        # sent to a worker are received more reliably. The
                        # socket API isn't particularly robust to our usage
                        # where we continuously start and stop listening on a
                        # socket, like the worker checkin mechanism does. In
                        # fact, it seems to spuriously accept connections
                        # just when a socket object is in the process of being
                        # destroyed. On the sending end, such a scenario results
                        # in a "Broken pipe" exception, which we catch here.
                        success = sock.recv(len(WorkerModel.ACK)) == WorkerModel.ACK
                    else:
                        success = True
                except socket.error:
                    pass

                if not success:
                    # Shouldn't be too expensive just to keep retrying.
                    # TODO: maybe exponential backoff
                    time.sleep(
                        0.3
                    )  # changed from 0.003 to keep from rate-limiting due to dead workers
                    continue

                if not autoretry:
                    # When messages are being sent from the worker, we don't
                    # have the problem with "Broken pipe" as above, since
                    # code waiting for a reply shouldn't just abruptly stop
                    # listening.
                    precondition(
                        sock.recv(len(WorkerModel.ACK)) == WorkerModel.ACK,
                        'Received invalid ack on socket.',
                    )

                sock.sendall(json.dumps(message).encode())
                return True

        return False

    def has_reply_permission(self, user_id, worker_id, socket_id):
        """
        Checks whether the given user running a worker with the given ID can
        reply on the socket with the given ID. Used to prevent a user from
        impersonating a worker from another user and replying to its messages.
        """
        with self._engine.begin() as conn:
            row = conn.execute(
                cl_worker_socket.select().where(
                    and_(
                        cl_worker_socket.c.user_id == user_id,
                        cl_worker_socket.c.worker_id == worker_id,
                        cl_worker_socket.c.socket_id == socket_id,
                    )
                )
            ).fetchone()
            if row:
                return True
            return False
