from copy import deepcopy
import os
import sqlite3

from codalab.lib.common import get_codalab_home, read_json_or_die, write_pretty_json


class CodaLabManagerState:
    """
    This is an abstract class representing the current state of a
    CodaLabManager. Broadly, it stores three components in its state:

    1. The authentication token(s) for any instances that the user has logged
       into from the CLI.
    2. Information about any sessions that the user has created. For now, this
       is the session's current worksheet uuid and the current CodaLab
       instance the session is connected to.
    3. A timestamp representing the last known time that _any_ CodaLabManager
       using this state file checked whether the current CodaLab version is
       older than the CodaLab version running on the instance.
    """

    def __init__(self, temporary):
        self.temporary = temporary
        # Read the state, creating it if it doesn't exist.
        self.initialize_state()

    def state_path(self):
        raise NotImplementedError

    def get_auth(self, server, default={}):
        raise NotImplementedError

    def set_auth(
        self, server, access_token, expires_at, refresh_token, scope, token_type, username
    ):
        raise NotImplementedError

    def delete_auth(self, server):
        raise NotImplementedError

    def get_session(self, name, default={}):
        raise NotImplementedError

    def set_session(self, name, address, worksheet_uuid):
        raise NotImplementedError

    def get_last_check_version_datetime(self, default=None):
        raise NotImplementedError

    def set_last_check_version_datetime(self, timestamp):
        raise NotImplementedError


class CodaLabManagerJsonState(CodaLabManagerState):
    """
    The CodaLabManagerJsonState stores the current state of a CodaLabManager as
    a JSON file that is read and updated on disk. The state is structured as a
    nested dictionary with three top-level keys:

    1. ``"auth"`` (Dict[str, Union[Dict[str, str], str]]) maps server
       addresses to authentication information. The authentication information
       takes the form of (1) a string key ``"token_info"`` with fields that represent
       the details of an authentication token and (2) a string key ``username``
       that stores a string username.
    2. ``"sessions"`` (Dict[Dict[str, str]]) maps session IDs to information about each
       session. Specifically, it stores a string "address" that represents the session's current
       CodaLab server address, and a string "worksheet_uuid" that represents the session's
       current worksheet uuid.
    3. ``"last_check_version_datetime"`` (str) stores a timestamp of the last time that a
       CodaLabManager compared the installed version with the version on the CodaLab server.
    """

    def initialize_state(self):
        if self.temporary:
            self.state = {'auth': {}, 'sessions': {}}
            return
        # Read state file, creating if it doesn't exist.
        if not os.path.exists(self.state_path):
            write_pretty_json(
                {
                    'auth': {},  # address -> {username, auth_token}
                    'sessions': {},  # session_name -> {address, worksheet_uuid}
                },
                self.state_path,
            )
        self.state = read_json_or_die(self.state_path)

    @property
    def state_path(self):
        return os.getenv('CODALAB_STATE', os.path.join(get_codalab_home(), 'state.json'))

    def get_auth(self, server, default={}):
        return deepcopy(self.state["auth"].get(server, default))

    def set_auth(
        self, server, access_token, expires_at, refresh_token, scope, token_type, username
    ):
        self.state["auth"][server] = {
            "token_info": {
                "access_token": access_token,
                "expires_at": expires_at,
                "refresh_token": refresh_token,
                "scope": scope,
                "token_type": token_type,
            },
            "username": username,
        }
        self._save_json_state()

    def delete_auth(self, server):
        self.state["auth"].pop(server)
        self._save_json_state()

    def get_session(self, name, default={}):
        return deepcopy(self.state["sessions"].get(name, default))

    def set_session(self, name, address, worksheet_uuid):
        self.state["sessions"][name] = {"address": address, "worksheet_uuid": worksheet_uuid}
        self._save_json_state()

    def get_last_check_version_datetime(self, default=None):
        return deepcopy(self.state.get("last_check_version_datetime", default))

    def set_last_check_version_datetime(self, timestamp):
        self.state["last_check_version_datetime"] = timestamp
        self._save_json_state()

    def _save_json_state(self):
        if self.temporary:
            return
        write_pretty_json(self.state, self.state_path)


class CodaLabManagerSqlite3State(CodaLabManagerState):
    """
    The CodaLabManagerSqlite3State stores the current state of a CodaLabManager as
    a local SQLite database that is read and updated on disk. The state is structured as
    3 distinct tables.

    1. ``"auth"`` maps server addresses to authentication information. The table contains the
       following columns:
           - server TEXT UNIQUE PRIMARY KEY
           - access_token TEXT
           - expires_at REAL
           - refresh_token TEXT
           - scope TEXT
           - token_type TEXT
           - username TEXT

    2. ``"sessions"`` maps session IDs to information about each session. The table contains the
       following columns:
           - name TEXT UNIQUE PRIMARY KEY
           - address TEXT
           - worksheet_uuid TEXT

    3. ``"misc"`` is a key-value store with the following columns:
           - key TEXT UNIQUE PRIMARY KEY
           - value TEXT
        The only key currently stored is "last_check_version_datetime", whose
        value is the timestamp that a CodaLabManager last checked whether the
        locally installed version was out of date.
    """

    def initialize_state(self):
        if self.temporary:
            self.connection = sqlite3.connect(":memory:")
        else:
            # Read state database, creating if it doesn't exist.
            self.connection = sqlite3.connect(self.state_path)
        self.connection.row_factory = sqlite3.Row
        # Create the necessary tables, if they don't exist
        with self.connection:
            c = self.connection.cursor()
            c.execute(
                'CREATE TABLE IF NOT EXISTS auth (server TEXT UNIQUE PRIMARY KEY, access_token TEXT, expires_at REAL, '
                'refresh_token TEXT, scope TEXT, token_type TEXT, username TEXT)'
            )
            c.execute(
                'CREATE TABLE IF NOT EXISTS sessions (name TEXT UNIQUE PRIMARY KEY, address TEXT, worksheet_uuid TEXT)'
            )
            c.execute('CREATE TABLE IF NOT EXISTS misc (key TEXT UNIQUE PRIMARY KEY, value TEXT)')

    @property
    def state_path(self):
        return os.getenv('CODALAB_STATE', os.path.join(get_codalab_home(), 'state.db'))

    def get_auth(self, server, default={}):
        with self.connection:
            c = self.connection.cursor()
            c.execute("SELECT * FROM auth WHERE server = ?", (server,))
            retrieved_auth = c.fetchone()
        if not retrieved_auth:
            return default
        # Format the retrieved authentication details in the same nested
        # format returned by the CodaLabManagerJsonState
        retrieved_auth = dict(retrieved_auth)
        return_auth = {}
        token_info_keys = ["access_token", "expires_at", "refresh_token", "scope", "token_type"]
        # Only add the keys that exist in retrieved_auth, and omit the ones
        # that were not returned. This is for backwards-compatibility with the
        # existing CodaLabManager#_authenticate function.
        token_info = {key: retrieved_auth[key] for key in token_info_keys if key in retrieved_auth}
        return_auth["token_info"] = token_info
        if "username" in retrieved_auth:
            return_auth["username"] = retrieved_auth["username"]
        return return_auth

    def set_auth(
        self, server, access_token, expires_at, refresh_token, scope, token_type, username
    ):
        with self.connection:
            c = self.connection.cursor()
            c.execute(
                "REPLACE INTO auth VALUES (?, ?, ?, ?, ?, ?, ?)",
                (server, access_token, expires_at, refresh_token, scope, token_type, username),
            )

    def delete_auth(self, server):
        with self.connection:
            c = self.connection.cursor()
            c.execute("DELETE FROM auth WHERE server = ?", (server,))

    def get_session(self, name, default={}):
        with self.connection:
            c = self.connection.cursor()
            c.execute("SELECT * FROM sessions WHERE name = ?", (name,))
            retrieved_session = c.fetchone()
        return dict(retrieved_session) if retrieved_session else default

    def set_session(self, name, address, worksheet_uuid):
        with self.connection:
            c = self.connection.cursor()
            c.execute("REPLACE INTO sessions VALUES (?, ?, ?)", (name, address, worksheet_uuid))

    def get_last_check_version_datetime(self, default=None):
        with self.connection:
            c = self.connection.cursor()
            c.execute("SELECT value FROM misc WHERE key = ?", ("last_check_version_datetime",))
            last_check_version_datetime = c.fetchone()
        return last_check_version_datetime["value"] if last_check_version_datetime else default

    def set_last_check_version_datetime(self, timestamp):
        with self.connection:
            c = self.connection.cursor()
            c.execute(
                "REPLACE INTO misc (key, value) VALUES (?, ?)",
                ("last_check_version_datetime", timestamp),
            )

    def __del__(self):
        """
        Clean up the CodaLabManagerState by closing the SQLite connection, if applicable.
        """
        if getattr(self, "connection", None):
            self.connection.close()


codalab_manager_state_types = {
    "json": CodaLabManagerJsonState,
    "sqlite3": CodaLabManagerSqlite3State,
}
