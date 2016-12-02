import contextlib

from collections import defaultdict


class SQLConnectionPool(object):
    def __init__(self, connection, max_size=10, use_gevent=False):
        super(SQLConnectionPool, self).__init__()

        # depending on whether or not gevent is to be used, determine
        # which queue instance to use
        if use_gevent:
            from gevent.queue import Queue
        else:
            from Queue import Queue

        # define custom properties
        self.__connection = connection
        self.__max_size = max_size
        self.__pool_size = defaultdict(lambda: 0)
        self.__pool = defaultdict(Queue)

    @contextlib.contextmanager
    def current_connection(self, write_access=False, isolation_level=None):
        """
        Returns the current native connection for this pool, opening a new
        connection if none is already created.

        :param write_access: <bool>
        :param isolation_level: <int> or None
        """
        conn = self.open_connection(write_access=write_access)
        try:
            if isolation_level is not None:
                if conn.isolation_level == isolation_level:
                    isolation_level = None
                else:
                    conn.set_isolation_level(isolation_level)
                yield conn
        except Exception:
            if self.__connection.is_native_connection_closed(conn):
                conn = None
                self.close_connections()
            else:
                conn = self.__connection.rollback_native_connection(conn)
            raise
        else:
            if not self.__connection.is_native_connection_closed(conn):
                self.__connection.commit_native_connection(conn)
        finally:
            if conn is not None and not self.__connection.is_native_connection_closed(conn):
                if isolation_level is not None:
                    conn.set_isolation_level(isolation_level)
                self.__pool[write_access].put(conn)

    def close_connections(self):
        """
        Closes the connections in the pool.
        """
        for pool in self.__pool.values():
            while not pool.empty():
                native_connection = pool.get_nowait()
                try:
                    self.__connection.close_native_connection(native_connection)
                except Exception:
                    pass

        # reset the pool size after closing all connections
        self.__pool_size.clear()

    def has_connections(self):
        """
        Returns whether or not this pool has any active connections.

        :return: <bool>
        """
        for pool in self.__pool.values():
            if not pool.empty():
                return True
        else:
            return False

    def open_connection(self, write_access=False):
        """
        Returns a pooled connection, or opens a new one to the given host.

        :param host: <str>

        :return: <variant>
        """
        pool = self.__pool[write_access]

        # wait for a connection to become available if one already exists,
        # or we have hit the maximum per-host limit
        if self.__pool_size[write_access] >= self.__max_size or pool.qsize():
            return pool.get()
        else:
            self.__pool_size[write_access] += 1
            try:
                conn = self.__connection.open_native_connection(write_access=write_access)
            except Exception:
                self.__pool_size[write_access] -= 1
                raise
            else:
                event = orb.events.ConnectionEvent(success=conn is not None, native=conn)
                orb.Database.connected.send(self.__connection.database(), event=event)
                return conn