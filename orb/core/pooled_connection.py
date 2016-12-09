import demandimport
import sys

from abc import abstractmethod
from .connection import Connection
from .connection_pool import ConnectionPool

with demandimport.enabled():
    import orb


class PooledConnection(Connection):
    def __init__(self, database, pool_size=None, use_gevent=None):
        super(PooledConnection, self).__init__(database)

        # create custom properties
        if use_gevent is None:
            use_gevent = orb.system.settings.worker_class == 'gevent'
        if pool_size is None:
            pool_size = int(orb.system.settings.max_connections)

        self.__use_gevent = use_gevent
        self.__pool = ConnectionPool(
            self,
            max_size=pool_size,
            use_gevent=use_gevent
        )

    @abstractmethod
    def close_native_connection(self, native_connection):
        """
        Closes the native connection.

        Args:
            native_connection: <variant>

        """
        pass

    @abstractmethod
    def commit_native_connection(self, native_connection):
        """
        Commits the changes to the native connection.

        :param native_connection: <variant>
        """
        return True

    def close(self):
        """
        Closes all open connections to the SQL database.
        """
        self.__pool.close_connections()

    def commit(self):
        """
        Commits the changes to the current database connection.

        :return: <bool> success
        """
        with self.__pool.current_connection(write_access=True) as conn:
            if not self.is_native_connection_closed(conn):
                return self.commit_native_connection(conn)
            else:  # pragma: no cover
                return False

    @abstractmethod
    def execute_native_command(self,
                               native_connection,
                               command,
                               payload=None,
                               returning=True,
                               mapper=dict):
        """
        Executes a native string command on the given native connection.

        :param native_connection: <variant>
        :param command: <str> or <unicode>
        :param payload: None or <dict>
        :param returning: <bool>
        :param mapper: None or <callable>
        """
        return None, 0

    def is_connected(self):
        """
        Returns whether or not this connection is currently
        active.

        :return: <bool>
        """
        return self.__pool.has_connections()

    @abstractmethod
    def interrupt_native_connection(self, native_connection):
        """
        Interrupts the native connection.

        :param native_connection: <variant>
        """
        pass

    @abstractmethod
    def open_native_connection(self, write_access=False):
        """
        Opens a new native connection for this SQL database.

        :param write_access: <bool>

        :return: <variant>
        """
        pass

    @abstractmethod
    def is_native_connection_closed(self, native_connection):
        """
        Returns whether or not the native SQL connection is closed.

        :param native_connection: <variant>

        :return: <bool>
        """
        return False

    def open(self, write_access=False):
        """
        Opens a new connection to the database.

        :param write_access: <bool>

        :return: <variant>
        """
        return self.__pool.open_connection(write_access=write_access)

    def pool(self):
        """
        Returns the connection pool associated with this instance.

        Returns:
            <orb.ConnectionPool>

        """
        return self.__pool

    def rollback(self):
        """
        Rolls back changes to this database.

        :return: <bool> success
        """
        with self.__pool.current_connection(write_access=True) as conn:
            try:
                self.rollback_native_connection(conn)
            except Exception:
                if self.__use_gevent:  # pragma: no cover
                    import gevent
                    gevent.get_hub().handle_error(conn, *sys.exc_info)
                return False
            else:
                return True

    @abstractmethod
    def rollback_native_connection(self, native_connection):
        """
        Rolls back the the native connection's changes.

        :param native_connection: <variant>
        """
        pass
