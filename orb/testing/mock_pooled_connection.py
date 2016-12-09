"""
Defines a mock backend database connection for pooled connection types
"""
import orb

from .mock_connection import MockConnectionMixin


class MockNativeConnection(object):
    def __init__(self, database, write_access=False):
        self.database = database
        self.write_access = write_access
        self.closed = False
        self.isolation_level = None
        self.rolled_back = False

    def close(self):
        """
        Placeholder for any mock close logic.

        Returns:
            <bool>

        """
        return True

    def commit(self):
        """
        Placeholder for any mock commit logic.

        Returns:
            <bool>

        """
        return True

    def execute(self,
                command,
                payload=None,
                returning=True,
                mapper=dict):
        """
        Placeholder for any execution logic.

        Args:
            command: <str>
            payload: <dict> or None
            returning: <bool>
            mapper: <callable>

        Returns:
            <variant> payload, <int> row_count

        """
        return {}, 0

    def interrupt(self):
        """
        Placeholder for any interruption logic.

        Returns:
            <bool>

        """
        return True

    def is_closed(self):
        """
        Placeholder for any closure logic.

        Returns:
            <bool>

        """
        return self.closed

    def rollback(self):
        """
        Placeholder for rollback logic.

        Returns:
            <bool>

        """
        self.rolled_back = True

    def set_isolation_level(self, lvl):
        """
        Mimics the method from a database connection.

        Args:
            lvl: <int>

        """
        self.isolation_level = lvl


class MockPooledConnectionMixin(object):
    __native_class__ = MockNativeConnection

    def close_native_connection(self, native_connection):
        """
        Closes the native connection.

        Args:
            native_connection: <orb.testing.MockNativeConnection>

        Returns:
            <bool>

        """
        assert isinstance(native_connection, MockNativeConnection)
        return native_connection.close()

    def commit_native_connection(self, native_connection):
        """
        Commits the content for this native connection.

        Args:
            native_connection: <orb.testing.MockNativeConnection>

        Returns:
            <bool>

        """
        assert isinstance(native_connection, MockNativeConnection)
        return native_connection.commit()

    def execute_native_command(self,
                               native_connection,
                               command,
                               payload=None,
                               returning=True,
                               mapper=dict):
        """
        Executes the given command for this connection.

        Args:
            native_connection: <orb.testing.MockNativeConnection>
            command: <unicode>
            payload: <dict> or None
            returning: <bool>
            mapper: <callable>

        Returns:
            <variant> payload, <int> count

        """
        assert isinstance(native_connection, MockNativeConnection)
        assert type(command) in (str, unicode)
        assert payload is None or type(payload) == dict
        assert returning in (True, False)
        assert callable(mapper)

        return native_connection.execute(command,
                                         payload=payload,
                                         returning=returning,
                                         mapper=mapper)

    def interrupt_native_connection(self, native_connection):
        """
        Interrupts the native connection.

        Args:
            native_connection: <orb.testing.MockNativeConnection>

        Returns:
            <bool>

        """
        return native_connection.interrupt()

    def open_native_connection(self, write_access=False):
        """
        Opens up a new native connection.

        Args:
            write_access: <bool>

        Returns:
            <orb.testing.MockNativeConnection>

        """
        return self.__native_class__(self.database(), write_access=write_access)

    def is_native_connection_closed(self, native_connection):
        """
        Returns whether or not this connection is currently closed.

        Args:
            native_connection: <orb.testing.MockNativeConnection>

        Returns:
            <bool>

        """
        assert isinstance(native_connection, MockNativeConnection)
        return native_connection.is_closed()

    def rollback_native_connection(self, native_connection):
        """
        Rollsback any changes from this connection.

        Args:
            native_connection:<orb.testing.MockNativeConnection>

        Returns:
            <bool>

        """
        assert isinstance(native_connection, MockNativeConnection)
        return native_connection.rollback()


class MockPooledConnection(MockPooledConnectionMixin,
                           MockConnectionMixin,
                           orb.PooledConnection):
    pass