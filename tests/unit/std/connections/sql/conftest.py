from __future__ import print_function
import pytest


@pytest.fixture()
def sql_equals():
    def _sql_equals(a, b, data=None):
        data = data or {}
        a = a % data

        print(a)

        normal_a = a.replace('\n', '').replace(' ', '')
        normal_b = b.replace('\n', '').replace(' ', '')
        return normal_a == normal_b
    return _sql_equals


@pytest.fixture()
def MockSQLConnection():
    from orb.std.connections.sql.connection import SQLConnection

    class MockSQLConnection(SQLConnection):
        def __init__(self, database, processors=None):
            super(MockSQLConnection, self).__init__(database)
            self.__processors = processors or {}

        def run(self, cmd, *args, **kw):
            default = kw.pop('default', None)

            try:
                method = self.__processors[cmd]
            except KeyError:
                return default
            else:
                return method(*args, **kw)

        def close_native_connection(self, native_connection):
            return self.run('close_native_connection', native_connection)

        def commit_native_connection(self, native_connection):
            return self.run('commit_native_connection', native_connection)

        def execute_native_command(self,
                                   native_connection,
                                   command,
                                   payload=None,
                                   returning=True,
                                   mapper=dict):
            return self.run('execute_native_command',
                            native_connection,
                            command,
                            payload=payload,
                            returning=returning,
                            mapper=mapper)

        def interrupt_native_connection(self, native_connection):
            return self.run('interrupt_native_connection', native_connection)

        def open_native_connection(self, write_access=False):
            return self.run('open_native_connection', write_access=write_access)

        def is_native_connection_closed(self, native_connection):
            return self.run('is_native_connection_closed', native_connection)

        def rollback_native_connection(self, native_connection):
            return self.run('rollback_native_connection', native_connection)

    return MockSQLConnection

@pytest.fixture()
def mock_sql_conn(MockSQLConnection, mock_db):
    def _mock_sql_conn(processors=None, responses=None):
        db = mock_db(responses=responses)
        conn = MockSQLConnection(db, processors=processors)
        return conn
    return _mock_sql_conn