import datetime
import demandimport
import os
import logging

from jinja2 import PackageLoader
from ..connection import SQLConnection

with demandimport.enabled():
    import psycopg2 as pg

log = logging.getLogger(__name__)


class PostgresConnection(SQLConnection):
    __plugin_name__ = 'Postgres'
    __default_namespace__ = 'public'
    __templates__ = PackageLoader('orb.std.connections.sql.postgres', 'templates')

    def commit_native_connection(self, native_connection):
        """
        Implements the SQLConnection.commit_native_connection abstract method.

        :param native_connection: <psycopg2.Connection>
        """
        native_connection.commit()

    def close_native_connection(self, native_connection):
        """
        Implements the SQLConnection.close_native_connection abstract method.

        :param native_connection: <psycopg2.Connection>
        """
        native_connection.close()

    def execute_native_command(self,
                               native_connection,
                               command,
                               data=None,
                               returning=True,
                               mapper=dict):
        """
        Executes the inputted command into the current connection cursor.

        :param native_connection: <pscyopg2.Connection>
        :param command: <unicode>
        :param data: <dict> or None
        :param returning: <bool>
        :param mapper: <callable>

        :return: [<variant> record, ..], <int> row count
        """
        from psycopg2 import extras as pg_extras
        from psycopg2 import extensions as pg_ext

        if data is None:
            data = {}

        cursor = native_connection.cursor(cursor_factory=pg_extras.DictCursor)

        # register the hstore option
        try:
            pg_extras.register_hstore(cursor, unicode=True)
        except pg.ProgrammingError:
            log.warning('HSTORE is not supported in this version of Postgres!')

        # register the json option
        try:
            pg_extras.register_json(cursor)
        except pg.ProgrammingError:
            log.warning('JSON is not supported in this version of Postgres!')

        start = datetime.datetime.now()

        log.debug(command % data)

        try:
            cursor.execute(command, data)
            rowcount = cursor.rowcount

        # look for a cancelled query
        except pg_ext.QueryCanceledError as cancelled:
            try:
                native_connection.rollback()
            except Exception as err:
                log.error('Rollback error: {0}'.format(err))
            log.critical(command)
            if data:
                log.critical(str(data))

            # raise more useful errors
            if 'statement timeout' in str(cancelled):
                raise orb.errors.QueryTimeout(command, (datetime.datetime.now() - start).total_seconds())
            else:
                raise orb.errors.Interruption()

        # look for a disconnection error
        except pg.InterfaceError:
            raise orb.errors.ConnectionLost()

        # look for integrity errors
        except (pg.IntegrityError, pg.OperationalError) as err:
            try:
                native_connection.rollback()
            except Exception:
                pass

            # look for a duplicate error
            duplicate_error = re.search('Key (.*) already exists.', nstr(err))
            if duplicate_error:
                key = duplicate_error.group(1)
                result = re.match('^\(lower\((?P<column>[^\)]+)::text\)\)=\((?P<value>[^\)]+)\)$', key)
                if not result:
                    result = re.match('^(?P<column>\w+)=(?P<value>\w+)', key)

                if result:
                    msg = '{value} is already being used.'.format(**result.groupdict())
                    raise orb.errors.DuplicateEntryFound(msg)
                else:
                    raise orb.errors.DuplicateEntryFound(duplicate_error.group())

            # look for a reference error
            reference_error = re.search('Key .* is still referenced from table ".*"', nstr(err))
            if reference_error:
                msg = 'Cannot remove this record, it is still being referenced.'
                raise orb.errors.CannotDelete(msg)

            # unknown error
            log.debug(traceback.print_exc())
            raise orb.errors.QueryFailed(command, data, nstr(err))

        # connection has closed underneath the hood
        except (pg.Error, pg.ProgrammingError) as err:
            try:
                native_connection.rollback()
            except Exception:
                pass

            log.error(traceback.print_exc())
            raise orb.errors.QueryFailed(command, data, nstr(err))

        try:
            results = [mapper(record) for record in cursor.fetchall()]
        except pg.ProgrammingError:
            results = []

        return results, rowcount

    def interrupt_native_connection(self, native_connection):
        """
        Implements the SQLConnection.interrupt_native_connection abstract method.

        :param native_connection: <psycopg2.Connection>
        """
        try:
            native_connection.cancel()
        except pg.Error:
            pass

    def is_native_connection_closed(self, native_connection):
        """
        Implements the SQLConnection.rollback_native_connection abstract method.

        :param native_connection: <psycopg2.Connection>
        """
        native_connection.rollback()

    def open_native_connection(self, write_access=False):
        """
        Implements the SQLConnection.open_native_connection abstract method.
        """
        db = self.database()

        # setup the default timeout information
        if db.timeout():
            os.environ['PGOPTIONS'] = '-c statement_timeout={0}'.format(db.timeout())

        # create the python connection
        try:
            conn = pg.connect(database=db.name(),
                              user=db.username(),
                              password=db.password(),
                              host=db.write_host() if write_access else db.host(),
                              port=db.port(),
                              connect_timeout=3)
        except pg.OperationalError as err:
            log.exception('Failed to connect to postgres')
            raise orb.errors.ConnectionFailed()
        else:
            return conn

    def rollback_native_connection(self, native_connection):
        """
        Implements the SQLConnection.rollback_native_connection abstract method.

        :param native_connection: <psycopg2.Connection>
        """
        native_connection.rollback()

