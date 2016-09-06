""" Defines the backend connection class for PostgreSQL databases. """

import datetime
import logging
import os
import orb
import re
import traceback

from projex.text import nativestring as nstr

from ..sqlconnection import SQLConnection
from ..sqlstatement import SQLStatement

log = logging.getLogger(__name__)

try:
    import psycopg2 as pg
    import psycopg2.extensions as pg_ext

    from psycopg2.extras import DictCursor, register_hstore, register_json

except ImportError:
    log.debug('For PostgreSQL backend, download the psycopg2 module')

    DictCursor = None
    register_hstore = None
    register_json = None
    pg = None
    pg_ext = None

else:
    # ensure that psycopg2 uses unicode for all the database strings
    pg_ext.register_type(pg_ext.UNICODE)
    pg_ext.register_type(pg_ext.UNICODEARRAY)


# ----------------------------------------------------------------------

class PSQLStatement(SQLStatement):
    pass


# noinspection PyAbstractClass
class PSQLConnection(SQLConnection):
    """ 
    Creates a PostgreSQL backend connection type for handling database
    connections to PostgreSQL databases.
    """

    # ----------------------------------------------------------------------
    # PROTECTED METHODS
    # ----------------------------------------------------------------------
    def _execute(self,
                 native,
                 command,
                 data=None,
                 returning=True,
                 mapper=dict):
        """
        Executes the inputted command into the current \
        connection cursor.
        
        :param      command    | <str>
                    data       | <dict> || None
                    autoCommit | <bool> | commit database changes immediately
                    autoClose  | <bool> | closes connections immediately
        
        :return     [{<str> key: <variant>, ..}, ..], <int> count
        """
        if data is None:
            data = {}

        cursor = native.cursor(cursor_factory=DictCursor)

        # register the hstore option
        try:
            register_hstore(cursor, unicode=True)
        except pg.ProgrammingError:
            log.warning('HSTORE is not supported in this version of Postgres!')

        # register the json option
        try:
            register_json(cursor)
        except pg.ProgrammingError:
            log.warning('JSON is not supported in this version of Postgres!')

        start = datetime.datetime.now()

        log.debug('***********************')
        log.debug(command % data)
        log.debug('***********************')

        try:
            cursor.execute(command, data)
            rowcount = cursor.rowcount

        # look for a cancelled query
        except pg_ext.QueryCanceledError as cancelled:
            try:
                native.rollback()
            except StandardError as err:
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
                native.rollback()
            except StandardError:
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
                native.rollback()
            except StandardError:
                pass

            log.error(traceback.print_exc())
            raise orb.errors.QueryFailed(command, data, nstr(err))

        try:
            results = [mapper(record) for record in cursor.fetchall()]
        except pg.ProgrammingError:
            results = []

        return results, rowcount

    def _open(self, db, writeAccess=False):
        """
        Handles simple, SQL specific connection creation.  This will not
        have to manage thread information as it is already managed within
        the main open method for the SQLBase class.
        
        :param      db | <orb.Database>
        
        :return     <variant> | backend specific database connection
        """
        if not pg:
            raise orb.errors.BackendNotFound('psycopg2 is not installed.')

        if db.timeout():
            os.environ['PGOPTIONS'] = '-c statement_timeout={0}'.format(db.timeout())

        # create the python connection
        try:
            return pg.connect(database=db.name(),
                              user=db.username(),
                              password=db.password(),
                              host=db.writeHost() if writeAccess else db.host(),
                              port=db.port(),
                              connect_timeout=3)
        except pg.OperationalError as err:
            log.exception('Failed to connect to postgres')
            raise orb.errors.ConnectionFailed()

    def _interrupt(self, threadId, connection):
        """
        Interrupts the given native connection from a separate thread.
        
        :param      threadId   | <int>
                    connection | <variant> | backend specific database.
        """
        try:
            connection.cancel()
        except pg.Error:
            pass

    # ----------------------------------------------------------------------

    @classmethod
    def statement(cls, code=''):
        """
        Returns the statement interface for this connection.
        
        :param      code | <str>
        
        :return     subclass of <orb.core.backends.sql.SQLStatement>
        """
        return PSQLStatement.byName(code) if code else PSQLStatement


# register the postgres backend
if pg:
    orb.Connection.registerAddon('Postgres', PSQLConnection)

