""" Defines the backend connection class for PostgreSQL databases. """

import logging
import orb
import re
import traceback

from projex.text import nativestring as nstr

from ..sqlconnection import SQLConnection
from ..sqlstatement import SQLStatement

log = logging.getLogger(__name__)

try:
    import pymysql
except ImportError:
    log.debug('For MySQL backend, download the PyMySQL module')
    pymysql = None

# ----------------------------------------------------------------------

class MySQLStatement(SQLStatement):
    pass


# noinspection PyAbstractClass
class MySQLConnection(SQLConnection):
    """
    Creates a PostgreSQL backend connection type for handling database
    connections to PostgreSQL databases.
    """

    # ----------------------------------------------------------------------
    # PROTECTED METHODS
    # ----------------------------------------------------------------------
    def _closed(self, native):
        return not bool(native.open)

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

        :return     [{<str> key: <variant>, ..}, ..], <int> count
        """
        if data is None:
            data = {}

        with native.cursor() as cursor:
            log.debug('***********************')
            log.debug(command % data)
            log.debug('***********************')

            try:
                rowcount = 0
                for cmd in command.split(';'):
                    cmd = cmd.strip()
                    if cmd:
                        cursor.execute(cmd.strip(';') + ';', data)
                        rowcount += cursor.rowcount

            # look for a disconnection error
            except pymysql.InterfaceError:
                raise orb.errors.ConnectionLost()

            # look for integrity errors
            except (pymysql.IntegrityError, pymysql.OperationalError) as err:
                native.rollback()

                # look for a duplicate error
                if err[0] == 1062:
                    raise orb.errors.DuplicateEntryFound(err[1])

                # look for a reference error
                reference_error = re.search('Key .* is still referenced from table ".*"', nstr(err))
                if reference_error:
                    msg = 'Cannot remove this record, it is still being referenced.'
                    raise orb.errors.CannotDelete(msg)

                # unknown error
                log.debug(traceback.print_exc())
                raise orb.errors.QueryFailed(command, data, nstr(err))

            # connection has closed underneath the hood
            except pymysql.Error as err:
                native.rollback()
                log.error(traceback.print_exc())
                raise orb.errors.QueryFailed(command, data, nstr(err))

            try:
                raw = cursor.fetchall()
                results = [mapper(record) for record in raw]
            except pymysql.ProgrammingError:
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
        if not pymysql:
            raise orb.errors.BackendNotFound('psycopg2 is not installed.')

        # create the python connection
        try:
            return pymysql.connect(db=db.name(),
                                   user=db.username(),
                                   passwd=db.password(),
                                   host=(db.writeHost() if writeAccess else db.host()) or 'localhost',
                                   port=db.port() or 3306,
                                   cursorclass=pymysql.cursors.DictCursor)
        except pymysql.OperationalError as err:
            log.exception('Failed to connect to postgres')
            raise orb.errors.ConnectionFailed()

    def _interrupt(self, threadId, connection):
        """
        Interrupts the given native connection from a separate thread.

        :param      threadId   | <int>
                    connection | <variant> | backend specific database.
        """
        try:
            connection.close()
        except pymysql.Error:
            pass

    def schemaInfo(self, context):
        info = super(MySQLConnection, self).schemaInfo(context)
        for v in info.values():
            v['fields'] = v['fields'].split(',')
            v['indexes'] = v['indexes'].split(',')
        return info

    # ----------------------------------------------------------------------

    @classmethod
    def statement(cls, code=''):
        """
        Returns the statement interface for this connection.

        :param      code | <str>

        :return     subclass of <orb.core.backends.sql.SQLStatement>
        """
        return MySQLStatement.byName(code) if code else MySQLStatement


# register the postgres backend
if pymysql:
    orb.Connection.registerAddon('MySQL', MySQLConnection)

