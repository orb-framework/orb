""" Defines the backend connection class for PostgreSQL databases. """

import logging
import orb
import re
import sqlite3 as sqlite
import traceback

from projex.text import nativestring as nstr

from ..sqlconnection import SQLConnection
from ..sqlstatement import SQLStatement

log = logging.getLogger(__name__)

# ----------------------------------------------------------------------

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

# ----------------------------------------------------------------------

class SQLiteStatement(SQLStatement):
    pass


# noinspection PyAbstractClass
class SQLiteConnection(SQLConnection):
    """ 
    Creates a PostgreSQL backend connection type for handling database
    connections to PostgreSQL databases.
    """

    # ----------------------------------------------------------------------
    # PROTECTED METHODS
    # ----------------------------------------------------------------------
    def _execute(self, native, command, data=None, autoCommit=True, autoClose=True,
                 returning=True, mapper=dict):
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

        cursor = native.cursor()

        try:
            cursor.execute(command, data)
            rowcount = cursor.rowcount

        # look for a disconnection error
        except sqlite.InterfaceError:
            raise orb.errors.ConnectionLost()

        # look for integrity errors
        except (sqlite.IntegrityError, sqlite.OperationalError), err:
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
        except sqlite.Error, err:
            try:
                native.rollback()
            except StandardError:
                pass

            log.error(traceback.print_exc())
            raise orb.errors.QueryFailed(command, data, nstr(err))

        try:
            results = [mapper(record) for record in cursor.fetchall()]
        except sqlite.ProgrammingError:
            results = []

        if autoCommit:
            self.commit()

        return results, rowcount

    def _open(self, db):
        """
        Handles simple, SQL specific connection creation.  This will not
        have to manage thread information as it is already managed within
        the main open method for the SQLBase class.
        
        :param      db | <orb.Database>
        
        :return     <variant> | backend specific database connection
        """
        dbname = db.name()

        # create the python connection
        try:
            conn = sqlite.connect(dbname)
            conn.row_factory = dict_factory
            return conn
        except sqlite.OperationalError, err:
            log.error(err)
            raise orb.errors.ConnectionFailed('Failed to connect to SQLite: {0}'.format(dbname))

    def _interrupt(self, threadId, connection):
        """
        Interrupts the given native connection from a separate thread.
        
        :param      threadId   | <int>
                    connection | <variant> | backend specific database.
        """
        try:
            connection.close()
        except sqlite.Error:
            pass

    # ----------------------------------------------------------------------

    @classmethod
    def statement(cls, code=''):
        """
        Returns the statement interface for this connection.
        
        :param      code | <str>
        
        :return     subclass of <orb.core.backends.sql.SQLStatement>
        """
        return SQLiteStatement.byName(code) if code else SQLiteStatement


# register the sqlite backend
orb.Connection.registerAddon('SQLite', SQLiteConnection)

