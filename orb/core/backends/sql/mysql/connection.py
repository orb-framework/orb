""" Defines the backend connection class for MySQL databases. """

import datetime
import logging
import os
import orb
import re
import traceback

from orb import errors
from projex.text import nativestring as nstr

from ..abstractconnection import SQLConnection
from ..abstractsql import SQL

log = logging.getLogger(__name__)

try:
    import pymysql
except ImportError:
    log.debug('For MySQL backend, download the PyMySQL module')
    pymysql = None

# ----------------------------------------------------------------------


# noinspection PyAbstractClass
class MySQLConnection(SQLConnection):
    """ 
    Creates a MySQL backend connection type for handling database
    connections to MySQL databases.
    """

    # ----------------------------------------------------------------------
    # PROTECTED METHODS
    # ----------------------------------------------------------------------
    def _execute(self, command, data=None, autoCommit=True, autoClose=True,
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
        rowcount = 0
        if data is None:
            data = {}

        # when in debug mode, simply log the command to the log
        elif self.database().commandsBlocked():
            log.info(command)
            return [], rowcount

        # create a new cursor for this transaction
        db = self.nativeConnection()
        if db is None:
            raise errors.ConnectionLost()

        with db.cursor() as cursor:
            start = datetime.datetime.now()

            try:
                cursor.execute(command, data)
                rowcount = cursor.rowcount

            # # look for a cancelled query
            # except QueryCanceledError as cancelled:
            #     try:
            #         db.rollback()
            #     except StandardError as err:
            #         log.error('Rollback error: {0}'.format(err))
            #     log.critical(command)
            #     if data:
            #         log.critical(str(data))
            #
            #     # raise more useful errors
            #     if 'statement timeout' in str(cancelled):
            #         raise errors.QueryTimeout(command, (datetime.datetime.now() - start).total_seconds())
            #     else:
            #         raise errors.Interruption()

            # look for a disconnection error
            except pymysql.InterfaceError:
                raise errors.ConnectionLost()

            # look for integrity errors
            except (pymysql.IntegrityError, pymysql.OperationalError), err:
                try:
                    db.rollback()
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
                        raise errors.DuplicateEntryFound(msg)
                    else:
                        raise errors.DuplicateEntryFound(duplicate_error.group())

                # look for a reference error
                reference_error = re.search('Key .* is still referenced from table ".*"', nstr(err))
                if reference_error:
                    msg = 'Cannot remove this record, it is still being referenced.'
                    raise errors.CannotDelete(msg)

                # unknown error
                log.debug(traceback.print_exc())
                raise errors.QueryFailed(command, data, nstr(err))

            # connection has closed underneath the hood
            except pymysql.Error, err:
                try:
                    db.rollback()
                except StandardError:
                    pass

                log.error(traceback.print_exc())
                raise errors.QueryFailed(command, data, nstr(err))

            try:
                results = [mapper(record) for record in cursor.fetchall()]
            except pymysql.ProgrammingError:
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
        if not pymysql:
            raise errors.BackendNotFound('PyMySQL is not installed.')

        dbname = db.databaseName()
        user = db.username()
        pword = db.password()
        host = db.host()
        if not host:
            host = 'localhost'

        port = db.port()
        if not port:
            port = 3306

        # create the python connection
        try:
            return pymysql.connect(db=dbname,
                                  user=user,
                                  passwd=pword,
                                  host=host,
                                  port=port,
                                  cursorclass=pymysql.cursors.DictCursor)
        except pymysql.OperationalError, err:
            log.error(err)
            raise errors.ConnectionFailed('Failed to connect to MySQL', db)

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

    # ----------------------------------------------------------------------

    @classmethod
    def sql(cls, code=''):
        """
        Returns the statement interface for this connection.
        
        :param      code | <str>
        
        :return     subclass of <orb.core.backends.sql.SQLStatement>
        """
        if code:
            return SQL.byName('MySQL').byName(code)
        else:
            return SQL.byName('MySQL')

# register the mysql backend
if pymysql:
    orb.Connection.registerAddon('MySQL', MySQLConnection)

