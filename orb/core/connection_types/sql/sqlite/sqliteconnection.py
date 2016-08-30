""" Defines the backend connection class for PostgreSQL databases. """

import logging
import orb
import re
import threading

from projex.text import nativestring as nstr

from ..sqlconnection import SQLConnection
from ..sqlstatement import SQLStatement

log = logging.getLogger(__name__)

try:
    import sqlite3 as sqlite

except ImportError:
    log.debug('For SQLite backend, ensure your python version supports sqlite3')
    sqlite = None


# -----------------------------------------------------------------------------
#   SQLITE EXTENSIONS
# -----------------------------------------------------------------------------

FORMAT_EXPR = re.compile('(%\(([^\)]+)\)s)')

def matches(expr, item):
    """
    Generates a regular expression function.

    :param      expr | <str>
                item | <str>

    :return     <bool>
    """
    return re.match(expr, item) is not None

def does_not_match(expr, item):
    """
    Generates a negated regular expression function.

    :param      expr | <str>
                item | <str>

    :return     <bool>
    """
    return re.match(expr, item) is None

def dict_factory(cursor, row):
    """
    Converts the cursor information from a SQLite query to a dictionary.

    :param      cursor | <sqlite3.Cursor>
                row    | <sqlite3.Row>

    :return     {<str> column: <variant> value, ..}
    """
    out = {}
    for i, col in enumerate(cursor.description):
        out[col[0]] = row[i]
    return out


# ----------------------------------------------------------------------

class SQLiteStatement(SQLStatement):
    pass


# noinspection PyAbstractClass
class SQLiteConnection(SQLConnection):
    """ 
    Creates a PostgreSQL backend connection type for handling database
    connections to PostgreSQL databases.
    """
    def __init__(self, *args, **kwds):
        super(SQLiteConnection, self).__init__(*args, **kwds)

        self.__threaded_connections = {}

    # ----------------------------------------------------------------------
    # PROTECTED METHODS
    # ----------------------------------------------------------------------
    def _closed(self, native):
        return self.__threaded_connections.get(native) != threading.current_thread().ident

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

        # check to make sure the connection hasn't been reset or lost
        cursor = native.cursor()

        # determine if we're executing multiple statements at once
        commands = [cmd for cmd in command.split(';') if cmd]
        if len(commands) > 1:
            native.isolation_level = 'IMMEDIATE'
            commands.insert(0, 'BEGIN TRANSACTION')
        else:
            native.isolation_level = None

        def _gen_sub_value(val):
            output = []
            replace = []

            for sub_value in val:
                if isinstance(sub_value, (list, tuple, set)):
                    cmd, vals = _gen_sub_value(sub_value)
                    replace.append(cmd)
                    output += vals
                else:
                    replace.append('?')
                    output.append(sub_value)

                return '({0})'.format(','.join(replace)), output

        rowcount = 0
        for cmd in commands:
            if not cmd.endswith(';'):
                cmd += ';'

            # map the dictionary keywords to the param based for sqlite
            # (sqlite requires ordered options vs. keywords)
            args = []
            for grp, key in FORMAT_EXPR.findall(cmd):
                value = data[key]
                if isinstance(value, (list, tuple, set)):
                    replace, values = _gen_sub_value(value)
                    cmd = cmd.replace(grp, replace)
                    args += values
                else:
                    cmd = cmd.replace(grp, '?')
                    args.append(value)


            log.debug('***********************')
            log.debug(command)
            log.debug(args)
            log.debug('***********************')

            try:
                cursor.execute(cmd, tuple(args))

                if cursor.rowcount != -1:
                    rowcount += cursor.rowcount

            # look for a cancelled query
            except sqlite.OperationalError as err:
                if err == 'interrupted':
                    raise orb.errors.Interruption()
                else:
                    log.exception('Unkown query error.')
                    raise orb.errors.QueryFailed(cmd, args, nstr(err))

            # look for duplicate entries
            except sqlite.IntegrityError as err:
                duplicate_error = re.search('UNIQUE constraint failed: (.*)', nstr(err))
                if duplicate_error:
                    result = duplicate_error.group(1)
                    msg = '{value} is already being used.'.format(value=result)
                    raise orb.errors.DuplicateEntryFound(msg)
                else:
                    # unknown error
                    log.exception('Unknown query error.')
                    raise orb.errors.QueryFailed(command, data, nstr(err))

            # look for any error
            except Exception as err:
                log.exception('Unknown query error.')
                raise orb.errors.QueryFailed(cmd, args, nstr(err))

        if returning:
            results = [mapper(record) for record in cursor.fetchall()]
            rowcount = len(results)  # for some reason, rowcount in sqlite3 returns -1 for selects...
        else:
            results = []

        native.isolation_level = None
        native.commit()

        return results, rowcount

    def _open(self, db, writeAccess=False):
        """
        Handles simple, SQL specific connection creation.  This will not
        have to manage thread information as it is already managed within
        the main open method for the SQLBase class.
        
        :param      db | <orb.Database>
        
        :return     <variant> | backend specific database connection
        """
        if not sqlite:
            raise orb.errors.BackendNotFound('sqlite is not installed.')

        dbname = db.name()

        try:
            sqlite_db = sqlite.connect(dbname)
            sqlite_db.create_function('REGEXP', 2, matches)
            sqlite_db.row_factory = dict_factory
            sqlite_db.text_factory = unicode

            self.__threaded_connections[sqlite_db] = threading.current_thread().ident

            return sqlite_db
        except sqlite.Error:
            log.exception('Failed to connect to sqlite')
            raise orb.errors.ConnectionFailed()

    def _interrupt(self, threadId, connection):
        """
        Interrupts the given native connection from a separate thread.
        
        :param      threadId   | <int>
                    connection | <variant> | backend specific database.
        """
        try:
            connection.interrupt()
        except StandardError:
            pass

    def delete(self, records, context):
        count = len(records)
        super(SQLiteConnection, self).delete(records, context)
        return [], count

    def schemaInfo(self, context):
        tables_sql = "select name from sqlite_master where type = 'table';"
        tables = [x['name'] for x in self.execute(tables_sql)[0]]

        output = {}
        for table in tables:
            if table.endswith('_i18n'):
                continue

            columns, _ = self.execute("PRAGMA table_info({table});".format(table=table))
            columns = [c['name'] for c in columns]

            indexes, _ = self.execute("PRAGMA index_list({table});".format(table=table))
            indexes = [i['name'] for i in indexes]

            if (table + '_i18n') in tables:
                i18n_columns, _ = self.execute("PRAGMA table_info({table}_i18n);".format(table=table))
                columns += [c['name'] for c in i18n_columns]

                i18n_indexes, _ = self.execute("PRAGMA index_list({table}_i18n);".format(table=table))
                indexes += [i['name'] for i in i18n_indexes]

            output[table] = {
                'fields': columns,
                'indexes': indexes
            }

        return output

    # ----------------------------------------------------------------------

    @classmethod
    def statement(cls, code=''):
        """
        Returns the statement interface for this connection.
        
        :param      code | <str>
        
        :return     subclass of <orb.core.backends.sql.SQLStatement>
        """
        return SQLiteStatement.byName(code) if code else SQLiteStatement


# register the postgres backend
if sqlite:
    orb.Connection.registerAddon('SQLite', SQLiteConnection)

