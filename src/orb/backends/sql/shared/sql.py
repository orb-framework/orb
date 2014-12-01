import logging
import orb

from orb import errors
from orb.backends.sql.abstractsql import SQL

log = logging.getLogger(__name__)

# A
#----------------------------------------------------------------------

class ADD_COLUMN(SQL):
    def collectFlags(self, column):
        # determine all the flags for this column
        flags = []
        Flags = orb.Column.Flags
        for flag in column.iterFlags():
            flag_sql = self.byName('Flag::{0}'.format(Flags(flag)))
            if flag_sql:
                flags.append(flag_sql)
            else:
                log.error('Unkown flag: {0}'.format(flag))
        return flags

    def render(self, column, **scope):
        """
        Generates the ADD COLUMN sql for an <orb.Column> in Postgres.

        :param      column      | <orb.Column>
                    **scope   | <keywords>

        :return     <str>
        """
        if column.reference() and not column.referenceModel():
            raise errors.TableNotFound(column.reference())

        type_name = column.columnTypeText()
        sql_type = self.byName('Type::{0}'.format(type_name))
        if not sql_type:
            raise errors.DatabaseError('Unknown column type: {0}'.format(type_name))

        new_scope = {
            'column': column,
            'field': column.fieldName(),
            'reference': column.referenceModel().schema().tableName() if column.reference() else '',
            'type': sql_type,
            'flags': self.collectFlags(column),
            'max_length': column.maxlength() or self.byName('Length::{0}'.format(type_name))
        }
        new_scope.update(scope)

        return super(ADD_COLUMN, self).render(**new_scope)

class ALTER_TABLE(SQL):
    def collectColumns(self, table, selection):
        schema = table.schema()
        columns = {
            'all': [],
            'i18n': [],
            'standard': [],
            'primary': schema.primaryColumns()
        }

        # extract the columns that we'll need
        for column in sorted(selection, key=lambda x: x.fieldName()):
            if column.isAggregate() or column.isJoined() or column.isProxy() or column.isReference():
                continue
            elif column.primary():
                if schema.inherits():
                    continue
                else:
                    columns['primary'].append(column)
                    columns['all'].append(column)
            elif column.isTranslatable():
                columns['all'].append(column)
                columns['i18n'].append(column)
            else:
                columns['all'].append(column)
                columns['standard'].append(column)

        return columns

    def render(self, table, added=None, removed=None, **scope):
        """
        Generates the ALTER TABLE sql for an <orb.Table>.

        :param      schema  | <orb.TableSchema>
                    added   | [<orb.Column>, ..] || None
                    removed | [<orb.Column>, ..] || None
                    **scope | <dict>

        :return     <str>
        """
        schema = table.schema()
        db = db or orb.manager.database()

        # define the new scope
        new_scope = {
            'table': schema.tableName(),
            'added': self.collectColumns(table, added),
            'removed': self.collectColumns(table, removed),
            'owner': db.username(),
            'inherits': schema.inheritsModel().schema().tableName() if schema.inherits() else '',

            # define some useful sql queries by default
            'ADD_COLUMN': self.byName('ADD_COLUMN'),
            'ADD_CONSTRAINT': self.byName('ADD_CONSTRAINT')
        }

        if not new_scope['primary_columns']:
            raise errors.DatabaseError('No primary keys defined for {0}.'.format(schema.name()))

        # update any user overrides
        new_scope.update(scope)

        return super(CREATE_TABLE, self).render(**new_scope)

# C
#----------------------------------------------------------------------

class CREATE_TABLE(SQL):
    def collectColumns(self, table):
        columns = {
            'all': [],
            'i18n': [],
            'standard': [],
            'primary': []
        }
        schema = table.schema()

        # extract the columns that we'll need
        for column in sorted(schema.columns(recurse=False), key=lambda x: x.fieldName()):
            if column.isAggregate() or column.isJoined() or column.isProxy() or column.isReference():
                continue
            elif column.primary():
                if schema.inherits():
                    continue
                else:
                    columns['primary'].append(column)
                    columns['all'].append(column)
            elif column.isTranslatable():
                columns['all'].append(column)
                columns['i18n'].append(column)
            else:
                columns['all'].append(column)
                columns['standard'].append(column)

        return columns

    def render(self, table, db=None, **scope):
        """
        Generates the CREATE TABLE sql for an <orb.Table>.

        :param      table   | <orb.Table>
                    **scope | <dict>

        :return     <str>
        """
        schema = table.schema()
        db = db or orb.manager.database()

        # define the new scope
        new_scope = {
            'table': schema.tableName(),
            'columns': self.collectColumns(table),
            'owner': db.username(),
            'inherits': schema.inheritsModel().schema().tableName() if schema.inherits() else '',

            # define some useful sql queries by default
            'ADD_COLUMN': self.byName('ADD_COLUMN'),
            'ADD_CONSTRAINT': self.byName('ADD_CONSTRAINT')
        }

        if not new_scope['primary_columns']:
            raise errors.DatabaseError('No primary keys defined for {0}.'.format(schema.name()))

        # update any user overrides
        new_scope.update(scope)

        return super(CREATE_TABLE, self).render(**new_scope)

# D
#----------------------------------------------------------------------

class DELETE(SQL):
    def render(self, table, query, **scope):
        """
        Generates the DELETE sql for an <orb.Table>.

        :param      table   | <orb.Table>
                    query   | <orb.Query>
                    **scope | <dict>

        :return     <str>
        """
        scope['table'] = table.schema().tableName()
        scope['schema'] = table.schema()
        scope['query'] = query

        return super(DELETE, self).render(**scope)

# E
#----------------------------------------------------------------------

class ENABLE_INTERNALS(SQL):
    def render(self, enabled, schema=None, **scope):
        scope['enabled'] = enabled
        scope['table'] = schema.tableName() if schema else ''

        return super(ENABLE_INTERNALS, self).render(**scope)

# I
#----------------------------------------------------------------------

class INSERT(SQL):
    def collectInsertions(self, records, columns, io, locale):
        columns = {
            'all': [],
            'standard': [],
            'i18n': [],
        }
        insertions = {
            'all': [],
            'standard': [],
            'i18n': []
        }

        for i, record in enumerate(records):
            values = record.recordValues(locale=locale)

            row_all = []
            row_standard = []
            row_i18n = []

            for column in columns:
                # store the columns we're using the first pass through
                if not i:
                    columns['all'].append(column)
                    if not column.isTranslatable():
                        columns['standard'].append(column)
                    else:
                        columns['i18n'].append(column)

                # extract the value from the column
                try:
                    value = values[column]
                except KeyError:
                    raise errors.ValueNotFound(record, column.name())

                # store the insertion key/value pairing
                key = len(io)
                key_ref = '%({0})s'.format(key)
                io[str(key)] = value

                row_all.append(key_ref)
                if not column.isTranslatable():
                    row_standard.append(key_ref)
                else:
                    row_i18n.append(key_ref)

            insertions['all'].append(row_all)
            insertions['standard'].append(row_all)
            insertions['i18n'].append(row_all)

        return columns, insertions

    def render(self, schema, records, columns=None, **scope):
        """
        Generates the INSERT sql for an <orb.Table>.

        :param      schema  | <orb.Table> || <orb.TableSchema>
                    records | [<orb.Table>, ..]
                    columns | [<str>, ..]
                    **scope | <dict>

        :return     <str>
        """
        if orb.Table.typecheck(schema):
            schema = schema.schema()

        if columns is None:
            columns = schema.columns(includeJoined=False,
                                     includeAggregates=False,
                                     includeProxies=False)
        else:
            columns = [schema.column(col) for col in columns]

        io = scope.get('IO', {})
        locale = scope.get('locale', orb.system.locale())
        io['locale'] = locale
        columns, insertions = self.collectInsertions(records, columns, io, locale)

        new_scope = {
            'table': schema.tableName(),
            'schema': schema,
            'records': records,
            'columns': columns,
            'insertions': insertions,
            'IO': io
        }
        new_scope.update(**scope)

        return super(INSERT, self).render(**new_scope)

class INSERTED_KEYS(SQL):
    def render(self, schema, count=1, **scope):
        """
        Generates the INSERTED KEYS sql for an <orb.Table> or <orb.TableSchema>.

        :param      schema  | <orb.Table> || <orb.TableSchema>
                    count   | <int>
                    **scope | <dict>

        :return     <str>
        """
        if orb.Table.typecheck(schema):
            schema = schema.schema()

        scope['schema'] = schema
        scope['field'] = schema.primaryColumns()[0].fieldName()
        scope['table'] = schema.tableName()
        scope['count'] = count

        return super(INSERTED_KEYS, self).render(**scope)

# S
#----------------------------------------------------------------------

class SELECT(SQL):
    def render(self, table, **scope):
        """
        Generates the TABLE EXISTS sql for an <orb.Table>.

        :param      table   | <orb.Table>
                    lookup  | <orb.LookupOptions>
                    options | <orb.DatabaseOptions>
                    **scope | <dict>

        :return     <str>
        """

        new_scope = {
            'table': table,
            'lookup': scope.get('lookup', orb.LookupOptions(**scope)),
            'options': scope.get('options', orb.DatabaseOptions(**scope))
        }
        new_scope.update(**scope)

        return super(SELECT, self).render(**new_scope)

class SELECT_AGGREGATE(SQL):
    def render(self, column, **scope):
        """
        Generates the SELECT AGGREGATE sql for an <orb.Table>.

        :param      column   | <orb.Column>
                    **scope  | <dict>

        :return     <str>
        """
        scope['column'] = column

        return super(SELECT_AGGREGATE, self).render(**scope)

class SELECT_COUNT(SQL):
    def render(self, table, **scope):
        """
        Generates the SELECT COUNT sql for an <orb.Table>.

        :param      table   | <orb.Table>
                    lookup  | <orb.LookupOptions>
                    options | <orb.DatabaseOptions>
                    **scope | <dict>

        :return     <str>
        """
        scope['table'] = table
        scope['lookup'] = scope.get('lookup', orb.LookupOptions(**scope))
        scope['options'] = scope.get('options', orb.DatabaseOptions(**scope))

        return super(SELECT_COUNT, self).render(**scope)

class SELECT_EXPAND(SQL):
    def render(self, **scope):
        return super(SELECT_EXPAND, self).render(**scope)

class SELECT_JOINER(SQL):
    def render(self, column, **scope):
        """
        Generates the SELECT JOINER sql for an <orb.Table>.

        :param      column   | <orb.Column>
                    **scope  | <dict>

        :return     <str>
        """
        scope['column'] = column

        return super(SELECT_JOINER, self).render(**scope)

# T
#----------------------------------------------------------------------

class TABLE_COLUMNS(SQL):
    def render(self, schema, **scope):
        """
        Generates the TABLE COLUMNS sql for an <orb.Table>.

        :param      schema  | <orb.Table> || <orb.TableSchema>
                    **scope | <dict>

        :return     <str>
        """
        if orb.Table.typecheck(schema):
            schema = schema.schema()

        scope['schema'] = schema

        return super(TABLE_COLUMNS, self).render(**scope)

class TABLE_EXISTS(SQL):
    def render(self, schema, **scope):
        """
        Generates the TABLE EXISTS sql for an <orb.Table>.

        :param      schema  | <orb.TableSchema> || <orb.Table>
                    **scope | <dict>

        :return     <str>
        """
        if orb.Table.typecheck(schema):
            schema = schema.schema()

        scope['schema'] = schema
        scope['table'] = schema.tableName()

        return super(TABLE_EXISTS, self).render(**scope)

# U
#----------------------------------------------------------------------

class UPDATE(SQL):
    def render(self, schema, changes, **scope):
        """
        Generates the UPDATE sql for an <orb.Table>.

        :param      schema  | <orb.Table> || <orb.TableSchema>
                    changes | [(<orb.Table>, [<orb.Column>, ..]) ..]
                    **scope | <dict>

        :return     <str>
        """
        if orb.Table.typecheck(schema):
            schema = schema.schema()

        scope['schema'] = schema
        scope['changes'] = changes

        return super(UPDATE, self).render(**scope)

# W
#----------------------------------------------------------------------

class WHERE(SQL):
    def render(self, where, baseSchema=None, **scope):
        """
        Generates the WHERE sql for an <orb.Table>.

        :param      where   | <orb.Query> || <orb.QueryCompound>
                    **scope | <dict>

        :return     <str>
        """
        scope['baseSchema'] = baseSchema
        scope['where'] = where

        return super(WHERE, self).render(**scope)

#----------------------------------------------------------------------

# define base column types
SQL.registerAddon('Type::BigInt',                   u'BIGINT')
SQL.registerAddon('Type::Bool',                     u'BOOL')
SQL.registerAddon('Type::ByteArray',                u'VARBINARY')
SQL.registerAddon('Type::Color',                    u'VARCHAR')
SQL.registerAddon('Type::Date',                     u'DATE')
SQL.registerAddon('Type::Datetime',                 u'DATETIME')
SQL.registerAddon('Type::DatetimeWithTimezone',     u'TIMESTAMP')
SQL.registerAddon('Type::Decimal',                  u'DECIMAL UNSIGNED')
SQL.registerAddon('Type::Directory',                u'VARCHAR')
SQL.registerAddon('Type::Dict',                     u'BLOB')
SQL.registerAddon('Type::Double',                   u'DOUBLE UNSIGNED')
SQL.registerAddon('Type::Email',                    u'VARCHAR')
SQL.registerAddon('Type::Enum',                     u'INT UNSIGNED')
SQL.registerAddon('Type::Filepath',                 u'VARCHAR')
SQL.registerAddon('Type::ForeignKey',               u'BIGINT UNSIGNED')
SQL.registerAddon('Type::Html',                     u'TEXT')
SQL.registerAddon('Type::Image',                    u'BLOB')
SQL.registerAddon('Type::Integer',                  u'INT UNSIGNED')
SQL.registerAddon('Type::Password',                 u'VARCHAR')
SQL.registerAddon('Type::Pickle',                   u'BLOB')
SQL.registerAddon('Type::Query',                    u'TEXT')
SQL.registerAddon('Type::String',                   u'VARCHAR')
SQL.registerAddon('Type::Text',                     u'TEXT')
SQL.registerAddon('Type::Time',                     u'TIME')
SQL.registerAddon('Type::Url',                      u'VARCHAR')
SQL.registerAddon('Type::Xml',                      u'TEXT')
SQL.registerAddon('Type::Yaml',                     u'TEXT')

# define the default lengths
SQL.registerAddon('Length::Color',                  25)
SQL.registerAddon('Length::String',                 256)
SQL.registerAddon('Length::Email',                  256)
SQL.registerAddon('Length::Password',               256)
SQL.registerAddon('Length::Url',                    500)
SQL.registerAddon('Length::Filepath',               500)
SQL.registerAddon('Length::Directory',              500)

# define the base flags
SQL.registerAddon('Flag::Unique',                   u'UNIQUE')
SQL.registerAddon('Flag::Required',                 u'NOT NULL')
SQL.registerAddon('Flag::AutoIncrement',            u'AUTO_INCREMENT')

# define the base operators
SQL.registerAddon('Op::Is',                              u'=')
SQL.registerAddon('Op::IsNot',                           u'!=')
SQL.registerAddon('Op::LessThan',                        u'<')
SQL.registerAddon('Op::Before',                          u'<')
SQL.registerAddon('Op::LessThanOrEqual',                 u'<=')
SQL.registerAddon('Op::GreaterThanOrEqual',              u'>=')
SQL.registerAddon('Op::GreaterThan',                     u'>')
SQL.registerAddon('Op::After',                           u'>')
SQL.registerAddon('Op::Matches',                         u'~*')
SQL.registerAddon('Op::Matches::CaseSensitive',          u'~')
SQL.registerAddon('Op::DoesNotMatch',                    u'!~*')
SQL.registerAddon('Op::DoesNotMatch::CaseSensitive',     u'!~*')
SQL.registerAddon('Op::Contains',                        u'ILIKE')
SQL.registerAddon('Op::Contains::CaseSensitive',         u'LIKE')
SQL.registerAddon('Op::Startswith',                      u'ILIKE')
SQL.registerAddon('Op::Startswith::CaseSensitive',       u'LIKE')
SQL.registerAddon('Op::Endswith',                        u'ILIKE')
SQL.registerAddon('Op::Endswith::CaseSensitive',         u'LIKE')
SQL.registerAddon('Op::DoesNotContain',                  u'NOT ILIKE')
SQL.registerAddon('Op::DoesNotContain::CaseSensitive',   u'NOT LIKE')
SQL.registerAddon('Op::DoesNotStartwith',                u'NOT ILIKE')
SQL.registerAddon('Op::DoesNotStartwith::CaseSensitive', u'NOT LIKE')
SQL.registerAddon('Op::DoesNotEndwith',                  u'NOT ILIKE')
SQL.registerAddon('Op::DoesNotEndwith::CaseSensitive',   u'NOT LIKE')
SQL.registerAddon('Op::IsIn',                            u'IN')
SQL.registerAddon('Op::IsNotIn',                         u'NOT IN')

# define the base functions
SQL.registerAddon('Func::Lower',                    u'lower({0})')
SQL.registerAddon('Func::Upper',                    u'upper({0})')
SQL.registerAddon('Func::Abs',                      u'abs({0})')
SQL.registerAddon('Func::AsString',                 u'{0}::varchar')

# define the base math operators
SQL.registerAddon('Math::Add',                      u'+')
SQL.registerAddon('Math::Subtract',                 u'-')
SQL.registerAddon('Math::Multiply',                 u'*')
SQL.registerAddon('Math::Divide',                   u'/')
SQL.registerAddon('Math::And',                      u'&')
SQL.registerAddon('Math::Or',                       u'|')

SQL.registerAddon('Math::Add::String',              u'||')
SQL.registerAddon('Math::Add::Text',                u'||')

