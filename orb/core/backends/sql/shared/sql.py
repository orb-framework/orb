import logging
import orb

from collections import defaultdict
from orb import errors
from orb.core.backends.sql.abstractsql import SQL

log = logging.getLogger(__name__)

# A
# ----------------------------------------------------------------------


class ADD_COLUMN(SQL):
    def collectFlags(self, column):
        # determine all the flags for this column
        flags = []
        Flags = orb.Column.Flags
        for flag in column.iterFlags():
            flag_sql = self.baseSQL().byName('Flag::{0}'.format(Flags(flag)))
            if flag_sql:
                flags.append(flag_sql)
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
        sql_type = self.baseSQL().byName('Type::{0}'.format(type_name))
        if not sql_type:
            raise errors.DatabaseError('Unknown column type: {0}'.format(type_name))

        new_scope = {
            'column': column,
            'field': column.fieldName(),
            'reference': column.referenceModel().schema().dbname() if column.reference() else '',
            'type': sql_type,
            'flags': self.collectFlags(column),
            'max_length': column.maxlength() or self.baseSQL().byName('Length::{0}'.format(type_name))
        }
        new_scope.update(scope)

        return super(ADD_COLUMN, self).render(**new_scope)


class ALTER_TABLE(SQL):
    @staticmethod
    def collectColumns(schema, selection):
        columns = defaultdict(list)
        if selection is None:
            return columns

        columns['primary'] = schema.primaryColumns()

        # extract the columns that we'll need
        for column in sorted(selection, key=lambda x: x.fieldName()):
            if column.isAggregate() or column.isJoined() or column.isProxy():
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
                columns['base'].append(column)

        return columns

    def render(self, schema, added=None, removed=None, **scope):
        """
        Generates the ALTER TABLE sql for an <orb.Table>.

        :param      schema  | <orb.TableSchema>
                    added   | [<orb.Column>, ..] || None
                    removed | [<orb.Column>, ..] || None
                    **scope | <dict>

        :return     <str>
        """
        if not isinstance(schema, orb.TableSchema):
            return ''

        db = scope.get('db', orb.system.database())

        new_columns = self.collectColumns(schema, added)
        old_columns = self.collectColumns(schema, removed)

        # define the new scope
        new_scope = {
            'schema': schema,
            'table': schema.dbname(),
            'added': new_columns,
            'removed': old_columns,
            'owner': db.username(),
            'inherits': schema.inheritsModel().schema().dbname() if schema.inherits() else '',

            # define some useful sql queries by default
            'ADD_COLUMN': self.baseSQL().byName('ADD_COLUMN'),
            'ADD_CONSTRAINT': self.baseSQL().byName('ADD_CONSTRAINT'),
            'CREATE_INDEX': self.baseSQL().byName('CREATE_INDEX')
        }

        # update any user overrides
        new_scope.update(scope)

        return super(ALTER_TABLE, self).render(**new_scope)


# C
# ----------------------------------------------------------------------

class CREATE_TABLE(SQL):
    @staticmethod
    def collectColumns(table):
        schema = table.schema()
        columns = defaultdict(list)

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
                columns['base'].append(column)

        return columns

    def render(self, table, db=None, **scope):
        """
        Generates the CREATE TABLE sql for an <orb.Table>.

        :param      table   | <orb.Table>
                    **scope | <dict>

        :return     <str>
        """
        # ensure this is in fact a table type
        if not orb.Table.typecheck(table):
            return ''

        schema = table.schema()
        db = db or orb.system.database()
        new_columns = self.collectColumns(table)

        # define the new scope
        new_scope = {
            'schema': schema,
            'table': schema.dbname(),
            'columns': new_columns,
            'owner': db.username(),
            'inherits': schema.inheritsModel().schema().dbname() if schema.inherits() else '',

            # define some useful sql queries by default
            'ADD_COLUMN': self.baseSQL().byName('ADD_COLUMN'),
            'ADD_CONSTRAINT': self.baseSQL().byName('ADD_CONSTRAINT'),
            'CREATE_INDEX': self.baseSQL().byName('CREATE_INDEX'),
        }

        if not (new_scope['columns']['primary'] or new_scope['inherits']):
            raise errors.DatabaseError('No primary keys defined for {0}.'.format(schema.name()))

        # update any user overrides
        new_scope.update(scope)

        return super(CREATE_TABLE, self).render(**new_scope)


class CREATE_VIEW(SQL):
    def render(self, view, db=None, **scope):
        # ensure this is in fact a table type
        if not orb.View.typecheck(view):
            return ''

        schema = view.schema()
        scope['schema'] = schema
        return super(CREATE_VIEW, self).render(**scope)


class CREATE_INDEX(SQL):
    def render(self, index_or_column, **scope):
        # create an index here
        if isinstance(index_or_column, orb.Index):
            scope['index'] = index_or_column
            scope['column'] = None
        else:
            scope['column'] = index_or_column
            scope['index'] = None

        return super(CREATE_INDEX, self).render(**scope)


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
        io = scope.get('IO', {})
        # noinspection PyShadowingNames
        WHERE = self.baseSQL().byName('WHERE')

        scope['table'] = table.schema().dbname()
        scope['schema'] = table.schema()
        scope['query'] = query
        scope['where'] = WHERE(table.schema(), query, IO=io)

        return super(DELETE, self).render(**scope)


# E
#----------------------------------------------------------------------

class ENABLE_INTERNALS(SQL):
    def render(self, enabled, schema=None, **scope):
        scope['enabled'] = enabled
        scope['table'] = schema.dbname() if schema else ''

        return super(ENABLE_INTERNALS, self).render(**scope)


# I
#----------------------------------------------------------------------

class INSERT(SQL):
    def collectInsertions(self, records, cols, io, locale):
        columns = defaultdict(list)
        insertions = defaultdict(list)

        for i, record in enumerate(records):
            values = record.recordValues(locale=locale, kind=orb.Column.Kind.Field, key='column')

            row_all = []
            row_base = []
            row_i18n = []

            store = self.datastore()

            for column in cols:
                # do not insert auto-incrementing columns
                if column.autoIncrement():
                    continue

                # extract the value from the column
                try:
                    value = store.store(column, values[column])
                except KeyError:
                    raise errors.ValueNotFound(record, column.name())

                # store the columns we're using the first pass through
                if not i:
                    columns['all'].append(column)
                    if not column.isTranslatable():
                        columns['base'].append(column)
                    else:
                        columns['i18n'].append(column)

                # store the insertion key/value pairing
                key = len(io)
                key_ref = '%({0})s'.format(key)
                io[str(key)] = value

                row_all.append(key_ref)
                if not column.isTranslatable():
                    row_base.append(key_ref)
                else:
                    row_i18n.append(key_ref)

            if row_all:
                insertions['all'].append(row_all)
            if row_base:
                insertions['base'].append(row_base)
            if row_i18n:
                insertions['i18n'].append(row_i18n)

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
        elif orb.View.typecheck(schema):
            raise errors.QueryInvalid('Views are read-only.')

        if columns is None:
            columns = schema.columns(kind=orb.Column.Kind.Field)
        else:
            columns = [schema.column(col) for col in columns]

        locale = scope.get('locale', orb.system.locale())
        io = scope.get('IO', {})
        io['locale'] = locale

        columns, insertions = self.collectInsertions(records, columns, io, locale)

        new_scope = {
            'table': schema.dbname(),
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
        elif orb.View.typecheck(schema):
            raise errors.QueryInvalid('Views are read-only.')

        scope['schema'] = schema
        scope['field'] = schema.primaryColumns()[0].fieldName()
        scope['table'] = schema.dbname()
        scope['count'] = count

        return super(INSERTED_KEYS, self).render(**scope)


# Q
#----------------------------------------------------------------------

class QUOTE(SQL):
    def render(self, *text, **scope):
        """
        Wraps the inputted text in SQL safe quotes for this language.

        :param      text | [<str>, ..]

        :return     <str>
        """
        scope.setdefault('joiner', '.')
        scope['text'] = text
        return super(QUOTE, self).render(**scope)


# S
#----------------------------------------------------------------------

class SCHEMA_INFO(SQL):
    def render(self, **scope):
        try:
            namespace = scope['options'].namespace or 'public'
        except (KeyError, AttributeError):
            namespace = 'public'

        scope['namespace'] = namespace

        return super(SCHEMA_INFO, self).render(**scope)


class SELECT(SQL):
    def render(self, table_or_records, **scope):
        """
        Generates the TABLE EXISTS sql for an <orb.Table>.

        :param      table_or_records   | <orb.Table> or <orb.RecordSet>
                    lookup             | <orb.LookupOptions>
                    options            | <orb.ContextOptions>
                    **scope            | <dict>

        :return     <str>
        """
        if orb.Table.typecheck(table_or_records) or orb.View.typecheck(table_or_records):
            new_scope = {
                'table': table_or_records,
                'lookup': orb.LookupOptions(**scope),
                'options': orb.ContextOptions(**scope)
            }
        else:
            new_scope = {
                'table': table_or_records.table(),
                'lookup': table_or_records.lookupOptions(**scope),
                'options': table_or_records.contextOptions(**scope)
            }

        # use default columns
        if not new_scope['lookup'].columns:
            default_columns = scope.pop('default_columns', None)
            if default_columns:
                schema = new_scope['table'].schema()
                new_scope['lookup'].columns = [schema.column(col) for col in default_columns]

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
                    options | <orb.ContextOptions>
                    **scope | <dict>

        :return     <str>
        """
        scope['table'] = table
        scope['lookup'] = scope.get('lookup', orb.LookupOptions(**scope))
        scope['options'] = scope.get('options', orb.ContextOptions(**scope))

        return super(SELECT_COUNT, self).render(**scope)


class SELECT_EXPAND(SQL):
    def render(self, **scope):
        # define optional arguments
        scope.setdefault('source_alias', '')
        scope.setdefault('column', None)
        scope.setdefault('pipe', None)
        scope.setdefault('reverseLookup', None)
        scope.setdefault('SELECT_EXPAND', self)

        return super(SELECT_EXPAND, self).render(**scope)


class SELECT_SHORTCUT(SQL):
    def render(self, column, **scope):
        scope['column'] = column

        return super(SELECT_SHORTCUT, self).render(**scope)


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
        elif orb.View.typecheck(schema):
            raise errors.QueryInvalid('Views are read-only.')

        scope['schema'] = schema
        scope['changes'] = changes

        return super(UPDATE, self).render(**scope)


# W
#----------------------------------------------------------------------

class WHERE(SQL):
    def queryToSQL(self, schema, query, alias=''):
        # noinspection PyShadowingNames
        QUOTE = self.baseSQL().byName('QUOTE')

        column = query.column(schema)
        output = QUOTE(alias or schema.dbname(), column.fieldName())

        # process any functions on the query
        for func in query.functions():
            sql_func = self.baseSQL().byName('Func::{0}'.format(orb.Query.Function(func)))
            if not sql_func:
                msg = 'Unknown function type {0}'.format(orb.Query.Function(func))
                raise errors.QueryInvalid(msg)
            else:
                output = sql_func.format(output)

        return output

    def render(self, schema, query, **scope):
        """
        Generates the WHERE sql for an <orb.Table>.

        :param      schema  | <orb.Schema>
                    where   | <orb.Query> || <orb.QueryCompound>
                    **scope | <dict>

        :return     <str>
        """
        if query is None:
            return ''

        scope.setdefault('IO', {})
        scope.setdefault('GLOBALS', {})

        io = scope['IO']
        glbls = scope['GLOBALS']
        db = scope.get('db', orb.system.database())
        query = query.expandShortcuts(schema.model())

        # create a compound query
        if orb.QueryCompound.typecheck(query):
            queries = [self(schema, subq, schema_alias=scope.get('schema_alias'), GLOBALS=glbls, IO=io) for subq in query.queries()]
            joiner = u' AND ' if query.operatorType() == orb.QueryCompound.Op.And else u' OR '
            result = joiner.join([q for q in queries if q])
            return u'({0})'.format(result) if result else ''

        # create a basic query
        else:
            glbls.setdefault('join_count', 0)
            glbls.setdefault('field_mapper', {})

            # grab the column from the query
            column = query.column(schema, db=db)
            if not column:
                raise errors.ColumnNotFound(schema.name(), query.columnName())

            # grab the field information
            try:
                field = glbls['field_mapper'][column]
            except KeyError:
                field = self.queryToSQL(schema, query, scope.get('schema_alias', ''))

            # calculate the field math modifications
            for op, target in query.math():
                opts = {
                    'math': orb.Query.Math(op),
                    'type': orb.ColumnType(column.columnType())
                }

                base = self.baseSQL()
                sql = base.byName('Math::{math}::{type}'.format(**opts)) or base.byName('Math::{math}'.format(**opts))
                if not sql:
                    msg = 'Cannot {math} {type} types.'.format(**opts)
                    raise errors.QueryInvalid(msg)
                else:
                    field += sql
                    if orb.Query.typecheck(target):
                        field += self.queryToSQL(schema, target, scope.get('schema_alias', ''))
                    else:
                        key = len(io)
                        io[str(key)] = target
                        field += '%({0})s'.format(key)

            # calculate the value
            operator = query.operatorType()
            value = query.value()

            # check for a record value
            if orb.Table.recordcheck(value) or orb.View.recordcheck(value):
                value = value.primaryKey()

            # calculate the sql operation
            op_name = orb.Query.Op(operator)
            op = self.baseSQL().byName('Op::{0}'.format(op_name))
            if op is None:
                raise orb.errors.QueryInvalid('{0} is an unknown operator.'.format(op_name))

            if query.caseSensitive():
                case = self.baseSQL().byName('Op::{0}::CaseSensitive'.format(op_name))
                op = case or op

            # update the scope
            scope.setdefault('schema_alias', '')
            scope['WHERE'] = self
            scope['schema'] = schema
            scope['query'] = query
            scope['column'] = column
            scope['field'] = field
            scope['value'] = value
            scope['operator'] = operator
            scope['op'] = op

            return super(WHERE, self).render(**scope)

#----------------------------------------------------------------------

# define base column types
SQL.registerAddon('Type::BigInt', u'BIGINT')
SQL.registerAddon('Type::Bool', u'BOOL')
SQL.registerAddon('Type::ByteArray', u'VARBINARY')
SQL.registerAddon('Type::Color', u'VARCHAR')
SQL.registerAddon('Type::Date', u'DATE')
SQL.registerAddon('Type::Datetime', u'DATETIME')
SQL.registerAddon('Type::DatetimeWithTimezone', u'TIMESTAMP')
SQL.registerAddon('Type::Decimal', u'DECIMAL UNSIGNED')
SQL.registerAddon('Type::Directory', u'VARCHAR')
SQL.registerAddon('Type::Dict', u'BLOB')
SQL.registerAddon('Type::Double', u'DOUBLE UNSIGNED')
SQL.registerAddon('Type::Email', u'VARCHAR')
SQL.registerAddon('Type::Enum', u'INT UNSIGNED')
SQL.registerAddon('Type::Filepath', u'VARCHAR')
SQL.registerAddon('Type::ForeignKey', u'BIGINT UNSIGNED')
SQL.registerAddon('Type::Html', u'TEXT')
SQL.registerAddon('Type::Image', u'BLOB')
SQL.registerAddon('Type::Integer', u'INT UNSIGNED')
SQL.registerAddon('Type::Password', u'VARCHAR')
SQL.registerAddon('Type::Pickle', u'BLOB')
SQL.registerAddon('Type::Query', u'TEXT')
SQL.registerAddon('Type::String', u'VARCHAR')
SQL.registerAddon('Type::Text', u'TEXT')
SQL.registerAddon('Type::Time', u'TIME')
SQL.registerAddon('Type::Url', u'VARCHAR')
SQL.registerAddon('Type::Xml', u'TEXT')
SQL.registerAddon('Type::Yaml', u'TEXT')
SQL.registerAddon('Type::JSON', u'TEXT')

# define the default lengths
SQL.registerAddon('Length::Color', 25)
SQL.registerAddon('Length::String', 256)
SQL.registerAddon('Length::Email', 256)
SQL.registerAddon('Length::Password', 256)
SQL.registerAddon('Length::Url', 500)
SQL.registerAddon('Length::Filepath', 500)
SQL.registerAddon('Length::Directory', 500)

# define the base flags
SQL.registerAddon('Flag::Unique', u'UNIQUE')
SQL.registerAddon('Flag::Required', u'NOT NULL')
SQL.registerAddon('Flag::AutoIncrement', u'AUTO_INCREMENT')

# define the base operators
SQL.registerAddon('Op::Is', u'=')
SQL.registerAddon('Op::IsNot', u'!=')
SQL.registerAddon('Op::LessThan', u'<')
SQL.registerAddon('Op::Before', u'<')
SQL.registerAddon('Op::LessThanOrEqual', u'<=')
SQL.registerAddon('Op::GreaterThanOrEqual', u'>=')
SQL.registerAddon('Op::GreaterThan', u'>')
SQL.registerAddon('Op::After', u'>')
SQL.registerAddon('Op::Matches', u'~*')
SQL.registerAddon('Op::Matches::CaseSensitive', u'~')
SQL.registerAddon('Op::DoesNotMatch', u'!~*')
SQL.registerAddon('Op::DoesNotMatch::CaseSensitive', u'!~*')
SQL.registerAddon('Op::Contains', u'ILIKE')
SQL.registerAddon('Op::Contains::CaseSensitive', u'LIKE')
SQL.registerAddon('Op::Startswith', u'ILIKE')
SQL.registerAddon('Op::Startswith::CaseSensitive', u'LIKE')
SQL.registerAddon('Op::Endswith', u'ILIKE')
SQL.registerAddon('Op::Endswith::CaseSensitive', u'LIKE')
SQL.registerAddon('Op::DoesNotContain', u'NOT ILIKE')
SQL.registerAddon('Op::DoesNotContain::CaseSensitive', u'NOT LIKE')
SQL.registerAddon('Op::DoesNotStartwith', u'NOT ILIKE')
SQL.registerAddon('Op::DoesNotStartwith::CaseSensitive', u'NOT LIKE')
SQL.registerAddon('Op::DoesNotEndwith', u'NOT ILIKE')
SQL.registerAddon('Op::DoesNotEndwith::CaseSensitive', u'NOT LIKE')
SQL.registerAddon('Op::IsIn', u'IN')
SQL.registerAddon('Op::IsNotIn', u'NOT IN')

# define the base functions
SQL.registerAddon('Func::Lower', u'lower({0})')
SQL.registerAddon('Func::Upper', u'upper({0})')
SQL.registerAddon('Func::Abs', u'abs({0})')
SQL.registerAddon('Func::AsString', u'{0}::varchar')

# define the base math operators
SQL.registerAddon('Math::Add', u'+')
SQL.registerAddon('Math::Subtract', u'-')
SQL.registerAddon('Math::Multiply', u'*')
SQL.registerAddon('Math::Divide', u'/')
SQL.registerAddon('Math::And', u'&')
SQL.registerAddon('Math::Or', u'|')

SQL.registerAddon('Math::Add::String', u'||')
SQL.registerAddon('Math::Add::Text', u'||')

