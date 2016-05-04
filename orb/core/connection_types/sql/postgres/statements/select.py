from collections import defaultdict
from projex.lazymodule import lazy_import
from ..psqlconnection import PSQLStatement

orb = lazy_import('orb')


class SELECT(PSQLStatement):
    def cmpcol(self, col_a, col_b):
        return cmp(col_a.field(), col_b.field())

    def __call__(self, model, context, fields=None):
        EXPAND_COL = self.byName('SELECT EXPAND COLUMN')
        EXPAND_PIPE = self.byName('SELECT EXPAND PIPE')
        EXPAND_REV = self.byName('SELECT EXPAND REVERSE')
        WHERE = self.byName('WHERE')

        # generate the where query
        base_where = model.baseQuery(context=context)
        if base_where:
            where = base_where & context.where
        else:
            where = context.where

        # determine what to expand
        schema = model.schema()
        expand = context.expandtree()
        expanded = bool(expand)
        columns = [schema.column(c) for c in context.columns] if context.columns else schema.columns().values()

        data = {
            'locale': context.locale,
            'default_locale': orb.system.settings().default_locale
        }
        fields = fields or {}
        sql_group_by = set()
        sql_columns = defaultdict(list)
        sql_joins = []

        # process columns to select
        for column in sorted(columns, self.cmpcol):
            if column.testFlag(column.Flags.Virtual):
                continue

            if column.testFlag(column.Flags.I18n):
                if context.locale == 'all':
                    sql = u'hstore_agg(hstore("i18n"."locale", "i18n"."{0}")) AS "{0}"'
                elif data['locale'] == data['default_locale'] or column.testFlag(column.Flags.I18n_NoDefault):
                    sql = u'(array_agg("i18n"."{0}"))[1] AS "{0}"'
                else:
                    sql = u'(coalesce((array_agg("i18n"."{0}"))[1], (array_agg("i18n_default"."{0}"))[1])) AS "{0}"'

                sql_columns['i18n'].append(sql.format(column.field()))
                sql_group_by.add(u'"{0}"."{1}"'.format(schema.dbname(), schema.idColumn().field()))
                fields[column] = u'"i18n"."{0}"'.format(column.field())
            else:
                # expand a reference
                if isinstance(column, orb.ReferenceColumn) and column.name() in expand:
                    sub_tree = expand.pop(column.name())
                    sql, sub_data = EXPAND_COL(column, sub_tree)
                    if sql:
                        sql_columns['standard'].append(sql)
                        data.update(sub_data)

                # select the base record
                sql_columns['standard'].append(u'"{0}"."{1}" AS "{1}"'.format(schema.dbname(),
                                                                             column.field(),
                                                                             column.field()))

        # expand any pipes
        if expand:
            for collector in schema.collectors().values():
                if collector.name() in expand:
                    if collector.testFlag(collector.Flags.Virtual):
                        continue

                    sub_tree = expand.pop(collector.name(), None)
                    if isinstance(collector, orb.Pipe):
                        sql, sub_data = EXPAND_PIPE(collector, sub_tree)
                    elif isinstance(collector, orb.ReverseLookup):
                        sql, sub_data = EXPAND_REV(collector, sub_tree)

                    if sql:
                        sql_columns['standard'].append(sql)
                        data.update(sub_data)

                if not expand:
                    break

        # generate sql ordering
        sql_order_by = []
        if context.order:
            for col, dir in context.order:
                column = schema.column(col)
                if not column:
                    raise orb.errors.ColumnNotFound(col)

                field = fields.get(column) or u'"{0}"."{1}"'.format(schema.dbname(), column.field())
                if sql_group_by:
                    sql_group_by.add(field)
                sql_order_by.append(u'{0} {1}'.format(field, dir.upper()))

                if context.distinct:
                    sql_columns['standard'].append(field)

        if context.distinct is True:
            cmd = ['SELECT DISTINCT {0} FROM "{1}"'.format(', '.join(sql_columns['standard'] + sql_columns['i18n']), schema.dbname())]
        elif isinstance(context.distinct, (list, set, tuple)):
            on_ = []
            for col in context.distinct:
                column = schema.column(col)
                if not column:
                    raise orb.errors.ColumnNotFound(col)
                else:
                    on_.append(fields.get(col) or u'"{0}"."{1}"'.format(schema.dbname(), col.field()))

            cmd = [u'SELECT DISTINCT ON ({0}) {1} FROM "{2}"'.format(', '.join(on_),
                                                                    ', '.join(sql_columns['standard'] + sql_columns['i18n']),
                                                                    schema.dbname())]
        else:
            cmd = [u'SELECT {0} FROM "{1}"'.format(', '.join(sql_columns['standard'] + sql_columns['i18n']), schema.dbname())]

        # add sql joins to the statement
        if sql_joins:
            cmd += sql_joins

        # join in the i18n table
        if sql_columns['i18n']:
            if context.locale == 'all':
                cmd.append(u'LEFT JOIN "{0}_i18n" AS "i18n" ON ("i18n"."{0}_id" = "id")'.format(schema.dbname()))
            else:
                sql = u'LEFT JOIN "{0}_i18n" AS "i18n" ON ("i18n"."{0}_id" = "id" AND "i18n"."locale" = %(locale)s)'
                if data['locale'] != data['default_locale']:
                    sql += u'\nLEFT JOIN "{0}_i18n" AS "i18n_default" ON ("i18n_default"."{0}_id" = "id" AND "i18n_default"."locale" = %(default_locale)s)'
                cmd.append(sql.format(schema.dbname()))

        # generate sql statements
        try:
            sql_where, sql_where_data = WHERE(model, where, fields=fields)
        except orb.errors.QueryIsNull:
            sql_where, sql_where_data = '', {}
        else:
            data.update(sql_where_data)

        if expanded:
            if sql_order_by:
                distinct = u'ON ({0})'.format(', '.join((order.split(' ')[0] for order in sql_order_by)))
            else:
                distinct = ''

            cmd.append(u'WHERE "{0}"."{1}" IN ('.format(schema.dbname(), schema.idColumn().field()))
            cmd.append(u'    SELECT DISTINCT {0} "{1}"."{2}"'.format(distinct, schema.dbname(), schema.idColumn().field()))
            cmd.append(u'    FROM "{0}"\n'.format(schema.dbname()))

            if sql_columns['i18n']:
                if context.locale == 'all':
                    cmd.append(u'    LEFT JOIN "{0}_i18n" AS "i18n" ON ("i18n"."{0}_id" = "id")'.format(schema.dbname()))
                else:
                    sql = u'LEFT JOIN "{0}_i18n" AS "i18n" ON ("i18n"."{0}_id" = "id" AND "i18n"."locale" = %(locale)s)'
                    cmd.append('    ' + sql.format(schema.dbname()))

            if sql_where:
                cmd.append(u'    WHERE {0}'.format(sql_where))
            if sql_group_by:
                cmd.append(u'    GROUP BY {0}'.format(', '.join(list(sql_group_by) + [order.split(' ')[0] for order in sql_order_by])))
            if sql_order_by:
                cmd.append(u'    ORDER BY {0}'.format(', '.join(sql_order_by)))
            if context.start:
                if not isinstance(context.limit, (int, long)):
                    raise orb.errors.DatabaseError('Invalid value provided for start')
                cmd.append(u'    OFFSET {0}'.format(context.start))
            if context.limit > 0:
                if not isinstance(context.limit, (int, long)):
                    raise orb.errors.DatabaseError('Invalid value provided for limit')
                cmd.append(u'    LIMIT {0}'.format(context.limit))

            cmd.append(u')')
            if sql_group_by:
                cmd.append(u'GROUP BY {0}'.format(', '.join(list(sql_group_by))))
        else:
            if sql_where:
                cmd.append(u'WHERE {0}'.format(sql_where))
            if sql_group_by:
                cmd.append(u'GROUP BY {0}'.format(', '.join(list(sql_group_by))))
            if sql_order_by:
                cmd.append(u'ORDER BY {0}'.format(', '.join(sql_order_by)))
            if context.start:
                if not isinstance(context.start, (int, long)):
                    raise orb.errors.DatabaseError('Invalid value provided for start')
                cmd.append(u'OFFSET {0}'.format(context.start))
            if context.limit > 0:
                if not isinstance(context.limit, (int, long)):
                    raise orb.errors.DatabaseError('Invalid value provided for limit')
                cmd.append(u'LIMIT {0}'.format(context.limit))

        return u'\n'.join(cmd), data

PSQLStatement.registerAddon('SELECT', SELECT())