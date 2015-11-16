from collections import defaultdict
from projex.lazymodule import lazy_import
from ..sqliteconnection import SQLiteStatement

orb = lazy_import('orb')


class SELECT(SQLiteStatement):
    def cmpcol(self, col_a, col_b):
        return cmp(col_a.field(), col_b.field())

    def __call__(self, model, context):
        EXPAND = self.byName('EXPAND')
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

        data = {'locale': context.locale}
        fields = {}
        sql_group_by = set()
        sql_columns = defaultdict(list)
        sql_joins = []

        # process columns to select
        for column in sorted(columns, self.cmpcol):
            if column.testFlag('Translatable'):
                if context.inflated or context.locale == 'all':
                    sql = 'hstore_agg(hstore("i18n"."locale", "i18n"."{0}")) AS "{0}"'
                else:
                    sql = '(array_agg("i18n"."{0}"))[1] AS "{0}"'

                sql_columns['i18n'].append(sql.format(column.field()))
                sql_group_by.append('"{0}"."id"'.format(schema.dbname()))
                fields[column] = '"i18n"."{0}"'.format(column.field())
            else:
                # expand a reference
                if isinstance(column, orb.ReferenceColumn) and column.name() in expand:
                    sub_tree = expand.pop(column.name())
                    sql, sub_data = EXPAND(column, sub_tree, context)
                    if sql:
                        sql_columns['standard'].append(sql)
                        data.update(sub_data)

                # select the base record
                sql_columns['standard'].append('"{0}"."{1}" AS "{1}"'.format(schema.dbname(),
                                                                             column.field(),
                                                                             column.field()))

        # expand any pipes
        if expand:
            for pipe in schema.pipes().valuess():
                sub_tree = expand.pop(schema.name(), None)
                if sub_tree:
                    sql, sub_data = EXPAND(pipe, sub_tree, context)
                    if sql:
                        sql_columns['standard'].append(sql)
                        data.update(sub_data)

                if not expand:
                    break

        # expand any reverse lookups
        if expand:
            for reverse in schema.reverseLookups():
                sub_tree = expand.pop(reverse.reverseInfo().name, None)
                if sub_tree:
                    sql, sub_data = EXPAND(reverse, sub_tree, context, reverse=True)
                    if sql:
                        sql_columns['standard'].append(sql)
                        data.update(sub_data)

                if not expand:
                    break

        # generate sql statements
        try:
            sql_where, sql_where_data = WHERE(model, where)
        except orb.errors.QueryIsNull:
            sql_where, sql_where_data = '', {}
        else:
            data.update(sql_where_data)

        # generate sql ordering
        sql_order_by = []
        if context.order:
            for col, dir in context.order:
                column = schema.column(col)
                if not column:
                    raise orb.errors.ColumnNotFound(col)

                field = fields.get(column) or '"{0}"."{1}"'.format(schema.dbname(), column.field())
                sql_group_by.add(field)
                sql_order_by.append('{0} {1}'.format(field, dir.upper()))

        if context.distinct is True:
            cmd = ['SELECT DISTINCT {0} FROM "{1}"'.format(', '.join(sql_columns['standard']), schema.dbname())]
        elif isinstance(context.distinct, (list, set, tuple)):
            on_ = []
            for col in context.distinct:
                column = schema.column(col)
                if not column:
                    raise orb.errors.ColumnNotFound(col)
                else:
                    on_.append(fields.get(col) or '"{0}"."{1}"'.format(schema.dbname(), col.field()))

            cmd = ['SELECT DISTINCT ON ({0}) {1} FROM "{2}"'.format(', '.join(on_),
                                                                    ', '.join(sql_columns['standard']),
                                                                    schema.dbname())]
        else:
            cmd = ['SELECT {0} FROM "{1}"'.format(', '.join(sql_columns['standard']), schema.dbname())]

        # add sql joins to the statement
        if sql_joins:
            cmd += sql_joins

        # join in the i18n table
        if sql_columns['i18n']:
            if context.infalted or context.locale == 'all':
                cmd.append('LEFT JOIN "{0}_i18n" AS "i18n" ON ("i18n"."{0}_id" = "id")'.format(schema.dbname()))
            else:
                sql = 'LEFT JOIN "{0}_i18n" AS "i18n" ON ("i18n"."{0}_id" = "id" AND "i18n"."locale" = %(locale)s)'
                cmd.append(sql.format(schema.dbname()))

        if expanded:
            if sql_order_by:
                distinct = 'ON ({0})'.format(', '.join(order.split(' ')[0] for order in sql_order_by))

            cmd.append('WHERE "{0}"."id" IN (')
            cmd.append('    SELECT DISTINCT {0} "{1}"."id"'.format(distinct, schema.dbname()))

            if sql_columns['i18n']:
                if context.inflated or context.locale == 'all':
                    cmd.append('    LEFT JOIN "{0}_i18n" AS "i18n" ON ("i18n"."{0}_id" = "id")'.format(schema.dbname()))
                else:
                    sql = 'LEFT JOIN "{0}_i18n" AS "i18n" ON ("i18n"."{0}_id" = "id" AND "i18n"."locale" = %(locale)s)'
                    cmd.append('    ' + sql.format(schema.dbname()))

            if sql_where:
                cmd.append('    WHERE {0}'.format(sql_where))
            if sql_group_by:
                cmd.append('    GROUP BY {0}'.format(', '.join(sql_group_by + [order.split(' ')[0] for order in sql_order_by])))
            if sql_order_by:
                cmd.append('    ORDER BY {0}'.format(', '.join(sql_order_by)))
            if context.start:
                if not isinstance(context.limit, (int, long)):
                    raise orb.errors.DatabaseError('Invalid value provided for start')
                cmd.append('    OFFSET {0}'.format(context.start))
            if context.limit > 0:
                if not isinstance(context.limit, (int, long)):
                    raise orb.errors.DatabaseError('Invalid value provided for limit')
                cmd.append('    LIMIT {0}'.format(context.limit))

            cmd.append(')')
            if sql_group_by:
                cmd.append('GROUP BY {0}'.format(', '.join(sql_group_by)))
        else:
            if sql_where:
                cmd.append('WHERE {0}'.format(sql_where))
            if sql_group_by:
                cmd.append('GROUP BY {0}'.format(sql_group_by))
            if sql_order_by:
                cmd.append('ORDER BY {0}'.format(sql_order_by))
            if context.start:
                if not isinstance(context.start, (int, long)):
                    raise orb.errors.DatabaseError('Invalid value provided for start')
                cmd.append('OFFSET {0}'.format(context.start))
            if context.limit > 0:
                if not isinstance(context.limit, (int, long)):
                    raise orb.errors.DatabaseError('Invalid value provided for limit')
                cmd.append('LIMIT {0}'.format(context.limit))

        return '\n'.join(cmd), data

SQLiteStatement.registerAddon('SELECT', SELECT())