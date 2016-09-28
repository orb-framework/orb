from collections import defaultdict
from projex.lazymodule import lazy_import
from ..sqliteconnection import SQLiteStatement

orb = lazy_import('orb')


class SELECT(SQLiteStatement):
    def cmpcol(self, col_a, col_b):
        return cmp(col_a.field(), col_b.field())

    def __call__(self, model, context, fields=None):
        WHERE = self.byName('WHERE')

        # generate the where query
        where = context.where
        if context.useBaseQuery:
            base_where = model.baseQuery(context=context)
            if base_where:
                where = base_where & where

        # determine what to expand
        schema = model.schema()
        columns = [schema.column(c) for c in context.columns] if context.columns else schema.columns().values()

        data = {
            'locale': context.locale,
            'default_locale': orb.system.settings().default_locale
        }
        fields = fields or {}
        sql_group_by = []
        sql_columns = defaultdict(list)
        sql_joins = []

        # process columns to select
        for column in sorted(columns, self.cmpcol):
            if column.testFlag(column.Flags.Virtual) and not issubclass(model, orb.View):
                continue

            if column.testFlag(column.Flags.I18n):
                if context.locale == 'all':
                    sql = u'hstore_agg(hstore(`i18n`.`locale`, `i18n`.`{0}`)) AS `{0}`'
                elif data['locale'] == data['default_locale'] or column.testFlag(column.Flags.I18n_NoDefault):
                    sql = u'(array_agg(`i18n`.`{0}`))[1] AS `{0}`'
                else:
                    sql = u'(coalesce((array_agg(`i18n`.`{0}`))[1], (array_agg(`i18n_default`.`{0}`))[1])) AS `{0}`'

                sql_columns['i18n'].append(sql.format(column.field()))
                sql_group_by.append(u'`{0}`.`id`'.format(schema.dbname()))
                fields[column] = u'`i18n`.`{0}`'.format(column.field())
            else:
                # select the base record
                sql_columns['standard'].append(u'`{0}`.`{1}` AS `{1}`'.format(schema.dbname(),
                                                                             column.field(),
                                                                             column.field()))

        # generate sql ordering
        sql_order_by = []
        if context.order:
            for col, dir in context.order:
                column = schema.column(col)
                if not column:
                    raise orb.errors.ColumnNotFound(schema=schema, column=col)

                field = fields.get(column) or u'`{0}`.`{1}`'.format(schema.dbname(), column.field())
                if sql_group_by:
                    sql_group_by.append(field)
                sql_order_by.append(u'{0} {1}'.format(field, dir.upper()))

        if context.distinct is True:
            cmd = ['SELECT DISTINCT {0} FROM `{1}`'.format(', '.join(sql_columns['standard'] + sql_columns['i18n']), schema.dbname())]
        elif isinstance(context.distinct, (list, set, tuple)):
            on_ = []
            for col in context.distinct:
                column = schema.column(col)
                if not column:
                    raise orb.errors.ColumnNotFound(schema=schema, column=col)
                else:
                    on_.append(fields.get(col) or u'`{0}`.`{1}`'.format(schema.dbname(), col.field()))

            cmd = [u'SELECT MIN({0}) {1} FROM `{2}`'.format(', '.join(on_),
                                                                    ', '.join(sql_columns['standard'] + sql_columns['i18n']),
                                                                    schema.dbname())]
            sql_group_by.append(on_)
        else:
            cmd = [u'SELECT {0} FROM `{1}`'.format(', '.join(sql_columns['standard'] + sql_columns['i18n']), schema.dbname())]

        # add sql joins to the statement
        if sql_joins:
            cmd += sql_joins

        # join in the i18n table
        if sql_columns['i18n']:
            if context.locale == 'all':
                cmd.append(u'LEFT JOIN `{0}_i18n` AS `i18n` ON (`i18n`.`{0}_id` = `id`)'.format(schema.dbname()))
            else:
                sql = u'LEFT JOIN `{0}_i18n` AS `i18n` ON (`i18n`.`{0}_id` = `id` AND `i18n`.`locale` = %(locale)s)'
                if data['locale'] != data['default_locale']:
                    sql += u'\nLEFT JOIN `{0}_i18n` AS `i18n_default` ON (`i18n_default`.`{0}_id` = `id` AND `i18n_default`.`locale` = %(default_locale)s)'
                cmd.append(sql.format(schema.dbname()))

        # generate sql statements
        try:
            sql_where, sql_where_data = WHERE(model, where, fields=fields)
        except orb.errors.QueryIsNull:
            sql_where, sql_where_data = '', {}
        else:
            data.update(sql_where_data)

        if sql_where:
            cmd.append(u'WHERE {0}'.format(sql_where))
        if sql_group_by:
            cmd.append(u'GROUP BY {0}'.format(', '.join(sql_group_by)))
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

SQLiteStatement.registerAddon('SELECT', SELECT())