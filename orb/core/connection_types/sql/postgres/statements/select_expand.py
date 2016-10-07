import os
import projex.text

from projex.lazymodule import lazy_import
from ..psqlconnection import PSQLStatement

orb = lazy_import('orb')


class SELECT_EXPAND(PSQLStatement):
    def generateSubTree(self, model, tree):
        schema = model.schema()

        expand_col = self.byName('SELECT EXPAND COLUMN')
        expand_pipe = self.byName('SELECT EXPAND PIPE')
        expand_rev = self.byName('SELECT EXPAND REVERSE')

        for name, sub_tree in tree.items():
            # cannot expand these keywords, they are reserved
            if name in ('ids', 'count', 'records'):
                continue

            # these are expanded via the sub-tree
            elif name in ('first', 'last'):
                for child in self.generateSubTree(model, sub_tree):
                    yield child

            # otherwise, lookup a column, pipe or reverse lookup
            else:
                column = schema.column(name, raise_=False)
                if column:
                    if not column.testFlag(column.Flags.Virtual) or issubclass(model, orb.View):
                        yield expand_col, column, sub_tree
                else:
                    collector = schema.collector(name)
                    if collector:
                        if not collector.testFlag(collector.Flags.Virtual):
                            if isinstance(collector, orb.Pipe):
                                yield expand_pipe, collector, sub_tree
                            elif isinstance(collector, orb.ReverseLookup):
                                yield expand_rev, collector, sub_tree
                    else:
                        raise orb.errors.ColumnNotFound(schema=schema, column=name)

    def collectSubTree(self, model, tree, alias='', context=None):
        sql = []
        data = {}
        for action, obj, sub_tree in self.generateSubTree(model, tree):
            sub_sql, sub_data = action(obj, sub_tree, alias=alias, context=context)
            if sub_sql:
                sql.append(sub_sql)
                data.update(sub_data)

        if sql:
            return u',\n' + u',\n'.join(sql), data
        else:
            return u'', data


class SELECT_EXPAND_COLUMN(SELECT_EXPAND):
    def __call__(self, column, tree, alias='', context=None):
        data = {}
        target = column.referenceModel()
        translation_columns = target.schema().columns(flags=orb.Column.Flags.I18n).values()
        target_name = projex.text.underscore(column.name())
        target_alias = '{0}_table'.format(target_name)
        target_id_field = target.schema().idColumn().field()

        # get the base table query
        target_q = target.baseQuery(context=context)
        if target_q:
            WHERE = self.byName('WHERE')
            filter_sql, filter_data = WHERE(target, target_q, context, aliases={target: target_alias})
            if filter_sql:
                data.update(filter_data)
                target_base_where = '({0}) AND '.format(filter_sql)
            else:
                target_base_where = ''
        else:
            target_base_where = ''

        # setup the base information
        target_data = u'"{0}".*'.format(target_alias)
        target_i18n = u''
        target_i18n_grouping = u''

        # be able to filter on
        if translation_columns:
            target_data_sql = u'(coalesce((array_agg("{0}_i18n"."{1}"))[1], ' \
                              u'(array_agg("{0}_i18n_default"."{1}"))[1])) AS "{1}"'
            target_data_options = [target_data]
            target_data_options += [
                target_data_sql.format(target_alias, c.field())
                for c in translation_columns
            ]
            target_data = ','.join(target_data_options)
            target_id_field = target.schema().idColumn().field()

            target_i18n_grouping = u"""
            GROUP BY "{target_alias}"."{target_id_field}"
            ORDER BY "{target_alias}"."{target_id_field}"
            """.format(target_alias=target_alias, target_id_field=target_id_field)

            # include translated columns
            target_i18n = u"""
            LEFT JOIN "{target_namespace}"."{target}_i18n" AS "{target_alias}_i18n" ON
                "{target_alias}"."{target_id_field}"    = "{target_alias}_i18n"."{target}_id" AND
                "{target_alias}_i18n"."locale"          = %(locale)s

            LEFT JOIN "{target_namespace}"."{target}_i18n" AS "{target_alias}_i18n_default" ON
                "{target_alias}"."{target_id_field}"    = "{target_alias}_i18n_default"."{target}_id" AND
                "{target_alias}_i18n_default"."locale"  = %(default_locale)s

            """

            target_i18n = target_i18n.format(target=target.schema().dbname(),
                                             target_namespace=target.schema().namespace() or 'public',
                                             target_alias=target_alias,
                                             target_id_field=target_id_field)


        target_expand, target_expand_data = self.collectSubTree(target, tree, alias=target_alias, context=context)
        data.update(target_expand_data)

        # generate the sql options
        sql_options = {
            'target_name': target_name,
            'target_data': target_data,
            'target_expand': target_expand,
            'target_alias': target_alias,
            'target_id_field': target_id_field,
            'target_base_where': target_base_where,
            'target_i18n': target_i18n,
            'target_i18n_grouping': target_i18n_grouping,
            'target_namespace': target.schema().namespace() or 'public',
            'target_table': target.schema().dbname(),
            'source_table': alias or column.schema().dbname(),
            'source_field': column.field()
        }

        # generate the sql
        sql = (
            u'(\n'
            u'  SELECT row_to_json({target_name}_row) FROM\n'
            u'  (\n'
            u'      SELECT {target_data} {target_expand}\n'
            u'      FROM "{target_namespace}"."{target_table}" AS "{target_alias}"\n'
            u'      {target_i18n}\n'
            u'      WHERE {target_base_where} "{target_alias}"."{target_id_field}" = "{source_table}"."{source_field}"\n'
            u'      {target_i18n_grouping}'
            u'  ) AS {target_name}_row\n'
            u') AS "{target_name}"'
        ).format(**sql_options)

        return sql, data


class SELECT_EXPAND_REVERSE(SELECT_EXPAND):
    def __call__(self, reversed, tree, alias='', context=None):
        data = {}
        target = reversed.referenceModel()
        source = reversed.schema().model()

        target_name = projex.text.underscore(reversed.name())
        target_records_alias = '{0}_records'.format(target_name)
        target_alias = '{0}_table'.format(target_name)
        has_translations = target.schema().hasTranslations()
        target_fields = []

        # get the base table query
        target_q = target.baseQuery(context=context)
        if target_q:
            WHERE = self.byName('WHERE')
            filter_sql, filter_data = WHERE(target, target_q, context, aliases={target: target_alias})
            if filter_sql:
                data.update(filter_data)
                target_base_where = '({0}) AND '.format(filter_sql)
            else:
                target_base_where = ''
        else:
            target_base_where = ''

        # collect keywords
        if 'ids' in tree:
            target_fields.append(u'array_agg({0}.{1}) AS ids'.format(target_records_alias, target.schema().idColumn().field()))
        if 'count' in tree:
            target_fields.append(u'count({0}.*) AS count'.format(target_records_alias))
        if 'first' in tree:
            target_fields.append(u'(array_agg(row_to_json({0}.*)))[1] AS first'.format(target_records_alias))
        if 'last' in tree:
            target_fields.append(u'(array_agg(row_to_json({0}.*)))[1][count({0}.*)] AS last'.format(target_records_alias))
        if 'records' in tree or not target_fields:
            target_fields.append(u'array_agg(row_to_json({0}.*)) AS records'.format(target_records_alias))

        if has_translations:
            target_data = u'"{target_alias}".*, "{target_alias}_i18n".*'.format(target_alias=target_alias)
            target_i18n = u'LEFT JOIN "{namespace}"."{table}_i18n" AS "{target_alias}_i18n" ON ' \
                          u'"{target_alias}"."{target_id_field}" = "{target_alias}_i18n"."{table}_id" AND ' \
                          u'"{target_alias}_i18n"."locale" = %(locale)s'
            target_i18n = target_i18n.format(target_alias=target_alias,
                                             namespace=target.schema().namespace() or 'public',
                                             table=target.schema().dbname())
        else:
            target_data = u'"{target_alias}".*'.format(target_alias=target_alias)
            target_i18n = ''

        target_expand, target_expand_data = self.collectSubTree(target, tree, alias=target_alias, context=context)
        data.update(target_expand_data)

        # define the sql options
        sql_options = {
            'target_name': target_name,
            'target_alias': target_alias,
            'target_data': target_data,
            'target_i18n': target_i18n,
            'target_base_where': target_base_where,
            'target_fields': u', '.join(target_fields),
            'target_table': target.schema().dbname(),
            'target_namespace': target.schema().namespace() or 'public',
            'target_id_field': target.schema().idColumn().field(),
            'target_expand': target_expand,
            'target_records_alias': target_records_alias,
            'source_table': alias or source.schema().dbname(),
            'source_field': reversed.targetColumn().field(),
            'source_id_field': source.schema().idColumn().field(),
            'limit_if_unique': 'LIMIT 1' if reversed.testFlag(reversed.Flags.Unique) else ''
        }

        # define the sql
        sql = (
            u'(\n'
            u'  SELECT row_to_json({target_name}_row) FROM (\n'
            u'      SELECT {target_fields}\n'
            u'      FROM (\n'
            u'          SELECT {target_data} {target_expand}\n'
            u'          FROM "{target_namespace}"."{target_table}" AS "{target_alias}"\n'
            u'          {target_i18n}\n'
            u'          WHERE {target_base_where} "{target_alias}"."{source_field}" = "{source_table}"."{source_id_field}"\n'
            u'          {limit_if_unique}'
            u'      ) AS {target_records_alias}\n'
            u'  ) AS {target_name}_row\n'
            u') AS "{target_name}"'
        ).format(**sql_options)

        return sql, data


class SELECT_EXPAND_PIPE(SELECT_EXPAND):
    def __call__(self, pipe, tree, alias='', context=None):
        WHERE = self.byName('WHERE')

        data = {}
        source = pipe.fromModel()

        source_col = pipe.fromColumn()
        through = pipe.throughModel()
        target_col = pipe.toColumn()

        target = pipe.toModel()
        target_records_alias = projex.text.underscore(pipe.name()) + '_records'

        target_fields = []

        # collect keywords
        if 'ids' in tree:
            target_fields.append(u'array_agg({0}.{1}) AS ids'.format(target_records_alias, target.schema().idColumn().field()))
        if 'count' in tree:
            target_fields.append(u'count({0}.*) AS count'.format(target_records_alias))
        if 'first' in tree:
            target_fields.append(u'(array_agg(row_to_json({0}.*)))[1] AS first'.format(target_records_alias))
        if 'last' in tree:
            target_fields.append(u'(array_agg(row_to_json({0}.*)))[1][count({0}.*)] AS last'.format(target_records_alias))
        if 'records' in tree or not target_fields:
            target_fields.append(u'array_agg(row_to_json({0}.*)) AS records'.format(target_records_alias))

        # define the sql options
        target_name = projex.text.underscore(pipe.name())
        target_alias = '{0}_table'.format(target_name)
        has_translations = target.schema().hasTranslations()

        # include the base table's filter, if one exists
        target_q = target.baseQuery(context=context)
        if target_q is not None:
            filter_sql, filter_data = WHERE(target, target_q, context, aliases={target: target_alias})
            if filter_sql:
                data.update(filter_data)
                target_base_where = '({0}) AND '.format(filter_sql)
            else:
                target_base_where = ''
        else:
            target_base_where = ''

        if has_translations:
            target_data = u'"{target_alias}".*, "{target_alias}_i18n".*'.format(target_alias=target_alias)
            target_i18n = u'LEFT JOIN "{namespace}"."{table}_i18n" AS "{target_alias}_i18n" ON ' \
                          u'"{target_alias}"."{target_id_field}" = "{target_alias}_i18n"."{table}_id" AND ' \
                          u'"{target_alias}_i18n"."locale" = %(locale)s'
            target_i18n = target_i18n.format(target_alias=target_alias,
                                             namespace=target.schema().namespace() or 'public',
                                             table=target.schema().dbname(),
                                             target_id_field=target.schema().idColumn().field())
        else:
            target_data = u'"{target_alias}".*'.format(target_alias=target_alias)
            target_i18n = ''

        target_expand, target_expand_data = self.collectSubTree(target, tree, alias=target_alias, context=context)
        data.update(target_expand_data)

        # define the sql options
        sql_options = {
            'target_fields': u', '.join(target_fields),
            'target_expand': target_expand,
            'target_data': target_data,
            'target_base_where': target_base_where,
            'target_alias': target_alias,
            'target_name': target_name,
            'target_namespace': target.schema().namespace() or 'public',
            'target_table': target.schema().dbname(),
            'target_i18n': target_i18n,
            'through_table': through.schema().dbname(),
            'through_namespace': through.schema().namespace() or 'public',
            'target_id_field': target.schema().idColumn().field(),
            'target_field': target_col.field(),
            'target_records_alias': target_records_alias,
            'source_table': alias or source.schema().dbname(),
            'source_id_field': source.schema().idColumn().field(),
            'source_field': source_col.field(),
            'limit_if_unique': 'LIMIT 1' if pipe.testFlag(pipe.Flags.Unique) else ''
        }

        # define the sql
        sql = (
            u'(\n'
            u'  SELECT row_to_json({target_name}_row) FROM ('
            u'      SELECT {target_fields}\n'
            u'      FROM (\n'
            u'          SELECT {target_data} {target_expand}\n'
            u'          FROM "{target_namespace}"."{target_table}" AS "{target_alias}"\n'
            u'          {target_i18n}\n'
            u'          WHERE {target_base_where} "{target_alias}"."{target_id_field}" IN (\n'
            u'              SELECT DISTINCT ON (t."{target_field}") t."{target_field}"\n'
            u'              FROM "{through_namespace}"."{through_table}" AS t\n'
            u'              WHERE t."{source_field}" = "{source_table}"."{source_id_field}"\n'
            u'              {limit_if_unique}\n'
            u'          )\n'
            u'      ) {target_records_alias}\n'
            u'  ) {target_name}_row\n'
            u') AS "{target_name}"'
        ).format(**sql_options)

        return sql, data

PSQLStatement.registerAddon('SELECT EXPAND COLUMN', SELECT_EXPAND_COLUMN())
PSQLStatement.registerAddon('SELECT EXPAND REVERSE', SELECT_EXPAND_REVERSE())
PSQLStatement.registerAddon('SELECT EXPAND PIPE', SELECT_EXPAND_PIPE())
