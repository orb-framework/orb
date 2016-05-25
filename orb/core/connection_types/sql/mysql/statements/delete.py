from collections import defaultdict
from projex.lazymodule import lazy_import
from ..mysqlconnection import MySQLStatement

orb = lazy_import('orb')


class DELETE(MySQLStatement):
    def __call__(self, records, data=None):
        # delete based on the collection's context
        if isinstance(records, orb.Collection) and not records.isLoaded():
            model = records.model()
            context = records.context()

            if context.where is not None:
                WHERE = self.byName('WHERE')
                where, data = WHERE(model, context.where)
            else:
                where, data = '', {}

            sql_options = {
                'namespace': model.schema().namespace() or context.db.name(),
                'table': model.schema().dbname(),
                'id_col': model.schema().idColumn().field(),
                'where': 'WHERE {0}'.format(where) if where else ''
            }
            sql = (
                u'DELETE FROM `{namespace}`.`{table}`\n'
                u'{where};'
            ).format(**sql_options)

            if model.schema().columns(flags=orb.Column.Flags.I18n):
                i18n_sql = (
                    u'DELETE FROM `{namespace}`.`{table}_i18n`\n'
                    u'WHERE `{table}_id` IN (\n'
                    u'    SELECT `{id_col}` FROM {table}'
                    u'    {where}'
                    u');\n'
                ).format(**sql_options)
                sql = i18n_sql + sql

            records.clear()
            return sql, data

        # otherwise, delete based on the record's ids
        else:
            delete_info = defaultdict(list)
            for record in records:
                schema = record.schema()
                delete_info[schema].append(record.get(record.schema().idColumn()))

            data = {}
            sql = []
            default_namespace = orb.Context().db.name()
            for schema, ids in delete_info.items():
                schema_sql = u'DELETE FROM `{0}`.`{1}` WHERE {2} IN %({1}_ids)s;'
                schema_sql = schema_sql.format(schema.namespace() or default_namespace,
                                               schema.dbname(),
                                               schema.idColumn().field())
                if schema.columns(flags=orb.Column.Flags.I18n):
                    i18n_sql = u'DELETE FROM `{0}`.`{1}_i18n` WHERE `{1}_id` IN %({1}_ids)s;'.format(schema.namespace(),
                                                                                                     schema.dbname())
                    schema_sql = i18n_sql + schema_sql
                sql.append(schema_sql)
                data[schema.dbname() + '_ids'] = tuple(ids)

            return u'\n'.join(sql), data

MySQLStatement.registerAddon('DELETE', DELETE())