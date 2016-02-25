from collections import defaultdict
from projex.lazymodule import lazy_import
from ..psqlconnection import PSQLStatement

orb = lazy_import('orb')


class DELETE(PSQLStatement):
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
                'table': model.schema().dbname(),
                'where': 'WHERE {0}'.format(where) if where else ''
            }
            sql = (
                u'DELETE FROM "{table}"\n'
                u'{where}'
                u'RETURNING *;'
            ).format(**sql_options)

            records.clear()

            print sql, data

            return sql, data

        # otherwise, delete based on the record's ids
        else:
            delete_info = defaultdict(list)
            for record in records:
                schema = record.schema()
                delete_info[schema].append(record.id())

            data = {}
            sql = []
            for schema, ids in delete_info.items():
                sql.append(u'DELETE FROM "{0}" WHERE id IN %({0}_ids)s RETURNING *;'.format(schema.dbname()))
                data[schema.dbname() + '_ids'] = tuple(ids)

            return u'\n'.join(sql), data

PSQLStatement.registerAddon('DELETE', DELETE())