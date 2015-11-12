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
            if context.where:
                WHERE = self.byName('WHERE')
                where, data = WHERE(context.where)
            else:
                where, data = '', {}

            if where:
                return 'DELETE FROM "{0}" WHERE {1} RETURNING *;'.format(model.schema().dbname(), where), data
            else:
                return 'DELETE FROM "{0}" RETURNING *;'.format(model.schema().dbname()), {}

        # otherwise, delete based on the record's ids
        else:
            delete_info = defaultdict(list)
            for record in records:
                schema = record.schema()
                delete_info.setdefault(schema, {})
                delete_info[schema].append(record.id())

            data = {}
            cmd = []
            for schema, ids in delete_info.items():
                cmd.append('DELETE FROM "{0}" WHERE id IN %({0}_ids)s RETURNING *;'.format(schema.dbname()))
                data[schema.dbname() + '_ids'] = tuple(ids)

            return '\n'.join(cmd), data

PSQLStatement.registerAddon('DELETE', DELETE())