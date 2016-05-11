from collections import defaultdict
from projex.lazymodule import lazy_import
from ..psqlconnection import PSQLStatement

orb = lazy_import('orb')


class INSERT(PSQLStatement):
    def __call__(self, records):
        # delete based on the collection's context
        if isinstance(records, orb.Collection):
            records = records.records()

        data = {}
        schema_meta = {}
        schema_records = defaultdict(lambda: defaultdict(list))
        for i, record in enumerate(records):
            schema = record.schema()

            # define the
            if not schema in schema_meta:
                i18n = []
                standard = []
                for col in schema.columns().values():
                    if col.testFlag(col.Flags.Virtual):
                        continue
                    if col.testFlag(col.Flags.I18n):
                        i18n.append(col)
                    else:
                        standard.append(col)

                schema_meta[schema] = {'i18n': i18n, 'standard': standard}

            if not schema in schema_meta:
                schema_meta[schema] = {'i18n': [], 'standard': []}

            for key, columns in schema_meta[schema].items():
                record_values = {
                    '{0}_{1}'.format(col.field(), i): col.dbStore('Postgres', record.get(col))
                    for col in columns
                }

                data.update(record_values)

                insert_values = []
                for col in columns:
                    value_key = '{0}_{1}'.format(col.field(), i)
                    if record_values[value_key] == 'DEFAULT':
                        insert_values.append('DEFAULT')
                    else:
                        insert_values.append('%({0})s'.format(value_key))

                schema_records[schema][key].append(','.join(insert_values))

        cmd = []
        for schema, columns in schema_meta.items():
            id_column = schema.idColumn()
            subcmd = ''
            if columns['standard']:
                cols = ', '.join(['"{0}"'.format(col.field()) for col in columns['standard']])
                values = schema_records[schema]['standard']
                subcmd += 'INSERT INTO "{0}"."{1}" ({2}) VALUES'.format(schema.namespace() or 'public',
                                                                        schema.dbname(),
                                                                        cols)
                for value in values[:-1]:
                    subcmd += '\n({0}),'.format(value)
                subcmd += '\n({0})'.format(values[-1])
                subcmd += '\nRETURNING "{0}";'.format(id_column.field())
            elif columns['i18n']:
                subcmd += '\nINSERT INTO "{0}"."{1}" DEFAULT VALUES RETURNING "{2}";'.format(schema.namespace() or 'public',
                                                                                             schema.dbname(),
                                                                                             id_column.field())

            if columns['i18n']:
                cols = ', '.join(['"{0}"'.format(col.field()) for col in columns['i18n']])
                values = schema_records[schema]['i18n']
                subcmd += '\nINSERT INTO "{0}"."{1}_i18n" ("{1}_id", "locale", {2}) VALUES'.format(schema.namespace() or 'public',
                                                                                                   schema.dbname(),
                                                                                                   cols)
                for i, value in enumerate(values[:-1]):
                    value_key = '{0}_{1}'.format(id_column.field(), i)
                    id_value = data[value_key]
                    if id_value == 'DEFAULT':
                        id_value = 'LASTVAL() - {0}'.format(len(values) - (i+1))

                    subcmd += '\n({0}, %(locale)s, {1}),'.format(id_value, value)

                value_key = '{0}_{1}'.format(id_column.field(), i)
                id_value = data[value_key]
                if id_value == 'DEFAULT':
                    id_value = 'LASTVAL()'

                subcmd += '\n({0}, %(locale)s, {1})'.format(id_value, values[-1])
                subcmd += '\nRETURNING "{0}_id" AS "{1}";'.format(schema.dbname(), id_column.field())

            cmd.append(subcmd)

        return '\n'.join(cmd), data

PSQLStatement.registerAddon('INSERT', INSERT())