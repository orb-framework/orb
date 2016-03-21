from collections import defaultdict
from projex.lazymodule import lazy_import
from ..sqliteconnection import SQLiteStatement

orb = lazy_import('orb')


class INSERT(SQLiteStatement):
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
                    elif not col.testFlag(col.Flags.AutoAssign):
                        standard.append(col)

                schema_meta[schema] = {'i18n': i18n, 'standard': standard}

            if not schema in schema_meta:
                schema_meta[schema] = {'i18n': [], 'standard': []}

            for key, columns in schema_meta[schema].items():
                data.update({'{0}_{1}'.format(col.field(), i): col.dbStore('SQLite', record.get(col)) for col in columns})
                values = ','.join(['%({0}_{1})s'.format(col.field(), i) for col in columns])
                schema_records[schema][key].append(values)

        cmd = []
        for schema, columns in schema_meta.items():
            id_column = schema.idColumn()
            subcmd = ''

            if columns['standard']:
                cols = ', '.join(['`{0}`'.format(col.field()) for col in columns['standard']])
                values = schema_records[schema]['standard']
                subcmd += 'INSERT INTO `{0}` ({1}) VALUES'.format(schema.dbname(), cols)
                for value in values[:-1]:
                    subcmd += '\n({0}),'.format(value)
                subcmd += '\n({0});'.format(values[-1])
                subcmd += '\nSELECT last_insert_rowid() AS `{0}`;'.format(id_column.field())

            elif columns['i18n']:
                subcmd += '\nINSERT INTO `{0}` DEFAULT VALUES;'.format(schema.dbname(), id_column.field())

            if columns['i18n']:
                cols = ', '.join(['`{0}`'.format(col.field()) for col in columns['i18n']])
                values = schema_records[schema]['i18n']
                subcmd += '\nINSERT INTO `{0}_i18n` (`{0}_id`, `locale`, {1}) VALUES'.format(schema.dbname(), cols)
                for i, value in enumerate(values[:-1]):
                    subcmd += '\n(last_insert_rowid() - {0}, %(locale)s, {1}),'.format(len(values) - (i+1), value)
                subcmd += '\n(last_insert_rowid(), %(locale)s, {0});'.format(values[-1])
                subcmd += '\nSELECT last_insert_rowid() AS `{1}`;'.format(schema.dbname(), id_column.field())

            cmd.append(subcmd)

        return '\n'.join(cmd), data

SQLiteStatement.registerAddon('INSERT', INSERT())