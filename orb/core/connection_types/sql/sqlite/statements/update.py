import os
from collections import defaultdict
from projex.lazymodule import lazy_import
from ..sqliteconnection import SQLiteStatement

orb = lazy_import('orb')


class UPDATE(SQLiteStatement):
    def __call__(self, records):
        sql = []
        data = {}

        for record in records:
            if record.isRecord():
                changes = record.changes()
                if changes:
                    sub_sql, sub_data = self.generateCommand(record, changes)
                    sql.append(sub_sql)
                    data.update(sub_data)

        return u'\n'.join(sql), data

    def generateCommand(self, record, changes):
        data = {}
        standard_values = []

        i18n_fields = defaultdict(list)
        i18n_values = defaultdict(list)
        i18n_keys = defaultdict(list)

        for column in changes:
            if column.testFlag(column.Flags.Virtual):
                continue

            if column.testFlag(column.Flags.I18n):
                for record_locale, value in record.get(column, locale='all').items():
                    value_key = '{0}_{1}'.format(column.field(), os.urandom(4).encode('hex'))
                    data[value_key] = column.dbStore('SQLite', value)
                    i18n_values[record_locale].append(u'`{0}` = %({1})s'.format(column.field(), value_key))
                    i18n_fields[record_locale].append(u'`{0}`'.format(column.field()))
                    i18n_keys[record_locale].append(u'%({0})s'.format(value_key))
            else:
                value_key = '{0}_{1}'.format(column.field(), os.urandom(4).encode('hex'))
                data[value_key] = column.dbStore('SQLite', record.get(column))
                standard_values.append('`{0}` = %({1})s'.format(column.field(), value_key))


        id_key = 'id_' + os.urandom(4).encode('hex')
        data[id_key] = record.get(record.schema().idColumn())

        sql = []
        if standard_values:
            standard_sql = (
                u'UPDATE `{table}`\n'
                u'SET {values}\n'
                u'WHERE `{table}`.`{field}` = %({id})s;'
            ).format(table=record.schema().dbname(), id=id_key,
                     values=', '.join(standard_values), field=record.schema().idColumn().field())
            sql.append(standard_sql)

        if i18n_fields:
            for locale in i18n_values:
                i18n_sql = (
                    u'DO $$\n'
                    u'BEGIN\n'
                    u'IF NOT EXISTS (\n'
                    u'  SELECT 1\n'
                    u'  FROM `{table}_i18n`\n'
                    u'  WHERE `{table}_id` = %({id})s AND `locale` = \'{locale}\'\n'
                    u')\n'
                    u'THEN\n'
                    u'  INSERT INTO `{table}_i18n` (`{table}_id`, `locale`, {fields})\n'
                    u'  VALUES (%({id})s, \'{locale}\', {keys});\n'
                    u'ELSE\n'
                    u'  UPDATE `{table}_i18n`\n'
                    u'  SET {values}\n'
                    u'  WHERE `{table}_id` = %({id})s AND `locale` = \'{locale}\';\n'
                    u'END IF;\n'
                    u'END $$;'
                ).format(table=record.schema().dbname(), id=id_key, locale=locale,
                         values=', '.join(i18n_values[locale]),
                         keys=', '.join(i18n_keys[locale]),
                         fields=', '.join(i18n_fields[locale]))
                sql.append(i18n_sql)

        return u'\n'.join(sql), data



SQLiteStatement.registerAddon('UPDATE', UPDATE())
