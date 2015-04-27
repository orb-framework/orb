<%
table_name = schema.dbname()
pcols = ['`{0}`'.format(pcol.fieldName()) for pcol in schema.primaryColumns()]
%>
% for record, columns in changes:
<%
updates = []
translation_updates = {}
translation_columns = {}
translation_values = {}
pkey = str(len(IO))
IO[pkey] = record.primaryKey()

for column in columns:
    if column.isTranslatable():
        for locale, value in record.recordValue(column.name(), locale='all').items():
            key = str(len(IO))
            IO[key] = SQL.datastore().store(column, value)

            translation_updates.setdefault(locale, [])
            translation_columns.setdefault(locale, [])
            translation_values.setdefault(locale, [])

            translation_updates[locale].append('`{0}` = %({1})s'.format(column.fieldName(), key))
            translation_columns[locale].append('`{0}`'.format(column.fieldName()))
            translation_values[locale].append('%({0})s'.format(key))
    else:
        key = str(len(IO))
        IO[key] = SQL.datastore().store(column,
                                                  record.recordValue(column.name()))
        updates.append('`{0}` = %({1})s'.format(column.fieldName(), key))
%>
% if updates:
UPDATE `${table_name}`
SET ${',\n    '.join(updates)}
WHERE (${','.join(pcols)}) = %(${pkey})s;
% endif
% if translation_updates:
% for locale in translation_updates:
DO $$
BEGIN
IF NOT EXISTS (
    SELECT  1
    FROM `${table_name}_i18n`
    WHERE `${table_name}_id` = %(${pkey})s AND `locale` = '${locale}'
)
THEN
    INSERT INTO `${table_name}_i18n` (`${table_name}_id`, `locale`, ${', '.join(translation_columns[locale])})
    VALUES (%(${pkey})s, '${locale}', ${', '.join(translation_values[locale])});
ELSE
    UPDATE `${table_name}_i18n`
    SET ${',\n    '.join(translation_updates[locale])}
    WHERE `${table_name}_id` = %(${pkey})s AND `locale` = '${locale}';
END IF;
END$$;
% endfor
% endif
% endfor