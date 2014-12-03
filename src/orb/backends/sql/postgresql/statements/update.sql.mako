<%
table_name = schema.tableName()
pcols = ['"{0}"'.format(pcol.fieldName()) for pcol in schema.primaryColumns()]
%>
% for record, columns in changes:
<%
updates = []
translation_updates = {}
pkey = str(len(IO))
IO[pkey] = record.primaryKey()

for column in columns:
    if column.isTranslatable():
        for locale, value in record.recordValue(column.name(), locale='all').items():
            key = str(len(IO))
            IO[key] = SQL.datastore().store(column, value)
            translation_updates.setdefault(locale, [])
            translation_updates[locale].append('"{0}" = %({1})s'.format(column.fieldName(), key))
    else:
        key = str(len(IO))
        IO[key] = SQL.datastore().store(column,
                                                  record.recordValue(column.name()))
        updates.append('"{0}" = %({1})s'.format(column.fieldName(), key))
%>
% if updates:
UPDATE "${table_name}"
SET ${',\n    '.join(updates)}
WHERE (${','.join(pcols)}) = %(${pkey})s;
% endif
% if translation_updates:
% for locale, updates in translation_updates.items():
UPDATE "${table_name}_i18n"
SET ${',\n    '.join(updates)}
WHERE "${table_name}_id" = %(${pkey})s AND "locale" = '${locale}';
% endfor
% endif
% endfor