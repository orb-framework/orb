<%
table_name = schema.tableName()
pcols = ['"{0}"'.format(pcol.fieldName()) for pcol in schema.primaryColumns()]
count = __data__.get('row_count', 0)
%>
% for record, columns in changes:
<%
updates = []
pkey = 'pkey_{0}'.format(count)
__data__['output'][pkey] = record.primaryKey()

for column in columns:
    key = '{0}_{1}'.format(column.name(), count)
    __data__['output'][key] = __sql__.datastore().store(column,
                                              record.recordValue(column.name()))
    updates.append('"{0}" = %({1})s'.format(column.fieldName(), key))

count += 1
%>
UPDATE "${table_name}"
SET ${',\n    '.join(updates)}
WHERE (${','.join(pcols)}) = %(${pkey})s;
% endfor
<% __data__['row_count'] = count %>