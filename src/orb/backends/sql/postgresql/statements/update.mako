<%
table_name = schema.tableName()
pcols = ['"{0}"'.format(pcol.fieldName()) for pcol in schema.primaryColumns()]
%>
% for record, columns in changes:
<%
updates = []
pkey = str(len(__data__['output']))
__data__['output'][pkey] = record.primaryKey()

for column in columns:
    key = str(len(__data__['output']))
    __data__['output'][key] = __sql__.datastore().store(column,
                                              record.recordValue(column.name()))
    updates.append('"{0}" = %({1})s'.format(column.fieldName(), key))
%>
UPDATE "${table_name}"
SET ${',\n    '.join(updates)}
WHERE (${','.join(pcols)}) = %(${pkey})s;
% endfor