<%
insert_columns = [column for column in columns if not column.autoIncrement()]
insertions = []
for record in records:
    values = record.recordValues()
    insertion = []
    for column in insert_columns:
        key = len(__data__['output'])
        insertion.append('%({0})s'.format(key))
        __data__['output'][str(key)] = __sql__.datastore().store(column, values[column.name()])
    
    insertions.append(','.join(insertion))
%>
INSERT INTO "${schema.tableName()}" (
    ${','.join(['"{0}"'.format(column.fieldName()) for column in insert_columns])}
)
VALUES (
    ${',\n    '.join(insertions)}
);