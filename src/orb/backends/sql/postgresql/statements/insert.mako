<%
count = __data__.get('row_count', 0)
insertions = []
for record in records:
    values = record.recordValues()
    insertion = []
    for column in columns:
        key = '{0}_{1}'.format(column.name(), count)
        __data__['output'][key] = __sql__.datastore().store(column, values[column.name()])
        insertion.append('%({0})s'.format(key))
    
    insertions.append(','.join(insertion))
    count += 1
__data__['row_count'] = count
%>
INSERT INTO "${table.schema().tableName()}"
    ${',\n    '.join(['"{0}"'.format(column.fieldName()) for column in columns])}
VALUES
    $(',\n    '.join(inertions)};