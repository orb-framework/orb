<%
WHERE = __sql__.byName('WHERE')
where_sql, _ = WHERE(where, __data__=__data__)
%>

DELETE FROM "${table.schema().tableName()}"
WHERE ${where_sql}
RETURNING *;