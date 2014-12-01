<% pcol = schema.primaryColumns()[0] %>
SELECT "${pcol.fieldName()}"
FROM "${schema.tableName()}"
WHERE "${pcol.fieldName()}" >= LASTVAL()-${count}+1
ORDER BY "${pcol.fieldName()}" ASC;