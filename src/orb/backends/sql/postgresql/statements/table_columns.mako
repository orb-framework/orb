SELECT column_name
FROM information_schema.columns
WHERE table_schema='public' AND
      (table_name='${table.schema().tableName()}' OR
       table_name='${table.schema().tableName()}_translation');