SELECT column_name
FROM information_schema.columns
WHERE table_schema='public' AND table_name IN ('${schema.tableName()}', '${schema.tableName()}_translation');