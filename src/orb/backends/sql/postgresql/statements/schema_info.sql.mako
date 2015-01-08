SELECT t.table_name AS name,
    (
        SELECT  array_agg(c.column_name::varchar)
        FROM    information_schema.columns AS c
        WHERE   c.table_schema = '${namespace}'
        AND     c.table_name IN (t.table_name, t.table_name || '_i18n')
    ) AS "columns",
    (
        SELECT  array_agg(i.indexname)
        FROM    pg_indexes AS i
        WHERE   i.schemaname = '${namespace}'
        AND     i.tablename IN (t.table_name, t.table_name || '_i18n')
    ) AS "indexes"
FROM    information_schema.tables AS t
WHERE   t.table_schema = '${namespace}'
AND     t.table_name NOT ILIKE '%%_i18n';