DELETE FROM "${table}"
WHERE ${WHERE(schema, query, IO=IO)}
RETURNING *;