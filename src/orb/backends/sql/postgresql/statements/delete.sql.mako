DELETE FROM "${table}"
WHERE ${WHERE(query, baseSchema=schema, IO=IO)}
RETURNING *;