DELETE FROM "${table}"
WHERE ${where}
RETURNING *;