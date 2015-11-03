DELETE FROM `${table}`
% if where:
WHERE ${where}
% endif
RETURNING *;