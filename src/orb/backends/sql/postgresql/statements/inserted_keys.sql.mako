SELECT "${field}"
FROM "${table}"
WHERE "${field}" >= LASTVAL()-${count}+1
ORDER BY "${field}" ASC;