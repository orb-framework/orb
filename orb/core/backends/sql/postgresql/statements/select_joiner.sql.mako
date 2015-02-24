<%
WHERE = SQL.byName('WHERE')

joiner = column.joiner()
src_table = column.schema().dbname()
ref_col = joiner.referenceColumn()
ref_table = ref_col.schema().dbname()
targ_col = joiner.targetColumn()

query = joiner.where(column)
if query is not None:
    where = WHERE(ref_col.schema(), query, GLOBALS=GLOBALS, IO=IO)
else:
    where = None

pcols = []
for pcol in column.schema().primaryColumns():
  pcols.append('"{0}"."{1}"'.format(src_table, pcol.fieldName()))

# create the join table name
join_count = GLOBALS.get('join_count', 0)
join_count += 1
join_table = 'join_{0}'.format(join_count)

join_col = '(ARRAY_AGG("{0}"."{1}"))[1] AS "{2}"'.format(join_table,
                                                         column.name(),
                                                         column.name())

GLOBALS.setdefault('field_mapper', {})
GLOBALS['join_count'] = join_count
GLOBALS['join_table'] = join_table
GLOBALS['join_column'] = join_col
GLOBALS['field_mapper'][column] = '"{0}"."{1}"'.format(join_table,
                                                        column.name())
%>

LEFT JOIN (
    SELECT DISTINCT ON ("${ref_col.fieldName()}")
      "${ref_col.fieldName()}", "${targ_col.fieldName()}" AS "${column.name()}"
    FROM "${ref_table}"
    % if where:
    WHERE ${where}
    % endif
) AS "${join_table}"
  ON "${join_table}"."${ref_col.fieldName()}" = (${','.join(pcols)})
