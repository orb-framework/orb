<%
WHERE = SQL.byName('WHERE')

aggr = column.aggregator()
src_table = column.schema().dbname()
ref_col = aggr.referenceColumn()
ref_table = ref_col.schema().dbname()
targ_col = aggr.targetColumn()

query = aggr.where(column)
if query is not None:
    where = WHERE(ref_col.schema(), query, GLOBALS=GLOBALS, IO=IO)
else:
    where = None

pcols = []
for pcol in column.schema().primaryColumns():
  pcols.append('`{0}`.`{1}`'.format(src_table, pcol.fieldName()))

# create the join table name
join_count = GLOBALS.get('join_count', 0)
join_count += 1
join_table = 'join_{0}'.format(join_count)

join_col = 'SUM(COALESCE(`{0}`.`{1}`,0)) AS `{1}`'.format(join_table,
                                                          column.name())

GLOBALS['join_count'] = join_count
GLOBALS['join_table'] = join_table
GLOBALS['join_column'] = join_col
GLOBALS.setdefault('field_mapper', {})

agg_type = aggr.aggregateType()

if agg_type == orb.QueryAggregate.Type.Count:
    command = 'COUNT(*)'
elif agg_type == orb.QueryAggregate.Type.Sum:
    command = 'SUM(`{0}`)'.format(targ_col.fieldName())
elif agg_type == orb.QueryAggregate.Type.Maximum:
    command = 'MAX(`{0}`)'.format(targ_col.fieldName())
elif agg_type == orb.QueryAggregate.Type.Minimum:
    command = 'MIN(`{0}`)'.format(targ_col.fieldName())
else:
    raise orb.QueryInvalid('Invalid aggregate type.')

GLOBALS['field_mapper'][column] = '`{0}`.`{1}`'.format(join_table, column.name())
%>

LEFT JOIN (
    SELECT `${ref_col.fieldName()}`, ${command} AS `${column.name()}`
    FROM `${ref_table}`
    % if where:
    WHERE ${where}
    % endif
    GROUP BY `${ref_col.fieldName()}`
) AS `${join_table}`
  ON `${join_table}`.`${ref_col.fieldName()}` = (${','.join(pcols)})
