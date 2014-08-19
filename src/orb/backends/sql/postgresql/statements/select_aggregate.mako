<%
WHERE = __sql__.byName('WHERE')

aggr = column.aggregator()
src_table = column.schema().tableName()
ref_col = aggr.referenceColumn()
ref_table = ref_col.schema().tableName()
targ_col = aggr.targetColumn()

query = aggr.where(column)
traversals = __data__.get('traversal', [])
__data__['traversal'] = []
if query is not None:
    where = WHERE(query,
                  baseSchema=ref_col.schema(),
                  __data__=__data__)[0]
else:
    where = None
new_traversals = __data__.get('traversal', [])
__data__['traversal'] = traversals

pcols = []
for pcol in column.schema().primaryColumns():
  pcols.append('"{0}"."{1}"'.format(src_table, pcol.fieldName()))

# create the join table name
join_count = __data__.get('join_count', 0)
join_count += 1
join_table = 'join_{0}'.format(join_count)

join_col = 'SUM(COALESCE("{0}"."{1}",0)) AS "{1}"'.format(join_table,
                                                          column.name())

__data__['join_count'] = join_count
__data__['join_table'] = join_table
__data__['join_column'] = join_col
__data__.setdefault('field_mapper', {})

agg_type = aggr.aggregateType()

if agg_type == orb.QueryAggregate.Type.Count:
    command = 'COUNT(*)'
elif agg_type == orb.QueryAggregate.Type.Sum:
    command = 'SUM("{0}")'.format(targ_col.fieldName())
elif agg_type == orb.QueryAggregate.Type.Maximum:
    command = 'MAX("{0}")'.format(targ_col.fieldName())
elif agg_type == orb.QueryAggregate.Type.Minimum:
    command = 'MIN("{0}")'.format(targ_col.fieldName())
else:
    raise orb.DatabaseQueryError('Invalid aggregate type.')

__data__['field_mapper'][column] = '"{0}"."{1}"'.format(join_table,
                                                        column.name())
%>

LEFT JOIN (
    SELECT "${ref_col.fieldName()}", ${command} AS "${column.name()}"
    FROM "${ref_table}"
    % if new_traversals:
    ${'\n'.join(new_traversals)}
    % endif
    % if where:
    WHERE ${where}
    % endif
    GROUP BY "${ref_col.fieldName()}"
) AS "${join_table}"
  ON "${join_table}"."${ref_col.fieldName()}" = (${','.join(pcols)})
