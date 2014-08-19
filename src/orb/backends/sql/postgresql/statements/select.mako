<%
SELECT_AGGREGATE = __sql__.byName('SELECT_AGGREGATE')
SELECT_JOINER = __sql__.byName('SELECT_JOINER')
WHERE = __sql__.byName('WHERE')

__data__['traversal'] = []
__data__['field_mapper'] = {}
__data__['select_tables'] = {table}

schema = table.schema()
table_name = schema.tableName()
translations = []

def cmpcol(a, b):
    result = cmp(a.isAggregate(), b.isAggregate())
    if not result:
        result = cmp(a.isJoined(), b.isJoined())
        if not result:
          return cmp(a.fieldName(), b.fieldName())
    return result

pcols = ['"{0}"."{1}"'.format(table_name, pcol.fieldName()) \
         for pcol in schema.primaryColumns()]

joined = []
columns = []
group_by = set()
if lookup.where:
    query_columns = lookup.where.columns(schema)
else:
    query_columns = []

for column in sorted(schema.columns(), cmpcol):
    if lookup.columns and \
       not (column.name() in lookup.columns or \
            column.fieldName() in lookup.columns or \
            column in lookup.columns):
       use_column = False
    else:
        use_column = True

    if column.isAggregate():
        if use_column or column in query_columns:
            aggr_sql = SELECT_AGGREGATE(column, __data__=__data__)[0]
            
            group_by.update(pcols)
            joined.append(aggr_sql)
            
            if use_column:
                columns.append(__data__['join_column'])

    elif column.isJoined():
        if use_column or column in query_columns:
            aggr_sql = SELECT_JOINER(column, __data__=__data__)[0]
            
            group_by.update(pcols)
            joined.append(aggr_sql)
            
            if use_column:
                columns.append(__data__['join_column'])

    elif column.isTranslatable():
        # process translation logic
        pass

    elif not column.isProxy() and use_column:
        columns.append('"{0}"."{1}" AS "{2}"'.format(table_name,
                                                     column.fieldName(),
                                                     column.name()))

if lookup.where:
    try:
      where = WHERE(lookup.where, baseSchema=schema, __data__=__data__)[0].strip()
    except orb.errors.EmptyQuery:
      where = orb.errors.EmptyQuery
else:
    where = ''

order = lookup.order or table.schema().defaultOrder()
if order:
    used = set()
    order_by = []
    for col, dir in order:
        col_obj = schema.column(col)
        if not col_obj:
          continue
        
        default = '"{0}"."{1}"'.format(table_name, col_obj.fieldName())
        field = __data__['field_mapper'].get(col_obj, default)
        
        if field != default:
          group_by.add(field)
        
        if lookup.columns and not col_obj.name() in lookup.columns:
            columns.append(field)
        
        order_by.append('{0} {1}'.format(field, dir.upper()))
else:
    order_by = []

select_tables = __data__['select_tables']
table_names = list({'"{0}"'.format(tbl.schema().tableName()) for tbl in select_tables})
%>
% if columns and where != orb.errors.EmptyQuery:
-- select from a single table
% if lookup.distinct:
SELECT DISTINCT
% else:
SELECT
% endif
    ${',\n    '.join(columns)}
FROM "${table_name}"
% if joined:
${'\n'.join(joined)}
% endif
% if __data__['traversal']:
${'\n'.join(__data__['traversal'])}
% endif
% if where and len(select_tables) > 1:
WHERE "${table_name}"."_id" IN (
    SELECT "${table_name}"."_id"
    FROM ${','.join(table_names)}
    WHERE ${where}
)
% elif where:
WHERE ${where}
% endif
% if group_by:
GROUP BY ${',\n        '.join(group_by)}
% endif
% if order_by:
ORDER BY ${',\n         '.join(order_by)}
% endif
% if lookup.start:
OFFSET ${lookup.start}
% endif
% if lookup.limit > 0:
LIMIT ${lookup.limit}
% endif
;
% endif