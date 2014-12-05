<%
    SELECT_AGGREGATE = SQL.byName('SELECT_AGGREGATE')
    SELECT_JOINER = SQL.byName('SELECT_JOINER')
    SELECT_EXPAND = SQL.byName('SELECT_EXPAND')
    WHERE = SQL.byName('WHERE')
    ID = orb.system.settings().primaryField()

    GLOBALS['traversal'] = []
    GLOBALS['field_mapper'] = {}
    GLOBALS['select_tables'] = {table}

    schema = table.schema()
    table_name = schema.tableName()

    def cmpcol(a, b):
        result = cmp(a.isAggregate(), b.isAggregate())
        if not result:
            result = cmp(a.isJoined(), b.isJoined())
            if not result:
              return cmp(a.fieldName(), b.fieldName())
        return result

    pcols = [QUOTE(table_name, pcol.fieldName()) for pcol in schema.primaryColumns()]
    joined = []
    columns = []
    i18n_columns = []
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
                aggr_sql = SELECT_AGGREGATE(column, GLOBALS=GLOBALS, IO=IO)

                group_by.update(pcols)
                joined.append(aggr_sql)

                if use_column:
                    columns.append(GLOBALS['join_column'])

        elif column.isJoined():
            if use_column or column in query_columns:
                aggr_sql = SELECT_JOINER(column, GLOBALS=GLOBALS, IO=IO)

                group_by.update(pcols)
                joined.append(aggr_sql)

                if use_column:
                    columns.append(GLOBALS['join_column'])

        elif use_column and column.isTranslatable():
            if options.inflateRecords or options.locale == 'all':
                # process translation logic
                col_sql = 'hstore_agg(hstore("i18n"."locale", "i18n"."{0}")) AS "{1}"'
                i18n_columns.append(col_sql.format(column.fieldName(), column.fieldName()))
                group_by.add('"{0}"."{1}"'.format(table_name, ID))
                GLOBALS['field_mapper'][column] = '"i18n"."{0}"'.format(column.fieldName())
            else:
                col_sql = '(array_agg("i18n"."{0}"))[1] AS "{1}"'
                i18n_columns.append(col_sql.format(column.fieldName(), column.fieldName()))
                group_by.add('"{0}"."{1}"'.format(table_name, ID))

                IO['locale'] = options.locale
                GLOBALS['field_mapper'][column] = '"i18n"."{0}"'.format(column.fieldName())

        elif not column.isProxy() and use_column:
            query_columns.append(column)

            # expand a reference column
            if column.isReference() and lookup.expand and column.name() in lookup.expand:
                col_sql = SELECT_EXPAND(column=column, lookup=lookup, options=options, GLOBALS=GLOBALS, IO=IO)
                if col_sql:
                    columns.append(col_sql)

            # or, just return the base record
            columns.append('"{0}"."{1}" AS "{2}"'.format(table_name,
                                                         column.fieldName(),
                                                         column.fieldName()))

    # include any additional expansions from pipes or reverse lookups
    if lookup.expand:
        # include reverse lookups
        for reverseLookup in schema.reverseLookups():
            name = reverseLookup.reversedName()
            if name in lookup.expand or name + '.ids' in lookup.expand or name + '.count' in lookup.expand:
                col_sql = SELECT_EXPAND(reverseLookup=reverseLookup, lookup=lookup, options=options, GLOBALS=GLOBALS, IO=IO)
                if col_sql:
                    columns.append(col_sql)

        # include pipes
        for pipe in schema.pipes():
            name = pipe.name()
            if name in lookup.expand or name + '.ids' in lookup.expand or name + '.count' in lookup.expand:
                col_sql = SELECT_EXPAND(pipe=pipe, lookup=lookup, options=options, GLOBALS=GLOBALS, IO=IO)
                if col_sql:
                    columns.append(col_sql)

    if lookup.where:
        try:
          where = WHERE(schema, lookup.where, GLOBALS=GLOBALS, IO=IO)
        except orb.errors.QueryIsNull:
          where = orb.errors.QueryIsNull
    else:
        where = ''

    order = lookup.order or table.schema().defaultOrder()
    if order:
        used = set()
        order_by = []
        for col, direction in order:
            col_obj = schema.column(col)
            if not col_obj:
              continue

            default = '"{0}"."{1}"'.format(table_name, col_obj.fieldName())
            field = GLOBALS['field_mapper'].get(col_obj, default)

            if field != default:
              group_by.add(field)

            if lookup.columns and not col_obj.name() in lookup.columns:
                columns.append(field)

            order_by.append('{0} {1}'.format(field, direction.upper()))
    else:
        order_by = []

    select_tables = GLOBALS['select_tables']
    table_names = list({'"{0}"'.format(tbl.schema().tableName()) for tbl in select_tables})

##    # ensure we have all selection items in our column
##    if group_by:
##        for col in set(query_columns):
##            default = '"{0}"."{1}"'.format(col.schema().tableName(), col.fieldName())
##            field = GLOBALS['field_mapper'].get(col, default)
##            group_by.all(field)
%>
% if (columns or i18n_columns) and where != orb.errors.QueryIsNull:
-- select from a single table
SELECT ${'DISTINCT' if lookup.distinct else ''}
    ${',\n    '.join(columns+i18n_columns)}
FROM "${table_name}"
${'\n'.join(joined) if joined else ''}
${'\n'.join(GLOBALS['traversal']) if GLOBALS['traversal'] else ''}

% if i18n_columns:
% if options.inflateRecords or options.locale == 'all':
LEFT JOIN "${table_name}_i18n" AS "i18n" ON (
    "i18n"."${table_name}_id" = "${ID}"
)
% else:
LEFT JOIN "${table_name}_i18n" AS "i18n" ON (
    "i18n"."${table_name}_id" = "${table_name}"."${ID}" AND "i18n"."locale" = %(locale)s
)
% endif
% endif
% if where and len(select_tables) > 1:
WHERE "${table_name}"."${ID}" IN (
    SELECT "${table_name}"."${ID}"
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