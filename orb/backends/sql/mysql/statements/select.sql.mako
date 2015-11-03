<%
    SELECT_AGGREGATE = SQL.byName('SELECT_AGGREGATE')
    SELECT_JOINER = SQL.byName('SELECT_JOINER')
    SELECT_EXPAND = SQL.byName('SELECT_EXPAND')
    SELECT_SHORTCUT = SQL.byName('SELECT_SHORTCUT')
    WHERE = SQL.byName('WHERE')
    ID = orb.system.settings().primaryField()

    GLOBALS['field_mapper'] = {}

    schema = table.schema()
    table_name = schema.dbname()

    def cmpcol(a, b):
        result = cmp(a.isAggregate(), b.isAggregate())
        if not result:
            result = cmp(a.isJoined(), b.isJoined())
            if not result:
              return cmp(a.fieldName(), b.fieldName())
        return result

    pcols = [QUOTE(table_name, pcol.fieldName()) for pcol in schema.primaryColumns()]
    expand_tree = lookup.expandtree()
    expanded = bool(expand_tree)

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
           not (column.name() in lookup.columns or
                column.fieldName() in lookup.columns or
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

        elif use_column and column.shortcut() and not isinstance(column.schema(), orb.ViewSchema):
            raise NotImplementedError('Shortcuts are not supported in PostgreSQL yet.')

        elif use_column and column.isTranslatable():
            if options.inflated or options.locale == 'all':
                # process translation logic
                col_sql = 'hstore_agg(hstore(`i18n`.`locale`, `i18n`.`{0}`)) AS `{1}`'
                i18n_columns.append(col_sql.format(column.fieldName(), column.fieldName()))
                group_by.add('`{0}`.`{1}`'.format(table_name, ID))
                GLOBALS['field_mapper'][column] = '`i18n`.`{0}`'.format(column.fieldName())
            else:
                col_sql = '(array_agg(`i18n`.`{0}`))[1] AS `{1}`'
                i18n_columns.append(col_sql.format(column.fieldName(), column.fieldName()))
                group_by.add('`{0}`.`{1}`'.format(table_name, ID))

                IO['locale'] = options.locale
                GLOBALS['field_mapper'][column] = '`i18n`.`{0}`'.format(column.fieldName())

        elif not column.isProxy() and use_column:
            query_columns.append(column)

            # expand a reference column
            if column.isReference() and column.name() in expand_tree:
                tree = expand_tree.pop(column.name())
                col_sql = SELECT_EXPAND(column=column, lookup=lookup, options=options, tree=tree, GLOBALS=GLOBALS, IO=IO)
                if col_sql:
                    columns.append(col_sql)

            # or, just return the base record
            columns.append('`{0}`.`{1}` AS `{2}`'.format(table_name,
                                                         column.fieldName(),
                                                         column.fieldName()))

    # include any additional expansions from pipes or reverse lookups
    if expand_tree:
        # include pipes
        for pipe in schema.pipes():
            name = pipe.name()
            tree = expand_tree.pop(name, None)

            if tree is not None:
                col_sql = SELECT_EXPAND(pipe=pipe, lookup=lookup, options=options, tree=tree, GLOBALS=GLOBALS, IO=IO)
                if col_sql:
                    columns.append(col_sql)
            if not expand_tree:
                break

    # include reverse lookups
    if expand_tree:
        for reverseLookup in schema.reverseLookups():
            name = reverseLookup.reversedName()
            tree = expand_tree.pop(name, None)

            if tree is not None:
                col_sql = SELECT_EXPAND(reverseLookup=reverseLookup, lookup=lookup, options=options, tree=tree, GLOBALS=GLOBALS, IO=IO)
                if col_sql:
                    columns.append(col_sql)
            if not expand_tree:
                break

    if lookup.where:
        try:
          where = WHERE(schema, lookup.where, GLOBALS=GLOBALS, IO=IO)
        except orb.errors.QueryIsNull:
          where = orb.errors.QueryIsNull
    else:
        where = ''

    if lookup.order:
        used = set()
        order_by = []
        for col, direction in lookup.order:
            col_obj = schema.column(col)
            if not col_obj:
                continue

            default = '`{0}`.`{1}`'.format(table_name, col_obj.fieldName())
            field = GLOBALS['field_mapper'].get(col_obj, default)

            if field != default:
                group_by.add(field)

            order_by.append('{0} {1}'.format(field, direction.upper()))
    else:
        order_by = []

%>
% if (columns or i18n_columns) and where != orb.errors.QueryIsNull:
SELECT ${'DISTINCT' if lookup.distinct else ''}
    ${',\n    '.join(columns+i18n_columns)}
FROM `${table_name}`
${'\n'.join(joined) if joined else ''}
% if i18n_columns:
    % if options.inflated or options.locale == 'all':
    LEFT JOIN `${table_name}_i18n` AS `i18n` ON (
        `i18n`.`${table_name}_id` = `${ID}`
    )
    % else:
    LEFT JOIN `${table_name}_i18n` AS `i18n` ON (
        `i18n`.`${table_name}_id` = `${table_name}`.`${ID}` AND `i18n`.`locale` = %(locale)s
    )
    % endif
% endif
% if expanded:
    % if where or order_by or lookup.start or lookup.limit:
    WHERE `${table_name}`.`id` IN (
        SELECT DISTINCT ${'ON ({0}) '.format(', '.join([col.split(' ')[0] for col in order_by])) if order_by else ''}`${table_name}`.`id`
        FROM `${table_name}`
        % if i18n_columns:
            % if options.inflated or options.locale == 'all':
            LEFT JOIN `${table_name}_i18n` AS `i18n` ON (
                `i18n`.`${table_name}_id` = `${ID}`
            )
            % else:
            LEFT JOIN `${table_name}_i18n` AS `i18n` ON (
                `i18n`.`${table_name}_id` = `${table_name}`.`${ID}` AND `i18n`.`locale` = %(locale)s
            )
            % endif
        % endif
        % if where:
        WHERE ${where}
        % endif
        % if group_by:
        GROUP BY ${', '.join(list(group_by) + [col.split(' ')[0] for col in order_by])}
        % endif
        % if order_by:
        ORDER BY ${', '.join(order_by)}
        % endif
        % if lookup.start:
        OFFSET ${lookup.start}
        % endif
        % if lookup.limit > 0:
        LIMIT ${lookup.limit}
        % endif
    )
    % endif
    % if group_by:
    GROUP BY ${', '.join(group_by)}
    % endif
% else:
    % if where:
    WHERE ${where}
    % endif
    % if group_by:
    GROUP BY ${', '.join(group_by)}
    % endif
    % if order_by:
    ORDER BY ${', '.join(order_by)}
    % endif
    % if lookup.start:
    OFFSET ${lookup.start}
    % endif
    % if lookup.limit > 0:
    LIMIT ${lookup.limit}
    % endif
% endif
;
% endif