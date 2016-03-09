<%
    import projex.text
    from orb import errors

    WHERE = SQL.byName('WHERE')

    def collect_sub_expand(schema, tree):
        output = {}
        for expand_name, sub_tree in tree.items():
            # cannot expand these keywords
            if expand_name in ('ids', 'count'):
                continue
            # these keywords are expanded via their sub-tree
            elif expand_name in ('first', 'last'):
                output.update(collect_sub_expand(schema, sub_tree))
            # lookup a column, pipe, or reverseLookup
            else:
                column = schema.column(expand_name)
                if column:
                    output[('column', column)] = sub_tree
                    continue

                pipe = schema.pipe(expand_name)
                if pipe:
                    output[('pipe', pipe)] = sub_tree
                    continue

                rev_lookup = schema.reverseLookup(expand_name)
                if lookup:
                    output[('reverseLookup', rev_lookup)] = sub_tree
                    continue

                raise errors.ColumnNotFound(schema.name(), expand_name)
        return output
%>

% if pipe:
    <%
        source_table = pipe.sourceReferenceModel()
        join_table = pipe.pipeReferenceModel()
        source_column = join_table.schema().column(pipe.sourceColumn())
        target_column = join_table.schema().column(pipe.targetColumn())
        target_table = pipe.targetReferenceModel()
        table_name = target_table.schema().dbname()
        has_translations = target_table.schema().hasTranslations()
        columns = []
        colname = pipe.name()
        col_name = projex.text.underscore(colname)

        alias = col_name + '_table'
        source_primary = source_table.schema().primaryColumn().fieldName()
        target_primary = target_column.schema().primaryColumn().fieldName()
        records_alias = col_name + '_records'
        record_primary = target_table.schema().primaryColumn().fieldName()

        # include the base table's filter, if one exists
        target_filter = target_table.baseQuery(lookup=lookup, options=options)
        if target_filter is not None:
            base_where = '(' + WHERE(target_table.schema(), target_filter, schema_alias=alias, GLOBALS=GLOBALS, IO=IO) + ') AND '
        else:
            base_where = ''

        source_table_name = source_alias or source_table.schema().dbname()

        if 'ids' in tree:
            columns.append('array_agg({0}.{1}) AS ids'.format(records_alias, target_primary))
        if 'count' in tree:
            columns.append('count({0}.*) AS count'.format(records_alias))
        if 'first' in tree:
            columns.append('(array_agg(row_to_json({0}.*)))[1] AS first'.format(records_alias)),
        if 'last' in tree:
            columns.append('(array_agg(row_to_json({0}.*)))[count({0}.*)] AS last'.format(records_alias)),
        if 'records' in tree or not columns:
            columns.append('array_agg(row_to_json({0}.*)) AS records'.format(records_alias))
    %>
    (
        SELECT row_to_json(${col_name}_row) FROM (
            SELECT ${', '.join(columns)}
            FROM (
                % if not has_translations:
                SELECT ${alias}.*
                % else:
                SELECT ${alias}.*, ${alias}_i18n.*
                % endif
                % for (type, object), sub_tree in collect_sub_expand(target_table.schema(), tree).items():
                ,${SELECT_EXPAND(**{type: object, 'tree': sub_tree, 'source_alias': alias, 'GLOBALS': GLOBALS, 'IO': IO, 'lookup': lookup, 'options': options})}
                % endfor
                FROM "${table_name}" AS "${alias}"
                % if has_translations:
                LEFT JOIN "${table_name}_i18n" AS "${alias}_i18n" ON ${alias}.${target_primary} = ${alias}_i18n.${table_name}_id AND ${alias}_i18n.locale = '${options.locale}'
                % endif
                WHERE ${base_where} "${alias}"."${target_primary}" IN (
                    SELECT DISTINCT ON (j."${target_column.fieldName()}") j."${target_column.fieldName()}"
                    FROM "${join_table.schema().dbname()}" AS j
                    WHERE j."${source_column.fieldName()}" = "${source_table_name}"."${source_primary}"
                    ${'LIMIT 1' if pipe.unique() else ''}
                )
            ) ${records_alias}
        ) ${col_name}_row
    ) AS "${colname}"

% elif reverseLookup:
    <%
        source_schema = reverseLookup.schema()
        source_column = reverseLookup
        table_name = source_schema.dbname()
        ref_schema = reverseLookup.referenceModel().schema()
        ref_table_name = source_alias or ref_schema.dbname()
        has_translations = source_schema.hasTranslations()

        colname = reverseLookup.reversedName()
        col_name = projex.text.underscore(colname)

        alias = col_name + '_table'
        records_alias = col_name + '_records'
        target_primary = ref_schema.primaryColumn().fieldName()
        source_primary = source_schema.primaryColumn().fieldName()

        # include the base table's filter, if one exists
        target_filter = source_schema.model().baseQuery(lookup=lookup, options=options)
        if target_filter is not None:
            base_where = '(' + WHERE(ref_schema, target_filter, schema_alias=alias, GLOBALS=GLOBALS, IO=IO) + ') AND '
        else:
            base_where = ''

        columns = []
        if 'ids' in tree:
            columns.append('array_agg({0}.{1}) AS ids'.format(records_alias, target_primary))
        if 'count' in tree:
            columns.append('count({0}.*) AS count'.format(records_alias))
        if 'first' in tree:
            columns.append('(array_agg(row_to_json({0}.*)))[1] AS first'.format(records_alias)),
        if 'last' in tree:
            columns.append('(array_agg(row_to_json({0}.*)))[count({0}.*)] AS last'.format(records_alias)),
        if 'records' in tree or not columns:
            columns.append('array_agg(row_to_json({0}.*)) AS records'.format(records_alias))

    %>
    (
        SELECT row_to_json(${col_name}_row) FROM (
            SELECT ${', '.join(columns)}
            FROM (
                % if not has_translations:
                SELECT ${alias}.*
                % else:
                SELECT ${alias}.*, ${alias}_i18n.*
                % endif
                % for (type, object), sub_tree in collect_sub_expand(source_schema, tree).items():
                ,${SELECT_EXPAND(**{type: object, 'tree': sub_tree, 'source_alias': alias, 'GLOBALS': GLOBALS, 'IO': IO, 'lookup': lookup, 'options': options})}
                % endfor
                FROM "${table_name}" AS "${alias}"
                % if has_translations:
                LEFT JOIN "${table_name}_i18n" AS "${alias}_i18n" ON ${alias}.${target_primary} = ${alias}_i18n.${table_name}_id AND ${alias}_i18n.locale = '${options.locale}'
                % endif
                WHERE ${base_where} "${alias}"."${source_column.fieldName()}" = "${ref_table_name}".${target_primary}
                ${'LIMIT 1' if source_column.unique() else ''}
            ) ${records_alias}
        ) ${col_name}_row
    ) AS "${colname}"

% elif column:
    <%
        reference = column.referenceModel()
        ref_table_name = source_alias or column.schema().dbname()
        colname = column.name()
        table_name = reference.schema().dbname()
        col_name = projex.text.underscore(colname)
        alias = projex.text.underscore(column.name()) + '_table'
        primary = reference.schema().primaryColumn().fieldName()
        has_translations = reference.schema().hasTranslations()

        # include the base table's filter, if one exists
        target_filter = reference.baseQuery(lookup=lookup, options=options)
        if target_filter is not None:
            base_where = '(' + WHERE(reference.schema(), target_filter, schema_alias=alias, GLOBALS=GLOBALS, IO=IO) + ') AND '
        else:
            base_where = ''
    %>
    (
        SELECT row_to_json(${col_name}_row) FROM (
            % if not has_translations:
            SELECT "${alias}".*
            % else:
            SELECT "${alias}".*, "${alias}_i18n".*
            % endif
            % for (type, object), sub_tree in collect_sub_expand(reference.schema(), tree).items():
            ,${SELECT_EXPAND(**{type: object, 'tree': sub_tree, 'source_alias': alias, 'GLOBALS': GLOBALS, 'IO': IO, 'lookup': lookup, 'options': options})}
            % endfor
            FROM "${table_name}" AS "${alias}"
            % if has_translations:
            LEFT JOIN "${table_name}_i18n" AS "${alias}_i18n" ON "${alias}"."${primary}" = "${alias}_i18n".${table_name}_id AND ${alias}_i18n.locale = '${options.locale}'
            % endif
            WHERE ${base_where} "${alias}"."${primary}" = "${ref_table_name}"."${column.fieldName()}"
        ) ${col_name}_row
    ) AS "${colname}"
% endif