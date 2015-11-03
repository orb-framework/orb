% if orb.Query.typecheck(value):
    <%
        val_schema = value.table().schema()
        val_col = value.column()
        query_field = QUOTE(schema_alias or val_schema.dbname(), val_col.fieldName())
    %>
    % if query.isInverted():
        ${query_field} ${op} ${field}
    % else:
        ${field} ${op} ${query_field}
    % endif

% elif value is None:
    % if operator == orb.Query.Op.Is:
    ${field} IS NULL
    % elif operator == orb.Query.Op.IsNot:
    ${field} IS NOT NULL
    % endif

% elif orb.RecordSet.typecheck(value):
    <% SELECT = SQL.byName('SELECT') %>
    ${field} ${op} (
       ${SELECT(value, default_columns=['id'], IO=IO).strip(';')}
    )
% else:
    <%
    if operator in (orb.Query.Op.IsIn, orb.Query.Op.IsNotIn) and not value:
        raise orb.errors.QueryIsNull()

    ID = schema.primaryColumn().fieldName()
    key = str(len(IO))
    key_id = '%({0})s'.format(key)

    if query.operatorType() in (query.Op.Contains, query.Op.DoesNotContain):
        store_value = '%{0}%'.format(value)
    elif query.operatorType() in (query.Op.Startswith, query.Op.DoesNotStartwith):
        store_value = '{0}%'.format(value)
    elif query.operatorType() in (query.Op.Endswith, query.Op.DoesNotEndwith):
        store_value = '%{0}'.format(value)
    else:
        store_value = value

    IO[key] = SQL.datastore().store(column, store_value)
    %>

    % if column.isTranslatable():
        ## check to see if this column has already been merged
        % if column in GLOBALS['field_mapper']:
        ${field} ${op} %(${key})s
        % else:
        <% table_name = schema.dbname() %>
        "${schema_alias or table_name}"."${ID}" IN (
            SELECT "${table_name}_id"
            FROM "${table_name}_i18n"
            % if query.isInverted():
            WHERE %(${key})s ${op} "${table_name}_i18n"."${column.fieldName()}"
            % else:
            WHERE "${table_name}_i18n"."${column.fieldName()}" ${op} %(${key})s
            % endif
        )
        % endif
    % elif query.isInverted():
    ${key_id} ${op} ${field}
    % else:
    ${field} ${op} ${key_id}
    % endif
% endif