% if orb.Query.typecheck(value):
    <%
        val_schema = value.table().schema()
        val_col = value.column()
        query_field = QUOTE(val_schema.dbname(), val_col.fieldName())
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

    ID = orb.system.settings().primaryField()
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

    op_sql = op
    field_sql = field
    if 'ILIKE' in op:
        op_sql = op_sql.replace('ILIKE', 'LIKE')
        key_id = 'lower({0})'.format(key_id)
        field_sql = 'lower({0})'.format(field_sql)
    %>

    % if column.isTranslatable():
        ## check to see if this column has already been merged
        % if column in GLOBALS['field_mapper']:
        ${field_sql} ${op_sql} %(${key})s
        % else:
        <% table_name = column.schema().dbname() %>
        "${table_name}"."${ID}" IN (
            SELECT "${table_name}_id`
            FROM `${table_name}_i18n`
            % if query.isInverted():
            WHERE %(${key})s ${op_sql} `${table_name}_i18n`.`${column.fieldName()}`
            % else:
            WHERE `${table_name}_i18n`.`${column.fieldName()}` ${op} %(${key})s
            % endif
        )
        % endif
    % elif query.isInverted():
    ${key_id} ${op_sql} ${field_sql}
    % else:
    ${field_sql} ${op_sql} ${key_id}
    % endif
% endif