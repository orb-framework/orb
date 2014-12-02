% if orb.Query.typecheck(value):
    <%
        val_schema = value.table().schema()
        val_col = value.column()

        if value.table():
          GLOBALS['select_tables'].add(value.table())

        query_field = QUOTE(val_schema.tableName(), val_col.fieldName())
    %>
    % if query.isInverted():
        ${query_field} ${op} ${field}
    % else:
        ${field} ${op} ${query_field}
    % endif

% elif value is None:
    % if op == orb.Query.Op.Is:
    ${field} IS NULL
    % elif op == orb.Query.Op.IsNot:
    ${field} IS NOT NULL
    % endif

% elif orb.RecordSet.typecheck(value):
    % if value.isLoaded():
      <%
      if not value:
          raise orb.errors.QueryIsNull()

      key = str(len(IO))
      key_id = '%({0})s'.format(key)
      IO[key] = SQL.datastore().store(column, value)
      %>
      % if query.isInverted():
      ${key_id} IN ${field}
      % else:
      ${field} IN ${key_id}
      % endif
    % else:
      <% SELECT = SQL.byName('SELECT') %>
      ${field} ${op} (
          ${SELECT(value.table(),
                   lookup=value.lookupOptions(columns=value.table().schema().primaryColumns()),
                   options=value.databaseOptions(),
                   output=IO)}
      )
    % endif

% else:
    <%
    if op in (orb.Query.Op.IsIn, orb.Query.Op.IsNotIn) and not value:
        raise orb.errors.QueryIsNull()

    ID = orb.system.settings().primaryField()
    key = str(len(IO))
    key_id = '%({0})s'.format(key)
    IO[key] = SQL.datastore().store(column, value)
    %>
    % if op in (orb.Query.Op.Contains, orb.Query.Op.DoesNotContain):
    <% IO[key] = '%{0}%'.format(IO[key]) %>
    % elif op in (orb.Query.Op.Startswith, orb.Query.Op.DoesNotStartwith):
    <% IO[key] = '{0}%'.format(IO[key]) %>
    % elif op in (orb.Query.Op.Endswith, orb.Query.Op.DoesNotEndwith):
    <% IO[key] = '%{0}'.format(IO[key]) %>
    % endif

    % if column.isTranslatable():
        ## check to see if this column has already been merged
        % if column in GLOBALS['field_mapper']:
        ${field} ${op} %(${key})s
        % else:
        <% table_name = column.schema().tableName() %>
        "${table_name}"."${ID}" IN (
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