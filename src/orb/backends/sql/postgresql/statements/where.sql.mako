% if orb.QueryCompound.typecheck(where):
    <%
    WHERE = SQL.byName('WHERE')
    sub_queries = []
    for subquery in where.queries():
        sub_q = WHERE(subquery, baseSchema=baseSchema, GLOBALS=GLOBALS, IO=IO)
        if sub_q:
            sub_queries.append(sub_q)
    %>
    % if sub_queries:
    % if where.operatorType() == orb.QueryCompound.Op.And:
    (${' AND '.join(sub_queries)})
    % else:
    (${' OR '.join(sub_queries)})
    % endif
    % endif

% else:
    <%
    WHERE = SQL.byName('WHERE')
    traversal = []
    column = where.column(baseSchema, traversal=traversal, db=__database__)

    # ensure the column exists
    if not column:
        raise orb.errors.ColumnNotFound(baseSchema.name(), where.columnName())

    GLOBALS.setdefault('select_tables', set())
    GLOBALS.setdefault('traversal', [])
    GLOBALS.setdefault('field_mapper', {})

    last_key = None
    join_count = GLOBALS.get('join_count', 0)
    if traversal:
        traversal.append(column)

        # join any traversal columns that we might find
        for i, curr in enumerate(traversal[1:]):
            join_count += 1
            join_table = 'join_{0}'.format(join_count)
            
            if not last_key:
                last_key = '"{0}"."{1}"'.format(traversal[i].schema().tableName(),
                                                traversal[i].fieldName())
            
            pcols = ['"{0}"."{1}"'.format(join_table, pcol.fieldName()) \
                     for pcol in curr.schema().primaryColumns()]
            if len(pcols) > 1:
                curr_key = '({0})'.format(','.join(pcols))
            else:
                curr_key = pcols[0]
            
            cmd = u'LEFT JOIN "{0}" AS "{3}" ON {1}={2}'
            
            GLOBALS['field_mapper'][curr] = '"{0}"."{1}"'.format(join_table,
                                                                  curr.fieldName())
            GLOBALS['traversal'].append(cmd.format(curr.schema().tableName(),
                                                    last_key,
                                                    curr_key,
                                                    join_table))

            last_key = curr_key
    
    # if we're not mapping the column already, we'll need to add it into the
    # selection 
    elif where.table() and where.table().schema() != baseSchema:
        GLOBALS['select_tables'].add(column.schema().model())
    
    GLOBALS['join_count'] = join_count

    op = where.operatorType()
    op_sql = SQL.byName('Op::{0}'.format(orb.Query.Op(op)))
    if where.caseSensitive():
      case = SQL.byName('Op::{0}::CaseSensitive'.format(orb.Query.Op(op)))
      op_sql = case or op_sql
    if op_sql is None:
        raise orb.errors.QueryInvalid('{0} is an unknown operator.'.format(orb.Query.Op(op)))
    
    value = where.value()
    if orb.Table.recordcheck(value):
        value = value.primaryKey()
    
    if column in GLOBALS.get('field_mapper', {}):
        field = GLOBALS['field_mapper'][column]
    else:
        def query_to_sql(q):
            column = q.column(baseSchema)
            output = '"{0}"."{1}"'.format(column.schema().tableName(),
                                          column.fieldName())
            
            # process any functions on the query
            for func in q.functions():
                qfunc = SQL.byName('Func::{0}'.format(orb.Query.Function(func)))
                if qfunc:
                    output = qfunc.format(output)
            
            return output
        
        field = query_to_sql(where)
        for op, target in where.math():
            math_key = orb.Query.Math(op)
            type_key = orb.ColumnType(column.columnType())
            
            sql = SQL.byName('Math::{0}::{1}'.format(math_key, type_key))
            if not sql:
                sql = SQL.byName('Math::{0}'.format(math_key))
            if not sql:
                msg = 'Cannot {0} {1} types.'.format(math_key, type_key)
                raise orb.errors.QueryInvalid(msg)
            
            field += sql
            if orb.Query.typecheck(target):
                field += query_to_sql(target)
            else:
                key = len(IO)
                IO[str(key)] = target
                field += '%({0})s'.format(key)
    %>
    % if orb.Query.typecheck(value):
        <%
            val_schema = value.table().schema()
            val_col = value.column()
            
            if value.table():
              GLOBALS['select_tables'].add(value.table())

            query_field = '"{0}"."{1}"'.format(val_schema.tableName(), val_col.fieldName())
        %>
        % if where.isInverted():
            ${query_field} ${op_sql} ${field}
        % else:
            ${field} ${op_sql} ${query_field}
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
          % if where.isInverted():
          ${key_id} IN ${field}
          % else:
          ${field} IN ${key_id}
          % endif
        % else:
          <% SELECT = SQL.byName('SELECT') %>
          ${field} ${op_sql} (
              ${SELECT(value.table(),
                       lookup=value.lookupOptions(columns=value.table().schema().primaryColumns()),
                       options=value.databaseOptions(),
                       output=IO)[0]}
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
            ${field} ${op_sql} %(${key})s
            % else:
            <% table_name = column.schema().tableName() %>
            "${table_name}"."${ID}" IN (
                SELECT "${table_name}_id"
                FROM "${table_name}_i18n"
                % if where.isInverted():
                WHERE %(${key})s ${op_sql} "${table_name}_i18n"."${column.fieldName()}"
                % else:
                WHERE "${table_name}_i18n"."${column.fieldName()}" ${op_sql} %(${key})s
                % endif
            )
            % endif
        % elif where.isInverted():
        ${key_id} ${op_sql} ${field}
        % else:
        ${field} ${op_sql} ${key_id}
        % endif
    % endif
% endif