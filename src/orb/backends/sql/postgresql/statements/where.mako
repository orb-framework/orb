% if orb.QueryCompound.typecheck(where):
    <%
    WHERE = __sql__.byName('WHERE')
    sub_queries = []
    for subquery in where.queries():
        sub_q = WHERE(subquery, baseSchema=baseSchema, __data__=__data__)[0]
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
    WHERE = __sql__.byName('WHERE')
    traversal = []
    column = where.column(baseSchema, traversal=traversal, db=__database__)

    # ensure the column exists
    if not column:
        raise orb.errors.ColumnNotFoundError(baseSchema, where.columnName())

    __data__.setdefault('select_tables', set())
    __data__.setdefault('traversal', [])
    __data__.setdefault('field_mapper', {})

    last_key = None
    join_count = __data__.get('join_count', 0)
    if traversal:
        traversal.append(column)

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
        
        __data__['field_mapper'][curr] = '"{0}"."{1}"'.format(join_table,
                                                              curr.fieldName())
        __data__['traversal'].append(cmd.format(curr.schema().tableName(),
                                                last_key,
                                                curr_key,
                                                join_table))

        last_key = curr_key

    __data__['join_count'] = join_count

    op = where.operatorType()
    op_sql = __sql__.byName('Op::{0}'.format(orb.Query.Op(op)))
    if where.caseSensitive():
      case = __sql__.byName('Op::{0}::CaseSensitive'.format(orb.Query.Op(op)))
      op_sql = case or op_sql
    
    value = where.value()
    if orb.Table.recordcheck(value):
        value = value.primaryKey()
    
    if column in __data__.get('field_mapper', {}):
        field = __data__['field_mapper'][column]
    else:
        def query_to_sql(q):
            column = q.column(baseSchema)
            output = '"{0}"."{1}"'.format(column.schema().tableName(),
                                          column.fieldName())
            
            # process any functions on the query
            for func in q.functions():
                qfunc = __sql__.byName('Func::{0}'.format(orb.Query.Function(func)))
                if qfunc:
                    output = qfunc.format(output)
            
            return output
        
        field = query_to_sql(where)
        for op, target in where.math():
            math_key = orb.Query.Math(op)
            type_key = orb.ColumnType(column.columnType())
            
            sql = __sql__.byName('Math::{0}::{1}'.format(math_key, type_key))
            if not sql:
                sql = __sql__.byName('Math::{0}'.format(math_key))
            if not sql:
                msg = 'Cannot {0} {1} types.'.format(math_key, type_key)
                raise orb.errors.DatabaseQueryError(msg)
            
            field += sql
            if orb.Query.typecheck(target):
                field += query_to_sql(target)
            else:
                key = len(__data__['output'])
                __data__['output'][str(key)] = target
                field += '%({0})s'.format(key)
    %>
    % if orb.Query.typecheck(value):
        <%
            val_schema = value.table().schema()
            val_col = value.column()
            
            if value.table():
              __data__['select_tables'].add(value.table())
        %>
        ${field} ${op_sql} ${'"{0}"."{1}"'.format(val_schema.tableName(),
                                                  val_col.fieldName())}
                                                  
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
              raise orb.errors.EmptyQuery()
          
          key = str(len(__data__['output']))
          __data__['output'][key] = __sql__.datastore().store(column, value)
          %>
          ${field} IN %(${key})s
          % else:
          <% SELECT = __sql__.byName('SELECT') %>
          ${field} ${op_sql} (
              ${SELECT(value.table(),
                       lookup=value.lookupOptions(columns=value.table().schema().primaryColumns()),
                       options=value.databaseOptions(),
                       __data__=__data__)[0]}
          )
        % endif
    
    % else:
        <%
        if op in (orb.Query.Op.IsIn, orb.Query.Op.IsNotIn) and not value:
            raise orb.errors.EmptyQuery()
        
        key = str(len(__data__['output']))
        __data__['output'][key] = __sql__.datastore().store(column, value)
        %>
        % if op in (orb.Query.Op.Contains, orb.Query.Op.DoesNotContain):
        <% __data__['output'][key] = '%{0}%'.format(__data__['output'][key]) %>
        % elif op in (orb.Query.Op.Startswith, orb.Query.Op.DoesNotStartwith):
        <% __data__['output'][key] = '{0}%'.format(__data__['output'][key]) %>
        % elif op in (orb.Query.Op.Endswith, orb.Query.Op.DoesNotEndwith):
        <% __data__['output'][key] = '%{0}'.format(__data__['output'][key]) %>
        % endif
        
        ${field} ${op_sql} %(${key})s
    % endif
% endif