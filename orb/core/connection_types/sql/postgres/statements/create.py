import projex.text

from collections import OrderedDict
from projex.lazymodule import lazy_import
from ..psqlconnection import PSQLStatement

orb = lazy_import('orb')


class CREATE(PSQLStatement):
    def __call__(self, model, owner='', includeReferences=True):
        if issubclass(model, orb.Table):
            return self._createTable(model, owner, includeReferences)
        elif issubclass(model, orb.View):
            return self._createView(model, owner, includeReferences)
        else:
            raise orb.errors.OrbError('Cannot create model for type: '.format(type(model)))

    def _createView(self, model, owner, includeReferences):
        schema = model.schema()
        data = {}

        namespace = schema.namespace() or 'public'
        id_column = schema.idColumn()
        base_model = id_column.referenceModel()
        base_schema = base_model.schema()
        base_id = base_schema.idColumn()

        preload = {}
        columns = []
        group_by = []
        joins = OrderedDict()

        def populate(schema, source, parts, alias):
            next_part = parts.pop(0)

            # look for an aggregate
            column = schema.column(next_part, raise_=False)

            # join in a column
            if column:
                if not isinstance(column, orb.ReferenceColumn):
                    return '"{0}"."{1}"'.format(alias, column.field())
                else:
                    ref_model = column.referenceModel()
                    alias = alias or ref_model.schema().dbname()
                    join_alias = alias + '_' + projex.text.underscore(column.name())
                    target = '"{0}"."{1}"'.format(join_alias, ref_model.schema().idColumn().field())
                    source = '"{0}"."{1}"'.format(alias, column.field())
                    join = {
                        'namespace': ref_model.schema().namespace() or 'public',
                        'table': ref_model.schema().dbname(),
                        'alias': join_alias,
                        'on': '{0} = {1}'.format(target, source)
                    }

                    joins.setdefault(join_alias, join)
                    group_by.append(target)

                    if not parts:
                        return target
                    else:
                        return populate(ref_model.schema(), target, parts, join_alias)

            # join a collector
            else:
                collector = schema.collector(next_part)
                if not collector:
                    raise orb.errors.ColumnNotFound(schema=schema, column=next_part)

                try:
                    record_part = parts.pop(0)
                    invert_dir = record_part == 'last'
                except IndexError:
                    raise orb.errors.QueryInvalid('Cannot join in a collection for a view')

                if isinstance(collector, orb.Pipe):
                    join_schema = collector.toModel().schema()
                    join_id = join_schema.idColumn()

                    pipe_schema = collector.throughModel().schema()
                    source_col = collector.fromColumn()
                    target_col = collector.toColumn()
                    join_field = '"{0}"'.format(source_col.field())

                    column_name = projex.text.underscore(collector.name())
                    order = join_schema.defaultOrder() or [(join_id.field(), 'asc')]
                    if invert_dir:
                        order = [(x[0], 'asc' if x[1] == 'desc' else 'desc') for x in order]

                    order = [
                        '"{0}"."{1}"."{2}" {3}'.format(join_schema.namespace() or 'public',
                                                       join_schema.dbname(),
                                                       join_schema.column(x[0]).field(), x[1].upper())
                        for x in order
                    ]

                    opts = {
                        'source': '"{0}"'.format(source_col.field()),
                        'namespace': '"{0}"'.format(join_schema.namespace() or 'public'),
                        'model': '"{0}"'.format(join_schema.dbname()),
                        'through_namespace': '"{0}"'.format(pipe_schema.namespace() or 'public'),
                        'through': '"{0}"'.format(pipe_schema.dbname()),
                        'target': '"{0}"'.format(target_col.field()),
                        'column': '"{0}"'.format(join_schema.dbname(), join_id.field()),
                        'order': ', '.join(order)
                    }

                    if record_part in ('first', 'last'):
                        join_table = '"{0}_{1}_{2}"'.format(join_schema.dbname(), column_name, record_part)
                        preload_as = '(' \
                                     '    SELECT DISTINCT ON (j.{source}) {model}.*, j.{source}' \
                                     '    FROM {namespace}.{model}' \
                                     '    LEFT JOIN {through_namespace}.{through} ON j.{target} = {column}' \
                                     '    ORDER BY j.{source}, {order}' \
                                     ')'
                        preload.setdefault(join_table, preload_as.format(**opts))
                    else:
                        join_table = '"{0}"."{1}"'.format(join_schema.namespace() or 'public',
                                                          join_schema.dbname())
                else:
                    join_schema = collector.targetModel()
                    join_id = join_schema.idColumn()
                    join_field = '"{0}"'.format(join_id.field())
                    column_name = projex.text.underscore(collector.name())
                    order = join_schema.defaultOrder() or [(join_id.field(), 'asc')]
                    if invert_dir:
                        order = [(x[0], 'asc' if x[1] == 'desc' else 'desc') for x in order]

                    order = [
                        '"{0}"."{1}"."{2}" {3}'.format(join_schema.namespace() or 'public',
                                                       join_schema.dbname(),
                                                       join_schema.column(x[0]).field(), x[1].upper())
                        for x in order
                    ]

                    opts = {
                        'source': collector.field(),
                        'namespace': join_schema.namespace() or 'public',
                        'target': join_schema.dbname(),
                        'column': join_id.field(),
                        'order': ', '.join(order)
                    }

                    if record_part in ('first', 'last'):
                        join_table = '"{0}"."{1}"."{2}"'.format(join_schema.dbname(), column_name, record_part)
                        preload_as = '(' \
                                     '    SELECT DISTINCT ON ({source}) {target}.*' \
                                     '    FROM "{namespace}"."{target}"' \
                                     '    ORDER BY {source}, {column}' \
                                     ')'
                        preload.setdefault(join_table, preload_as.format(**opts))
                    else:
                        join_table = '"{0}"."{1}"'.format(join_schema.namespace() or 'public',
                                                          join_schema.dbname())

                join_alias = '"{0}_{1}_{2}"'.format(join_schema.dbname(), column_name, record_part)
                target = '{0}."{1}"'.format(join_alias, join_id.field())

                opts = {
                    'table': join_table,
                    'alias': join_alias,
                    'on': '{0}.{1} = {2}'.format(join_alias, join_field, source)
                }
                joins.setdefault(join_alias, opts)

                if record_part == 'count':
                    return 'count({0}.*)'.format(join_alias)
                elif record_part == 'ids':
                    return 'array_agg({0})'.format(target)
                elif not parts:
                    group_by.append(target)
                    return target
                else:
                    return populate(join_schema, target, parts, join_alias)

        curr_schema = base_model.schema()
        curr_field = '"{0}"."{1}"'.format(base_model.schema().dbname(), base_id.field())

        columns.append('{0} AS "{1}"'.format(curr_field, schema.idColumn().field()))
        group_by.append(curr_field)

        for column in schema.columns().values():
            if column == schema.idColumn():
                continue
            else:
                parts = column.shortcut().split('.')
                if not (len(parts) > 1 and parts[0] == schema.idColumn().name()):
                    raise orb.errors.QueryInvalid('All view columns must originate from the id')

                field_name = populate(curr_schema, curr_field, parts[1:], curr_schema.dbname())
                columns.append('{0} AS "{1}"'.format(field_name, column.field()))

        kwds = {
            'materialized': 'MATERIALIZED' if schema.testFlags(schema.Flags.Static) else '',
            'view': schema.dbname(),
            'base_table': base_schema.dbname(),
            'preload': ','.join(['{0} AS {1}'.format(k, v) for k, v in preload.items()]),
            'columns': ','.join(columns),
            'joins': '\n'.join(['LEFT JOIN {table} AS {alias} ON {on}'.format(**join) for join in joins.values()]),
            'group_by': ','.join(group_by)
        }

        statements = (
            'DROP {materialized} VIEW IF EXISTS "{view}";',
            'CREATE {materialized} VIEW "{view}" AS (',
            '   {preload}',
            '   SELECT {columns}',
            '   FROM {base_table}',
            '   {joins}',
            '   GROUP BY {group_by}'
            ');'
        )
        sql = '\n'.join(statements).format(**kwds)
        return sql, data

    def _createTable(self, model, owner, includeReferences):
        ADD_COLUMN = self.byName('ADD COLUMN')

        data = {}
        add_i18n = []
        add_standard = []

        # divide columns between standard and translatable
        for col in model.schema().columns(recurse=False).values():
            if not includeReferences and isinstance(col, orb.ReferenceColumn):
                continue

            # virtual flags do not exist in the database
            elif col.testFlag(col.Flags.Virtual):
                continue

            if col.testFlag(col.Flags.I18n):
                add_i18n.append(col)
            else:
                add_standard.append(col)

        # create the standard model
        cmd_body = []
        if add_standard:
            field_statements = []

            for col in add_standard:
                field_statement, field_data = ADD_COLUMN(col)
                data.update(field_data)
                field_statements.append(field_statement)

            cmd_body += [statement.replace('ADD COLUMN ', '') for statement in field_statements]

        # get the primary column
        pcol = ''
        id_column = model.schema().idColumn()
        if id_column:
            pcol = '"{0}"'.format(id_column.field())
            cmd_body.append('CONSTRAINT "{0}_pkey" PRIMARY KEY ({1})'.format(model.schema().dbname(), pcol))

        body = ',\n\t'.join(cmd_body)
        if body:
            body = '\n\t' + body + '\n'

        inherits = model.schema().inherits()
        if inherits:
            inherits_model = orb.system.model(inherits)
            if not inherits_model:
                raise orb.errors.ModelNotFound(schema=inherits)

            inherits = '\nINHERITS ("{0}")\n'.format(inherits_model.schema().dbname())

        cmd  = 'CREATE TABLE IF NOT EXISTS "{namespace}"."{table}" ({body}) {inherits}WITH (OIDS=FALSE);\n'
        if owner:
            cmd += 'ALTER TABLE "{namespace}"."{table}" OWNER TO "{owner}";'
        cmd = cmd.format(namespace=model.schema().namespace() or 'public',
                         table=model.schema().dbname(),
                         body=body,
                         inherits=inherits,
                         owner=owner)

        # create the i18n model
        if add_i18n:
            id_column = model.schema().idColumn()
            id_type = id_column.dbType('Postgres')

            field_statements = []

            for col in add_i18n:
                field_statement, field_data = ADD_COLUMN(col)
                data.update(field_data)
                field_statements.append(field_statement)

            i18n_body = ',\n\t'.join([statement.replace('ADD COLUMN ', '') for statement in field_statements])

            i18n_cmd  = 'CREATE TABLE "{namespace}"."{table}_i18n" (\n'
            i18n_cmd += '   "locale" CHARACTER VARYING(5),\n'
            i18n_cmd += '   "{table}_id" {id_type} REFERENCES "{namespace}"."{table}" ({pcol}),\n'
            i18n_cmd += '   {body},\n'
            i18n_cmd += '   CONSTRAINT "{table}_i18n_pkey" PRIMARY KEY ("{table}_id", "locale")\n'
            i18n_cmd += ') WITH (OIDS=FALSE);\n'
            if owner:
                i18n_cmd += 'ALTER TABLE "{namespace}"."{table}_i18n" OWNER TO "{owner}";'

            i18n_cmd = i18n_cmd.format(
                namespace=model.schema().namespace() or 'public',
                table=model.schema().dbname(),
                id_type=id_type,
                pcol=pcol,
                body=i18n_body,
                owner=owner
            )

            cmd += '\n' + i18n_cmd

        return cmd, data

PSQLStatement.registerAddon('CREATE', CREATE())