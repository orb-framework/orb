<%
    import projex.text
    from collections import OrderedDict
    from orb import errors

    primary = schema.primaryColumns()[0]
    reference_model = primary.referenceModel()
    reference_table = reference_model.schema().dbname()

    preload = {}
    columns = []
    group_by = []
    joins = OrderedDict()

    def populate(schema, source, parts, alias):
        next_part = parts.pop(0)

        # look for an aggregate
        column = schema.column(next_part)

        # join in a column
        if column:
            if not column.isReference():
                return '{0}.{1}'.format(alias, QUOTE(column.fieldName()))
            else:
                ref_model = column.referenceModel()
                join_alias = QUOTE(ref_model.schema().dbname() + '_' + projex.text.underscore(column.name()))
                target = '{0}.{1}'.format(join_alias, QUOTE(ref_model.schema().primaryColumns()[0].fieldName()))
                source = '{0}.{1}'.format(alias, QUOTE(column.fieldName()))
                join = {'table': QUOTE(ref_model.schema().dbname()),
                        'alias': join_alias,
                        'on': '{0} = {1}'.format(target, source)}

                joins.setdefault(join_alias, join)
                group_by.append(target)

                if not parts:
                    return target
                else:
                    return populate(ref_model.schema(), target, parts, join_alias)

        # join in a lookup or pipe
        else:

            pipe = schema.pipe(next_part)
            if not pipe:
                rev_lookup = schema.reverseLookup(next_part)
                if not rev_lookup:
                    raise errors.ColumnNotFound(schema.name(), next_part)

            try:
                record_part = parts.pop(0)
                invert_dir = record_part == 'last'
            except IndexError:
                raise errors.QueryInvalid('Cannot join in a record set for a View.')

            # define x here or mako will not render this template (preprocessor fails to find the
            # value within list compression)
            x = 0
            if pipe:
                join_schema = pipe.targetReferenceModel().schema()
                join_primary = join_schema.primaryColumns()[0]

                pipe_schema = pipe.pipeReferenceModel().schema()
                source_col = pipe_schema.column(pipe.sourceColumn())
                target_col = pipe_schema.column(pipe.targetColumn())
                join_field = QUOTE(source_col.fieldName())

                column_name = projex.text.underscore(pipe.name())
                order = join_schema.defaultOrder() or [(join_primary.name(), 'asc')]
                if invert_dir:
                    order = [(x[0], 'asc' if x[1] == 'desc' else 'desc') for x in order]

                order = [QUOTE(join_schema.dbname(), join_schema.column(x[0]).fieldName()) + ' ' + x[1].upper()
                         for x in order]
                opts = (
                    QUOTE(source_col.fieldName()),
                    QUOTE(join_schema.dbname()),
                    QUOTE(pipe_schema.dbname()),
                    QUOTE(target_col.fieldName()),
                    QUOTE(join_schema.dbname(), join_primary.fieldName()),
                    ', '.join(order)
                )

                if record_part in ('first', 'last'):
                    join_table = QUOTE('_'.join((join_schema.dbname(), column_name, record_part)))
                    preload_as = '(SELECT DISTINCT ON (j.{0}) {1}.*, j.{0} FROM {1} ' \
                                 'LEFT JOIN {2} j ON j.{3} = {4} ORDER BY j.{0}, {5})'

                    preload.setdefault(join_table, preload_as.format(*opts))
                else:
                    join_table = QUOTE(join_schema.dbname())
            else:
                join_schema = rev_lookup.schema()
                join_primary = join_schema.primaryColumns()[0]
                join_field = QUOTE(rev_lookup.fieldName())
                column_name = projex.text.underscore(rev_lookup.reversedName())
                order = join_schema.defaultOrder() or [(join_primary.name(), 'asc')]
                if invert_dir:
                    order = [(x[0], 'asc' if x[1] == 'desc' else 'desc') for x in order]

                order = [QUOTE(join_schema.dbname(), join_schema.column(x[0]).fieldName()) + ' ' + x[1].upper()
                         for x in order]
                opts = (
                    QUOTE(rev_lookup.fieldName()),
                    QUOTE(join_schema.dbname()),
                    QUOTE(join_primary.fieldName()),
                    ', '.join(order)
                )

                if record_part in ('first', 'last'):
                    join_table = QUOTE('_'.join((join_schema.dbname(), column_name, record_part)))
                    preload_as = '(SELECT DISTINCT ON ({0}) {1}.* FROM {1} ORDER BY {0}, {3})'
                    preload.setdefault(join_table, preload_as.format(*opts))
                else:
                    join_table = QUOTE(join_schema.dbname())

            join_alias = QUOTE('_'.join((join_schema.dbname(), column_name, record_part)))
            target = '{0}.{1}'.format(join_alias, QUOTE(join_primary.fieldName()))

            join = {'table': join_table,
                    'alias': join_alias,
                    'on': '{0}.{1} = {2}'.format(join_alias, join_field, source)}
            joins.setdefault(join_alias, join)

            if record_part == 'count':
                return 'count({0}.*)'.format(join_alias)
            elif record_part == 'ids':
                return 'array_agg({0})'.format(target)
            elif not parts:
                group_by.append(target)
                return target
            else:
                return populate(join_schema, target, parts, join_alias)

    curr_schema = reference_model.schema()
    curr_field = QUOTE(reference_table, reference_model.schema().primaryColumns()[0].fieldName())

    columns.append('{0} AS {1}'.format(curr_field, QUOTE(primary.fieldName())))
    group_by.append(curr_field)

    for column in schema.columns():
        if column.primary():
            continue

        parts = column.shortcut().split('.')
        if not (len(parts) > 1 and parts[0] == primary.name()):
            msg = 'Invalid column ({0}) on View ({1}).  All columns must be shortcuts.'
            raise errors.OrbError(msg.format(column.name(), curr_schema.name()))

        field_name = populate(curr_schema, curr_field, parts[1:], QUOTE(curr_schema.dbname()))
        columns.append('{0} AS {1}'.format(field_name, column.fieldName()))
%>
DROP ${'MATERIALIZED' if schema.isStatic() else ''} VIEW IF EXISTS `${schema.dbname()}`;
CREATE ${'MATERIALIZED' if schema.isStatic() else ''} VIEW `${schema.dbname()}` AS (
% if preload:
    WITH
    % for i, (preload_table, preload_as) in enumerate(preload.items()):
    ${preload_table if not i else ',' + preload_table} AS ${preload_as}
    % endfor
% endif
    SELECT      ${',\n                '.join(columns)}
    FROM        `${reference_table}`
    % for join in joins.values():
    LEFT JOIN   ${join['table']} AS ${join['alias']} ON ${join['on']}
    % endfor
    GROUP BY    ${',\n                '.join(group_by)}
);