import os
from projex.lazymodule import lazy_import
from ..psqlconnection import PSQLStatement

orb = lazy_import('orb')


class WHERE(PSQLStatement):
    def __call__(self, model, query, context, aliases=None, fields=None):
        if query is None or model is None:
            return u'', {}

        aliases = aliases or {}
        fields = fields or {}
        data = {}
        query = query.expand(model)
        if query is None:
            return u'', {}

        # generate a query compound
        if isinstance(query, orb.QueryCompound):
            sub_query_sql = []
            for sub_query in query:
                try:
                    sub_sql, sub_data = self(model, sub_query, context, aliases, fields)

                # if a sub-query is null, for OR'd queries, we can just ignore that
                # criteria since it will not affect it, for AND'd criteria we
                # can propagate the error up since it will affect all of the
                # rest of the query
                except orb.errors.QueryIsNull:
                    if query.op() == query.Op.And:
                        raise
                    else:
                        continue

                if sub_sql:
                    sub_query_sql.append(sub_sql)
                    data.update(sub_data)

            joiner = u' AND ' if query.op() == query.Op.And else u' OR '
            sql = u'({0})'.format(joiner.join(sub_query_sql))

        else:
            try:
                column = query.column(model)
            except orb.errors.ColumnNotFound:
                # check to see if the query has a collector vs. column
                collector = query.collector(model)

                # determine if the collector has a filter
                if collector:
                    query_filter = collector.queryFilterMethod()
                    if query_filter:
                        new_query = query_filter(model, query)
                        return self(model, new_query, context, aliases=aliases, fields=fields)
                    else:
                        raise
                else:
                    raise

            # generate the sql field
            field = fields.get(column) or self.generateField(model, column, query, aliases)
            value_key = u'{0}_{1}'.format(column.field(), os.urandom(4).encode('hex'))

            # calculate any math operations to the sql field
            for op, target in query.math():
                field = column.dbMath('Default', field, op, target)

            # get the additional information
            value = query.value()
            op = query.op()
            case_sensitive = query.caseSensitive()
            invert = query.isInverted()

            try:
                sql_op = self.opSql(op, case_sensitive)
            except KeyError:
                raise orb.errors.QueryInvalid('{0} is an unknown operator'.format(orb.Query.Op(op)))

            def convert_value(val):
                if isinstance(val, orb.Model):
                    return val.get(val.schema().idColumn(), inflated=False)
                elif isinstance(val, (tuple, list, set)):
                    return tuple(convert_value(v) for v in val)
                else:
                     return val

            value = convert_value(value)

            # convert data from a query
            if isinstance(value, (orb.Query, orb.QueryCompound)):
                val_model = value.model()
                val_column = value.column()
                val_field = self.generateField(val_model, val_column, value, aliases)
                if invert:
                    sql = u' '.join((val_field, sql_op, field))
                else:
                    sql = u' '.join((field, sql_op, val_field))

            # convert a null value
            elif value is None:
                if op in (orb.Query.Op.Is, orb.Query.Op.Matches):
                    sql = u'{0} IS NULL'.format(field)
                elif op in (orb.Query.Op.IsNot, orb.Query.Op.DoesNotMatch):
                    sql = u'{0} IS NOT NULL'.format(field)
                else:
                    raise orb.errors.QueryInvalid('Invalid operation for NULL: {0}'.format(orb.Query.Op(op)))

            # convert a collection value
            elif isinstance(value, orb.Collection):
                SELECT = self.byName('SELECT')
                context = value.context()
                if not context.columns:
                    context.columns = [value.model().schema().idColumn()]
                    context.distinct = True

                sub_sql, sub_data = SELECT(value.model(), context, fields=fields)
                if sub_sql:
                    sql = u'{0} {1} ({2})'.format(field, sql_op, sub_sql.strip(';'))
                    data.update(sub_data)
                else:
                    raise orb.errors.QueryInvalid('Could not create sub-query')

            # convert all other data
            else:
                # if the query is value IS IN <empty list>, the result will always be
                # empty, no need to query
                if op == orb.Query.Op.IsIn and not value:
                    raise orb.errors.QueryIsNull()

                # if the query is value IS NOT IN <empty list>, the result will
                # always return all records because the value will never be found
                # in an empty list -- we can just ignore this filter
                elif op == orb.Query.Op.IsNotIn and not value:
                    return '', {}
                elif op in (orb.Query.Op.Contains, orb.Query.Op.DoesNotContain):
                    value = u'%{0}%'.format(value)
                elif op in (orb.Query.Op.Startswith, orb.Query.Op.DoesNotStartwith):
                    value = u'{0}%'.format(value)
                elif op in (orb.Query.Op.Endswith, orb.Query.Op.DoesNotEndwith):
                    value = u'%{0}'.format(value)

                if invert:
                    opts = (u'%({0})s'.format(value_key), sql_op, field)
                else:
                    opts = (field, sql_op, u'%({0})s'.format(value_key))

                sql = u' '.join(opts)
                data[value_key] = value

                if column.testFlag(column.Flags.I18n) and column not in fields:
                    model_name = aliases.get(model) or model.schema().dbname()
                    i18n_sql = u'"{name}"."{field}" IN (' \
                          u'    SELECT "{name}_id"' \
                          u'    FROM "{namespace}"."{name}_i18n"' \
                          u'    WHERE {sub_sql}' \
                          u')'

                    sub_sql = sql.replace('"{0}"'.format(model_name), '"{0}_i18n"'.format(model_name))

                    if context.locale != 'all':
                        sub_sql += ' AND locale = %(locale)s'

                    sql = i18n_sql.format(name=model_name,
                                          namespace=model.schema().namespace() or 'public',
                                          sub_sql=sub_sql,
                                          field=model.schema().idColumn().field())

        return sql, data

    def generateField(self, model, column, query, aliases):
        alias = aliases.get(model) or model.schema().dbname()
        field = column.field()

        sql_field = '"{0}"."{1}"'.format(alias, field)

        # process any functions on the query
        for func in query.functions():
            try:
                sql_func = self.funcSql(func)
                sql_field = sql_func.format(sql_field)
            except KeyError:
                msg = 'Unknown function type: {0}'.format(orb.Query.Function(func))
                raise orb.errors.QueryInvalid(msg)

        return sql_field

    @staticmethod
    def opSql(op, caseSensitive=False):
        general_mapping = {
            orb.Query.Op.Is: u'=',
            orb.Query.Op.IsNot: u'!=',
            orb.Query.Op.LessThan: u'<',
            orb.Query.Op.Before: u'<',
            orb.Query.Op.LessThanOrEqual: u'<=',
            orb.Query.Op.GreaterThanOrEqual: u'>=',
            orb.Query.Op.GreaterThan: u'>',
            orb.Query.Op.After: u'>',
            orb.Query.Op.IsIn: u'IN',
            orb.Query.Op.IsNotIn: u'NOT IN'
        }

        sensitive_mapping = {
            orb.Query.Op.Matches: u'~',
            orb.Query.Op.DoesNotMatch: u'!~',
            orb.Query.Op.Contains: u'LIKE',
            orb.Query.Op.DoesNotContain: u'NOT LIKE',
            orb.Query.Op.Startswith: u'LIKE',
            orb.Query.Op.Endswith: u'LIKE'
        }

        non_sensitive_mapping = {
            orb.Query.Op.Matches: u'~*',
            orb.Query.Op.DoesNotMatch: u'!~*',
            orb.Query.Op.Contains: u'ILIKE',
            orb.Query.Op.DoesNotContain: u'NOT ILIKE',
            orb.Query.Op.Startswith: u'ILIKE',
            orb.Query.Op.Endswith: u'ILIKE'
        }

        return general_mapping.get(op) or (sensitive_mapping[op] if caseSensitive else non_sensitive_mapping[op])

    @staticmethod
    def funcSql(func):
        func_mapping = {
            orb.Query.Function.Lower: u'lower({0})',
            orb.Query.Function.Upper: u'upper({0})',
            orb.Query.Function.Abs: u'abs({0})',
            orb.Query.Function.AsString: u'{0}::varchar'
        }
        return func_mapping[func]

PSQLStatement.registerAddon('WHERE', WHERE())
