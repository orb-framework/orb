import orb

from .connection import PostgresConnection


def add_op(column, field, math_op, value):
    """
    Generates a math operator for addition.  If the column
    is a string kind it needs a special operator.

    :param column: <orb.Column>
    :param field: <str> or <unicode>
    :param math_op: <orb.Query.Math>
    :param value: <variant>

    :return: <unicode>
    """
    if isinstance(column, orb.StringColumn):
        return u'({0} || {1})'.format(field, value)
    else:
        return u'({0} + {1})'.format(field, value)


def get_reference_type(column, context):
    """
    Returns the reference type based on the referred to column.

    :param column: <orb.ReferenceColumn>

    :return: <str>
    """
    reference_model = column.reference_model()
    reference_schema = reference_model.schema()
    reference_column = column.reference_column()

    column_type = orb.PostgresConnection.get_column_type(reference_column)
    namespace = reference_schema.namespace(context=context) or 'public'
    dbname = reference_schema.dbname()
    col_name = reference_column.field()

    return u'{0} REFERENCES "{1}"."{2}" ("{3}")'.format(column_type, namespace, dbname, col_name)

# register column information
PostgresConnection.register_type_mapping(orb.BooleanColumn, u'BOOLEAN')
PostgresConnection.register_type_mapping(orb.DataColumn, u'TEXT')
PostgresConnection.register_type_mapping(orb.DateColumn, u'DATE')
PostgresConnection.register_type_mapping(orb.DatetimeColumn, u'TIMESTAMP WITHOUT TIME ZONE')
PostgresConnection.register_type_mapping(orb.DatetimeWithTimezoneColumn, u'TIMESTAMP WITHOUT TIME ZONE')
PostgresConnection.register_type_mapping(orb.DecimalColumn,
                                         lambda x, y: u'DECIMAL({0}, {1})'.format(x.precision(), x.scale()))
PostgresConnection.register_type_mapping(orb.FloatColumn, u'DOUBLE PRECISION')
PostgresConnection.register_type_mapping(orb.IntegerColumn, u'INTEGER')
PostgresConnection.register_type_mapping(orb.IntervalColumn, u'INTERVAL')
PostgresConnection.register_type_mapping(orb.LongColumn, u'BIGINT')
PostgresConnection.register_type_mapping(orb.ReferenceColumn, get_reference_type)
PostgresConnection.register_type_mapping(orb.StringColumn, lambda x, y: u'CHARACTER VARYING({0})'.format(x.max_length()))
PostgresConnection.register_type_mapping(orb.TextColumn, u'TEXT')
PostgresConnection.register_type_mapping(orb.TimeColumn, u'TIME')
PostgresConnection.register_type_mapping(orb.TimestampColumn, u'BIGINT')
PostgresConnection.register_type_mapping(orb.UTC_DatetimeColumn, u'TIMESTAMP')
PostgresConnection.register_type_mapping(orb.UTC_TimestampColumn, u'BIGINT')

# register function information
PostgresConnection.register_function_mapping(orb.Query.Function.Lower, u'lower({0})')
PostgresConnection.register_function_mapping(orb.Query.Function.Upper, u'upper({0})')
PostgresConnection.register_function_mapping(orb.Query.Function.Abs, u'abs({0})')
PostgresConnection.register_function_mapping(orb.Query.Function.AsString, u'{0}::varchar')

# register math information
PostgresConnection.register_math_mapping(orb.Query.Math.Add, add_op)
PostgresConnection.register_math_mapping(orb.Query.Math.Subtract, u'({0} - {1})')
PostgresConnection.register_math_mapping(orb.Query.Math.Multiply, u'({0} * {1})')
PostgresConnection.register_math_mapping(orb.Query.Math.Divide, u'({0} / {1})')