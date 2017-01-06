def test_postgres_std_column_mapping(pg_conn, Employee):
    import orb

    assert pg_conn.get_column_type(orb.BooleanColumn()) == 'BOOLEAN'
    assert pg_conn.get_column_type(orb.DataColumn()) == 'TEXT'
    assert pg_conn.get_column_type(orb.DateColumn()) == 'DATE'
    assert pg_conn.get_column_type(orb.DatetimeColumn()) == 'TIMESTAMP WITHOUT TIME ZONE'
    assert pg_conn.get_column_type(orb.DatetimeWithTimezoneColumn()) == 'TIMESTAMP WITHOUT TIME ZONE'
    assert pg_conn.get_column_type(orb.DecimalColumn(precision=1, scale=2)) == 'DECIMAL(1, 2)'
    assert pg_conn.get_column_type(orb.FloatColumn()) == 'DOUBLE PRECISION'
    assert pg_conn.get_column_type(orb.IntegerColumn()) == 'INTEGER'
    assert pg_conn.get_column_type(orb.IntervalColumn()) == 'INTERVAL'
    assert pg_conn.get_column_type(orb.LongColumn()) == 'BIGINT'
    assert pg_conn.get_column_type(orb.ReferenceColumn(model=Employee)) == 'BIGINT REFERENCES "public"."employees" ("id")'
    assert pg_conn.get_column_type(orb.StringColumn()) == 'CHARACTER VARYING(255)'
    assert pg_conn.get_column_type(orb.StringColumn(max_length=5)) == 'CHARACTER VARYING(5)'
    assert pg_conn.get_column_type(orb.TextColumn()) == 'TEXT'
    assert pg_conn.get_column_type(orb.TimeColumn()) == 'TIME'
    assert pg_conn.get_column_type(orb.TimestampColumn()) == 'BIGINT'


def test_postgres_std_query_op_mapping(pg_conn):
    import orb

    assert pg_conn.get_query_op(orb.Column(), orb.Query.Op.Is) == '='
    assert pg_conn.get_query_op(orb.Column(), orb.Query.Op.IsNot) == '!='
    assert pg_conn.get_query_op(orb.Column(), orb.Query.Op.LessThan) == '<'
    assert pg_conn.get_query_op(orb.Column(), orb.Query.Op.LessThanOrEqual) == '<='
    assert pg_conn.get_query_op(orb.Column(), orb.Query.Op.GreaterThan) == '>'
    assert pg_conn.get_query_op(orb.Column(), orb.Query.Op.GreaterThanOrEqual) == '>='
    assert pg_conn.get_query_op(orb.Column(), orb.Query.Op.After) == '>'
    assert pg_conn.get_query_op(orb.Column(), orb.Query.Op.Before) == '<'
    assert pg_conn.get_query_op(orb.Column(), orb.Query.Op.IsIn) == 'IN'
    assert pg_conn.get_query_op(orb.Column(), orb.Query.Op.IsNotIn) == 'NOT IN'
    assert pg_conn.get_query_op(orb.Column(), orb.Query.Op.Matches) == '~'
    assert pg_conn.get_query_op(orb.Column(), orb.Query.Op.Matches, case_sensitive=True) == '~*'
    assert pg_conn.get_query_op(orb.Column(), orb.Query.Op.DoesNotMatch) == '!~'
    assert pg_conn.get_query_op(orb.Column(), orb.Query.Op.DoesNotMatch, case_sensitive=True) == '!~*'

    for op in (orb.Query.Op.Contains,
               orb.Query.Op.Startswith,
               orb.Query.Op.Endswith):
        assert pg_conn.get_query_op(orb.Column(), op) == 'ILIKE'
        assert pg_conn.get_query_op(orb.Column(), op, case_sensitive=True) == 'LIKE'

    for op in (orb.Query.Op.DoesNotContain,
               orb.Query.Op.DoesNotStartwith,
               orb.Query.Op.DoesNotEndwith):
        assert pg_conn.get_query_op(orb.Column(), op) == 'NOT ILIKE'
        assert pg_conn.get_query_op(orb.Column(), op, case_sensitive=True) == 'NOT LIKE'


def test_postgres_std_function_mapping(pg_conn):
    import orb

    column = orb.Column(name='test')

    assert pg_conn.wrap_query_function(column, orb.Query.Function.Lower) == 'lower(test)'
    assert pg_conn.wrap_query_function(column, orb.Query.Function.Upper) == 'upper(test)'
    assert pg_conn.wrap_query_function(column, orb.Query.Function.Abs) == 'abs(test)'
    assert pg_conn.wrap_query_function(column, orb.Query.Function.AsString) == 'test::varchar'


def test_postgres_std_math_mapping(pg_conn):
    import orb

    column = orb.Column(name='test')
    str_column = orb.StringColumn(name='test')

    assert pg_conn.wrap_query_math(column, orb.Query.Math.Add, '1') == '(test + 1)'
    assert pg_conn.wrap_query_math(str_column, orb.Query.Math.Add, '1') == '(test || 1)'
    assert pg_conn.wrap_query_math(column, orb.Query.Math.Subtract, '1') == '(test - 1)'
    assert pg_conn.wrap_query_math(column, orb.Query.Math.Multiply, '1') == '(test * 1)'
    assert pg_conn.wrap_query_math(column, orb.Query.Math.Divide, '1') == '(test / 1)'
    assert pg_conn.wrap_query_math(column, orb.Query.Math.And, '1') == '(test & 1)'
    assert pg_conn.wrap_query_math(column, orb.Query.Math.Or, '1') == '(test | 1)'

