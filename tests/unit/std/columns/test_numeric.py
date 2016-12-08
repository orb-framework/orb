import pytest


def test_abstract_number_column():
    import orb

    col = orb.AbstractNumericColumn()

    assert col.minimum() is None
    assert col.maximum() is None

    assert col.validate(-10)
    assert col.validate(10)

    col.set_minimum(4)
    col.set_maximum(6)

    assert col.minimum() == 4
    assert col.maximum() == 6

    with pytest.raises(orb.errors.ValidationError):
        assert col.validate(3) is None

    assert col.validate(4)  # lower bound
    assert col.validate(5)  # middle
    assert col.validate(6)  # upper bound

    with pytest.raises(orb.errors.ValidationError):
        assert col.validate(7) is None


def test_abstract_number_column_copy():
    import orb

    col = orb.AbstractNumericColumn(minimum=1, maximum=10)
    col_b = col.copy()
    col_c = col.copy(minimum=2, maximum=9)

    assert col.minimum() == 1
    assert col.maximum() == 10
    assert col_b.minimum() == 1
    assert col_b.maximum() == 10
    assert col_c.minimum() == 2
    assert col_c.maximum() == 9


def test_abstract_column_from_value():
    import orb

    col = orb.AbstractNumericColumn()

    assert col.value_from_string(None) == None
    assert col.value_from_string(1.0) == 1.0
    assert col.value_from_string('-1') == -1
    assert col.value_from_string('10.5') == 10.5

    with pytest.raises(orb.errors.ValidationError):
        assert col.value_from_string('failure') is None


def test_decimal_column():
    import orb

    col = orb.DecimalColumn()
    assert col.precision() == 65
    assert col.scale() == 30

    col.set_precision(40)
    col.set_scale(20)

    assert col.precision() == 40
    assert col.scale() == 20


def test_decimal_column_copy():
    import orb

    col = orb.DecimalColumn(precision=20, scale=5)
    col_b = col.copy()
    col_c = col.copy(precision=10, scale=2)

    assert col.precision() == col_b.precision() == 20
    assert col.scale() == col_b.scale() == 5
    assert col_c.precision() == 10
    assert col_c.scale() == 2


def test_integer_column():
    import orb

    col = orb.IntegerColumn()

    # 32-bit min/max for integer columns
    int_32 = (2**31) - 1

    assert col.minimum() == -int_32
    assert col.maximum() == int_32

    assert col.restore(10.2) == 10
    assert col.store(10.1) == 10


def test_enum_column():
    import orb

    col = orb.EnumColumn(enum=orb.Column.Flags)
    assert col.enum() == orb.Column.Flags

    col.set_enum(orb.Query.Function)
    col_b = col.copy()
    col_c = col.copy(enum=orb.Query.Op)

    assert col_b.enum() == orb.Query.Function
    assert col_c.enum() == orb.Query.Op


def test_id_column():
    import orb

    col = orb.IdColumn()

    assert isinstance(col, orb.LongColumn)

    assert col.test_flag(orb.Column.Flags.Unique)
    assert col.test_flag(orb.Column.Flags.AutoIncrement)
    assert col.test_flag(orb.Column.Flags.Required)
