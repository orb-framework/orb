def test_basic_column_creation():
    from orb.core.column import Column

    column = Column()

    assert column.name() == ''
    assert column.shortcut() == ''
    assert column.display() == ''
    assert column.flags() == 0
    assert column.default() is None
    assert column.schema() is None
    assert column.order() == 99999

    assert column.alias() == ''
    assert column.field() == ''

    assert column.read_permit() is None
    assert column.write_permit() is None

    assert column.gettermethod() is None
    assert column.settermethod() is None
    assert column.filtermethod() is None