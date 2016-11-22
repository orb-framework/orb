def test_basic_column_engine():
    from orb.core.column import Column
    from orb.core.column_engine import ColumnEngine

    assert isinstance(Column.__default_engine__, ColumnEngine)
    assert Column.get_engine(None) == Column.__default_engine__
    assert Column.get_engine('Postgres') == Column.__default_engine__
    assert Column.get_engine('MySQL') == Column.__default_engine__


def test_backend_specific_engine():
    from orb.core.column import Column
    from orb.core.column_engine import ColumnEngine

    a_engine = ColumnEngine()
    b_engine = ColumnEngine()

    class TestColumn(Column):
        __engines__ = {
            'Postgres': a_engine,
            'MySQL': b_engine
        }

    assert TestColumn.get_engine('SQLite') == TestColumn.__default_engine__
    assert TestColumn.get_engine('Postgres') == a_engine
    assert TestColumn.get_engine('MySQL') == b_engine


def test_custom_engine():
    from orb.core.column import Column
    from orb.core.column_engine import ColumnEngine

    class CustomEngine(ColumnEngine):
        pass

    a_engine = CustomEngine()

    class TestColumn(Column):
        __default_engine__ = a_engine

    assert TestColumn.get_engine('SQLite') == a_engine
    assert TestColumn.get_engine('Postgres') == a_engine
    assert TestColumn.get_engine('MySQL') == a_engine


def test_engine_operators():
    from orb.core.column import Column
    from orb.core.column_engine import ColumnEngine
    from orb.core.query import Query as Q

    column = Column()

    typ = 'Postgres'
    e = ColumnEngine()

    assert e.get_math_statement(column, typ, 'a', Q.Math.Add, 'b') == 'a + b'
    assert e.get_math_statement(column, typ, 'a', Q.Math.Subtract, 'b') == 'a - b'
    assert e.get_math_statement(column, typ, 'a', Q.Math.Multiply, 'b') == 'a * b'
    assert e.get_math_statement(column, typ, 'a', Q.Math.Divide, 'b') == 'a / b'
    assert e.get_math_statement(column, typ, 'a', Q.Math.And, 'b') == 'a & b'
    assert e.get_math_statement(column, typ, 'a', Q.Math.Or, 'b') == 'a | b'


def test_engine_operator_override():
    from orb.core.column import Column
    from orb.core.column_engine import ColumnEngine
    from orb.core.query import Query as Q

    column = Column()

    typ = 'Postgres'
    e = ColumnEngine()
    e.assign_operator(Q.Math.Add, '||')
    e.assign_operators({
        Q.Math.Subtract: '--',
        Q.Math.Multiply: '**'
    })

    assert e.get_math_statement(column, typ, 'a', Q.Math.Add, 'b') == 'a || b'
    assert e.get_math_statement(column, typ, 'a', Q.Math.Subtract, 'b') == 'a -- b'
    assert e.get_math_statement(column, typ, 'a', Q.Math.Multiply, 'b') == 'a ** b'


def test_engine_get_column_type():
    from orb.core.column import Column
    from orb.core.column_engine import ColumnEngine

    column = Column()
    engine = ColumnEngine()

    assert engine.get_column_type(column, 'Postgres') is None

    engine = ColumnEngine({
        'Postgres': 'INT',
        'MySQL': 'INTEGER',
        'default': 'long'
    })

    assert engine.get_column_type(column, 'Postgres') == 'INT'
    assert engine.get_column_type(column, 'MySQL') == 'INTEGER'
    assert engine.get_column_type(column, 'SQLite') == 'long'
    assert engine.get_column_type(column, 'SQL Server') == 'long'


def test_engine_get_api_value():
    from orb.core.column import Column
    from orb.core.column_engine import ColumnEngine

    column = Column()
    column_i18n = Column(flags={'I18n'})
    engine = ColumnEngine()

    assert engine.get_api_value(column, '', 10) == 10
    assert engine.get_api_value(column_i18n, '', {'en_US': 10}) == {'en_US': 10}
    assert engine.get_api_value(column_i18n, '', "{'en_US': 10}") == {'en_US': 10}
    assert engine.get_api_value(column_i18n, '', 'testing') == {'en_US': 'testing'}
    assert engine.get_api_value(column_i18n, '', "{'en_US': pass}") == {'en_US': "{'en_US': pass}"}


def test_engine_get_database_value():
    import orb

    from orb.core.column import Column
    from orb.core.collection import Collection
    from orb.core.column_engine import ColumnEngine

    column = Column()
    engine = ColumnEngine()

    class User(orb.Table):
        id = orb.IdColumn()

    a = User({'id': 1})
    b = User({'id': 2})
    a.mark_loaded()
    b.mark_loaded()

    coll = Collection([a, b])

    assert engine.get_database_value(column, '', 10) == 10
    assert engine.get_database_value(column, '', [1,2,3]) == (1,2,3)
    assert engine.get_database_value(column, '', coll.first()) == 1
    assert engine.get_database_value(column, '', coll) == (1,2)