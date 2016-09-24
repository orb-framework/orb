import pytest
from tests.test_marks import requires_pg


# ----
# test SQL statement generation

@pytest.mark.run(order=2)
@requires_pg
def test_pg_statement_add_column(User, pg_sql):
    st = pg_sql.statement('ADD COLUMN')
    assert st is not None

    statement, data = st(User.schema().column('username'))
    assert statement == 'ADD COLUMN "username" CHARACTER VARYING(255) UNIQUE'

@pytest.mark.run(order=2)
@requires_pg
def test_pg_statement_create_table(User, pg_sql):
    st = pg_sql.statement('CREATE')
    assert st is not None

    statement, data = st(User)
    assert 'CREATE TABLE IF NOT EXISTS "public"."users"' in statement

@pytest.mark.run(order=2)
@requires_pg
def test_pg_statement_create_table_in_namespace(User, pg_sql):
    import orb
    with orb.Context(namespace='custom'):
        st = pg_sql.statement('CREATE')
        assert st is not None

        statement, data = st(User)
        assert 'CREATE TABLE IF NOT EXISTS "custom"."users"' in statement

@pytest.mark.run(order=2)
@requires_pg
def test_pg_statement_insert_records(orb, User, pg_sql):
    st = pg_sql.statement('INSERT')
    assert st is not None

    user_a = User(username='bob')
    user_b = User(username='sally')
    statement, data = st([user_a, user_b])
    assert 'INSERT INTO "public"."users"' in statement

@pytest.mark.run(order=2)
@requires_pg
def test_pg_statement_insert_records_in_namespace(orb, User, pg_sql):
    user_a = User(username='bob')
    user_b = User(username='sally')

    with orb.Context(namespace='custom'):
        st = pg_sql.statement('INSERT')
        assert st is not None

        statement, data = st([user_a, user_b])
        assert 'INSERT INTO "custom"."users"' in statement

@pytest.mark.run(order=2)
@requires_pg
def test_pg_statement_expand_column(GroupUser, pg_sql):
    col = GroupUser.schema().column('user')
    st = pg_sql.statement('SELECT EXPAND COLUMN')
    assert st is not None

    statement, data = st(col, {})

@pytest.mark.run(order=2)
@requires_pg
def test_pg_statement_alter(orb, GroupUser, pg_sql):
    add = [orb.StringColumn(name='test_add')]
    remove = [orb.StringColumn(name='test_remove')]
    st = pg_sql.statement('ALTER')
    assert st is not None

    statement, data = st(GroupUser, add, remove)
    assert 'ALTER' in statement

    add = [orb.StringColumn(name='test_add_i18n', flags={'I18n'})]
    statement, data = st(GroupUser, add)
    assert 'ALTER' in statement

@pytest.mark.run(order=2)
@requires_pg
def test_pg_statement_alter_invalid(orb, pg_sql):
    st = pg_sql.statement('ALTER')
    assert st is not None

    with pytest.raises(orb.errors.OrbError):
        statement, data = st(orb.View)

@pytest.mark.run(order=2)
@requires_pg
def test_pg_statement_create_index(orb, GroupUser, pg_sql):
    index = orb.Index(
        name='byGroupAndUser',
        columns=[
            orb.ReferenceColumn(name='group'),
            orb.ReferenceColumn(name='user')
        ]
    )
    index.setSchema(GroupUser.schema())
    st = pg_sql.statement('CREATE INDEX')
    assert st is not None

    statement, data = st(index)
    assert 'CREATE INDEX' in statement

    statement, data = st(index, checkFirst=True)
    assert 'DO $$' in statement

# ----
# test SQL statement execution

@pytest.mark.run(order=2)
@requires_pg
def test_pg_create_table(User, pg_sql, pg_db):
    st = pg_sql.statement('CREATE')
    sql, data = st(User)
    conn = pg_db.connection()
    conn.execute(sql)
    assert True

@pytest.mark.run(order=2)
@requires_pg
def test_pg_insert_bob(orb, User, pg_sql, pg_db):
    st = pg_sql.statement('INSERT')
    user_a = User({
        'username': 'bob',
        'password': 'T3st1ng!'
    })
    sql, data = st([user_a])
    conn = pg_db.connection()

    # if this has run before, then it will raise a duplicate entry
    try:
        conn.execute(sql, data)
    except orb.errors.DuplicateEntryFound:
        pass
    assert True

@pytest.mark.run(order=2)
@requires_pg
def test_pg_insert_sally(orb, User, pg_sql, pg_db):
    st = pg_sql.statement('INSERT')
    user_a = User({
        'username':'sally',
        'password': 'T3st1ng!'
    })

    sql, data = st([user_a])
    conn = pg_db.connection()

    # if this has run before, then it will raise a duplicate entry
    try:
        conn.execute(sql, data)
    except orb.errors.DuplicateEntryFound:
        pass
    assert True

@pytest.mark.run(order=2)
@requires_pg
def test_pg_select_all(orb, User, pg_sql, pg_db):
    st = pg_sql.statement('SELECT')
    sql, data = st(User, orb.Context())
    conn = pg_db.connection()
    records, count = conn.execute(sql, data)
    assert len(records) == count

@pytest.mark.run(order=2)
@requires_pg
def test_pg_select_one(orb, User, pg_sql, pg_db):
    st = pg_sql.statement('SELECT')
    sql, data = st(User, orb.Context(limit=1))
    conn = pg_db.connection()
    records, count = conn.execute(sql, data)
    assert count == 1

@pytest.mark.run(order=2)
@requires_pg
def test_pg_select_bob(orb, User, pg_sql, pg_db):
    st = pg_sql.statement('SELECT')
    sql, data = st(User, orb.Context(where=orb.Query('username') == 'bob'))
    conn = pg_db.connection()
    records, count = conn.execute(sql, data)
    assert count == 1 and records[0]['username'] == 'bob'

@pytest.mark.run(order=2)
@requires_pg
def test_pg_select_count(orb, User, pg_sql, pg_db):
    select_st = pg_sql.statement('SELECT')
    select_sql, data = select_st(User, orb.Context())

    conn = pg_db.connection()
    records, count = conn.execute(select_sql, data)

    select_count_st = pg_sql.statement('SELECT COUNT')
    select_count_sql, data = select_count_st(User, orb.Context())
    results, _ = conn.execute(select_count_sql, data)

    assert results[0]['count'] == count

@pytest.mark.run(order=2)
@requires_pg
def test_pg_select_bob_or_sally(orb, pg_sql, pg_db, User):
    st = pg_sql.statement('SELECT')
    q = orb.Query('username') == 'bob'
    q |= orb.Query('username') == 'sally'

    sql, data = st(User, orb.Context(where=q))
    conn = pg_db.connection()
    records, count = conn.execute(sql, data)
    assert count == 2 and records[0]['username'] in ('bob', 'sally') and records[1]['username'] in ('bob', 'sally')

@pytest.mark.run(order=2)
@requires_pg
def test_pg_select_bob_and_sally(orb, pg_sql, pg_db, User):
    st = pg_sql.statement('SELECT')
    q = orb.Query('username') == 'bob'
    q &= orb.Query('username') == 'sally'

    sql, data = st(User, orb.Context(where=q))
    conn = pg_db.connection()
    _, count = conn.execute(sql, data)
    assert count == 0

