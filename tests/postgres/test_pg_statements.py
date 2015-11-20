import pytest
from test_marks import requires_pg


# ----
# test SQL statement generation

@requires_pg
def test_pg_statement_add_column(User, pg_sql):
    st = pg_sql.statement('ADD COLUMN')
    assert st is not None

    statement, data = st(User.schema().column('username'))
    assert statement == 'ADD COLUMN "username" CHARACTER VARYING(256) UNIQUE'
@requires_pg
def test_pg_statement_create_table(User, pg_sql):
    st = pg_sql.statement('CREATE')
    assert st is not None

    statement, data = st(User)
    assert 'CREATE TABLE IF NOT EXISTS "users"' in statement

@requires_pg
def test_pg_statement_insert_records(orb, User, pg_sql):
    st = pg_sql.statement('INSERT')
    assert st is not None

    user_a = User(username='bob')
    user_b = User(username='sally')
    statement, data = st([user_a, user_b])
    assert 'INSERT INTO "users"' in statement

@requires_pg
def test_pg_statement_expand_column(GroupUser, pg_sql):
    col = GroupUser.schema().column('user')
    st = pg_sql.statement('SELECT EXPAND COLUMN')
    assert st is not None

    statement, data = st(col, {})

# ----
# test SQL statement execution

@requires_pg
def test_pg_create_table(User, pg_sql, pg_db):
    st = pg_sql.statement('CREATE')
    sql, data = st(User)
    conn = pg_db.connection()
    conn.execute(sql)
    assert True

@requires_pg
def test_pg_insert_bob(orb, User, pg_sql, pg_db):
    st = pg_sql.statement('INSERT')
    user_a = User(username='bob', password='T3st1ng!')
    sql, data = st([user_a])
    conn = pg_db.connection()

    # if this has run before, then it will raise a duplicate entry
    try:
        conn.execute(sql, data)
    except orb.errors.DuplicateEntryFound:
        pass
    assert True

@requires_pg
def test_pg_insert_sally(orb, User, pg_sql, pg_db):
    st = pg_sql.statement('INSERT')
    user_a = User(username='sally', password='T3st1ng!')

    sql, data = st([user_a])
    conn = pg_db.connection()

    # if this has run before, then it will raise a duplicate entry
    try:
        conn.execute(sql, data)
    except orb.errors.DuplicateEntryFound:
        pass
    assert True

@requires_pg
def test_pg_select_all(orb, User, pg_sql, pg_db):
    st = pg_sql.statement('SELECT')
    sql, data = st(User, orb.Context())
    conn = pg_db.connection()
    records, count = conn.execute(sql, data)
    assert len(records) == count

@requires_pg
def test_pg_select_one(orb, User, pg_sql, pg_db):
    st = pg_sql.statement('SELECT')
    sql, data = st(User, orb.Context(limit=1))
    conn = pg_db.connection()
    records, count = conn.execute(sql, data)
    assert count == 1

@requires_pg
def test_pg_select_bob(orb, User, pg_sql, pg_db):
    st = pg_sql.statement('SELECT')
    sql, data = st(User, orb.Context(where=orb.Query('username') == 'bob'))
    conn = pg_db.connection()
    records, count = conn.execute(sql, data)
    assert count == 1 and records[0]['username'] == 'bob'

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

@requires_pg
def test_pg_select_bob_or_sally(orb, pg_sql, pg_db, User):
    st = pg_sql.statement('SELECT')
    q = orb.Query('username') == 'bob'
    q |= orb.Query('username') == 'sally'

    sql, data = st(User, orb.Context(where=q))
    conn = pg_db.connection()
    records, count = conn.execute(sql, data)
    assert count == 2 and records[0]['username'] in ('bob', 'sally') and records[1]['username'] in ('bob', 'sally')

@requires_pg
def test_pg_select_bob_and_sally(orb, pg_sql, pg_db, User):
    st = pg_sql.statement('SELECT')
    q = orb.Query('username') == 'bob'
    q &= orb.Query('username') == 'sally'

    sql, data = st(User, orb.Context(where=q))
    conn = pg_db.connection()
    _, count = conn.execute(sql, data)
    assert count == 0

