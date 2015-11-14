import pytest
from test_marks import requires_pg


# ----
# test SQL statements

@requires_pg
def test_pg_statement_add_column(user_table, pg_sql):
    st = pg_sql.statement('ADD COLUMN')
    assert st is not None

    statement = st(user_table.schema().column('username'))
    assert statement == 'ADD COLUMN "username" CHARACTER VARYING(256) UNIQUE'

@requires_pg
def test_pg_statement_create_table(user_table, pg_sql):
    st = pg_sql.statement('CREATE')
    assert st is not None

    statement = st(user_table)
    assert 'CREATE TABLE IF NOT EXISTS "users"' in statement

@requires_pg
def test_pg_statement_insert_records(orb, user_table, pg_sql):
    st = pg_sql.statement('INSERT')
    assert st is not None

    user_a = user_table(username='bob')
    user_b = user_table(username='sally')
    statement, data = st([user_a, user_b])
    assert 'INSERT INTO "users"' in statement

# ----
# test SQL insertions

@requires_pg
def test_pg_create_table(user_table, pg_sql, pg_db):
    st = pg_sql.statement('CREATE')
    sql = st(user_table)
    conn = pg_db.connection()
    conn.execute(sql)
    assert True

@requires_pg
def test_pg_insert_bob(orb, user_table, pg_sql, pg_db):
    st = pg_sql.statement('INSERT')
    user_a = user_table(username='bob', password='T3st1ng!')
    sql, data = st([user_a])
    conn = pg_db.connection()

    # if this has run before, then it will raise a duplicate entry
    try:
        conn.execute(sql, data)
    except orb.errors.DuplicateEntryFound:
        pass
    assert True

@requires_pg
def test_pg_insert_sally(orb, user_table, pg_sql, pg_db):
    st = pg_sql.statement('INSERT')
    user_a = user_table(username='sally', password='T3st1ng!')
    sql, data = st([user_a])
    conn = pg_db.connection()

    # if this has run before, then it will raise a duplicate entry
    try:
        conn.execute(sql, data)
    except orb.errors.DuplicateEntryFound:
        pass
    assert True

@requires_pg
def test_pg_select_all(orb, user_table, pg_sql, pg_db):
    st = pg_sql.statement('SELECT')
    sql, data = st(user_table, orb.Context())
    conn = pg_db.connection()
    records, count = conn.execute(sql, data)
    assert len(records) == count

@requires_pg
def test_pg_select_one(orb, user_table, pg_sql, pg_db):
    st = pg_sql.statement('SELECT')
    sql, data = st(user_table, orb.Context(limit=1))
    conn = pg_db.connection()
    records, count = conn.execute(sql, data)
    assert count == 1

@requires_pg
def test_pg_select_bob(orb, user_table, pg_sql, pg_db):
    st = pg_sql.statement('SELECT')
    sql, data = st(user_table, orb.Context(where=orb.Query('username') == 'bob'))
    conn = pg_db.connection()
    records, count = conn.execute(sql, data)
    assert count == 1 and records[0]['username'] == 'bob'

@requires_pg
def test_pg_select_count(orb, user_table, pg_sql, pg_db):
    select_st = pg_sql.statement('SELECT')
    select_sql, data = select_st(user_table, orb.Context())

    conn = pg_db.connection()
    records, count = conn.execute(select_sql, data)

    select_count_st = pg_sql.statement('SELECT COUNT')
    select_count_sql, data = select_count_st(user_table, orb.Context())
    results, _ = conn.execute(select_count_sql, data)

    assert results[0]['count'] == count
