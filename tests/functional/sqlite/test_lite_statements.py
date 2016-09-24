def test_lite_statement_add_column(User, lite_sql):
    st = lite_sql.statement('ADD COLUMN')
    assert st is not None

    statement, data = st(User.schema().column('username'))
    assert statement == 'ADD COLUMN `username` TEXT(255) UNIQUE'

def test_lite_statement_create_table(User, lite_sql):
    st = lite_sql.statement('CREATE')
    assert st is not None

    statement, data = st(User)
    assert 'CREATE TABLE IF NOT EXISTS `users`' in statement

def test_lite_statement_insert_records(orb, User, lite_sql):
    st = lite_sql.statement('INSERT')
    assert st is not None

    user_a = User(username='bob')
    user_b = User(username='sally')
    statement, data = st([user_a, user_b])
    assert 'INSERT INTO `users`' in statement

def test_lite_statement_alter(orb, GroupUser, lite_sql):
    add = [orb.StringColumn(name='test_add')]
    remove = [orb.StringColumn(name='test_remove')]
    st = lite_sql.statement('ALTER')
    assert st is not None

    statement, data = st(GroupUser, add, remove)
    assert 'ALTER' in statement

    add = [orb.StringColumn(name='test_add_i18n', flags={'I18n'})]
    statement, data = st(GroupUser, add)
    assert 'ALTER' in statement

def test_lite_statement_alter_invalid(orb, lite_sql):
    st = lite_sql.statement('ALTER')
    assert st is not None

    with pytest.raises(orb.errors.OrbError):
        statement, data = st(orb.View)

def test_lite_statement_create_index(orb, GroupUser, lite_sql):
    index = orb.Index(
        name='byGroupAndUser',
        columns=[
            orb.ReferenceColumn(name='group'),
            orb.ReferenceColumn(name='user')
        ]
    )
    index.setSchema(GroupUser.schema())
    st = lite_sql.statement('CREATE INDEX')
    assert st is not None

    statement, data = st(index)
    assert 'CREATE INDEX' in statement

    statement, data = st(index, checkFirst=True)
    assert 'DO $$' in statement

def test_lite_create_table(User, lite_sql, lite_db):
    st = lite_sql.statement('CREATE')
    sql, data = st(User)
    conn = lite_db.connection()
    conn.execute(sql)
    assert True

def test_lite_insert_bob(orb, User, lite_sql, lite_db):
    st = lite_sql.statement('INSERT')
    user_a = User({
        'username': 'bob',
        'password': 'T3st1ng!'
    })
    sql, data = st([user_a])
    conn = lite_db.connection()

    # if this has run before, then it will raise a duplicate entry
    try:
        conn.execute(sql, data)
    except orb.errors.DuplicateEntryFound:
        pass
    assert True

def test_lite_insert_sally(orb, User, lite_sql, lite_db):
    st = lite_sql.statement('INSERT')
    user_a = User({
        'username':'sally',
        'password': 'T3st1ng!'
    })

    sql, data = st([user_a])
    conn = lite_db.connection()

    # if this has run before, then it will raise a duplicate entry
    try:
        conn.execute(sql, data)
    except orb.errors.DuplicateEntryFound:
        pass
    assert True

def test_lite_select_all(orb, User, lite_sql, lite_db):
    st = lite_sql.statement('SELECT')
    sql, data = st(User, orb.Context())
    conn = lite_db.connection()
    records, count = conn.execute(sql, data)
    assert len(records) == count

def test_lite_select_one(orb, User, lite_sql, lite_db):
    st = lite_sql.statement('SELECT')
    sql, data = st(User, orb.Context(limit=1))
    conn = lite_db.connection()
    records, count = conn.execute(sql, data)
    assert count == 1

def test_lite_select_bob(orb, User, lite_sql, lite_db):
    st = lite_sql.statement('SELECT')
    sql, data = st(User, orb.Context(where=orb.Query('username') == 'bob'))
    conn = lite_db.connection()
    records, count = conn.execute(sql, data)
    assert count == 1 and records[0]['username'] == 'bob'

def test_lite_select_count(orb, User, lite_sql, lite_db):
    select_st = lite_sql.statement('SELECT')
    select_sql, data = select_st(User, orb.Context())

    conn = lite_db.connection()
    records, count = conn.execute(select_sql, data)

    select_count_st = lite_sql.statement('SELECT COUNT')
    select_count_sql, data = select_count_st(User, orb.Context())
    results, _ = conn.execute(select_count_sql, data)

    assert results[0]['count'] == count

def test_lite_select_bob_or_sally(orb, lite_sql, lite_db, User):
    st = lite_sql.statement('SELECT')
    q = orb.Query('username') == 'bob'
    q |= orb.Query('username') == 'sally'

    sql, data = st(User, orb.Context(where=q))
    conn = lite_db.connection()
    records, count = conn.execute(sql, data)
    assert count == 2 and records[0]['username'] in ('bob', 'sally') and records[1]['username'] in ('bob', 'sally')

def test_lite_select_bob_and_sally(orb, lite_sql, lite_db, User):
    st = lite_sql.statement('SELECT')
    q = orb.Query('username') == 'bob'
    q &= orb.Query('username') == 'sally'

    sql, data = st(User, orb.Context(where=q))
    conn = lite_db.connection()
    _, count = conn.execute(sql, data)
    assert count == 0

