def test_my_statement_add_column(User, my_sql):
    st = my_sql.statement('ADD COLUMN')
    assert st is not None

    statement, data = st(User.schema().column('username'))
    assert statement == 'ADD COLUMN `username` varchar(255) UNIQUE'

def test_my_statement_create_table(User, my_sql):
    st = my_sql.statement('CREATE')
    assert st is not None

    statement, data = st(User)
    assert 'CREATE TABLE IF NOT EXISTS `orb_testing`.`users`' in statement

def test_my_statement_create_table_in_namespace(User, my_sql):
    import orb
    with orb.Context(namespace='custom'):
        st = my_sql.statement('CREATE')
        assert st is not None

        statement, data = st(User)
        assert 'CREATE TABLE IF NOT EXISTS `custom`.`users`' in statement

def test_my_statement_insert_records(orb, User, my_sql):
    st = my_sql.statement('INSERT')
    assert st is not None

    user_a = User(username='bob')
    user_b = User(username='sally')
    statement, data = st([user_a, user_b])
    assert 'INSERT INTO `orb_testing`.`users`' in statement

def test_my_statement_insert_records_in_namespace(orb, User, my_sql):
    user_a = User(username='bob')
    user_b = User(username='sally')

    with orb.Context(namespace='custom'):
        st = my_sql.statement('INSERT')
        assert st is not None

        statement, data = st([user_a, user_b])
        assert 'INSERT INTO `custom`.`users`' in statement

def test_my_statement_alter(orb, GroupUser, my_sql):
    add = [orb.StringColumn(name='test_add')]
    remove = [orb.StringColumn(name='test_remove')]
    st = my_sql.statement('ALTER')
    assert st is not None

    statement, data = st(GroupUser, add, remove)
    assert 'ALTER' in statement

    add = [orb.StringColumn(name='test_add_i18n', flags={'I18n'})]
    statement, data = st(GroupUser, add)
    assert 'ALTER' in statement

def test_my_statement_alter_invalid(orb, my_sql):
    st = my_sql.statement('ALTER')
    assert st is not None

    with pytest.raises(orb.errors.OrbError):
        statement, data = st(orb.View)

def test_my_statement_create_index(orb, GroupUser, my_sql):
    index = orb.Index(name='byGroupAndUser', columns=[orb.ReferenceColumn(name='group'), orb.ReferenceColumn('user')])
    index.setSchema(GroupUser.schema())
    st = my_sql.statement('CREATE INDEX')
    assert st is not None

    statement, data = st(index)
    assert 'CREATE INDEX' in statement

    statement, data = st(index, checkFirst=True)
    assert 'DO $$' in statement

def test_my_create_table(User, my_sql, my_db):
    st = my_sql.statement('CREATE')
    sql, data = st(User)
    conn = my_db.connection()
    conn.execute(sql)
    assert True

def test_my_insert_bob(orb, User, my_sql, my_db):
    st = my_sql.statement('INSERT')
    user_a = User({
        'username': 'bob',
        'password': 'T3st1ng!'
    })
    sql, data = st([user_a])
    conn = my_db.connection()

    # if this has run before, then it will raise a duplicate entry
    try:
        conn.execute(sql, data)
    except orb.errors.DuplicateEntryFound:
        pass
    assert True

def test_my_insert_sally(orb, User, my_sql, my_db):
    st = my_sql.statement('INSERT')
    user_a = User({
        'username':'sally',
        'password': 'T3st1ng!'
    })

    sql, data = st([user_a])
    conn = my_db.connection()

    # if this has run before, then it will raise a duplicate entry
    try:
        conn.execute(sql, data)
    except orb.errors.DuplicateEntryFound:
        pass
    assert True

def test_my_select_all(orb, User, my_sql, my_db):
    st = my_sql.statement('SELECT')
    sql, data = st(User, orb.Context())
    conn = my_db.connection()
    records, count = conn.execute(sql, data)
    assert len(records) == count

def test_my_select_one(orb, User, my_sql, my_db):
    st = my_sql.statement('SELECT')
    sql, data = st(User, orb.Context(limit=1))
    conn = my_db.connection()
    records, count = conn.execute(sql, data)
    assert count == 1

def test_my_select_bob(orb, User, my_sql, my_db):
    st = my_sql.statement('SELECT')
    sql, data = st(User, orb.Context(where=orb.Query('username') == 'bob'))
    conn = my_db.connection()
    records, count = conn.execute(sql, data)
    assert count == 1 and records[0]['username'] == 'bob'

def test_my_select_count(orb, User, my_sql, my_db):
    select_st = my_sql.statement('SELECT')
    select_sql, data = select_st(User, orb.Context())

    conn = my_db.connection()
    records, count = conn.execute(select_sql, data)

    select_count_st = my_sql.statement('SELECT COUNT')
    select_count_sql, data = select_count_st(User, orb.Context())
    results, _ = conn.execute(select_count_sql, data)

    assert results[0]['count'] == count

def test_my_select_bob_or_sally(orb, my_sql, my_db, User):
    st = my_sql.statement('SELECT')
    q = orb.Query('username') == 'bob'
    q |= orb.Query('username') == 'sally'

    sql, data = st(User, orb.Context(where=q))
    conn = my_db.connection()
    records, count = conn.execute(sql, data)
    assert count == 2 and records[0]['username'] in ('bob', 'sally') and records[1]['username'] in ('bob', 'sally')

def test_my_select_bob_and_sally(orb, my_sql, my_db, User):
    st = my_sql.statement('SELECT')
    q = orb.Query('username') == 'bob'
    q &= orb.Query('username') == 'sally'

    sql, data = st(User, orb.Context(where=q))
    conn = my_db.connection()
    _, count = conn.execute(sql, data)
    assert count == 0

