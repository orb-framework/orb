def test_context(orb, user_table):
    with orb.Context(database='testing'):
        user_a = user_table()

    with orb.Context(database='testing_2'):
        user_b = user_table()

    assert user_a.context().database == 'testing'
    assert user_b.context().database == 'testing_2'


def test_context_scope(orb, user_table):
    scope = {'session': 123}
    with orb.Context(scope=scope):
        user_a = user_table()

    assert user_a.context().scope == scope


def test_nested_context_scope(orb, user_table):
    scope_a = {'session': 123}
    scope_b = {'session': 234}

    with orb.Context(scope=scope_a):
        user_a = user_table()
        with orb.Context(scope=scope_b):
            user_b = user_table()

    assert user_a.context().scope == scope_a
    assert user_b.context().scope == scope_b


def test_locale_scope(orb, user_table):
    with orb.Context(locale='en_US'):
        user_a = user_table()
        user_b = user_table()

        with orb.Context(locale='fr_FR'):
            user_c = user_table()

    assert user_a.context().locale == 'en_US'
    assert user_b.context().locale == 'en_US'
    assert user_c.context().locale == 'fr_FR'