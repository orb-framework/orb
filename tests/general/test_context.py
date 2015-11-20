def test_context(orb, User):
    with orb.Context(database='testing'):
        user_a = User()

    with orb.Context(database='testing_2'):
        user_b = User()

    assert user_a.context().database == 'testing'
    assert user_b.context().database == 'testing_2'


def test_context_scope(orb, User):
    scope = {'session': 123}
    with orb.Context(scope=scope):
        user_a = User()

    assert user_a.context().scope == scope


def test_nested_context_scope(orb, User):
    scope_a = {'session': 123}
    scope_b = {'session': 234}

    with orb.Context(scope=scope_a):
        user_a = User()
        with orb.Context(scope=scope_b):
            user_b = User()

    assert user_a.context().scope == scope_a
    assert user_b.context().scope == scope_b


def test_locale_scope(orb, User):
    with orb.Context(locale='en_US'):
        user_a = User()
        user_b = User()

        with orb.Context(locale='fr_FR'):
            user_c = User()

    assert user_a.context().locale == 'en_US'
    assert user_b.context().locale == 'en_US'
    assert user_c.context().locale == 'fr_FR'

def test_context_update(orb):
    context_a = orb.Context()
    context_a.update({'limit': 1})
    assert context_a.limit == 1

def test_context_merge_by_dict(orb):
    context_a = orb.Context(limit=1)
    context_b = context_a.copy()
    context_b.update({'locale': 'fr_FR'})
    assert context_a.limit == 1
    assert context_b.limit == 1
    assert context_b.locale == 'fr_FR'

def test_context_merge_by_context(orb):
    context_a = orb.Context(limit=1)
    context_c = orb.Context(locale='fr_FR')
    context_b = context_a.copy()
    context_b.update(context_c)
    assert context_a.limit == 1
    assert context_b.limit == 1
    assert context_b.locale == 'fr_FR'