import pytest


# ----
# Empty Table
# ----

def test_empty_model(orb):
    assert orb.Model.schema() is None

def test_empty_table(orb):
    assert orb.Table.schema() is None

def test_empty_view(orb):
    assert orb.View.schema() is None

# -----
# Empty Definition
# -----

def test_empty_user(EmptyUser):
    assert EmptyUser.schema().name() == 'EmptyUser'

def test_empty_user_columns(EmptyUser):
    assert len(EmptyUser.schema().columns()) == 0

def test_empty_user_indexes(EmptyUser):
    assert len(EmptyUser.schema().indexes()) == 0

def test_empty_user_pipes(EmptyUser):
    assert len(EmptyUser.schema().collectors()) == 0

# ----
# Basic Model Definition
# ----

def test_user_columns(orb, User):
    assert len(User.schema().columns()) == 6
    assert len(User.schema().columns(flags=orb.Column.Flags.Virtual)) == 1
    assert len(User.schema().columns(flags=~orb.Column.Flags.Virtual)) == 5

def test_user_indexes(User):
    assert len(User.schema().indexes()) == 1

def test_private_properties(User):
    u = User()
    data = u.__json__()
    assert 'password' not in data

def test_user_make_record(User):
    assert User() is not None

def test_user_create_with_properties(User):
    record = User({'username': 'bob'})
    assert record.get('username') == 'bob'

def test_user_collection(orb, User):
    records = User.all()
    assert isinstance(records, orb.Collection)

def test_user_good_password(User):
    record = User({'username': 'bob'})
    assert record.set('password', 'T3st1ng!')

def test_user_inflate(orb, User):
    record = User.inflate({'username': 'bob'})
    assert record.get('username') == 'bob'

def test_user_empty_reverse_lookup(orb, User):
    user = User()
    grps = user.get('userGroups')
    assert len(grps) == 0

def test_user_token(orb, User):
    user = User()
    assert user.get('token') not in (None, '')

def test_user_empty_pipe(orb, User):
    user = User()
    grps = user.get('groups')
    assert len(grps) == 0

def test_empty_collection(orb):
    coll = orb.Collection()

    assert coll.count() == 0
    assert coll.records() == []
    assert coll.first() is None
    assert coll.last() is None
    assert coll.ids() == []

def test_virtual_column(orb, User):
    u = User()

    assert not u.hasGroups()

    has_groups = u.schema().column('has_groups')
    assert isinstance(has_groups, orb.BooleanColumn)
    assert has_groups.testFlag(has_groups.Flags.Virtual)
    assert has_groups.testFlag(has_groups.Flags.ReadOnly)

def test_virtual_collector(orb, User):
    u = User()

    assert len(u.myGroups()) == 0
    my_groups = u.schema().collector('my_groups')
    assert my_groups is not None
    assert my_groups.testFlag(my_groups.Flags.Virtual)
    assert my_groups.testFlag(my_groups.Flags.ReadOnly)

# ----
# Schema Definition
# ----

def test_schema_name(GroupUser):
    schema = GroupUser.schema()

    assert schema.display() == 'Group User'
    assert schema.name() == 'GroupUser'
    assert schema.dbname() == 'group_users'

def test_schema_json_export(User):
    json = User.schema().__json__()

    assert 'columns' in json
    assert 'collectors' in json
    assert json['model'] == 'User'
    assert json['dbname'] == 'users'

    # ensure virtual objects exist
    assert 'my_groups' in json['collectors']


def test_virtual_schema_columns_and_collectors_are_class_specific(orb):
    class A(orb.Table):
        @orb.virtual(orb.Column)
        def value(self, **context):
            pass

        @orb.virtual(orb.Collector)
        def options(self, **context):
            pass

    class B(A):
        @orb.virtual(orb.Column)
        def value(self, **context):
            pass

        @orb.virtual(orb.Collector)
        def options(self, **context):
            pass

    assert A.schema().column('value') != B.schema().column('value')
    assert A.schema().collector('options') != B.schema().collector('options')