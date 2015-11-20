import pytest


def test_version(orb):
    assert orb.__version__ != '0.0.0'

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
    assert len(EmptyUser.schema().pipes()) == 0

# ----
# Basic Model Definition
# ----

def test_user_columns(User):
    assert len(User.schema().columns()) == 3

def test_user_indexes(User):
    assert len(User.schema().indexes()) == 1

def test_user_properties(User):
    assert None not in (getattr(User, 'id', None) != None,
                        getattr(User, 'username', None) != None,
                        getattr(User, 'password', None) != None,
                        getattr(User, 'setUsername', None) != None,
                        getattr(User, 'setPassword', None) != None,
                        getattr(User, 'byUsername', None) != None)

def test_user_make_record(User):
    assert User() is not None

def test_user_create_with_properties(User):
    record = User(username='bob')
    assert record.username() == 'bob'
    assert record.get('username') == 'bob'

def test_user_collection(orb, User):
    records = User.all()
    assert isinstance(records, orb.Collection)

def test_user_good_password(User):
    record = User(username='bob')
    assert record.setPassword('T3st1ng!')

def test_user_bad_password(orb, User):
    record = User(username='bob')
    with pytest.raises(orb.errors.ColumnValidationError):
        record.setPassword('bad')

def test_user_inflate(orb, User):
    record = User.inflate({'username': 'bob'})
    assert record.get('username') == 'bob'
    assert record.username() == 'bob'

def test_user_empty_reverse_lookup(orb, User):
    user = User()
    grps = user.userGroups()
    assert len(grps) == 0

def test_user_empty_pipe(orb, User):
    user = User()
    grps = user.groups()
    assert len(grps) == 0

# ----
# Schema Definition
# ----

def test_schema_name(GroupUser):
    schema = GroupUser.schema()

    assert schema.display() == 'Group User'
    assert schema.name() == 'GroupUser'
    assert schema.dbname() == 'group_users'