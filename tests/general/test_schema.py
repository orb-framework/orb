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

def test_empty_user_table(empty_user_table):
    assert empty_user_table.schema().name() == 'User'

def test_empty_user_columns(empty_user_table):
    assert len(empty_user_table.schema().columns()) == 0

def test_empty_user_indexes(empty_user_table):
    assert len(empty_user_table.schema().indexes()) == 0

def test_empty_user_pipes(empty_user_table):
    assert len(empty_user_table.schema().pipes()) == 0

# ----
# Basic Model Definition
# ----

def test_user_columns(user_table):
    assert len(user_table.schema().columns()) == 3

def test_user_indexes(user_table):
    assert len(user_table.schema().indexes()) == 1

def test_user_properties(user_table):
    assert None not in (getattr(user_table, 'id', None) != None,
                        getattr(user_table, 'username', None) != None,
                        getattr(user_table, 'password', None) != None,
                        getattr(user_table, 'setUsername', None) != None,
                        getattr(user_table, 'setPassword', None) != None,
                        getattr(user_table, 'byUsername', None) != None)

def test_user_make_record(user_table):
    assert user_table() is not None

def test_user_create_with_properties(user_table):
    record = user_table(username='bob')
    assert record.username() == 'bob'
    assert record.get('username') == 'bob'

def test_user_collection(orb, user_table):
    records = user_table.all()
    assert isinstance(records, orb.Collection)

def test_user_good_password(user_table):
    record = user_table(username='bob')
    assert record.setPassword('T3st1ng!')

def test_user_bad_password(orb, user_table):
    record = user_table(username='bob')
    with pytest.raises(orb.errors.ColumnValidationError):
        record.setPassword('bad')