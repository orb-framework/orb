import pytest
from test_marks import requires_pg


@requires_pg
def test_pg_api_select_bob(orb, pg_sql, pg_db, User):
    record = User.select(where=orb.Query('username') == 'bob').first()
    assert record is not None and record.get('username') == 'bob'

@requires_pg
@pytest.mark.run(order=1)
def test_pg_api_save_bill(orb, pg_db, User):
    user = User(username='bill', password='T3st1ng!')
    user.save()

    assert user.isRecord()

@requires_pg
@pytest.mark.run(order=2)
def test_pg_api_delete_bill(orb, pg_db, User):
    user = User.byUsername('bill')
    assert user and user.isRecord()

    user.delete()
    assert not user.isRecord()

    user_again = User.byUsername('bill')
    assert not user_again

@requires_pg
def test_pg_api_update_bob(orb, pg_sql, pg_db, User):
    record = User.select(where=orb.Query('username') == 'bob').first()

    assert record is not None
    assert record.username() == 'bob'

    st = pg_sql.statement('UPDATE')
    conn = pg_db.connection()

    # set to tim
    record.set('username', 'tim')
    sql, data = st([record])
    result, count = conn.execute(sql, data)

    record_tim = User.select(where=orb.Query('username') == 'tim').first()
    assert record_tim is not None
    assert record_tim.id() == record.id()

    # set back to bob
    record_tim.set('username', 'bob')
    sql, data = st([record_tim])
    result, count = conn.execute(sql, data)

    record_bob = User.select(where=orb.Query('username') == 'bob').first()
    assert record_bob is not None
    assert record_bob.id() == record.id() and record_bob.id() == record_tim.id()

@requires_pg
def test_pg_api_create_admins(orb, User, GroupUser, Group):
    user = User.byUsername('bob')
    assert user is not None and user.username() == 'bob'

    group = Group.ensureExists(name='admins')
    assert group is not None

    group_user = GroupUser.ensureExists(group=group, user=user)
    assert group_user.isRecord()

@requires_pg
def test_pg_api_get_user_groups(orb, User):
    user = User.byUsername('bob')
    assert user is not None

    groups = user.groups()
    assert len(groups) == 1

@requires_pg
def test_pg_api_get_group_users(orb, Group):
    grp = Group.select(where=orb.Query('name') == 'admins').first()
    assert grp is not None and grp.name() == 'admins'

    users = grp.users()
    assert len(users) == 1
    assert users[0].username() == 'bob'

@requires_pg
def test_pg_api_get_group_users_reverse(orb, User, Group):
    bob = User.byUsername('bob')
    assert len(bob.userGroups()) == 1

    admins = Group.byName('admins')
    assert len(admins.groupUsers()) == 1

@requires_pg
def test_pg_api_get_group_users_by_unique_index(orb, GroupUser, User, Group):
    u = User.byUsername('bob')
    g = Group.byName('admins')

    admin = GroupUser.byUserAndGroup(u, g)
    assert admin is not None

@requires_pg
def test_pg_api_get_group_users_by_index(orb, GroupUser, User):
    u = User.byUsername('bob')
    users = GroupUser.byUser(u)
    assert len(users) == 1
    assert users[0].user() == u

@requires_pg
def test_pg_api_select_with_join(orb, Group, User, GroupUser):
    q  = orb.Query('id') == orb.Query(GroupUser, 'user')
    q &= orb.Query(GroupUser, 'group') == orb.Query(Group, 'id')
    q &= orb.Query(Group, 'name') == 'admins'

    records = User.select(where=q)

    assert len(records) == 1
    assert records[0].username() == 'bob'

@requires_pg
def test_pg_api_select_standard_with_shortcut(orb, GroupUser):
    q = orb.Query('group.name') == 'admins'
    records = GroupUser.select(where=q)

    assert len(records) == 1
    assert records[0].user().username() == 'bob'

@requires_pg
def test_pg_api_select_reverse_with_shortcut(orb, User):
    q = orb.Query('userGroups.group.name') == 'admins'
    records = User.select(where=q)

    assert len(records) == 1
    assert records[0].username() == 'bob'

@requires_pg
def test_pg_api_select_pipe_with_shortcut(orb, User):
    q = orb.Query('groups.name') == 'admins'
    records = User.select(where=q)

    assert len(records) == 1
    assert records[0].username() == 'bob'

@requires_pg
def test_pg_api_expand(orb, GroupUser):
    group_user = GroupUser.select(expand='user').first()
    assert group_user is not None

@requires_pg
def test_pg_api_expand_json(orb, GroupUser):
    group_user = GroupUser.select(expand='user').first()
    print group_user.__json__()
    assert False