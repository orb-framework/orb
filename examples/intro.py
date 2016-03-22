import orb

class UserSchema(orb.ModelMixin):
  id = orb.IdColumn()
  username = orb.StringColumn(flags={'Required'})
  password = orb.StringColumn(flags={'Required'})
  first_name = orb.StringColumn()
  last_name = orb.StringColumn()

  addresses = orb.ReverseLookup(from_column='Address.user')
  preferences = orb.ReverseLookup(from_column='Preference.user', flags={'Unique'})
  groups = orb.Pipe(through_path='GroupUser.user.group')

  byUsername = orb.Index(columns=['username'], flags={'Unique'})
  byName = orb.Index(columns=['first_name', 'last_name'])


class User(UserSchema, orb.Table):
  def username(self):
      print 'getting username'
      return self.get('username', useMethod=False)


class Preference(orb.Table):
  id = orb.IdColumn()
  user = orb.ReferenceColumn(reference='User', flags={'Unique'})
  notifications_enabled = orb.BooleanColumn()

class Address(orb.Table):
  id = orb.IdColumn()
  user = orb.ReferenceColumn(reference='User')
  name = orb.StringColumn()
  street = orb.StringColumn()
  city = orb.StringColumn()
  state = orb.StringColumn()
  zipcode = orb.IntegerColumn()

class Group(orb.Table):
  id = orb.IdColumn()
  name = orb.StringColumn()

  users = orb.Pipe(through_path='GroupUser.group.user')

  byName = orb.Index(columns=['name'], flags={'Unique'})

class GroupUser(orb.Table):
  id = orb.IdColumn()
  user = orb.ReferenceColumn(reference='User')
  group = orb.ReferenceColumn(reference='Group')

# create a new sqlite db
db = orb.Database('SQLite', 'intro.db')
db.activate()