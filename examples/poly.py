import orb

class Asset(orb.Table):
  id = orb.IdColumn()
  code = orb.StringColumn(flags={'Unique'})
  display_name = orb.StringColumn()
  parent = orb.ReferenceColumn(reference='Asset')
  project = orb.ReferenceColumn(reference='Project')

  type = orb.StringColumn(flags={'Polymorphic'})

  children = orb.ReverseLookup(from_column='Asset.parent')
  dependsOn = orb.Pipe(through_path='Dependency.target.source')
  dependencies = orb.Pipe(through_path='Dependency.source.target')

class Dependency(orb.Table):
  id = orb.IdColumn()
  source = orb.ReferenceColumn(reference='Asset')
  target = orb.ReferenceColumn(reference='Asset')

class Project(Asset):
  budget = orb.LongColumn()
  supervisors = orb.Pipe(through_path='ProjectSupervisor.project.user')

  def onInit(self, event):
    self.set('project', self)

class ProjectSupervisor(orb.Table):
  id = orb.IdColumn()
  project = orb.ReferenceColumn(reference='Project')
  user = orb.ReferenceColumn(reference='User')

class Character(Asset):
  polycount = orb.LongColumn()
  is_hero = orb.BooleanColumn()

# create a new sqlite db
db = orb.Database('SQLite', 'poly.db')
db.activate()