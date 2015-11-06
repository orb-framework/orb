import orb

class CreationMixin(orb.ModelMixin):
    created_at = orb.DatetimeColumn()
    created_by = orb.ReferenceColumn(reference='User')
    updated_at = orb.DatetimeColumn()
    updated_by = orb.ReferenceColumn(reference='User')

class User(orb.Table):
    id = orb.SerialColumn()
    username = orb.StringColumn()
    password = orb.PasswordColumn()

class Comment(CreationMixin, orb.Table):
    id = orb.SerialColumn()
    text = orb.TextColumn()

class Session(CreationMixin, orb.Table):
    id = orb.SerialColumn()
    description = orb.StringColumn()
    comments = orb.Pipe(through='SessionComment', source='session', target='comment')

class SessionComment(orb.Table):
    id = orb.SerialColumn()
    session = orb.ReferenceColumn(reference='Session')
    comment = orb.ReferenceColumn(reference='Comment')