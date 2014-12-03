import orb

class User(orb.Table):
    __db_columns__ = [
        orb.Column(orb.ColumnType.String, 'username')
    ]

class Address(orb.Table):
    __db_columns__ = [
        orb.Column(orb.ColumnType.ForeignKey, 'user', reference='User', reversedName='addresses')
    ]
