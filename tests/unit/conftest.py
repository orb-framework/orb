import pytest

@pytest.fixture()
def orb():
    import orb
    from projex import security

    key = security.generateKey('T3st!ng')
    orb.system.security().setKey(key)

    return orb

@pytest.fixture()
def PrivateClass(orb):
    class Private(orb.Table):
        id = orb.IdColumn()
        public = orb.StringColumn()
        private = orb.StringColumn(flags={'Private'})

        @classmethod
        def __auth__(cls, **context):
            return True

    return Private
