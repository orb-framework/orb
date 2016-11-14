def test_create_basic_metamodel():
    from orb.core.metamodel import MetaModel

    class NotAModel(object):
        __register__ = False
        __metaclass__ = MetaModel
        __model__ = False

    class ExplicitModel(object):
        __register__ = False
        __metaclass__ = MetaModel
        __model__ = True

    class ImplicitModel(object):
        __register__ = False
        __metaclass__ = MetaModel

    assert NotAModel.schema() is None
    assert ExplicitModel.schema() is not None
    assert ImplicitModel.schema() is not None


def test_basic_metamodel_definition_with_objects():
    from orb.core.column import Column
    from orb.core.index import Index
    from orb.core.collector import Collector
    from orb.core.metamodel import MetaModel

    class Model(object):
        __metaclass__ = MetaModel
        __register__ = False

        column = Column()
        index = Index()
        collector = Collector()

    schema = Model.schema()
    assert schema.column('column')
    assert schema.index('index')
    assert schema.collector('collector')


def test_metamodel_virtual_objects():
    from orb.decorators import virtual
    from orb.core.column import Column
    from orb.core.collector import Collector
    from orb.core.metamodel import MetaModel

    class Model(object):
        __metaclass__ = MetaModel
        __register__ = False

        @virtual(Column)
        def column(self, **context):
            return 'column'

        @classmethod
        @virtual(Collector)
        def collector(cls, **context):
            return 'collector'

    schema = Model.schema()
    column = schema.column('column')
    collect = schema.collector('collector')

    assert column is not None
    assert column.test_flag(column.Flags.Virtual)
    assert column.test_flag(column.Flags.ReadOnly)

    assert collect is not None
    assert collect.test_flag(collect.Flags.Virtual)
    assert collect.test_flag(collect.Flags.ReadOnly)
    assert collect.test_flag(collect.Flags.Static)


def test_metamodel_mixins():
    import orb
    from orb.core.metamodel import MetaModel

    class MixinA(orb.ModelMixin):
        column_a = orb.Column()

    class MixinB(orb.ModelMixin):
        @orb.virtual(orb.Column)
        def column_b(self, **context):
            pass

    class ModelA(MixinA, MixinB):
        __metaclass__ = MetaModel
        __register__ = False

    class ModelB(MixinA, MixinB):
        __metaclass__ = MetaModel
        __register__ = False

    schema_a = ModelA.schema()
    schema_b = ModelB.schema()

    assert schema_a != schema_b
    assert schema_a.column('column_a') is not schema_b.column('column_a')
    assert schema_a.column('column_b') is not schema_b.column('column_b')
    assert schema_a.column('column_b').gettermethod() == schema_b.column('column_b').gettermethod()


def test_metamodel_inheritance():
    import orb

    system = orb.System()

    class ModelA(orb.Model):
        __system__ = system
        __id__ = 'column_a'

        column_a = orb.Column()

    class ModelB(ModelA):
        column_b = orb.Column()

    schema_a = ModelA.schema()
    schema_b = ModelB.schema()

    assert schema_a.column('column_a') is schema_b.column('column_a')
    assert schema_a.column('column_b', raise_=False) is not schema_b.column('column_b')
    assert schema_b.inherits() == schema_a.name()
    assert schema_b.ancestry().next() is schema_a