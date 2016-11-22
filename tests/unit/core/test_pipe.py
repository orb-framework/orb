import pytest


@pytest.fixture()
def mock_pipe_schema():
    import orb

    system = orb.System()

    class Source(orb.Table):
        __system__ = system

        id = orb.IdColumn()
        targets = orb.Pipe('Through.source.target')

    class Through(orb.Table):
        __system__ = system

        id = orb.IdColumn()
        source = orb.ReferenceColumn('Source')
        target = orb.ReferenceColumn('Target')

    class Target(orb.Table):
        __system__ = system

        id = orb.IdColumn()
        sources = orb.Pipe(through_model='Through',
                           from_column='target',
                           to_column='source',
                           model='Source')

    return (Source, Through, Target)


def test_pipe_initialization(mock_pipe_schema):
    Source, Through, Target = mock_pipe_schema

    targets = Source.schema().collector('targets')
    sources = Target.schema().collector('sources')

    assert targets.from_model() is Source
    assert targets.through_model() is Through
    assert targets.model() is Target
    assert targets.from_column() is Through.schema().column('source')
    assert targets.to_column() is Through.schema().column('target')

    assert sources.from_model() is Target
    assert sources.through_model() is Through
    assert sources.model() is Source
    assert sources.from_column() is Through.schema().column('target')
    assert sources.to_column() is Through.schema().column('source')


def test_pipe_duplication(mock_pipe_schema):
    Source, _, _ = mock_pipe_schema

    targets_a = Source.schema().collector('targets')
    targets_b = targets_a.copy()

    assert targets_a.from_model() is targets_b.from_model()
    assert targets_a.through_model() is targets_b.through_model()
    assert targets_a.model() is targets_b.model()
    assert targets_a.from_column() is targets_b.from_column()
    assert targets_a.to_column() is targets_b.to_column()
