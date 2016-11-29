import pytest


def test_abstract_search_engine():
    import orb

    with pytest.raises(TypeError):
        engine = orb.SearchEngine()


def test_basic_search_engine():
    import orb

    class BasicSearchEngine(orb.SearchEngine):
        def search(self, model, terms, **context):
            return orb.Collection()

    engine = BasicSearchEngine()
    assert engine is not None


def test_basic_search_engine_from_factory():
    import orb

    class BasicSearchEngine(orb.SearchEngine):
        __factory__ = 'basic'

        def search(self, model, terms, **context):
            return orb.Collection()

    engine = orb.SearchEngine.factory('basic')
    assert engine is not None

    with pytest.raises(RuntimeError):
        assert orb.SearchEngine.factory('elastic') is None


