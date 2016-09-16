import pytest

def test_invalid_database(orb):
    with pytest.raises(orb.errors.BackendNotFound):
        db = orb.Database('Foo')
