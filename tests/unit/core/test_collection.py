def test_collection_is_null():
    from orb.core.collection import Collection

    col = Collection()
    assert col.isNull()