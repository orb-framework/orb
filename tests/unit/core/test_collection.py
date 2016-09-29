def test_collection_is_null():
    from orb.core.collection import Collection

    col = Collection()
    assert col.isNull()


def test_collection_iterator():
    from orb.core.collection import Collection, BatchIterator

    records = orb.Collection(range(100))
    iterator = BatchIterator(records, batch=10)

    count = 0
    for record in iterator:
        count += 1

    assert count == 100