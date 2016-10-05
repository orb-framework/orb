def test_batch_iterator():
    from orb.core.collection import Collection, BatchIterator

    collection = Collection(list(range(100)))
    batch_iter = BatchIterator(collection, batch=10)

    assert collection.pageCount(pageSize=10) == 10

    count = 0
    for record in batch_iter:
        count += 1

    assert count == 100


def test_batch_iterator_from_helper_method():
    from orb.core.collection import Collection

    collection = Collection(list(range(100)))

    assert collection.pageCount(pageSize=10) == 10

    count = 0
    for record in collection.iterate(batch=10):
        count += 1

    assert count == 100


def test_create_collection_with_bound_model():
    import orb
    from orb.core.collection import Collection

    class User(orb.Model):
        __register__ = False
        id = orb.IdColumn()
        username = orb.StringColumn()

    collection = Collection([User(), User(), User()])
    assert collection.model() == User

