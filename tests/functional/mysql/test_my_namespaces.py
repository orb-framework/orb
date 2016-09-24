def test_my_namespace_sync(orb, my_db, namespace_models):
    conn = my_db.connection()

    with orb.Context(namespace='test_namespace_a'):
        my_db.sync(models=namespace_models.keys())

        result_default = conn.execute('SELECT * FROM test_namespace_a.test_defaults ORDER BY id DESC LIMIT 1')[0]
        result_explicit = conn.execute('SELECT * FROM test_explicit.test_explicits ORDER BY id DESC LIMIT 1')[0]

        assert result_default[0]['name'] == 'test'
        assert result_explicit[0]['name'] == 'test'

def test_my_second_namespace_sync(orb, my_db, namespace_models):
    conn = my_db.connection()

    with orb.Context(namespace='test_namespace_b'):
        my_db.sync(models=namespace_models.keys())

        result_default = conn.execute('SELECT * FROM test_namespace_b.test_defaults ORDER BY id DESC LIMIT 1')[0]
        result_explicit = conn.execute('SELECT * FROM test_explicit.test_explicits ORDER BY id DESC LIMIT 1')[0]

        assert result_default[0]['name'] == 'test'
        assert result_explicit[0]['name'] == 'test'

def test_my_check_namespace_ids(orb, namespace_models):
    TestDefault = namespace_models['TestDefault']

    with orb.Context(namespace='test_namespace_a'):
        record_a = TestDefault(1)

    with orb.Context(namespace='test_namespace_b'):
        record_b = TestDefault(1)

    assert record_a.id() == 1
    assert record_b.id() == 1
    assert record_a.context().namespace == 'test_namespace_a'
    assert record_b.context().namespace == 'test_namespace_b'