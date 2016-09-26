def test_virtual_decorator():
    import orb
    from orb.decorators import virtual

    @virtual(orb.StringColumn)
    def my_method():
        pass

    assert my_method.__orb__ is not None