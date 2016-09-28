"""
Tests for the orb.decorators module
"""

def test_create_virtual_string():
    import orb
    from orb.decorators import virtual

    @virtual(orb.StringColumn)
    def my_method():
        pass

    assert my_method.__orb__ is not None
    assert isinstance(my_method.__orb__, orb.StringColumn)
    assert my_method.__orb__.name() == 'my_method'
    assert my_method.__orb__.gettermethod() == my_method
    assert my_method.__orb__.settermethod() is None
    assert my_method.__orb__.queryFilterMethod() is None
    assert my_method.__orb__.flags() == (orb.Column.Flags.Virtual | orb.Column.Flags.ReadOnly)


def test_create_virtual_string_with_flags_as_set():
    import orb
    from orb.decorators import virtual

    @virtual(orb.StringColumn, flags={'RequiresExpand'})
    def my_method():
        pass

    assert my_method.__orb__.flags() == (orb.Column.Flags.Virtual |
                                         orb.Column.Flags.ReadOnly |
                                         orb.Column.Flags.RequiresExpand)


def test_create_virtual_string_with_flags_as_integer():
    import orb
    from orb.decorators import virtual

    @virtual(orb.StringColumn, flags=orb.Column.Flags.RequiresExpand)
    def my_method():
        pass

    assert my_method.__orb__.flags() == (orb.Column.Flags.Virtual |
                                         orb.Column.Flags.ReadOnly |
                                         orb.Column.Flags.RequiresExpand)


def test_create_virtual_string_with_a_settermethod():
    import orb
    from orb.decorators import virtual

    @virtual(orb.StringColumn)
    def param():
        pass

    @param.setter()
    def set_param(value):
        pass

    # ensure that when a virtual column has defined a settermethod, it is
    # no longer flagged as read-only
    assert param.__orb__.flags() == orb.Column.Flags.Virtual
    assert param.__orb__.settermethod() == set_param


def test_create_virtual_string_with_a_query_filter():
    import orb
    from orb.decorators import virtual

    @virtual(orb.StringColumn)
    def param():
        pass

    @param.queryFilter()
    def param_filter():
        pass

    assert param.__orb__.queryFilterMethod() == param_filter


def test_create_virtual_string_with_a_name_override():
    import orb
    from orb.decorators import virtual

    @virtual(orb.StringColumn, name='name')
    def get_name():
        pass

    @get_name.setter()
    def set_name(name):
        pass

    assert get_name.__orb__.gettermethod() == get_name
    assert get_name.__orb__.settermethod() == set_name
    assert get_name.__orb__.name() == 'name'


def test_create_virtual_collector():
    import orb
    import pytest
    from orb.decorators import virtual

    @virtual(orb.Collector, model='Property')
    def properties():
        pass

    @properties.setter()
    def set_properties(values):
        pass

    @properties.queryFilter()
    def properties_filter(query):
        pass

    assert properties.__orb__ is not None
    assert isinstance(properties.__orb__, orb.Collector)
    assert properties.__orb__.gettermethod() == properties
    assert properties.__orb__.settermethod() == set_properties
    assert properties.__orb__.queryFilterMethod() == properties_filter

    with pytest.raises(orb.errors.ModelNotFound):
        properties.__orb__.model()


def test_create_virtual_reverse_lookup():
    import orb
    import pytest
    from orb.decorators import virtual

    @virtual(orb.ReverseLookup, model='Property')
    def properties():
        pass

    @properties.setter()
    def set_properties(values):
        pass

    @properties.queryFilter()
    def properties_filter(query):
        pass

    assert properties.__orb__ is not None
    assert isinstance(properties.__orb__, orb.Collector)
    assert isinstance(properties.__orb__, orb.ReverseLookup)
    assert properties.__orb__.gettermethod() == properties
    assert properties.__orb__.settermethod() == set_properties
    assert properties.__orb__.queryFilterMethod() == properties_filter

    with pytest.raises(orb.errors.ModelNotFound):
        properties.__orb__.model()


def test_create_virtual_pipe():
    import orb
    import pytest
    from orb.decorators import virtual

    @virtual(orb.Pipe, model='Property')
    def properties():
        pass

    @properties.setter()
    def set_properties(values):
        pass

    @properties.queryFilter()
    def properties_filter(query):
        pass

    assert properties.__orb__ is not None
    assert isinstance(properties.__orb__, orb.Collector)
    assert isinstance(properties.__orb__, orb.Pipe)
    assert properties.__orb__.gettermethod() == properties
    assert properties.__orb__.settermethod() == set_properties
    assert properties.__orb__.queryFilterMethod() == properties_filter

    with pytest.raises(orb.errors.ModelNotFound):
        properties.__orb__.model()
