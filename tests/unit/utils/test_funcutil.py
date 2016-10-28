def test_extract_keywords_on_function():
    from orb.utils import funcutil

    def my_function(this, is_a=None, test=None):
        pass

    kwds = funcutil.extract_keywords(my_function)
    assert kwds == ('is_a', 'test')


def test_extract_keywords_on_method():
    from orb.utils import funcutil

    class MyClass(object):
        def my_method(self, this, is_a=None, test=None):
            pass

    obj = MyClass()
    kwds = funcutil.extract_keywords(obj.my_method)
    assert kwds == ('is_a', 'test')