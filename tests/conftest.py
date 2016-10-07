import pytest

@pytest.fixture()
def assert_dict_equals():
    """
    Compares the keys of a against the values of
    b.  If b has more values, that is ok, but the
    values that a has must be present in b for this
    assertion to pass.

    :param a: <dict>
    :param b: <dict>
    """
    def _assert_dict_equals(a, b):
        for k, v in a.items():
            assert b.get(k) == v
    return _assert_dict_equals