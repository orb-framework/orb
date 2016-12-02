import pytest


@pytest.fixture()
def sql_equals():
    def _sql_equals(a, b):
        normal_a = a.replace('\n', '').replace(' ', '')
        normal_b = b.replace('\n', '').replace(' ', '')
        return normal_a == normal_b
    return _sql_equals

