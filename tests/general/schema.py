import pytest

def test_version():
    import orb
    assert orb.__version__ != '0.0.0'