# -*- coding: utf-8
import pytest


@pytest.fixture()
def entries():
    return {
        'ascii': 'test string',
        'unicode': u'虎',
        'bytes': '\xe8\x99\x8e'
    }


def test_decoded(entries):
    from orb.utils.text import decoded

    assert decoded(entries['ascii']) == entries['ascii']
    assert decoded(entries['unicode']) == entries['unicode']
    assert decoded(entries['bytes']) == entries['unicode']
    assert decoded(entries['bytes'], None) == u'Yrþ'
    assert decoded(10) == '10'

def test_encoded(entries):
    from orb.utils.text import encoded

    assert encoded(entries['ascii']) == entries['ascii']
    assert encoded(entries['unicode']) == entries['bytes']
    assert encoded(entries['bytes']) == entries['bytes']
    assert encoded(10) == '10'

def test_nativestring():
    from orb.utils.text import nativestring

    assert nativestring('testing') == 'testing'
    assert nativestring(u'testing') == u'testing'
    assert nativestring(10) == u'10'


def test_safe_eval():
    from orb.utils.text import safe_eval

    assert safe_eval(10) == 10
    assert safe_eval('true') == True
    assert safe_eval('false') == False
    assert safe_eval('null') == None
    assert safe_eval('None') == None
    assert safe_eval('10') == 10
    assert safe_eval('def testing():') == 'def testing():'


def test_to_ascii():
    from orb.utils.text import to_ascii

    assert to_ascii(u'虎') == ''
