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