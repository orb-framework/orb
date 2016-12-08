def test_boolean_column_value_from_string():
    import orb

    c = orb.BooleanColumn()

    # positive matches
    assert c.value_from_string('true') is True
    assert c.value_from_string('True') is True
    assert c.value_from_string('TRUE') is True
    assert c.value_from_string('not empty') is True

    # negative matches
    assert c.value_from_string('false') is False
    assert c.value_from_string('False') is False
    assert c.value_from_string('FALSE') is False
    assert c.value_from_string('') is False

    # non-string matches
    assert c.value_from_string(None) is False
    assert c.value_from_string(False) is False
    assert c.value_from_string(True) is True