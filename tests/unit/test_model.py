def test_overwritten_auth_respects_private_column_flag(PrivateClass):
    record = PrivateClass()
    cols = set([ col for col, value in record])
    assert len(cols) == 2
    assert cols == set(['id', 'public'])
