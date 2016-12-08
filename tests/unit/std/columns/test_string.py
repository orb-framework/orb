def test_string_column():
    import orb

    col = orb.StringColumn()
    assert col.max_length() == 255

    col.set_max_length(20)
    assert col.max_length() == 20

    col_b = col.copy()
    assert col.max_length() == 20


def test_string_column_encryption():
    import orb
    from orb.utils import security

    key = security.generate_key('testing')

    col = orb.StringColumn(security_key=key, flags={'Encrypted'})
    col.set_security_key(key)
    value = col.store('testing')

    assert security.decrypt(value, key) == 'testing'


def test_text_column():
    import orb

    col = orb.TextColumn()
    assert col.max_length() is None


def test_html_column():
    import orb

    col = orb.HtmlColumn()
    value = col.store('<i>italic</i><b>bold</b><script>alert("here")</script>')
    assert value == '<i>italic</i><b>bold</b>&lt;script&gt;alert("here")&lt;/script&gt;'


def test_plain_text_column():
    import orb

    col = orb.PlainTextColumn()
    value = col.store('<i>italic</i><b>bold</b><script>alert("here")</script>')
    assert value == 'italicboldalert("here")'


def test_token_column():
    import orb

    col = orb.TokenColumn()

    assert col.test_flag(orb.Column.Flags.Unique)
    assert col.test_flag(orb.Column.Flags.Required)

    col_b = orb.TokenColumn(bits=64)
    col_c = col_b.copy()

    assert col.bits() == 32
    assert col.max_length() == 64

    assert col_b.bits() == 64
    assert col_c.bits() == 64
    assert col_b.max_length() == 128

    col_b.set_bits(32)
    assert col_b.max_length() == 64


def test_token_column_generation():
    import orb

    col = orb.TokenColumn()
    assert len(col.generate_token()) == 64
    assert len(col.default()) == 64

    a = col.default()
    b = col.default()
    c = col.generate_token()
    d = col.generate_token()

    assert a != b != c != d

