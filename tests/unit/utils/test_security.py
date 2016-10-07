import pytest


@pytest.fixture
def secure_key():
    from orb.utils import security
    return security.generate_key('secret_key', 32)


def test_encryption_requires_key():
    from orb.utils import security

    with pytest.raises(StandardError):
        assert security.encrypt('testing')


def test_16_bit_encryption_key_generation():
    from orb.utils import security

    key_16_a = security.generate_key('testing', 16)
    assert len(key_16_a) == 16

    key_16_b = security.generate_key('testing', 16)
    assert len(key_16_b) == 16
    assert key_16_a == key_16_b


def test_32_bit_encryption_key_generation():
    from orb.utils import security

    key_32_a = security.generate_key('testing', 32)
    assert len(key_32_a) == 32

    key_32_b = security.generate_key('testing', 32)
    assert len(key_32_b) == 32
    assert key_32_a == key_32_b


def test_invalid_encryption_key_generation():
    from orb.utils import security

    with pytest.raises(RuntimeError):
        security.generate_key('testing', 12)


def test_encryption_method(secure_key):
    from orb.utils import security

    encrypted_text = security.encrypt('my-password',
                                      secure_key)
    assert len(encrypted_text) == 64

    encrypted_text_with_salt = security.encrypt('my-password',
                                                secure_key)

    assert encrypted_text_with_salt != encrypted_text


def test_decryption_method(secure_key):
    from orb.utils import security

    enc_text = security.encrypt('my-password', secure_key)
    enc_text_2 = security.encrypt('my-password', secure_key)

    dec_text = security.decrypt(enc_text, secure_key)
    dec_text_2 = security.decrypt(enc_text_2, secure_key)

    assert enc_text != enc_text_2
    assert dec_text == dec_text_2
    assert dec_text == 'my-password'


def test_decryption_failure():
    from orb.utils import security

    with pytest.raises(RuntimeError):
        security.decrypt('1234')