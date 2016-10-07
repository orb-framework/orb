"""
Security module used for encryption and decryption within the ORB system.
"""

import base64
import demandimport
import hashlib

with demandimport.enabled():
    import orb

    from Crypto.Cipher import AES
    from Crypto import Random


def decrypt(text, key=None):
    """
    Decrypts the inputted text using the inputted key.

    :param      text    | <str>
                key     | <str>

    :return     <str>
    """
    if key is None:
        key = orb.system.settings().security_key

    if not key:
        raise RuntimeError('Invalid decryption key')

    text = base64.b64decode(text)
    iv = text[:16]
    cipher = AES.new(key, AES.MODE_CBC, iv)
    return unpad(cipher.decrypt(text[16:]))


def encrypt(text, key=None):
    """
    Encrypts the inputted text using the AES cipher.  If the PyCrypto
    module is not included, this will simply encode the inputted text to
    base64 format.

    :param      text    | <str>
                key     | <str>

    :return     <str>
    """
    if key is None:
        key = orb.system.settings().security_key

    if not key:
        raise RuntimeError('Invalid encryption key')

    bits = len(key)
    text = pad(text, bits)
    iv = Random.new().read(16)
    cipher = AES.new(key, AES.MODE_CBC, iv)
    return base64.b64encode(iv + cipher.encrypt(text))


def generate_key(secret, bits=32):
    """
    Generates a new encryption key off the given code string.

    :param secret: <str>
    :param bits: <int>

    :return     <str>
    """
    if bits == 32:
        hasher = hashlib.sha256
    elif bits == 16:
        hasher = hashlib.md5
    else:
        raise RuntimeError('Invalid hash type')

    return hasher(secret).digest()


def pad(text, bits=32):
    """
    Pads the inputted text to ensure it fits the proper block length
    for encryption.

    :param      text | <str>
                bits | <int>

    :return     <str>
    """
    return text + (bits - len(text) % bits) * chr(bits - len(text) % bits)


def unpad(text):
    """
    Unpads the text from the given block size.

    :param      text | <str>

    :return     <str>
    """
    return text[0:-ord(text[-1])]



