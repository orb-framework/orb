import projex.security

from projex.enum import enum
from projex.lazymodule import lazy_import

orb = lazy_import('orb')


class Security(object):
    def __init__(self, key):
        self.__key = key

    def decrypt(self, text):
        if not self.__key:
            raise orb.errors.EncryptionDisabled('No security key defined.')
        return projex.security.decrypt(text, self.__key)

    def encrypt(self, text):
        if not self.__key:
            raise orb.errors.EncryptionDisabled('No security key defined.')
        return projex.security.encrypt(text, self.__key)

    def key(self):
        return self.__key

    def setKey(self, key):
        self.__key = key

