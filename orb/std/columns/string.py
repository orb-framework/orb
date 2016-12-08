import demandimport
import cgi
import os

from orb.core.column import Column
from orb.utils import security, text

with demandimport.enabled():
    import bleach
    from webhelpers.text import strip_tags


class StringColumn(Column):
    def __init__(self,
                 max_length=255,
                 security_key=None,
                 **kw):
        super(StringColumn, self).__init__(**kw)

        # define custom properties
        self.__max_length = max_length
        self.__security_key = security_key

    def copy(self, **kw):
        """
        Re-implements the copy method to include the custom properties for this column.

        Args:
            **kw: <dict>

        Returns:
            <orb.StringColumn>

        """
        kw.setdefault('max_length', self.__max_length)
        kw.setdefault('security_key', self.__security_key)

        return super(StringColumn, self).copy(**kw)

    def max_length(self):
        """
        Returns the maximum length for this string.

        Returns:
            <int>

        """
        return self.__max_length

    def restore(self, value, context=None):
        """
        Restores the value from a table cache for usage.

        Args:
            value: <variant>
            context: <orb.Context> or None
        """
        if text.is_string(value):
            value = text.decoded(value)

        return super(StringColumn, self).restore(value, context)

    def security_key(self):
        """
        Returns the encryption key that will be used for this column.  If
        not explicitly set at the column level, the global system security
        key will be used instead.

        Returns:
            <str> or None

        """
        return self.__security_key

    def set_max_length(self, max_length):
        """
        Assigns the maximum length for this column.

        Args:
            max_length: <int>

        """
        self.__max_length = max_length

    def set_security_key(self, key):
        """
        Assigns the security key for this column.

        Args:
            key: <str> or None

        """
        self.__security_key = key

    def store(self, value, context=None):
        """
        Processes the given value using the filters associated with this column.

        Args:
            value: <variant>

        Returns:
            <unicode> or None
        """
        if value is None:
            return value
        else:
            if self.test_flag(self.Flags.Encrypted):
                value = security.encrypt(value, self.security_key())

            return super(StringColumn, self).store(value, context=context)


class TextColumn(StringColumn):
    def __init__(self, **kw):
        kw.setdefault('max_length', None)
        super(TextColumn, self).__init__(**kw)


class HtmlColumn(TextColumn):
    def __init__(self, bleach_options=None, **kw):
        super(HtmlColumn, self).__init__(**kw)

        # define custom properties
        self.__bleach_options = bleach_options or {}

        # add HTML clean up filters
        self.add_value_filter(self.clean_html)

    def clean_html(self, text):
        """
        Cleans up the given HTML text using the `bleach` library.  This
        will ensure only acceptable HTML content is added to the backend.

        Args:
            text: <str>

        Returns:
            <str>

        """
        return bleach.clean(text, **self.__bleach_options)


class PlainTextColumn(StringColumn):
    def __init__(self, **kw):
        super(PlainTextColumn, self).__init__(**kw)

        # add text clean up filters
        self.add_value_filter(strip_tags)  # strip HTML tags
        self.add_value_filter(cgi.escape)  # escape remaining tags


class TokenColumn(StringColumn):
    """ """
    def __init__(self, bits=32, **kwds):
        super(TokenColumn, self).__init__(**kwds)

        # set standard properties
        self.set_flag(self.Flags.Unique)
        self.set_flag(self.Flags.Required)

        # set custom properties
        self.__bits = bits
        self.set_bits(bits)

    def bits(self):
        """
        Returns the bit length for this column.

        Returns:
            <int>
        """
        return self.__bits

    def copy(self, **kw):
        """
        Copies the bits for this column.

        Args:
            **kw: <dict>

        Returns:
            <orb.TokenColumn>

        """
        kw.setdefault('bits', self.__bits)
        return super(TokenColumn, self).copy(**kw)

    def default(self):
        """
        Returns the default value for this token.  This will be a random
        string value based on the generated value.

        Returns:
            <str>
        """
        return self.generate_token()

    def generate_token(self):
        """
        Generates a new token for this column based on its bit length.  This method
        will not ensure uniqueness in the model itself, that should be checked against
        the model records in the database first.

        Returns:
            <str>
        """
        return os.urandom(self.__bits).encode('hex')

    def set_bits(self, bits):
        """
        Sets the length in bits that this column will create a token for.

        Args:
            bits: <int>
        """
        self.__bits = bits
        self.set_max_length(bits * 2)

