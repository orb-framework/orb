import demandimport
import cgi
import os
import projex.text
import random
import re
import warnings

from orb.core.column import Column
from orb.utils import security

with demandimport.enabled():
    import orb


class StringColumn(Column):
    def __init__(self, max_length=255, cleaned=False, escaped=False, **kwds):
        super(StringColumn, self).__init__(**kwds)

        # define custom properties
        self.__cleaned = cleaned
        self.__escaped = escaped
        self.__max_length = max_length

    def clean(self, py_value):
        """
        Cleans the value before storing it.

        :param:     py_value : <str>

        :return:    <str>
        """
        try:
            from webhelpers.text import strip_tags
            return strip_tags(py_value)
        except ImportError:
            warnings.warn('Unable to clean string column without webhelpers installed.')
            return py_value

    def cleaned(self):
        """
        Returns whether or not the column should be cleaned before storing to the database.

        :return:    <bool>
        """
        return self.__cleaned

    def escape(self, value):
        """
        Escapes the given value, blocking invalid characters.

        :param value: <str>

        :return: <str>
        """
        return cgi.escape(value)

    def escaped(self):
        """
        Returns whether or not this column escape's HTML characters

        :return: <bool>
        """
        return self.__escaped

    def max_length(self):
        return self.__max_length

    def restore(self, value, context=None):
        """
        Restores the value from a table cache for usage.

        :param      value   | <variant>
                    context | <orb.Context> || None
        """
        # ensure this value is a string type
        if isinstance(value, (str, unicode)):
            value = projex.text.decoded(value)

        return super(StringColumn, self).restore(value, context)

    def setMaxLength(self, max_length):
        return self.__max_length

    def store(self, value, context=None):
        """
        Converts the value to one that is safe to store on a record within
        the record values dictionary

        :param      value | <variant>

        :return     <variant>
        """
        if isinstance(value, (str, unicode)) and self.test_flag(self.Flags.Encrypted):
            value = security.encrypt(value)

        return super(StringColumn, self).store(value, context=context)


class TextColumn(StringColumn):
    def __init__(self, **kw):
        kw.setdefault('max_length', None)
        super(TextColumn, self).__init__(**kw)


# define custom string class types
class ColorColumn(StringColumn):
    pass


class DirectoryColumn(StringColumn):
    pass


class EmailColumn(StringColumn):
    def __init__(self, pattern='[\w\-\.]+\@\w+\.\w+', **kwds):
        super(EmailColumn, self).__init__(**kwds)

        # define custom properties
        self.__pattern = pattern

    def validate(self, value):
        """
        Validates the value provided is a valid email address,
        at least, on paper.

        :param value: <str>

        :return: <bool>
        """
        if isinstance(value, (str, unicode)) and not re.match(self.__pattern, value):
            raise orb.errors.ColumnValidationError(self, 'The email provided is not valid.')
        else:
            return super(EmailColumn, self).validate(value)


class FilepathColumn(StringColumn):
    pass


class HtmlColumn(TextColumn):
    def __init__(self, bleachOptions=None, **kwds):
        kwds.setdefault('escaped', False)

        super(HtmlColumn, self).__init__(**kwds)

        self.__bleachOptions = bleachOptions or {}

    def clean(self, py_value):
        """
        Cleans the value before storing it.

        :param:     py_value : <str>

        :return:    <str>
        """
        try:
            import bleach
            return bleach.clean(py_value, **self.__bleachOptions)
        except ImportError:
            warnings.warn('Unable to clean string column without webhelpers installed.')
            return py_value


class PlainTextColumn(StringColumn):
    def __init__(self, **kwds):
        kwds.setdefault('escaped', True)
        kwds.setdefault('cleaned', True)

        super(PlainTextColumn, self).__init__(**kwds)


class PasswordColumn(StringColumn):
    def __init__(self,
                 minlength=8,
                 requireUppercase=True,
                 requireLowercase=True,
                 requireNumber=True,
                 requireWildcard=False,
                 allowUnicode=False,
                 invalidCharacters='',
                 invalidCharacterRule='',
                 **kwds):
        super(PasswordColumn, self).__init__(**kwds)

        # setup default options
        self.set_flag(self.Flags.Required)
        self.set_flag(self.Flags.Encrypted)
        self.set_flag(self.Flags.Private)

        # define custom properties
        self.__minlength = minlength
        self.__allowUnicode = allowUnicode
        self.__requireUppercase = requireUppercase
        self.__requireLowercase = requireLowercase
        self.__requireNumber = requireNumber
        self.__requireWildcard = requireWildcard
        self.__invalidCharacters = invalidCharacters
        self.__invalidCharacterRule = invalidCharacterRule

    def rules(self):
        """
        Returns the rules for this password based on the configured
        options.

        :return: <str>
        """
        rules = ['Passwords need to be at least {0} characters long'.format(self.__minlength)]

        if self.__requireUppercase:
            rules.append('have at least one uppercase letter')
        if self.__requireLowercase:
            rules.append('have at least one lowercase letter')
        if self.__requireNumber:
            rules.append('have at least one number')
        if self.__requireWildcard:
            rules.append('have at least one non alpha-number character')

        if len(rules) == 1:
            return rules[0]
        else:
            return ', '.join(rules[:-1]) + ' and ' + rules[-1]

    def validate(self, value):
        """
        Ensures that the password follows the following criteria:

        :param value: <str>

        :return: True
        """
        if not isinstance(value, (str, unicode)):
            raise orb.errors.ColumnValidationError(self, 'Invalid password.')

        elif not self.__allowUnicode and value != projex.text.toAscii(value):
            raise orb.errors.ColumnValidationError(self, 'Only ASCII characters are allowed for your password.')

        elif len(value) < self.__minlength or \
                (self.__requireUppercase and not re.search('[A-Z]', value)) or \
                (self.__requireLowercase and not re.search('[a-z]', value)) or \
                (self.__requireNumber and not re.search('[0-9]', value)) or \
                (self.__requireWildcard and not re.search('[^a-zA-Z0-9]', value)):
            raise orb.errors.ColumnValidationError(self, self.rules())

        # check for invalid characters
        elif self.__invalidCharacters and re.search(self.__invalidCharacters, value):
            raise orb.errors.ColumnValidationError(self, self.__invalidCharactersRule)

        else:
            return super(PasswordColumn, self).validate(value)


class TokenColumn(StringColumn):
    def __init__(self, bits=32, **kwds):
        kwds['max_length'] = bits * 2

        super(TokenColumn, self).__init__(**kwds)

        # set standard properties
        self.set_flag(self.Flags.Unique)
        self.set_flag(self.Flags.Required)

        # set custom properties
        self.__bits = bits

    def bits(self):
        """
        Returns the bit length for this column.

        :return:    <int>
        """
        return self.__bits

    def default(self):
        """
        Returns the default value for this token.  This will be a random
        string value based on the generated value.

        :return:    <str>
        """
        return self.generate()

    def generate(self):
        """
        Generates a new token for this column based on its bit length.  This method
        will not ensure uniqueness in the model itself, that should be checked against
        the model records in the database first.

        :return:    <str>
        """
        try:
            model = self.schema().model()
        except AttributeError:
            return os.urandom(self.__bits).encode('hex')
        else:
            while True:
                token = os.urandom(self.__bits).encode('hex')
                if model.select(where=orb.Query(self) == token).count() == 0:
                    return token

    def setBits(self, bits):
        """
        Sets the length in bits that this column will create a token for.

        :param bits: <int>
        """
        self.__bits = bits


class UrlColumn(StringColumn):
    pass


class XmlColumn(TextColumn):
    def __init__(self, **kwds):
        kwds.setdefault('escaped', False)
        super(XmlColumn, self).__init__(**kwds)

    def clean(self, py_value):
        """
        No cleaning right now for XML columns.  The strip tags and HTML cleaners will erase
        options from the text.
        """
        return py_value

