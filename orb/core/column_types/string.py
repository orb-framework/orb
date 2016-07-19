import cgi
import os
import projex.text
import random
import re
import warnings

from projex.lazymodule import lazy_import
from ..column import Column


orb = lazy_import('orb')

class AbstractStringColumn(Column):
    MathMap = Column.MathMap.copy()
    MathMap['Default']['Add'] = '||'

    def __init__(self, cleaned=False, escaped=False, **kwds):
        kwds.setdefault('defaultOrder', 'desc')

        super(AbstractStringColumn, self).__init__(**kwds)

        self.__cleaned = cleaned
        self.__escaped = escaped

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

    def dbStore(self, typ, py_value):
        """
        Prepares to store this column for the a particular backend database.

        :param backend: <orb.Database>
        :param py_value: <variant>

        :return: <variant>
        """
        if py_value is not None:
            py_value = projex.text.decoded(py_value)

            if self.cleaned():
                py_value = self.clean(py_value)

            if self.escaped():
                py_value = self.escape(py_value)

            return py_value
        else:
            return py_value

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

    def random(self):
        """
        Returns a random value that fits this column's parameters.

        :return: <variant>
        """
        return ''

    def restore(self, value, context=None):
        """
        Restores the value from a table cache for usage.

        :param      value   | <variant>
                    context | <orb.Context> || None
        """
        # ensure this value is a string type
        if isinstance(value, (str, unicode)):
            value = projex.text.decoded(value)

        return super(AbstractStringColumn, self).restore(value, context)

    def store(self, value, context=None):
        """
        Converts the value to one that is safe to store on a record within
        the record values dictionary

        :param      value | <variant>

        :return     <variant>
        """
        if isinstance(value, (str, unicode)) and self.testFlag(self.Flags.Encrypted):
            value = orb.system.security().encrypt(value)

        return super(AbstractStringColumn, self).store(value, context=context)


# define base string based class types
class StringColumn(AbstractStringColumn):
    TypeMap = {
        'Postgres': 'CHARACTER VARYING',
        'SQLite': 'TEXT',
        'MySQL': 'varchar'
    }

    def __init__(self,
                 maxLength=255,
                 **kwds):
        super(StringColumn, self).__init__(**kwds)

        # define custom properties
        self.__maxLength = maxLength

    def dbType(self, connectionType):
        typ = super(StringColumn, self).dbType(connectionType)

        if self.maxLength():
            return typ + '({0})'.format(self.maxLength())
        else:
            return typ

    def loadJSON(self, jdata):
        """
        Loads JSON data for this column type.

        :param jdata: <dict>
        """
        super(StringColumn, self).loadJSON(jdata)

        # load additional info
        self.__maxLength = jdata.get('maxLength') or self.__maxLength

    def maxLength(self):
        """
        Returns the max length for this column.  This property
        is used for the varchar data type.

        :return     <int>
        """
        return self.__maxLength

    def setMaxLength(self, length):
        """
        Sets the maximum length for this string column to the given length.

        :param length: <int>
        """
        self.__maxLength = length


class TextColumn(AbstractStringColumn):
    TypeMap = {
        'Postgres': 'TEXT',
        'SQLite': 'TEXT',
        'MySQL': 'TEXT'
    }


# define custom string class types
class ColorColumn(StringColumn):
    def random(self):
        """
        Returns a random value that fits this column's parameters.

        :return: <variant>
        """
        return '#' + random.randrange(256).encode('hex') + \
               random.randrange(256).encode('hex') + random.randrange(256).encode('hex')



class DirectoryColumn(StringColumn):
    def random(self):
        """
        Returns a random value that fits this column's parameters.

        :return: <variant>
        """
        return '/usr/tmp/'


class EmailColumn(StringColumn):
    def __init__(self, pattern='[\w\-\.]+\@\w+\.\w+', **kwds):
        super(EmailColumn, self).__init__(**kwds)

        # define custom properties
        self.__pattern = pattern

    def random(self):
        """
        Returns a random value that fits this column's parameters.

        :return: <variant>
        """
        return '/usr/tmp/'

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
        self.setFlag(self.Flags.Required)
        self.setFlag(self.Flags.Encrypted)
        self.setFlag(self.Flags.Private)

        # define custom properties
        self.__minlength = minlength
        self.__allowUnicode = allowUnicode
        self.__requireUppercase = requireUppercase
        self.__requireLowercase = requireLowercase
        self.__requireNumber = requireNumber
        self.__requireWildcard = requireWildcard
        self.__invalidCharacters = invalidCharacters
        self.__invalidCharacterRule = invalidCharacterRule

    def random(self):
        """
        Returns a random value that fits this column's parameters.

        :return: <variant>
        """
        return '******'

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
        kwds['maxLength'] = bits * 2

        super(TokenColumn, self).__init__(**kwds)

        # set standard properties
        self.setFlag(self.Flags.Unique)
        self.setFlag(self.Flags.Required)

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

    def random(self):
        """
        Returns a random value that fits this column's parameters.

        :return: <variant>
        """
        return os.urandom(self.__bits).encode('hex')

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


# register string column types
Column.registerAddon('String', StringColumn)
Column.registerAddon('Text', TextColumn)

Column.registerAddon('Color', ColorColumn)
Column.registerAddon('Directory', DirectoryColumn)
Column.registerAddon('Email', EmailColumn)
Column.registerAddon('Filepath', FilepathColumn)
Column.registerAddon('Html', HtmlColumn)
Column.registerAddon('Password', PasswordColumn)
Column.registerAddon('Url', UrlColumn)
Column.registerAddon('Xml', XmlColumn)
