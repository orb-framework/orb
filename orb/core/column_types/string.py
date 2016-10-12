import demandimport
import cgi
import os
import projex.text
import random
import re
import warnings

from ..column import Column
from ..column_engine import ColumnEngine
from ...utils import security

with demandimport.enabled():
    import orb


class StringColumnEngine(ColumnEngine):
    def __init__(self, type_map=None):
        super(StringColumnEngine, self).__init__(type_map)

        # override the default operator mapping
        self.assign_operator(orb.Query.Math.Add, '||')

    def get_column_type(self, column, plugin_name):
        """
        Re-implements the get_column_type method from `orb.ColumnEngine`.

        String columns have a couple additional attributes that can be used
        during type definition.

        :param column: <orb.AbstractStringColumn>
        :param plugin_name: <str>

        :return: <str>
        """
        base_type = super(StringColumnEngine, self).get_column_type(column, plugin_name)
        max_len = column.maxLength() or ''
        return base_type.format(length=max_len).replace('()', '')

    def get_database_value(self, column, plugin_name, py_value, context=None):
        """
        Re-implements the get_database_value method from ColumnEngine.

        This method will take a Python value and prepre it for saving to
        the database.

        :param column: <orb.Column>
        :param plugin_name: <str>
        :param py_value: <variant>

        :return: <variant> database value
        """
        if py_value is None:
            return None
        else:
            py_value = projex.text.decoded(py_value)

            if column.cleaned():
                py_value = column.clean(py_value)

            if column.escaped():
                py_value = column.escape(py_value)

            return py_value


class AbstractStringColumn(Column):
    def __init__(self, cleaned=False, escaped=False, **kwds):
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

    def random_value(self):
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
        if isinstance(value, (str, unicode)) and self.test_flag(self.Flags.Encrypted):
            value = security.encrypt(value)

        return super(AbstractStringColumn, self).store(value, context=context)


# define base string based class types
class StringColumn(AbstractStringColumn):
    __default_engine__ = StringColumnEngine(type_map={
        'Postgres': 'CHARACTER VARYING({length})',
        'MySQL': 'varchar({length})',
        'SQLite': 'TEXT'
    })

    def __init__(self,
                 maxLength=255,
                 **kwds):
        super(StringColumn, self).__init__(**kwds)

        # define custom properties
        self.__maxLength = maxLength

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
    __default_engine__ = StringColumnEngine(type_map={
        'Postgres': 'TEXT',
        'SQLite': 'TEXT',
        'MySQL': 'TEXT'
    })


# define custom string class types
class ColorColumn(StringColumn):
    def random_value(self):
        """
        Returns a random value that fits this column's parameters.

        :return: <variant>
        """
        return '#' + random.randrange(256).encode('hex') + \
               random.randrange(256).encode('hex') + random.randrange(256).encode('hex')



class DirectoryColumn(StringColumn):
    def random_value(self):
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

    def random_value(self):
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

    def random_value(self):
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

    def random_value(self):
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

