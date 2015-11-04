import projex.text
import re

from projex.lazymodule import lazy_import
from ..column import Column

orb = lazy_import('orb')

class AbstractStringColumn(Column):
    def __init__(self, **kwds):
        kwds.setdefault('defaultOrder', 'desc')

        super(AbstractStringColumn, self).__init__(**kwds)

    def extract(self, value, context=None):
        """
        Extracts data from the database.

        :param value: <variant>
        :param context: <orb.ContextOptions>

        :return: <variant>
        """

        # restore translatable column
        if self.testFlag(self.Flags.Translatable):
            if isinstance(value, (str, unicode)) and value.startswith('{'):
                try:
                    value = projex.text.safe_eval(value)
                except StandardError:
                    value = None
            elif context and context.locale != 'all':
                value = {context.locale: value}
            else:
                value = {self.currentLocale(): value}
            return value
        else:
            return super(AbstractStringColumn, self).extract(value, context=context)

    def restore(self, value, context=None, inflated=False):
        """
        Restores the value from a table cache for usage.

        :param      value   | <variant>
                    context | <orb.ContextOptions> || None
        """
        context = context or orb.ContextOptions()

        # check to see if this column is translatable before restoring
        if self.testFlag(self.Flags.Translatable):
            locales = context.locale.split(',')
            if isinstance(value, (str, unicode)):
                value = {locales[0]: value}

            if value is None:
                return ''

            # return all the locales
            elif 'all' in locales:
                return value

            if len(locales) == 1:
                return value.get(locales[0])
            else:
                return {locale: value.get(locale) or '' for locale in locales}

        # ensure this value is a string type
        elif isinstance(value, (str, unicode)):
            return projex.text.decoded(value)
        else:
            return super(AbstractStringColumn, self).restore(value, context)

    def store(self, value, context):
        """
        Converts the value to one that is safe to store on a record within
        the record values dictionary

        :param      value | <variant>

        :return     <variant>
        """
        if self.testFlag(self.Flags.Translatable):
            if isinstance(value, (str, unicode)):
                return {context.locale: value}
        elif isinstance(value, (str, unicode)) and self.testFlag(self.Flags.Encrypted):
            return orb.system.encrypt(value)
        else:
            return super(AbstractStringColumn, self).store(value)


# define base string based class types
class StringColumn(AbstractStringColumn):
    def __init__(self,
                 maxlength=None,
                 **kwds):
        super(AbstractStringColumn, self).__init__(**kwds)

        # define custom properties
        self.__maxlength = maxlength

    def loadJSON(self, jdata):
        """
        Loads JSON data for this column type.

        :param jdata: <dict>
        """
        super(StringColumn, self).loadJSON(jdata)

        # load additional info
        self.__maxlength = jdata.get('maxlength') or self.__maxlength

    def maxLength(self):
        """
        Returns the max length for this column.  This property
        is used for the varchar data type.

        :return     <int>
        """
        return self.__maxlength

    def setMaxLength(self, length):
        """
        Sets the maximum length for this string column to the given length.

        :param length: <int>
        """
        self.__maxlength = length

class TextColumn(AbstractStringColumn):
    pass


# define custom string class types
class ColorColumn(StringColumn):
    pass

class DirectoryColumn(StringColumn):
    pass

class EmailColumn(StringColumn):
    def __init__(self, pattern='[\w\-\.]+\@\w+\.\w+]', **kwds):
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
        if not re.match(self.__pattern, value):
            raise orb.errors.ColumnValidationError(self, 'The email provided is not valid.')
        else:
            return super(EmailColumn, self).validate(value)

class FilepathColumn(StringColumn):
    pass

class HtmlColumn(TextColumn):
    pass

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
        self.setFlag(self.Flags.Encrypted)

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
                (self.__requirLowercase and not re.search('[a-z]', value)) or \
                (self.__requireNumber and not re.search('[0-9]', value)) or \
                (self.__requireWildcard and not re.search('[^a-zA-Z0-9]', value)):
            raise orb.errors.ColumnValidationError(self, self.rules())

        # check for invalid characters
        elif self.__invalidCharacters and re.search(self.__invalidCharacters, value):
            raise orb.errors.ColumnValidationError(self, self.__invalidCharactersRule)

        else:
            return super(PasswordColumn, self).validate(value)


class UrlColumn(StringColumn):
    pass

class XmlColumn(TextColumn):
    pass

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
