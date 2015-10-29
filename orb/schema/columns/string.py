import projex.text
import re

from projex.lazymodule import lazy_import
from ..column import Column

orb = lazy_import('orb')

class AbstractStringColumn(Column):
    def __init__(self, **kwds):
        kwds.setdefault('defaultOrder', 'desc')

        super(AbstractStringColumn, self).__init__(**kwds)

    def restore(self, value, context=None):
        """
        Restores the value from a table cache for usage.

        :param      value   | <variant>
                    context | <orb.ContextOptions> || None
        """
        if isinstance(value, (str, unicode)):
            return projex.text.decoded(value)
        else:
            return super(AbstractStringColumn, self).restore(value, context)

    def store(self, value):
        """
        Converts the value to one that is safe to store on a record within
        the record values dictionary

        :param      value | <variant>

        :return     <variant>
        """
        if isinstance(value, (str, unicode)) and self.testFlag(self.Flags.Encrypted):
            return orb.system.encrypt(value)
        else:
            return super(AbstractStringColumn, self).store(value)


# define base string based class types
class StringColumn(AbstractStringColumn):
    def __init__(self,
                 maxlength=None,
                 **kwds):
        super(AbstractStringColumn, self).__init__(name, **kwds)

        # define custom properties
        self._maxlength = maxlength

    def maxlength(self):
        """
        Returns the max length for this column.  This property
        is used for the varchar data type.

        :return     <int>
        """
        return self._maxlength

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
        self._pattern = pattern

    def validate(self, value):
        """
        Validates the value provided is a valid email address,
        at least, on paper.

        :param value: <str>

        :return: <bool>
        """
        if not re.match(self._pattern, value):
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
        self._minlength = minlength
        self._allowUnicode = allowUnicode
        self._requireUppercase = requireUppercase
        self._requireLowercase = requireLowercase
        self._requireNumber = requireNumber
        self._requireWildcard = requireWildcard
        self._invalidCharacters = invalidCharacters
        self._invalidCharacterRule = invalidCharacterRule

    def rules(self):
        """
        Returns the rules for this password based on the configured
        options.

        :return: <str>
        """
        rules = ['Passwords need to be at least {0} characters long'.format(self._minlength)]

        if self._requireUppercase:
            rules.append('have at least one uppercase letter')
        if self._requireLowercase:
            rules.append('have at least one lowercase letter')
        if self._requireNumber:
            rules.append('have at least one number')
        if self._requireWildcard:
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

        elif not self._allowUnicode and value != projex.text.toAscii(value):
            raise orb.errors.ColumnValidationError(self, 'Only ASCII characters are allowed for your password.')

        elif len(value) < self._minlength or \
                (self._requireUppercase and not re.search('[A-Z]', value)) or \
                (self._requirLowercase and not re.search('[a-z]', value)) or \
                (self._requireNumber and not re.search('[0-9]', value)) or \
                (self._requireWildcard and not re.search('[^a-zA-Z0-9]', value)):
            raise orb.errors.ColumnValidationError(self, self.rules())

        # check for invalid characters
        elif self._invalidCharacters and re.search(self._invalidCharacters, value):
            raise orb.errors.ColumnValidationError(self, self._invalidCharactersRule)

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
