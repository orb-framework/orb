"""
Defines utility functions for manipulating text.
"""
import ast
import sys

from encodings.aliases import aliases

if sys.version_info[0] == 3:  # pragma: no cover
    unicode_type = str
    bytes_type = bytes
else:  # pragma: no cover
    unicode_type = unicode
    bytes_type = str


CONSTANTS = {
    'true': True,
    'false': False,
    'null': None
}
DEFAULT_ENCODING = 'utf-8'
SUPPORTED_ENCODINGS = list(sorted(set(aliases.values())))


def decoded(text, encoding=DEFAULT_ENCODING):
    """
    Attempts to decode the inputted unicode/string variable using the
    given encoding type.  If no encoding is provided, then it will attempt
    to use one of the ones available from the default list.

    :param      text     | <variant>
                encoding | <str> || None

    :return     <unicode>
    """
    # return the unicode if already provided
    if type(text) == unicode_type:
        return text

    # decode the bytes to unicode
    elif type(text) == bytes_type:
        for e in [encoding] + SUPPORTED_ENCODINGS:
            if not e:
                continue
            else:
                try:
                    result = text.decode(e)
                except Exception:  # pragma: no cover
                    pass
                else:
                    if result:
                        return result
        else:  # pragma: no cover
            return u'????'

    # convert an object to text
    else:
        try:
            return unicode_type(text)
        except Exception:  # pragma: no cover
            try:
                return encoded(bytes_type(text))
            except Exception:
                return u'????'


def encoded(text, encoding=DEFAULT_ENCODING):
    """
    Encodes the inputted unicode/string variable with the given encoding type.

    Args:
        text: <variant>
        encoding: <str>

    Returns:
        <str> or <unicode>

    """
    # return the bytes if already provided
    if type(text) == bytes_type:
        return text

    # encode a unicode string item to bytes
    elif type(text) == unicode_type:
        try:
            return text.encode(encoding)
        except Exception:
            return text.encode(encoding, errors='ignore')

    # create a text representation of the value
    else:
        try:
            return bytes_type(text)
        except Exception:  # pragma: no cover
            return '????'


def is_string(text):
    """
    Returns whether or not the given text value is a string.

    Args:
        text: <variant>

    Returns:
        <bool>

    """
    return isinstance(text, (unicode_type, bytes_type))


def nativestring(text):
    """
    Ensures the given text value is a native python string value.

    :param text: <variant>

    :return: <unicode> or <str>
    """
    # if it is already a native python string, don't do anything
    if isinstance(text, basestring):
        return text

    # otherwise, attempt to return a decoded value
    try:
        return unicode(text)
    except Exception:  # pragma: no cover
        return str(text)


def safe_eval(text):
    """
    Converts the given text to a Python value.

    :param text: <str> or <variant>

    :return: <variant>
    """
    # check if this value is already a text object
    if not isinstance(text, basestring):
        return text

    # look for constants
    try:
        return CONSTANTS[text]

    # try using the literal value
    except KeyError:
        try:
            return ast.literal_eval(text)
        except Exception:
            return text


def to_ascii(text):
    """
    Safely converts the inputted text to standard ASCII characters.

    Args:
        text: <unicode>

    Returns:
        <str>
    """
    return bytes_type(encoded(text, 'ascii'))