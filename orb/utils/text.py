"""
Defines utility functions for manipulating text.
"""
import ast

CONSTANTS = {
    'true': True,
    'false': False,
    'null': None
}

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
    except StandardError:  # pragma: no cover
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
        except StandardError:
            return text
