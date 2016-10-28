"""
Defines methods to improve common functionality with the json module.
"""

import datetime
import decimal
import json
import re


# generate conversion expressions
datetime_expr = re.compile('^(\d{4})-(\d{2})-(\d{2})(\s|T)(\d{2}):(\d{2}):(\d{2}):?(\d*)$')
date_expr = re.compile('^\d{4}-\d{2}-\d{2}$')
time_expr = re.compile('^\d{2}:\d{2}:\d{2}:?(\d*)$')


def dumps(obj, default=None, indent=4, sort_keys=True):
    """
    Dumps the python object to JSON format.  This method will use the
    py2json method to serialize the python content.

    :param obj: <variant>
    :param default: None or <callable>
    :param indent: <int>
    :param sort_keys: <bool>

    :return: <str>
    """
    return json.dumps(obj,
                      default=default or py2json,
                      indent=indent,
                      sort_keys=sort_keys)


def json2py(obj):
    """
    Converts a JSON object to Python

    :param obj: <variant>

    :return: <variant>
    """
    # convert a dictionary object
    if type(obj) == dict:
        return {k: json2py(v) for k, v in obj.items()}

    # convert a normal object
    elif not isinstance(obj, (str, unicode)):
        return obj

    # convert a string date value
    elif date_expr.match(obj):
        return datetime.date(*(int(x) for x in obj.split('-')))

    # convert a string time value
    elif time_expr.match(obj):
        return datetime.time(*(int(x) for x in obj.split(':')))

    else:
        # convert a string datetime value
        result = datetime_expr.match(obj)
        if result:
            return datetime.datetime(*(int(x) for x in result.groups() if x.isdigit()))
        else:
            return obj


def loads(json_data, object_hook=None):
    """
    Loads in a JSON object string and converts it to python using the
    json2py method.

    :param json_data: <str>
    :param object_hook: None or <callable>

    :return: <dict>
    """
    return json.loads(json_data, object_hook=object_hook or json2py)


def py2json(obj):
    """
    Converts a Python object to JSON

    :param obj: <variant>

    :return: <variant>
    """
    json_data = getattr(obj, '__json__', None)

    if callable(json_data):
        return json_data()
    elif json_data is not None:
        return json_data
    elif type(obj) in (datetime.datetime, datetime.date, datetime.time):
        return obj.isoformat()
    elif type(obj) is set:
        return list(obj)
    elif type(obj) is decimal.Decimal:
        return str(obj)
    else:
        opts = (obj, type(obj))
        raise TypeError('Unserializable object {} of type {}'.format(*opts))

