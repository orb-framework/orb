from orb.core.column import Column


class BooleanColumn(Column):
    """
    Defines a boolean field.  This column will return True or False, or None if no value
    has been set.  The default value for this column is None.
    """
    def value_from_string(self, value, extra=None, db=None):
        """
        Converts the given string to a boolean.  This will look for
        a bool as a string (True, true, TRUE, etc.) and fall back on
        a boolean for the emptyness of a string ('not empty' vs. '').

        :param value: <str> or <unicode>
        :param extra: <variant>
        :param db: <orb.Database> or None

        :return: <bool>
        """
        if not isinstance(value, (str, unicode)):
            return bool(value)
        else:
            normalized = value.lower()
            if normalized in ('true', 'false'):
                return normalized == 'true'
            else:
                return bool(normalized)

