"""
Defines an enumeration class type used throughout the ORB project.

:usage:

    from orb.utils.enum import enum

    Flags = enum('Required', 'Unique')

    assert Flags.Required == 1
    assert Flags.Unique == 2
    assert (Flags.Required | Flags.Unique) == 3
    assert Flags.from_set({'Required', 'Unique'}) == 3
"""


class enum(object):
    def __init__(self, *binary_keys, **enum_values):
        super(enum, self).__init__()

        # store the base types for different values
        binary_values = {key: 2 ** i for i, key in enumerate(binary_keys)}
        enum_values.update(binary_values)

        # set the properties
        for name, value in enum_values.items():
            setattr(self, name, value)

        self.__enum = enum_values

    def __getitem__(self, key):
        if type(key) in (int, long):
            for k, v in self.__enum.items():
                if v == key:
                    return k
            else:
                raise KeyError(key)
        else:
            return self.__enum[key]

    def __json__(self):
        return dict(self)

    def __iter__(self):
        for k, v in self.__enum.items():
            yield k, v

    def __call__(self, key):
        """
        Same as __getitem__.  This will cast the inputted key to its corresponding
        value in the enumeration.  This works for both numeric and alphabetical
        values.

        :param      key | <str> || <int>

        :return     <int> || <str>
        """
        if isinstance(key, set):
            return self.from_set(key)
        else:
            return self[key]

    def all(self):
        """
        Returns all the values joined together.

        :return     <int>
        """
        out = 0
        for key, value in self.__enum.items():
            out |= value
        return out

    def from_set(self, values):
        """
        Generates a flag value based on the given set of values.

        :param values: <set>

        :return: <int>
        """
        value = 0
        for flag in values:
            value |= self(flag)
        return value

    def to_set(self, flags):
        """
        Generates a flag value based on the given set of values.

        :param values: <set>

        :return: <int>
        """
        return {key for key, value in self.__enum.items() if value & flags}

    def test_flag(self, source_flags, check_flag):
        """
        Tests to see if the given flag is used within
        an enumerated set.

        :param source_flags: <int>
        :param check_flag: <int> or <str> or <set>

        :return: <bool>
        """
        # convert a flag from a string to the enumerated value
        if isinstance(check_flag, (str, unicode)):
            check_flag = self(check_flag)
        elif isinstance(check_flag, set):
            check_flag = self.from_set(check_flag)
        return bool(source_flags & check_flag) if check_flag >= 0 else not bool(source_flags & ~check_flag)