from ..column import Column
from .numeric import LongColumn


class EnumColumn(LongColumn):
    def __init__(self, enum=None, **kwds):
        super(EnumColumn, self).__init__(**kwds)

        # define custom properties
        self.__enum = enum

    def enum(self):
        """
        Returns the enumeration that is associated with this column.  This can
        help for automated validation when dealing with enumeration types.

        :return     <projex.enum.enum> || None
        """
        return self.__enum


    def setEnum(self, cls):
        """
        Sets the enumeration that is associated with this column to the inputted
        type.  This is an optional parameter but can be useful when dealing
        with validation and some of the automated features of the ORB system.

        :param      cls | <projex.enum.enum> || None
        """
        self.__enum = cls


Column.registerAddon('Enum', EnumColumn)