""" [desc] """

import logging

from projex.addon import AddonManager

log = logging.getLogger(__name__)


class DataConverter(AddonManager):
    """
    Handles safe mapping of record values to database values.
    """
    def convert(self, value):
        """
        Converts the inputted value to a standard Python value.
        
        :param      value | <variant>
        
        :return     <variant>
        """
        return value
    
    @staticmethod
    def toPython(value):
        """
        Converts the inputted value to a basic Python value that can be wrapped
        for the database.
        
        :param      value | <variant>
        
        :return     <variant>
        """
        for converter in DataConverter.addons().values():
            value = converter.convert(value)
        return value

# import the data converter classes
from .converters import __plugins__
DataConverter.registerAddonModule(__plugins__)

