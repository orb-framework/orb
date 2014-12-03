""" [desc] """

# define authorship information
__authors__         = ['Eric Hulser']
__author__          = ','.join(__authors__)
__credits__         = []
__copyright__       = 'Copyright (c) 2011, Projex Software, LLC'
__license__         = 'LGPL'

__maintainer__      = 'Projex Software, LLC'
__email__           = 'team@projexsoftware.com'

import binascii
import projex.text
import logging

from projex.addon import AddonManager
from projex.lazymodule import LazyModule
from projex.text import nativestring as nstr

log = logging.getLogger(__name__)


class DataConverter(AddonManager):
    """
    Handles safe mapping of record values to database values.
    """
    def convert(self, value):
        """
        Converts the inputed value to a standard Python value.
        
        :param      value | <variant>
        
        :return     <variant>
        """
        return value
    
    @staticmethod
    def toPython(value):
        """
        Converts the inputed value to a basic Python value that can be wrapped
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

