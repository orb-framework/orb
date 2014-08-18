##!/usr/bin/python

"""
Defines the global query building syntzx for generating db
agnostic queries quickly and easily.
"""

# define authorship information
__authors__         = ['Eric Hulser']
__author__          = ','.join(__authors__)
__credits__         = []
__copyright__       = 'Copyright (c) 2011, Projex Software'
__license__         = 'LGPL'

# maintanence information
__maintainer__      = 'Projex Software'
__email__           = 'team@projexsoftware.com'

import os
import re

FIELD_SYNTAX = os.environ.get('ORB_FIELD_SYNTAX', '(?P<%s>[\w_\.]+)')
VALUE_SYNTAX = os.environ.get('ORB_VALUE_SYNTAX', 
                              '(?P<%s>([\w\-_\.,]+|"[^"]+")|\[[^\]]+\])')


class QueryPattern(object):
    def __init__(self, syntax):
        self._syntax = syntax
        
        field       = FIELD_SYNTAX % 'field'
        value       = VALUE_SYNTAX % 'value'
        val_min     = VALUE_SYNTAX % 'min'
        val_max     = VALUE_SYNTAX % 'max'
        
        opts = {'value': value, 
                'min': val_min, 
                'max': val_max,
                'field': field}
        
        expr = syntax % opts
        
        self._pattern = re.compile(expr)
    
    def pattern(self):
        """
        Returns the regular expression pattern for this pattern.
        
        :return     <re.SRE_Pattern>
        """
        return self._pattern
    
    def syntax(self):
        """
        Returns the string syntax to be used for this pattern.
        
        :return     <str>
        """
        return self._syntax