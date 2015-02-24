#!/usr/bin/python

""" Defines common properties and types used by the different modules. """

# define authorship information
__authors__         = ['Eric Hulser']
__author__          = ','.join(__authors__)
__credits__         = []
__copyright__       = 'Copyright (c) 2011, Projex Software'
__license__         = 'LGPL'

# maintanence information
__maintainer__      = 'Projex Software'
__email__           = 'team@projexsoftware.com'

#------------------------------------------------------------------------------

from projex.enum import enum

# C
#------------------------------------------------------------------------------

ColumnType = enum(
    # simple types
    'Bool',
    'Decimal',
    'Double',
    'Integer',      # 32-bit integer
    'BigInt',       # 64-bit integer
    'Enum',         # equates to an integer in databases, but links to an enum
    
    # string types
    'String',       # used for limited char sets
    'Text',         # used for larger string data
    'Url',          # similar to String, but uses url regex validation
    'Email',        # similar to String, but uses email regex validation
    'Password',     # similar to String, but uses password regex validation
    'Filepath',     # similar to String, but uses filepath regex validation
    'Directory',    # similar to String, but uses directory regex validation
    'Xml',          # similar to Text,   but auto-escapse data stored
    'Html',         # similar to Text,   but store rich text data & auto-escape
    'Color',        # similar to String, stores the HEX value of a color (#ff00)
    
    # date/time types
    'Datetime',
    'Date',
    'Interval',
    'Time',
    'DatetimeWithTimezone',
    
    # data types
    'Image',        # stores images in the database as binary
    'ByteArray',    # stores additional binary information
    'Dict',         # stores python dictionary types
    'Pickle',       # stores python pickle data
    'Yaml',         # stores python data as yaml (requires PyYaml)
    'Query',        # stores an orb.Query class as xml
    
    # relation types
    'ForeignKey',   # tells the system to use the relation's information
)

# D
#------------------------------------------------------------------------------

DatabaseFlags = enum('SafeInsert')
DeleteFlags = enum('Cascaded', 'Blocked')

# R
#------------------------------------------------------------------------------

RemovedAction = enum('DoNothing', 'Cascade', 'Block')

# S
#------------------------------------------------------------------------------

SearchMode = enum('Any', 'All')
SelectionMode = enum( 'Normal', 'Count', 'Distinct' )