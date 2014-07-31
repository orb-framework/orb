#!/usr/bin/python

""" Defines the backend connection class for Quickbase remote API's. """

# define authorship information
__authors__         = ['Eric Hulser']
__author__          = ','.join(__authors__)
__credits__         = []
__copyright__       = 'Copyright (c) 2011, Projex Software'
__license__         = 'LGPL'

# maintanence information
__maintainer__      = 'Projex Software'
__email__           = 'team@projexsoftware.com'

# define version information (major,minor,maintanence)
__depends__        = ['']
__version_info__   = (0, 0, 0)
__version__        = '%i.%i.%i' % __version_info__


from .quickbase import Quickbase