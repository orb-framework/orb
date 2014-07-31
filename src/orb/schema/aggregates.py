#!/usr/bin/python

""" 
Defines methods for aggregation within the database system.
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

from projex.lazymodule import LazyModule

orb = LazyModule('orb')


class Aggregator(object):
    def __init__(self, table):
        self._table = table
    
    def __call__(self, column):
        raise NotImplementedError
    
    def table(self):
        """
        Returns the table associated with this aggregate.
        
        :return     <subclass of orb.Table>
        """
        return self._table

#----------------------------------------------------------------------

class Counter(Aggregator):
    """ Defines a counting aggregate for a column. """
    def __init__(self,
                 table,
                 reference=None,
                 referenceColumn=None,
                 where=None):
        super(Counter, self).__init__(table)
        
        if reference is None:
            reference = table
        
        self._reference = reference
        self._referenceColumn = referenceColumn
        self._where = where

    def __call__(self, column):
        return orb.Query.COUNT(self.reference(), where=self.where())

    def reference(self):
        """
        Returns the reference table for this counter.
        
        :return     <orb.Table> || None
        """
        if isinstance(self._reference, basestring):
            return orb.system.model(self._reference)
        
        return self._reference

    def where(self):
        """
        Returns the where for this counter.
        
        :return     <orb.Query> || None
        """
        if self._where is None and self._referenceColumn:
            query = orb.Query(self.reference(), self._referenceColumn)
            return query == orb.Query(self.table())
        return self._where

