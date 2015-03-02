"""
Defines the global query building syntax for generating db
agnostic queries quickly and easily.
"""

from projex.enum import enum
from projex.lazymodule import lazy_import

orb = lazy_import('orb')


class QueryAggregate(object):
    Type = enum(
        'Count',
        'Maximum',
        'Minimum',
        'Sum'
    )

    def __init__(self, typ, table, **options):
        self._type = typ
        self._table = table
        self._column = options.get('column', None)
        self._lookupOptions = orb.LookupOptions(**options)
    
    def columns(self):
        """
        Returns the column associated with this aggregate.
        
        :return     [<orb.Column>, ..]
        """
        if self._column:
            if not isinstance(self._column, orb.Column):
                col = self._table.schema().column(self._column)
            else:
                col = self._column
            return col,
        else:
            return self._table.schema().primaryColumns()
    
    def lookupOptions(self):
        """
        Returns the lookup options instance for this aggregate.
        
        :return     <orb.LookupOptions>
        """
        return self._lookupOptions
    
    def table(self):
        """
        Returns the table associated with this aggregate.
        
        :return     <orb.Table>
        """
        return self._table
    
    def type(self):
        """
        Returns the type for this aggregate.
        
        :return     <str>
        """
        return self._type

