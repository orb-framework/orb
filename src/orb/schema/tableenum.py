""" 
Defines a class for generating enumerated types based on table indexes.
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

import logging

from projex.lazymodule import LazyModule

log = logging.getLogger(__name__)
orb = LazyModule('orb')


class TableEnum(object):
    def __init__( self, modelName, index, cached = True ):
        """
        Initializes the enumeration by providing a table name and index
        name for the table.
        
        :param      modelName | <str>
                    index     | <str>
                    cached    | <bool>
        """
        self._model     = None
        self._modelName = modelName
        self._index     = index
        self._cached    = cached
        self._cache     = {}
    
    def __getattr__(self, key):
        """
        Retrieves a record based on the inputed key.  This would be a value
        normally passed to the defined index for this enum.
        
        :param      key | <str>
        
        :return     <Table> || None
        """
        if key in self._cache:
            return self._cache
        
        model = self.model()
        if model:
            if hasattr(model, self._index):
                record = getattr(model, self._index)(key)
                if self._cached:
                    self._cache[key] = record
                return record
            else:
                msg  = '{0} is not an index of {1}.'
                log.error(msg.format(self._index, self._modelName))
        else:
            log.error('{0} is not a valid model.'.format(self._modelName))
        
        return None
    
    def model(self):
        """
        Returns the model associated with this enum.
        
        :return     <subclass of Table>
        """
        if self._model is None:
            self._model = orb.system.model(self._modelName)
        return self._model