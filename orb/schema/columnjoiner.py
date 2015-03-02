"""
Defines methods for joining columns within the database system.
"""

from projex.lazymodule import lazy_import

orb = lazy_import('orb')


class ColumnJoiner(object):
    def __init__(self,
                 reference,
                 referenceColumn=None,
                 targetColumn=None,
                 where=None):
        self._reference = reference
        self._referenceColumn = referenceColumn
        self._targetColumn = targetColumn
        self._where = where

    def query(self, column):
        """
        Generates the where query for this joiner for the given column.
        
        :param      column | <orb.Column>
        
        :return     <orb.Query>
        """
        where = self.where(column)
        out = where if where is not None else orb.Query()
        out &= orb.Query(self.referenceColumn()) == orb.Query(column.schema().model())
        return out

    def reference(self):
        """
        Returns the reference model associated with this joiner.
        
        :return     subclass of <orb.Table>
        """
        model = orb.system.model(self._reference)
        if not model:
            raise orb.errors.TableNotFound(self._reference)
        return model

    def referenceColumn(self):
        """
        Returns the reference column associated with this joiner.
        
        :return     <orb.Column>
        """
        ref = orb.system.schema(self._reference)
        if not ref:
            raise orb.errors.TableNotFound(self._reference)

        return ref.column(self._referenceColumn)

    def targetColumn(self):
        """
        Returns the target column for the inputted joiner.
        
        :return     <orb.Column>
        """
        ref = orb.system.schema(self._reference)
        if not ref:
            raise orb.errors.TableNotFound(self._reference)

        return ref.column(self._targetColumn)

    def where(self, column):
        """
        Generates the join logic for the inputted column.
        
        :param      column | <orb.Column>
        
        :return     <orb.Query>
        """
        if callable(self._where):
            return self._where(column)
        return self._where

