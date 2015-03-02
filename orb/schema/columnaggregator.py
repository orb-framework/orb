"""
Defines methods for aggregation within the database system.
"""

from projex.lazymodule import lazy_import

orb = lazy_import('orb')


class ColumnAggregator(object):
    def __init__(self,
                 type,
                 reference,
                 referenceColumn=None,
                 targetColumn=None,
                 where=None):
        """
        Defines a new column aggregation mechanism.  This class allows
        defining a column on a schema that generates an <orb.QueryAggregate>
        rather than an actual column in the backend database.
        
        :param      type            | <orb.QueryAggregate.Type>
                    reference       | <str>
                    referenceColumn | <str>
                    targetColumn    | <str>
                    where           | <orb.Query> or None
        """
        self._aggregateType = type
        self._reference = reference
        self._referenceColumn = referenceColumn
        self._targetColumn = targetColumn
        self._where = where
    
    def aggregateType(self):
        """
        Returns the aggregation type for this instance.
        
        :return     <orb.QueryAggregate.Type>
        """
        return self._aggregateType

    def generate(self, column):
        """
        Generates a new <orb.QueryAggregate> for the inputted <orb.Column>.
        
        :param      column  | <orb.Column>
        
        :return     <orb.QueryAggregate>
        """
        return orb.QueryAggregate(self.aggregateType(),
                                  self.reference(),
                                  column=self.referenceColumn(),
                                  where=self.query(column))

    def query(self, column):
        """
        Generates an <orb.Query> instance to use for the where clause
        for an <orb.QueryAggregate>.
        
        :return     <orb.Query>
        """
        out = orb.Query()
        
        # create the reference query
        ref_col = self.referenceColumn()
        if ref_col:
            source = column.schema().model()
            out &= orb.Query(ref_col) == orb.Query(source)
        
        return out & self.where(column)

    def reference(self):
        """
        Returns the reference table that is associated with this aggregation.
        
        :return     <orb.Table> || None
        """
        table = orb.system.model(self._reference)
        if not table:
            raise orb.errors.TableNotFound(self._reference)
        return table

    def referenceColumn(self):
        """
        Returns the reference column that is associated with this aggregation.
        
        :return     <orb.Column> || None
        """
        if self._reference and self._referenceColumn:
            ref = orb.system.schema(self._reference)
            if ref:
                return ref.column(self._referenceColumn)
            else:
                raise orb.errors.TableNotFound(self._reference)
        else:
            return None

    def targetColumn(self):
        """
        Returns the target column that is used as the target for this
        aggregation.
        
        :return     <orb.Column> || None
        """
        if self._reference and self._targetColumn:
            ref = orb.system.schema(self._reference)
            if ref:
                return ref.column(self._targetColumn)
            else:
                raise orb.errors.TableNotFound(self._reference)
        else:
            return None

    def where(self, column):
        """
        Returns an <orb.Query> for the inputted column.
        
        :param      column | <orb.Column>
        
        :return     <orb.Query> || None
        """
        if callable(self._where):
            return self._where(column)
        return self._where

