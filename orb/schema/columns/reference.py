from projex.lazymodule import lazy_import
from projex.enum import enum
from ..column import Column

orb = lazy_import('orb')


class ReferenceColumn(Column):
    """
    The ReferenceColumn class type will allow for relational references between models.


    Usage
    ----

        import orb

        class Comment(orb.Table):
            created_by = orb.RelationColumn(reference='User',
                                            reverse=orb.ReferenceColumn.Reversed(name='commments'))

    """
    RemoveAction = enum(
        'DoNothing',
        'Cascade',
        'Block'
    )

    class Reversed(object):
        def __init__(self, name='', cached=False, timeout=None):
            self.name = name
            self.cached = cached
            self.timeout = timeout

    def __init__(self,
                 reference='',
                 removeAction=RemoveAction.Block,
                 reverse=None,
                 **kwds):
        super(ReferenceColumn, self).__init__(**kwds)

        # store reference options
        self._reference = reference
        self._referenceModel = None
        self._removeAction = removeAction
        self._reverse = reverse

    def referenceModel(self):
        """
        Returns the model that this column references.

        :return     <Table> || None
        """
        dbname = self.schema().databaseName() or None
        model = orb.system.model(self.reference(), database=dbname)
        if not model:
            raise orb.errors.TableNotFound(self.reference())
        return model

    def reverseInfo(self):
        """
        Returns the reversal information for this column type, if any.

        :return     <orb.ReferenceColumn.Reversed> || None
        """
        return self._reverse

# register the column addon
Column.registerAddon('Reference', ReferenceColumn)