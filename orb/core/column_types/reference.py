import logging
import projex.text

from projex.lazymodule import lazy_import
from projex.enum import enum
from ..column import Column

orb = lazy_import('orb')
log = logging.getLogger(__name__)


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
        'DoNothing',    # 1
        'Cascade',      # 2
        'Block'         # 4
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
        self.__reference = reference
        self.__removeAction = removeAction
        self.__reverse = reverse

    def dbType(self, connectionType):
        if connectionType == 'Postgres':
            model = self.referenceModel()
            return 'BIGINT REFERENCES "{0}"'.format(model.schema().dbname())
        else:
            return ''

    def extract(self, value, context=None):
        """
        Extracts the value provided back from the database.

        :param value: <variant>
        :param context: <orb.Context>

        :return: <variant>
        """
        if isinstance(value, (str, unicode)) and value.startswith('{'):
            try:
                value = projex.text.safe_eval(value)
            except StandardError:
                log.exception('Invalid reference found')
                raise orb.errors.OrbError('Invalid reference found.')

        if isinstance(value, dict):
            cls = self.referenceModel()
            if not cls:
                raise orb.errors.ModelNotFound(self.reference())
            else:
                load_event = orb.events.LoadEvent(data=value)
                value = cls(context=context)
                value.onLoad(load_event)
            return value
        else:
            return super(ReferenceColumn, self).extract(value, context=context)

    def loadJSON(self, jdata):
        """
        Loads the given JSON information for this column.

        :param jdata: <dict>
        """
        super(ReferenceColumn, self).loadJSON(jdata)

        # load additional information
        self.__reference = jdata.get('reference') or self.__reference
        self.__removeAction = jdata.get('removeAction') or self.__removeAction

        reverse = jdata.get('reverse')
        if reverse:
            self.__reverse = ReferenceColumn.Reversed(**reverse)

    def reference(self):
        return self.__reference

    def referenceModel(self):
        """
        Returns the model that this column references.

        :return     <Table> || None
        """
        model = orb.system.model(self.__reference)
        if not model:
            raise orb.errors.ModelNotFound(self.__reference)
        return model

    def restore(self, value, context=None):
        """
        Returns the inflated value state.  This method will match the desired inflated state.

        :param value: <variant>
        :param inflated: <bool>

        :return: <variant>
        """
        context = context or orb.Context()

        if not context.inflated and isinstance(value, orb.Model):
            return value.id()
        elif context.inflated and value is not None:
            return self.referenceModel().fetch(value)
        else:
            return value

    def reverseInfo(self):
        """
        Returns the reversal information for this column type, if any.

        :return     <orb.ReferenceColumn.Reversed> || None
        """
        return self.__reverse

# register the column addon
Column.registerAddon('Reference', ReferenceColumn)