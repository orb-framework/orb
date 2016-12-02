import demandimport
import logging

from orb.core.column import Column
from orb.utils.enum import enum
from orb.utils.text import safe_eval

with demandimport.enabled():
    import orb

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

    def __init__(self,
                 model='',
                 column='',
                 removeAction=RemoveAction.Block,
                 **kwds):

        # support legacy keyword
        model = kwds.pop('reference', model)

        super(ReferenceColumn, self).__init__(**kwds)

        # store reference options
        self.__reference = model
        self.__reference_column = column
        self.__removeAction = removeAction

    def __json__(self):
        output = super(ReferenceColumn, self).__json__()
        output['reference'] = self.__reference
        output['removeAction'] = self.RemoveAction(self.__removeAction)
        return output

    def _restore(self, value, context=None):
        """
        Restores the value for this column.  If the return type for the context is `values`
        then the ID of the model will be returned.  If the return type for the context is `data`
        then the dict data will be returned for the value.  Otherwise, the value will be
        returned as the reference model type for this column.

        :param value: <variant> or <orb.Model>
        :param value: <orb.Context> or None

        :return: <variant>
        """
        context = context or orb.Context()
        is_model = isinstance(value, orb.Model)

        if value is None:
            return value
        elif context.returning == 'values':
            return value.id() if is_model else value
        elif context.returning == 'data':
            return dict(value) if is_model else value
        elif not is_model:
            model = self.reference_model()
            return model(value, context=context)
        else:
            return value

    def copy(self):
        out = super(ReferenceColumn, self).copy()
        out.__reference = self.__reference
        out.__removeAction = self.__removeAction
        return out

    def database_restore(self, db_value, context=None):
        """
        Re-implements the `orb.Column.database_restore` method.

        Converts the data restored from the backend to a reference instance.

        :param db_value: <variant>
        :param context: <orb.Context>

        :return: <variant>
        """
        # restore a dict record
        if isinstance(db_value, (str, unicode)) and db_value.startswith('{') and db_value.endswith('}'):
            try:
                db_value = safe_eval(db_value)
            except StandardError:
                log.exception('Invalid reference found')
                raise orb.errors.OrbError('Invalid reference found.')

        if isinstance(db_value, dict):
            model = self.reference_model()

            # update the expansion information to not propagate to references
            if context:
                context = context.copy()
                expand = context.expandtree(model)
                sub_expand = expand.pop(self.name(), {})
                context.expand = context.raw_values['expand'] = sub_expand

            # create a new instance of the model
            record = model(db_value, context=context)
            record.mark_loaded()

            return super(ReferenceColumn, self).database_restore(record, context=context)
        else:
            return super(ReferenceColumn, self).database_restore(db_value, context=context)

    def default_field(self):
        """
        Re-implements the `orb.Column.default_field` method to
        define a custom default that includes the `_id` at the end of
        the name.
        """
        field = super(ReferenceColumn, self).default_field()
        if not field.endswith('_id'):
            field += '_id'
        return field

    def loadJSON(self, jdata):
        """
        Loads the given JSON information for this column.

        :param jdata: <dict>
        """
        super(ReferenceColumn, self).loadJSON(jdata)

        # load additional information
        self.__reference = jdata.get('reference') or self.__reference
        self.__removeAction = jdata.get('removeAction') or self.__removeAction

    def reference(self):
        return self.__reference

    def reference_model(self):
        """
        Returns the model that this column references.

        :return     <Table> || None
        """
        return self.schema().system().model(self.__reference)

    def reference_column(self):
        """
        Returns the column that this reference column refers to.  By default,
        it will be the ID column for the reference model.

        :return: <orb.Column>
        """
        model = self.reference_model()
        if self.__reference_column:
            return model.schema().column(self.__reference_column)
        else:
            return model.schema().id_column()

    def restore(self, value, context=None):
        """
        Returns the inflated value state.  This method will match the desired inflated state.

        :param value: <variant>
        :param inflated: <bool>

        :return: <variant>
        """
        context = context or orb.Context()
        value = super(ReferenceColumn, self).restore(value, context=context)

        # check to make sure that we're processing the right values
        if self.test_flag(self.Flags.I18n) and context.locale == 'all':
            return {locale: self._restore(val, context) for locale, val in value.items()}
        else:
            return self._restore(value, context)

    def validate(self, value):
        """
        Re-implements the orb.Column.validate method to verify that the
        reference model type that is used with this column instance is
        the type of value being provided.

        :param value: <variant>

        :return: <bool>
        """
        ref_model = self.reference_model()
        if isinstance(value, orb.Model):
            expected_schema = ref_model.schema().name()
            received_schema = value.schema().name()

            if expected_schema != received_schema:
                raise orb.errors.InvalidReference(self.name(),
                                                  expects=expected_schema,
                                                  received=received_schema)

        return super(ReferenceColumn, self).validate(value)

    def value_from_string(self, value, context=None):
        """
        Re-implements the orb.Column.value_from_string method to
        lookup a reference object based on the given value.

        :param value: <str>
        :param context: <orb.Context> || None

        :return: <orb.Model> || None
        """
        model = self.reference_model()
        return model(value, context=context)

