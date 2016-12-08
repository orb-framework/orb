import demandimport
import logging
import json

from orb.core.column import Column
from orb.utils.enum import enum

with demandimport.enabled():
    import orb

log = logging.getLogger(__name__)


class ReferenceColumn(Column):
    RemoveAction = enum(
        'DoNothing',    # 1
        'Cascade',      # 2
        'Block'         # 4
    )

    def __init__(self,
                 model='',
                 column='',
                 remove_action=1,  # do nothing
                 **kw):

        # support legacy keyword
        model = kw.pop('reference', None) or model

        super(ReferenceColumn, self).__init__(**kw)

        # define custom properties
        self.__reference = model
        self.__reference_column = column
        self.__remove_action = remove_action

    def copy(self, **kw):
        """
        Creates a copy of this reference column with the custom
        properties.

        Args:
            **kw: <dict>

        Returns:
            <orb.ReferenceColumn>

        """
        kw.setdefault('model', self.__reference)
        kw.setdefault('column', self.__reference_column)
        kw.setdefault('remove_action', self.__remove_action)
        return super(ReferenceColumn, self).copy(**kw)

    def default_alias(self):
        """
        Re-implements the `orb.Column.default_alias` method to
        define a custom default that includes the `_id` at the end of
        the name.
        """
        alias = super(ReferenceColumn, self).default_alias()
        if not alias.endswith('_id'):
            alias += '_id'
        return alias

    def reference(self):
        """
        Returns the reference name for this column.

        Returns:
            <str>

        """
        try:
            is_model = issubclass(self.__reference, orb.Model)
        except Exception:
            is_model = False

        return self.__reference.schema().name() if is_model else self.__reference

    def reference_model(self):
        """
        Returns the model that this column references.

        Returns:
            <orb.Table> or None
        """
        try:
            is_model = issubclass(self.__reference, orb.Model)
        except Exception:
            is_model = False

        return self.__reference if is_model else self.schema().system().model(self.__reference)

    def reference_column(self):
        """
        Returns the column that this reference column refers to.  By default,
        it will be the ID column for the reference model.

        Returns:
            <orb.Column>
        """
        model = self.reference_model()
        if self.__reference_column:
            return model.schema().column(self.__reference_column)
        else:
            return model.schema().id_column()

    def remove_action(self):
        """
        Returns the setting for how to handle when a record with this pointer
        is removed.

        Returns:
            <orb.ReferenceColumn.RemoveAction>

        """
        return self.__remove_action

    def restore(self, value, context=None):
        """
        Returns the inflated value state.  This method will match the desired inflated state.

        Args:
            value: <variant>
            context: <orb.Context> or None

        Returns:
            <variant>

        """
        context = context or orb.Context()

        # restore json data
        if isinstance(value, (str, unicode)) and value.startswith('{') and value.endswith('}'):
            try:
                value = json.loads(value)
            except Exception:  # pragma: no cover
                pass

        # check to make sure that we're processing the right values
        if self.test_flag(self.Flags.I18n):
            if type(value) is dict and context.locale == 'all':
                return {locale: self.restore_value(val, context) for locale, val in value.items()}
            else:
                return self.restore_value(value.get(context.locale), context)
        else:
            return self.restore_value(value, context)

    def restore_value(self, value, context=None):
        """
        Restores the value for this column.  If the return type for the context is `values`
        then the ID of the model will be returned.  If the return type for the context is `data`
        then the dict data will be returned for the value.  Otherwise, the value will be
        returned as the reference model type for this column.

        Args:
            value: <variant> or <orb.Model>
            context: <orb.Context> or None

        Returns:
            <variant>

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

            if context:
                context = context.copy()
                expand = context.expandtree(model)
                sub_expand = expand.pop(self.name(), {})
                context.expand = context.raw_values['expand'] = sub_expand

            record = model(value, context=context)
            record.mark_loaded()
            return record
        else:
            return value

    def set_remove_action(self, action):
        """
        Sets the setting for how to handle when a record with this pointer
        is removed.

        Args:
            action: <orb.ReferenceColumn.RemoveAction>

        """
        self.__remove_action = action

    def validate(self, value):
        """
        Re-implements the orb.Column.validate method to verify that the
        reference model type that is used with this column instance is
        the type of value being provided.

        Args:
            value: <variant>

        Raises:
            <orb.errors.InvalidReference> column when the given value is not a valid
            record based on the reference model for this instance

        Returns:
            <bool>
        """
        ref_model = self.reference_model()
        if isinstance(value, orb.Model):
            value_model = type(value)
            valid_model = value_model is ref_model or issubclass(value_model, ref_model)
            if not valid_model:
                raise orb.errors.InvalidReference(self.name(),
                                                  expects=ref_model,
                                                  received=value_model)

        return super(ReferenceColumn, self).validate(value)

    def value_from_string(self, value, context=None):
        """
        Re-implements the orb.Column.value_from_string method to
        lookup a reference object based on the given value.

        Args:
            value: <str> or <unicode>
            context: <orb.Context> or None

        Returns:
            <orb.Model>
        """
        if value is None:
            return value
        else:
            if value.startswith('{') and value.endswith('}'):
                try:
                    value = json.loads(value)
                except Exception:  # pragma: no cover
                    pass

            model = self.reference_model()
            return model(value, context=context)

