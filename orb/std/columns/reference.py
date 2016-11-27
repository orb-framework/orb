import demandimport
import logging
import projex.text

from orb.core.column import Column
from orb.core.column_engine import ColumnEngine
from orb.utils.enum import enum

with demandimport.enabled():
    import orb

log = logging.getLogger(__name__)


class ReferenceColumnEngine(ColumnEngine):
    def get_column_type(self, column, plugin_name):
        """
        Re-implements the get_column_type method from `orb.ColumnEngine`.

        This method will apply additional logic for defining the database type for a
        reference based on the id column of the model type that is being referenced
        by the column.

        :param column: <orb.ReferenceColumn>
        :param plugin_name: <str>

        :return: <str>
        """
        # extract the reference model information
        model = column.reference_model()
        schema = model.schema()

        # extract the id column and type
        id_column = schema.id_column()
        id_engine = id_column.get_engine(plugin_name)
        id_type = id_engine.get_column_type(id_column, plugin_name)

        # apply cusotm logic
        if plugin_name == 'Postgres':
            id_type = id_type if id_type != 'SERIAL' else 'BIGINT'
            namespace = id_column.schema().namespace() or 'public'
        elif plugin_name == 'MySQL':
            id_type = id_type.replace('AUTO_INCREMENT', '').strip()
            namespace = id_column.schema().namespace() or 'public'

        # generate the formatting options
        opts = {
            'table': schema.dbname(),
            'field': id_column.field(),
            'id_type': id_type,
            'namespace': namespace
        }

        base_type = super(ReferenceColumnEngine, self).get_column_type(column, plugin_name)
        return base_type.format(**opts)

    def get_api_value(self, column, plugin_name, db_value, context=None):
        """
        Extracts the db_value provided back from the database.

        :param db_value: <variant>
        :param context: <orb.Context>

        :return: <variant>
        """
        if isinstance(db_value, (str, unicode)) and db_value.startswith('{'):
            try:
                db_value = projex.text.safe_eval(db_value)
            except StandardError:
                log.exception('Invalid reference found')
                raise orb.errors.OrbError('Invalid reference found.')

        if isinstance(db_value, dict):
            cls = column.reference_model()
            if not cls:
                raise orb.errors.ModelNotFound(schema=column.reference())
            else:
                # update the expansion information to not propagate to references
                if context:
                    context = context.copy()
                    expand = context.expandtree(cls)
                    sub_expand = expand.pop(column.name(), {})
                    context.expand = context.raw_values['expand'] = sub_expand

                db_value = cls(db_value, context=context)
                db_value.mark_loaded()

        return super(ReferenceColumnEngine, self).get_api_value(column, plugin_name, db_value, context=context)



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
    __default_engine__ = ReferenceColumnEngine(type_map={
        'Postgres': u'{id_type} REFERENCES "{namespace}"."{table}"("{field}")',
        'MySQL': u'{id_type} REFERENCES `{namespace}`.`{table}`(`{field}`)',
        'default': u'{id_type}'
    })

    RemoveAction = enum(
        'DoNothing',    # 1
        'Cascade',      # 2
        'Block'         # 4
    )

    def __init__(self,
                 reference='',
                 removeAction=RemoveAction.Block,
                 **kwds):
        super(ReferenceColumn, self).__init__(**kwds)

        # store reference options
        self.__reference = reference
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

    def random_value(self):
        """
        Returns a random value that fits this column's parameters.

        :return: <variant>
        """
        return self.reference_model().schema().id_column().random_value()

    def reference(self):
        return self.__reference

    def reference_model(self):
        """
        Returns the model that this column references.

        :return     <Table> || None
        """
        model = self.schema().system().model(self.__reference)
        if not model:
            raise orb.errors.ModelNotFound(schema=self.__reference)
        return model

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

