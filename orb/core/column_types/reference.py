import demandimport
import logging
import projex.text

from ..column import Column
from ...utils.enum import enum

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

    def __json__(self):
        output = super(ReferenceColumn, self).__json__()
        output['reference'] = self.__reference
        output['removeAction'] = self.RemoveAction(self.__removeAction)
        return output

    def __init__(self,
                 reference='',
                 removeAction=RemoveAction.Block,
                 **kwds):
        super(ReferenceColumn, self).__init__(**kwds)

        # store reference options
        self.__reference = reference
        self.__removeAction = removeAction

    def _restore(self, value, context=None):
        if not context.inflated and isinstance(value, orb.Model):
            return value.id()

        elif context.inflated and value is not None:
            model = self.reference_model()

            if not isinstance(value, orb.Model):
                return model.fetch(value, context=context)
            else:
                return value

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

    def dbType(self, connectionType):
        model = self.reference_model()
        namespace = model.schema().namespace()
        dbname = model.schema().dbname()
        id_column = model.schema().id_column()

        if connectionType == 'Postgres':
            typ = id_column.dbType(connectionType)
            if typ == 'SERIAL':
                typ = 'BIGINT'
            return '{0} REFERENCES "{1}"."{2}"'.format(typ, namespace or 'public', dbname)
        elif connectionType == 'SQLite':
            return id_column.dbType(connectionType)
        elif connectionType == 'MySQL':
            typ = id_column.dbType(connectionType).replace('AUTO_INCREMENT', '').strip()
            return '{0} REFERENCES `{1}`.`{2}`'.format(typ, namespace, dbname)
        else:
            return ''

    def dbRestore(self, db_value, context=None):
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
            cls = self.reference_model()
            if not cls:
                raise orb.errors.ModelNotFound(schema=self.reference())
            else:
                load_event = orb.events.LoadEvent(data=db_value)

                # update the expansion information to not propagate to references
                if context:
                    context = context.copy()
                    expand = context.expandtree(cls)
                    sub_expand = expand.pop(self.name(), {})
                    context.expand = context.raw_values['expand'] = sub_expand

                db_value = cls(loadEvent=load_event, context=context)

        return super(ReferenceColumn, self).dbRestore(db_value, context=context)

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

