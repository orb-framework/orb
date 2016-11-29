"""
Defines the ReverseLookup class, one of the standard Collectors.  A reverse lookup creates
a one-to-many relationship between models by generating a collection of records using a
lookup to a related model via references.
"""

import demandimport

from .collector import Collector
from ..utils.enum import enum

with demandimport.enabled():
    import orb


class ReverseLookup(Collector):
    RemoveAction = enum(
        'Unset',
        'Delete'
    )

    def __init__(self,
                 path='',
                 model=None,
                 column=None,
                 remove_action='Unset',
                 **options):
        if path:
            model, column = path.split('.', 1)

        options['model'] = model

        super(ReverseLookup, self).__init__(**options)

        # custom options
        if isinstance(remove_action, (str, unicode)):
            remove_action = ReverseLookup.RemoveAction(remove_action)

        self.__remove_action = remove_action
        self.__column = column

    def __json__(self):
        """
        Serializes the reverse lookup to a json object.

        :return: <dict>
        """
        output = super(ReverseLookup, self).__json__()
        output['target'] = self.column().alias()
        return output

    def add_record(self, source_record, target_record, **context):
        """
        Adds a record to this collector's reference set.  For a reverse
        lookup, this will set the `column` on the given target record
        and save it to the backend.

        :param source_record: <orb.Model>
        :param target_record: <orb.Model>
        :param context: <orb.Context> descriptor

        :return: <orb.Model> generated relationship
        """
        model = self.model()
        if not isinstance(target_record, model):
            msg = '{0}.add_record expected {1} got {2}'.format(self.name(),
                                                               model.schema().name(),
                                                               target_record)
            raise orb.errors.ValidationError(msg)
        else:
            target_record.set(self.column(), source_record)
            target_record.save()
            return target_record

    def create_record(self, source_record, values, **context):
        """
        Creates a new record for this lookup.  This will generate a new record
        of the `model` that is associated with this collector, setting it's
        `column` property to the given source record.

        :param source_record: <orb.Model>
        :param values: <dict> or <orb.Model>
        :param context: <orb.Context> descriptor

        :return: <orb.Model>
        """
        if type(values) is dict:
            values.setdefault(self.column().name(), source_record)
            return self.model().create(values, **context)
        else:
            return self.add_record(source_record, values, **context)

    def collect(self, source_record, **context):
        """
        Creates a new collection of target `model`'s that are referencing the
        source_record through the `column` defined for this collector.

        :param source_record: <orb.Model>
        :param context: <orb.Context> descriptor

        :return: <orb.Collection>
        """
        if not source_record.id():
            return orb.Collection()
        else:
            q  = orb.Query(self.column()) == source_record
            context['where'] = q & context.get('where')
            records = self.model().select(**context)
            return records.bind_collector(self).bind_source_record(source_record)

    def collect_expand(self, query, tree, **context):
        """
        Creates the query to select this collector's records within a model lookup.

        :param query: <orb.Query>
        :param tree: <list>
        :param context: <orb.Context> descriptor

        :return: <orb.Collection
        """
        model = self.model()
        sub_q = query.copy(
            model=self.model(),
            schema_object='.'.join(tree[1:])
        )
        return model.select(columns=[self.column()], where=sub_q)

    def column(self, **context):
        """
        Returns the column object that this reverse lookup will reference from
        it's related model.

        :return: <orb.Column>
        """
        schema = self.model().schema()
        return schema.column(self.__column)

    def copy(self, **kw):
        """
        Creates a copy of this reverse lookup.

        :return: <orb.ReverseLookup>
        """
        kw.setdefault('column', self.__column)
        kw.setdefault('remove_action', self.__remove_action)
        return super(ReverseLookup, self).copy(**kw)

    def delete_records(self, source_record, collection, **context):
        """
        Deletes records from this collection based on the `remove_action`.  If
        set to RemoveAction.Delete then the records in the collection will
        be removed from the backend.  If set to RemoveAction.Unset (default)
        then the records will de-reference the source model and update the backend.

        :param source_record: <orb.Model>
        :param collection: <orb.Collection> or [<orb.Model>, ..]
        :param context: <orb.Context> descriptor

        :return: <int> number of records removed
        """
        if not source_record.id():
            return 0
        else:
            model = self.model()
            column = self.column()
            changed = set()
            for record in collection:
                if isinstance(record, model) and record.get(column) == source_record:
                    record.set(column, None)
                    changed.add(record)

            if changed and self.remove_action() == ReverseLookup.RemoveAction.Unset:
                orb.Collection(list(changed)).save()
                return len(changed)
            elif changed:
                return orb.Collection(list(changed)).delete()
            else:
                return 0

    def remove_action(self):
        """
        Defines the action that should be taken when a model is removed from the collection generated
        by this reverse lookup.  The default action is RemoveAction.Unset, which will simply de-reference the source
        model from the target.  If you set the action to RemoveAction.Delete, then the target model will be
        fully deleted from the backend when being removed.

        :return:  <ReverseLookup.RemoveAction>
        """
        return self.__remove_action

    def remove_record(self, source_record, target_record, **context):
        """
        Removes the target record from the collection.  Depending on the `remove_action` that is defined,
        the behavior of this method will change.  For a collector whose `remove_action` is set to `Unset` (default),
        this method will update the `column` for the `target_record` and set it to None, saving the record
        to the backend.  If the `remove_action` is set to `Delete`, then removing the record will completely delete
        it from the backend.

        :param source_record: <orb.Model>
        :param target_record: <orb.Model>
        :param context: <orb.Context> descriptor

        :return: <int> number of records removed
        """
        if not isinstance(target_record, self.model()):
            return 0
        elif self.remove_action() == ReverseLookup.RemoveAction.Delete:
            return int(target_record.delete())
        else:
            target_record.set(self.column(), None)
            target_record.save()
            return 1

    def set_remove_action(self, action):
        """
        Sets the remove action that should be taken when a model is removed from the collection generated by
        this reverse lookup.  Valid actions are "unset" or "delete", any other values will raise an exception.

        :param action: <ReverseLookup.RemoveAction> or <str>
        """
        if isinstance(action, (str, unicode)):
            action = ReverseLookup.RemoveAction(action)
        self.__remove_action = action

    def update_records(self, source_record, records, **context):
        """
        Updates the records in this collection by updating the `column` and setting it to the `source_record`
        for each record in the collection.  This method will also remove any reords that are _not_ in the
        collection, according to the `remove_action` defined for the collector.

        :param source_record: <orb.Model>
        :param records: <orb.Collection> or [<orb.Model>, ..]
        :param context: <orb.Context> descriptor

        :return: None
        """
        orb_context = orb.Context(**context)
        target_column = self.column()
        target_model = target_column.schema().model()

        # create links to the reverse lookup instance
        collection_ids = []
        for record in records:
            if record.is_record():
                collection_ids.append(record.get('id'))

            record.set(target_column, source_record)
            if source_record.is_record():
                record.save()
            else:
                record.save_after(source_record)

        # delete old links if necessary
        if source_record.is_record() and collection_ids:
            q = orb.Query(target_column) == source_record
            q &= orb.Query(target_model).not_in(collection_ids)

            # determine the records to remove from this collection
            remove_records = target_model.select(where=q, context=orb_context)

            # check the remove action to determine how to handle this situation
            if self.remove_action() == ReverseLookup.RemoveAction.Delete:
                remove_records.delete()
            else:
                for record in remove_records:
                    record.set(target_column, None)
                    record.save()
