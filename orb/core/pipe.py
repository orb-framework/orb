"""
Defines a many-to-many relationship within an ORB system.  The
Pipe collector will lookup up records through an intermediary
model.
"""

import demandimport

from .collector import Collector
with demandimport.enabled():
    import orb


class Pipe(Collector):
    def __init__(self,
                 path='',
                 through_model=None,
                 from_column=None,
                 to_column=None,
                 **options):
        super(Pipe, self).__init__(**options)

        if path:
            through_model, from_column, to_column = path.split('.')

        self.__through_model = through_model
        self.__from_column = from_column
        self.__to_column = to_column

    def __json__(self):
        """
        Returns a representation of this Pipe instance as a JSON
        dictionary.

        :return: <dict>
        """
        output = super(Pipe, self).__json__()
        output['through'] = self.through_model().schema().name()
        output['from'] = self.from_column().alias()
        output['to'] = self.to_column().alias()
        output['model'] = self.model().schema().name()
        return output

    def add_record(self, source_record, target_record, **context):
        """
        Adds a new record to this Pipe object.  This method will create
        a new instance of the `through_model` intermediary model type,
        with the from and to columns set to the source and target
        records.

        :param source_record: <orb.Model>
        :param target_record: <orb.Model>
        :param context: <orb.Context> descriptor

        :return: <orb.Model> instance of `through_model`
        """
        through_model = self.through_model()
        attrs = {
            self.from_column(): source_record,
            self.to_column(): target_record
        }
        return through_model.ensure_exists(attrs, **context)

    def create_record(self, source_record, values, **context):
        """
        Creates a new record for this pipe object.  If the given values
        are a dictionary, a new instance of the target `model` type
        will NOT be created for a Pipe.  This is to ensure accidental generation
        of target models is not done.

        :param source_record: <orb.Model>
        :param values: <dict>
        :param context: <orb.Context> descriptor

        :return: <orb.Model> instance of `model`
        """
        model = self.model()
        to_col = self.to_column()
        orb_context = orb.Context(**context)

        # add the target based on the name or field
        if to_col.name() in values:
            target_record = values.get(to_col.name())
        elif to_col.field() in values:
            target_record = values.get(to_col.field())
        elif to_col.alias() in values:
            target_record = values.get(to_col.alias())
        else:
            target_record = values

        # ensure the record exists as a model
        if not isinstance(target_record, model):
            target_record = model(target_record, context=orb_context)

        # create the record
        self.add_record(source_record, target_record)

        return target_record

    def collect(self, source_record, **context):
        """
        Collects models of the `model` type that are associated
        with the given `source_record` instance through this pipe.

        :param source_record: <orb.Model>
        :param context: <orb.Context> descriptor
        """
        if not source_record.id():
            return orb.Collection()
        else:
            target = self.model()
            through = self.through_model()

            # create the pipe query
            q  = orb.Query(target) == orb.Query(through, self.to_column())
            q &= orb.Query(through, self.from_column()) == source_record

            context['where'] = q & context.get('where')

            # generate the pipe query for this record
            records = target.select(**context)
            return records.bind_collector(self).bind_source_record(source_record)

    def collect_expand(self, query, tree, **context):
        """
        Creates the expansion query for this collector.  This method will be
        invoked from the backend system when including this pipe during a lookup.

        :param query: <orb.Query>
        :param tree: [<str>, ..]

        :return: <orb.Collection>
        """
        through_model = self.through_model()
        target_model = self.model()

        sub_q = query.copy(
            column='.'.join(tree[1:]),
            model=target_model
        )
        to_records = target_model.select(columns=[target_model.schema().id_column()], where=sub_q)
        pipe_q = orb.Query(through_model, self.to_column()).in_(to_records)
        return through_model.select(columns=[self.from_column()], where=pipe_q)

    def copy(self, **kw):
        """
        Defines a copy of this pipe.

        :param kw: <orb.Context> descriptor

        :return: <orb.Collector>
        """
        kw.setdefault('through_model', self.__through_model)
        kw.setdefault('to_column', self.__to_column)
        kw.setdefault('from_column', self.__from_column)

        return super(Pipe, self).copy(**kw)

    def delete_records(self, source_record, collection, **context):
        """
        Deletes any records from the `through_model` for this pipe given a list
        of target models.

        :param source_record: <orb.Model>
        :param collection: <orb.Collection> or [<orb.Model> model, ..]
        :param context: <orb.Context> descriptor

        :return: <int> number of records removed
        """
        through = self.through_model()

        # remove them from the system
        orb_context = orb.Context(**context)

        q = orb.Query(self.from_column()) == source_record
        q &= orb.Query(self.to_column()).in_(collection)
        context['where'] = q & context.get('where')

        # lookup the records that are about to be removed
        # in order to run their deletion event
        records = through.select(**context)

        delete_records = list(orb.events.DeleteEvent.process(records))

        # delete the records from the connection
        if delete_records:
            conn = orb_context.db.connection()
            count = conn.delete(delete_records, orb_context)[1]
        else:
            count = 0

        return count

    def from_column(self, **context):
        """
        Returns the source record's column definition for this Pipe instance.
        The `from_column` will be the first part of the lookup - where the
        record is coming from before passing through to the target model.

        :param context: <orb.Context> descriptor

        :return: <orb.Column>
        """
        through = self.through_model()
        return through.schema().column(self.__from_column)

    def from_model(self):
        """
        Returns the reference model that is associatd with the `from_column`
        for this Pipe.  This is the reference column that points to the
        model type for the source of the many-to-many relationship.

        :return: <orb.Model>
        """
        col = self.from_column()
        return col.reference_model() if col else None

    def model(self):
        """
        Returns the target model that this Pipe will terminate at.  The
        model type returned from this method will be the type of records
        that are returned from the `collect` method.

        :return: <orb.Model>
        """
        col = self.to_column()
        return col.reference_model() if col else None

    def remove_record(self, source_record, target_record, **context):
        """
        Removes a record from the backend by removing the intermediary
        model that maps to the given source and target records.

        :param source_record: <orb.Model>
        :param target_record: <orb.Model>
        :param context: <orb.Context> descriptor

        :return: <int> number of records removed
        """
        through = self.through_model()
        q = orb.Query(self.from_column()) == source_record
        q &= orb.Query(self.to_column()) == target_record

        context['where'] = q

        orb_context = orb.Context(**context)
        records = through.select(context=orb_context)
        delete_records = list(orb.events.DeleteEvent.process(records))

        if delete_records:
            conn = orb_context.db.connection()
            conn.delete(delete_records, orb_context)
            return len(delete_records)
        else:
            return 0

    def to_column(self, **context):
        """
        Returns the column that map to the target model for this
        pipe.

        :param context: <orb.Context> descriptor

        :return: <orb.Column>
        """
        model = self.through_model()
        return model.schema().column(self.__to_column)

    def through_model(self, **context):
        """
        Returns the model that will act as the intermediary model
        for this pipe.

        :return: <orb.Model>
        """
        schema = self.schema()
        system = schema.system() if schema else orb.Context(**context).system
        return system.model(self.__through_model)

    def update_records(self, source_record, records, **context):
        """
        Updates the records associated with this collector.  This method
        will set the records mapped through the intermediary model to
        the given list of target models.

        :param source_record: <orb.Model>
        :param records: <orb.Collection> or [<orb.Model>, ..]
        :param context: <orb.Context> descriptor
        """
        orb_context = orb.Context(**context)

        through = self.through_model()
        new_ids = [record.id() for record in records]

        from_column = self.from_column()
        to_column = self.to_column()

        # remove existing records
        if source_record.is_record():
            remove_ids = orb.Query(through, from_column) == source_record
            remove_ids &= orb.Query(through, to_column).notIn(new_ids)
            orb_context.where = remove_ids
            through.select(context=orb_context).delete()

            for record in records:
                through.ensure_exists({
                    from_column: source_record,
                    to_column: record
                })

        # create new records
        else:
            for record in records:
                through_record = through({
                    from_column: source_record,
                    to_column: record
                })
                through_record.save_after(source_record)

