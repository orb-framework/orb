import orb


class Callback(object):
    """
    Helper class used to listen for an event and then trigger a call
    to a waiting method
    """
    def __init__(self, method, *args, **kwds):
        self.method = method
        self.args = args
        self.kwds = kwds

    def __call__(self, event):
        return self.method(*self.args, **self.kwds)


class Event(object):
    """ Base class used for propagating events throughout the system """
    def __init__(self):
        self.preventDefault = False


# database events
# ---------------


class DatabaseEvent(Event):
    def __init__(self, database=None):
        super(DatabaseEvent, self).__init__()

        self.database = database


class ConnectionEvent(DatabaseEvent):
    def __init__(self, success=True, native=None, **options):
        super(ConnectionEvent, self).__init__(**options)

        self.success = success
        self.native = native


# model events
# ---------------


class ModelEvent(Event):
    def __init__(self, model=None):
        super(ModelEvent, self).__init__()

        self.model = model


class SyncEvent(ModelEvent):
    def __init__(self, context=None, **options):
        super(SyncEvent, self).__init__(**options)

        self.context = context


# record events
# ---------------


class RecordEvent(Event):
    def __init__(self, record=None):
        super(RecordEvent, self).__init__()

        self.record = record


class ChangeEvent(RecordEvent):
    def __init__(self, column=None, old=None, value=None, **options):
        super(ChangeEvent, self).__init__(**options)

        self.column = column
        self.old = old
        self.value = value

    @property
    def inflated_value(self):
        if isinstance(self.column, orb.ReferenceColumn):
            model = self.column.reference_model()
            if not isinstance(self.value, model):
                return model(self.value)

    @property
    def inflated_old_value(self):
        if isinstance(self.column, orb.ReferenceColumn):
            model = self.column.reference_model()
            if not isinstance(self.old, model):
                return model(self.old)


class DeleteEvent(RecordEvent):
    def __init__(self, context=None, **options):
        super(DeleteEvent, self).__init__(**options)

        self.context = context

    @classmethod
    def process(cls, records, **kw):
        """
        Creates an event per record based on
        the class and keyword arguments.  If
        the event processes without the default
        action, then the record will be yielded.

        :param records: <iterable>
        :param **kw: keywords for event construction

        :return: <generator> (unprevented records)
        """
        for record in records:
            event = cls(**kw)
            record.onDelete(event)
            if not event.preventDefault:
                yield record


class InitEvent(RecordEvent):
    pass


class LoadEvent(RecordEvent):
    def __init__(self, data=None, **options):
        super(LoadEvent, self).__init__(**options)

        self.data = data


class SaveEvent(RecordEvent):
    def __init__(self, context=None, newRecord=False, changes=None, result=True, **options):
        super(SaveEvent, self).__init__(**options)

        self.context = context
        self.newRecord = newRecord
        self.result = result
        self.changes = changes


class PreSaveEvent(SaveEvent):
    pass


class PostSaveEvent(SaveEvent):
    pass

