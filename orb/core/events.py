import orb


class Callback(object):
    """
    Helper class used to listen for an event and then trigger a call
    to a waiting method
    """
    def __init__(self, method, sender=None, signal=None, args=None, kwargs=None, single_shot=True):
        self.signal = signal
        self.method = method
        self.args = args or tuple()
        self.kwds = kwargs or {}
        self.single_shot = single_shot
        self.sender = sender

        # enable the callback
        self.enable()

    def __call__(self, sender, event=None):
        result = self.method(*self.args, **self.kwds)

        # disconnect the connection from the signal
        if self.single_shot:
            self.disable()

        return result

    def enable(self):
        """
        Enables this callback to listen to triggers from the event signal.
        """
        self.signal.connect(self, sender=self.sender, weak=False)

    def disable(self):
        """
        Disables this callback from the event signal.
        """
        self.signal.disconnect(self)


class Event(object):
    """ Base class used for propagating events throughout the system """
    def __init__(self):
        self.prevent_default = False


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
    def __init__(self, changes=None, **options):
        super(ChangeEvent, self).__init__(**options)

        # store the changes
        self.changes = changes

    @property
    def inflated_changes(self):
        output = {}
        for col, (old_value, new_value) in self.changes.items():
            if isinstance(col, orb.ReferenceColumn) and not isinstance(old_value, orb.Model):
                reference_model = col.reference_model()
                old_value = reference_model(old_value)

            if isinstance(col, orb.ReferenceColumn) and not isinstance(new_value, orb.Model):
                reference_model = col.reference_model()
                new_value = reference_model(new_value)

            output[col] = (old_value, new_value)

        return output


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
            record.on_delete(event)
            if not event.prevent_default:
                yield record


class InitEvent(RecordEvent):
    pass


class SaveEvent(RecordEvent):
    def __init__(self,
                 context=None,
                 new_record=False,
                 changes=None,
                 result=True,
                 **options):
        super(SaveEvent, self).__init__(**options)

        self.context = context
        self.new_record = new_record
        self.result = result
        self.changes = changes

