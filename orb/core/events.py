import orb


class Event(object):
    def __init__(self):
        self.preventDefault = False

class ConnectionEvent(Event):
    def __init__(self, success=True, native=None):
        super(ConnectionEvent, self).__init__()

        self.success = success
        self.native = native

class ChangeEvent(Event):
    def __init__(self, column=None, old=None, value=None):
        super(ChangeEvent, self).__init__()

        self.column = column
        self.old = old
        self.value = value

    @property
    def inflated_value(self):
        if isinstance(self.column, orb.ReferenceColumn):
            model = self.column.referenceModel()
            if not isinstance(self.value, model):
                return model(self.value)

    @property
    def inflated_old_value(self):
        if isinstance(self.column, orb.ReferenceColumn):
            model = self.column.referenceModel()
            if not isinstance(self.old, model):
                return model(self.old)

class InitEvent(Event):
    pass


class SaveEvent(Event):
    def __init__(self, context=None, newRecord=False, changes=None, result=True):
        super(SaveEvent, self).__init__()

        self.context = context
        self.newRecord = newRecord
        self.result = result
        self.changes = changes

class DeleteEvent(Event):
    def __init__(self, context=None):
        super(DeleteEvent, self).__init__()

        self.context = context

class LoadEvent(Event):
    def __init__(self, data):
        super(LoadEvent, self).__init__()

        self.data = data


class SyncEvent(Event):
    def __init__(self, context=None):
        super(SyncEvent, self).__init__()

        self.context = context