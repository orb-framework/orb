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


class CommitEvent(Event):
    def __init__(self, context=None, result=True):
        super(CommitEvent, self).__init__()

        self.context = context
        self.result = result


class LoadEvent(Event):
    def __init__(self, data):
        super(LoadEvent, self).__init__()

        self.data = data


class RemoveEvent(Event):
    pass
