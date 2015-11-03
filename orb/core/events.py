class Event(object):
    def __init__(self):
        self.preventDefault = False

class ConnectionEvent(Event):
    def __init__(self, success=True):
        self.success = success

class ChangeEvent(Event):
    def __init__(self, column=None, old=None, value=None):
        self.column = column
        self.old = old
        self.value = value


class CommitEvent(Event):
    def __init__(self, context=None, result=True):
        self.context = context
        self.result = result


class DatabaseLoadEvent(Event):
    def __init__(self, data):
        self.data = data


class RemoveEvent(Event):
    pass
