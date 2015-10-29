class Event(object):
    def __init__(self):
        self.preventDefault = False


class DatabaseLoadEvent(Event):
    def __init__(self, data):
        self.data = data


class PreCommitEvent(Event):
    pass


class PreRemoveEvent(Event):
    pass


class PostCommitEven(Event):
    pass

class PostRemoveEvent(Event):
    pass