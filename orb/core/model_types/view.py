from ..model import Model
from ... import errors


class View(Model):
    __orb__ = {'bypass': True}

    def commit(self, **context):
        raise errors.OrbError('View models are read-only.')

    def delete(self, **context):
        raise errors.OrbError('View models are read-only.')


Model.registerAddon('View', View)