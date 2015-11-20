from projex.lazymodule import lazy_import
from ..model import Model

orb = lazy_import('orb')


class View(Model):
    __orb__ = {'bypass': True}

    def delete(self, **context):
        raise orb.errors.OrbError('View models are read-only.')

    def save(self, **context):
        raise orb.errors.OrbError('View models are read-only.')

