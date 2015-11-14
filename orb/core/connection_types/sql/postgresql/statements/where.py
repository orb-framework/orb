from projex.lazymodule import lazy_import
from ..psqlconnection import PSQLStatement

orb = lazy_import('orb')


class WHERE(PSQLStatement):
    def __call__(self, model, query):
        return '', {}


PSQLStatement.registerAddon('WHERE', WHERE())
