import demandimport
import logging

from orb.utils import json2
from orb.core.column import Column

with demandimport.enabled():
    import orb
    import cPickle
    import yaml

log = logging.getLogger(__name__)


class DataColumn(Column):
    def __init__(self, loader=None, dumper=None, **kw):
        super(DataColumn, self).__init__(**kw)

        # define custom properties
        self.__loader = loader
        self.__dumper = dumper

    def restore_value(self, db_value, context=None):
        """
        Restores the database value from the database.  This method will use
        the loader for this data column to convert the database value back to a
        data value from the loader.

        :param db_value: <unicode> or <str>

        :return: <variant>
        """
        if db_value is None:
            return None
        else:
            try:
                return self.__loader(db_value)
            except Exception:
                raise orb.errors.DataStoreError('Fail to load serialized data')

    def store_value(self, py_value, context=None):
        """
        Stores the python value to the database.  This method will use the
        dumper for this data column to convert the data to a database acceptable
        value.

        :param py_value: <variant>

        :return: <unicode> or <str>
        """
        if py_value is None:
            return None
        else:
            try:
                return self.__dumper(py_value)
            except Exception:
                raise orb.errors.DataStoreError('Failed to serialize data for database')


class BinaryColumn(DataColumn):
    def __init__(self, **kw):
        kw.setdefault('loader', cPickle.loads)
        kw.setdefault('dumper', cPickle.dumps)

        super(BinaryColumn, self).__init__(**kw)


class JSONColumn(DataColumn):
    def __init__(self, **kw):
        kw.setdefault('loader', json2.loads)
        kw.setdefault('dumper', json2.dumps)

        super(JSONColumn, self).__init__(**kw)


class QueryColumn(JSONColumn):
    def restore_value(self, db_value, context=None):
        """
        Restores a query object from the database by restoring
        it's JSON value, and converting the resulting dictionary to
        a Query instance.

        :param db_value: <str> or <unicode>

        :return: <orb.Query> or None
        """
        value = super(QueryColumn, self).restore_value(db_value, context=context)
        if value is not None:
            return orb.Query.load(value)
        else:
            return None


class YAMLColumn(DataColumn):
    def __init__(self, **kw):
        kw.setdefault('loader', yaml.load)
        kw.setdefault('dumper', yaml.dump)

        super(YAMLColumn, self).__init__(**kw)
