import cPickle
import demandimport
import logging
import os
import projex.text
import yaml
import json

from projex import rest

from orb.core.column import Column
from orb.core.column_engine import ColumnEngine

with demandimport.enabled():
    import orb
    import random

log = logging.getLogger(__name__)


class DataColumnEngine(ColumnEngine):
    def __init__(self, serializer, loader='loads', dumper='dumps', type_map=None):
        super(DataColumnEngine, self).__init__(type_map=type_map)

        # custom properties
        self.__serializer = serializer
        self.__loader = loader
        self.__dumper = dumper

    def get_api_value(self, column, plugin_name, db_value, context=None):
        """
        Re-implements the get_api_value method from ColumnEngine.

        :param column: <orb.Column>
        :param plugin_name: <str>
        :param db_value: <variant>
        :param context: <orb.Context>

        :return: <variant> python value
        """
        if db_value is None:
            return None
        else:
            loader = getattr(self.__serializer, self.__loader)
            try:
                return loader(str(db_value))
            except StandardError:
                raise orb.errors.DataStoreError('Failed to load serialized data.')

    def get_database_value(self, column, plugin_name, py_value, context=None):
        """
        Re-impleents the get_database_value method from ColumnEngine.

        :param column: <orb.Column>
        :param plugin_name: <str>
        :param py_value: <variant>

        :return: <variant> database value
        """
        if py_value is None:
            return py_value
        else:
            dumper = getattr(self.__serializer, self.__dumper)
            try:
                return dumper(py_value)
            except StandardError:
                raise orb.errors.DataStoreError('Failed to serialize data for database')


class JSONDataColumnEngine(ColumnEngine):
    def get_api_value(self, column, plugin_name, db_value, context=None):
        """
        Re-implements the get_api_value method from ColumnEngine.

        :param column: <orb.Column>
        :param plugin_name: <str>
        :param db_value: <variant>
        :param context: <orb.Context>

        :return: <variant> python value
        """
        if db_value is None:
            return None
        else:
            try:
                return rest.unjsonify(db_value)
            except StandardError:
                raise orb.errors.DataStoreError('Failed to load serialized data.')

    def get_database_value(self, column, plugin_name, py_value, context=None):
        """
        Re-impleents the get_database_value method from ColumnEngine.

        :param column: <orb.Column>
        :param plugin_name: <str>
        :param py_value: <variant>

        :return: <variant> database value
        """
        if py_value is None:
            return py_value
        else:
            try:
                return rest.jsonify(py_value)
            except StandardError:
                raise orb.errors.DataStoreError('Failed to serialize data for database')


class QueryColumnEngine(JSONDataColumnEngine):
    def get_api_value(self, column, plugin_name, db_value, context=None):
        """
        Re-implements the get_api_value from the DataColumnEngine class.

        :param column: <orb.Column>
        :param plugin_name: <str>
        :param db_value: <variant>
        :param context: <orb.Context>

        :return: <variant> python value
        """
        jdata = super(QueryColumnEngine, self).get_api_value(column, plugin_name, db_value, context=context)
        if jdata is not None:
            return orb.Query.load(jdata)
        else:
            return None


class BinaryColumn(Column):
    __default_engine__ = DataColumnEngine(cPickle, type_map={
        'Postgres': 'TEXT',
        'SQLite': 'BLOB',
        'MySQL': 'TEXT'
    })

    def random_value(self):
        """
        Returns a random value that fits this column's parameters.

        :return: <variant>
        """
        return os.urandom(16).encode('hex')


class JSONColumn(Column):
    __default_engine__ = JSONDataColumnEngine(type_map={
        'Postgres': 'TEXT',
        'SQLite': 'BLOB',
        'MySQL': 'TEXT'
    })

    def random_value(self):
        """
        Returns a random value that fits this column's parameters.

        :return: <variant>
        """
        blob = {
            'property': 'random_value',
            'value': random.randrange(100)
        }
        return blob


class QueryColumn(JSONColumn):
    __default_engine__ = QueryColumnEngine(type_map={
        'Postgres': 'TEXT',
        'SQLite': 'BLOB',
        'MySQL': 'TEXT'
    })

    def random_value(self):
        """
        Returns a random value that fits this column's parameters.

        :return: <variant>
        """
        q = (orb.Query('column') == 'value')
        return q.__json__()


class YAMLColumn(Column):
    __default_engine__ = DataColumnEngine(yaml, loader='load', dumper='dump', type_map={
        'Postgres': 'TEXT',
        'SQLite': 'BLOB',
        'MySQL': 'TEXT'
    })

    def random_value(self):
        """
        Returns a random value that fits this column's parameters.

        :return: <variant>
        """
        blob = {
            'property': 'random_value',
            'value': random.randrange(100)
        }
        return blob

