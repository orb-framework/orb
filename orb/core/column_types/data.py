import logging
import os
import projex.text
import random
import yaml

from projex.lazymodule import lazy_import
from projex import rest

from ..column import Column

log = logging.getLogger(__name__)
pickle = lazy_import('cPickle')
orb = lazy_import('orb')


class BinaryColumn(Column):
    TypeMap = {
        'Postgres': 'TEXT',
        'SQLite': 'BLOB',
        'MySQL': 'TEXT'
    }

    def random(self):
        """
        Returns a random value that fits this column's parameters.

        :return: <variant>
        """
        return os.urandom(16).encode('hex')

    def dbRestore(self, db_value, context=None):
        """
        Converts a stored database value to Python.

        :param py_value: <variant>
        :param context: <orb.Context>

        :return: <variant>
        """
        if db_value is not None:
            try:
                return pickle.loads(str(db_value))
            except StandardError:
                log.exception('Failed to restore pickle')
                raise orb.errors.DataStoreError('Failed to restore pickle.')
        else:
            return None

    def dbStore(self, typ, py_value):
        if py_value is not None:
            try:
                return pickle.dumps(py_value)
            except StandardError:
                log.exception('Failed to store pickle')
                raise orb.errors.DataStoreError('Failed to store pickle')
        else:
            return py_value


class JSONColumn(Column):
    TypeMap = {
        'Postgres': 'TEXT',
        'SQLite': 'TEXT',
        'MySQL': 'TEXT'
    }

    def random(self):
        """
        Returns a random value that fits this column's parameters.

        :return: <variant>
        """
        blob = {
            'property': 'random_value',
            'value': random.randrange(100)
        }
        return blob

    def dbRestore(self, db_value, context=None):
        """
        Converts a stored database value to Python.

        :param py_value: <variant>
        :param context: <orb.Context>

        :return: <variant>
        """
        if db_value is not None:
            try:
                return rest.unjsonify(db_value)
            except StandardError:
                log.exception('Failed to restore json')
                raise orb.errors.DataStoreError('Failed to restore json.')
        else:
            return db_value

    def dbStore(self, typ, py_value):
        if py_value is not None:
            try:
                return rest.jsonify(py_value)
            except StandardError:
                log.exception('Failed to store json')
                raise orb.errors.DataStoreError('Failed to store json')
        else:
            return py_value


class QueryColumn(JSONColumn):
    def random(self):
        """
        Returns a random value that fits this column's parameters.

        :return: <variant>
        """
        q = (orb.Query('column') == 'value')
        return q.__json__()

    def dbRestore(self, db_value, context=None):
        """
        Converts a stored database value to Python.

        :param py_value: <variant>
        :param context: <orb.Context>

        :return: <variant>
        """
        if db_value is not None:
            jdata = super(QueryColumn, self).dbRestore(db_value, context=context)
            return orb.Query.fromJSON(jdata)
        else:
            return db_value


class YAMLColumn(Column):
    TypeMap = {
        'Postgres': 'TEXT',
        'SQLite': 'TEXT',
        'MySQL': 'TEXT'
    }

    def random(self):
        """
        Returns a random value that fits this column's parameters.

        :return: <variant>
        """
        blob = {
            'property': 'random_value',
            'value': random.randrange(100)
        }
        return blob

    def dbRestore(self, db_value, context=None):
        """
        Converts a stored database value to Python.

        :param py_value: <variant>
        :param context: <orb.Context>

        :return: <variant>
        """
        if db_value is not None:
            return yaml.load(projex.text.nativestring(db_value))
        else:
            return db_value

    def dbStore(self, typ, py_value):
        if py_value is not None:
            try:
                return yaml.dump(py_value)
            except StandardError:
                log.exception('Failed to store yaml')
                raise orb.errors.DataStoreError('Failed to store yaml')
        else:
            return py_value

# register the column type addons
Column.registerAddon('Binary', BinaryColumn)
Column.registerAddon('JSON', JSONColumn)
Column.registerAddon('Query', QueryColumn)
Column.registerAddon('YAML', YAMLColumn)