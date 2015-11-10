import logging
import projex.text

from ..column import Column
from projex.lazymodule import lazy_import
from projex import rest

log = logging.getLogger(__name__)
pickle = lazy_import('cPickle')
orb = lazy_import('orb')
yaml = lazy_import('yaml')

class BinaryColumn(Column):
    def dbRestore(self, db_value, context=None):
        """
        Converts a stored database value to Python.

        :param py_value: <variant>
        :param context: <orb.Context>

        :return: <variant>
        """
        try:
            return pickle.loads(projex.text.nativestring(db_value))
        except StandardError:
            log.exception('Failed to restore pickle')
            raise orb.errors.DataStoreError('Failed to restore pickle.')

    def dbStore(self, py_value, context=None):
        try:
            return pickle.dumps(py_value)
        except StandardError:
            log.exception('Failed to store pickle')
            raise orb.errors.DataStoreError('Failed to store pickle')


class JSONColumn(Column):
    def dbRestore(self, db_value, context=None):
        """
        Converts a stored database value to Python.

        :param py_value: <variant>
        :param context: <orb.Context>

        :return: <variant>
        """
        try:
            return rest.unjsonify(db_value)
        except StandardError:
            log.exception('Failed to restore json')
            raise orb.errors.DataStoreError('Failed to restore json.')

    def dbStore(self, py_value, context=None):
        try:
            return rest.jsonify(py_value)
        except StandardError:
            log.exception('Failed to store json')
            raise orb.errors.DataStoreError('Failed to store json')


class QueryColumn(JSONColumn):
    def dbRestore(self, db_value, context=None):
        """
        Converts a stored database value to Python.

        :param py_value: <variant>
        :param context: <orb.Context>

        :return: <variant>
        """
        jdata = super(QueryColumn, self).dbRestore(db_value, context=context)
        return orb.Query.fromJSON(jdata)


class YAMLColumn(Column):
    def dbRestore(self, db_value, context=None):
        """
        Converts a stored database value to Python.

        :param py_value: <variant>
        :param context: <orb.Context>

        :return: <variant>
        """
        try:
            yaml.loads(projex.text.nativestring(db_value))
        except StandardError:
            log.exception('Failed to restore yaml')
            raise orb.errors.DataStoreError('Failed to restore yaml.')

    def dbStore(self, py_value, context=None):
        try:
            return yaml.dumps(py_value)
        except ImportError:
            raise orb.errors.DependencyNotFound('PyYaml')
        except StandardError:
            log.exception('Failed to store yaml')
            raise orb.errors.DataStoreError('Failed to store json')

# register the column type addons
Column.registerAddon('Binary', BinaryColumn)
Column.registerAddon('JSON', JSONColumn)
Column.registerAddon('Query', QueryColumn)
Column.registerAddon('YAML', YAMLColumn)