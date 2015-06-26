"""
Defines the DataStore class that will convert Column value types for different
backends to a base type.
"""

import time
import cPickle
import datetime
import decimal
import orb
import orb.errors
import projex.rest

from projex.addon import AddonManager
from projex.lazymodule import lazy_import
from projex.text import nativestring as nstr
from .converter import DataConverter

yaml = lazy_import('yaml')
pytz = lazy_import('pytz')


class DataStore(AddonManager):
    def restore(self, column, db_value):
        """
        Restores the inputted value from the database to a Python value.
        
        :param      column   | <orb.Column>
                    db_value | <variant>
        
        :return     <variant> | python value
        """
        if not column:
            return db_value

        col_type = orb.ColumnType.base(column.columnType())
        
        if db_value is None:
            return db_value
        
        elif col_type == orb.ColumnType.Pickle:
            try:
                return cPickle.loads(nstr(db_value))
            except StandardError:
                raise orb.errors.DataStoreError('Failed to restore pickle.')
        
        elif col_type == orb.ColumnType.Yaml:
            try:
                return yaml.loads(nstr(db_value))
            except StandardError:
                raise orb.errors.DataStoreError('Failed to restore yaml.')

        elif col_type == orb.ColumnType.JSON:
            try:
                return projex.rest.unjsonify(db_value)
            except StandardError:
                raise orb.errors.DataStoreError('Failed to restore JSON.')

        elif col_type == orb.ColumnType.Query:
            if type(db_value) == dict:
                return orb.Query.fromDict(db_value)
            else:
                try:
                    return orb.Query.fromXmlString(nstr(db_value))
                except StandardError:
                    raise orb.errors.DataStoreError('Failed to restore query.')

        elif col_type in (orb.ColumnType.Timestamp, orb.ColumnType.Timestamp_UTC):
            if isinstance(db_value, (int, float)):
                return datetime.datetime.fromtimestamp(db_value)

        elif col_type == orb.ColumnType.Dict:
            return projex.rest.unjsonify(nstr(db_value))
        
        elif column.isString():
            return projex.text.decoded(db_value)
        
        elif type(db_value) == decimal.Decimal:
            return float(db_value)
        
        else:
            return db_value

    def fromString(self, value_str):
        """
        Converts the inputted string to a standard Python value.
        
        :param      value_str | <str>
        
        :return     <variant>
        """
        try:
            return eval(value_str)
        except StandardError:
            return value_str

    def store(self, column, py_value):
        """
        Prepares the inputted value from Python to a value that the database
        can store.
        
        :param      column   | <orb.Column>
                    py_value | <variant>
        
        :return     <variant>
        """
        col_type = orb.ColumnType.base(column.columnType())
        py_value = DataConverter.toPython(py_value)
        
        if py_value is None:
            return None
        
        # save a query
        elif col_type == orb.ColumnType.Query:
            if type(py_value) == dict:
                py_value = orb.Query.fromDict(py_value)

            if orb.Query.typecheck(py_value):
                try:
                    return py_value.toXmlString()
                except StandardError:
                    raise orb.errors.DataStoreError('Unable to convert Query to XML')
        
        # save a pickle
        elif col_type == orb.ColumnType.Pickle:
            return cPickle.dumps(py_value)
        
        # save a yaml
        elif col_type == orb.ColumnType.Yaml:
            try:
                return yaml.dumps(py_value)
            except ImportError:
                raise orb.errors.DependencyNotFound('PyYaml')
            except StandardError:
                raise orb.errors.DataStoreError('Unable to convert to yaml')

        # save as JSON
        elif col_type == orb.ColumnType.JSON:
            try:
                return projex.rest.jsonify(py_value)
            except StandardError:
                raise orb.errors.DataStoreError('Unable to convert to JSON')

        # save a record set
        elif orb.RecordSet.typecheck(py_value):
            return py_value.primaryKeys()
        
        # save a record
        elif orb.Table.recordcheck(py_value) or orb.View.recordcheck(py_value):
            return py_value.primaryKey() if py_value.isRecord() else None
        
        # save a list/tuple/set
        elif type(py_value) in (list, tuple, set):
            return tuple([self.store(column, x) for x in py_value])
        
        # save a timedelta
        elif type(py_value) == datetime.timedelta:
            now = datetime.datetime.now()
            dtime = now + py_value
            return self.store(column, dtime)
        
        # save a datetime
        elif type(py_value) == datetime.datetime:
            if col_type in (orb.ColumnType.Timestamp, orb.ColumnType.Timestamp_UTC):
                return time.mktime(py_value.timetuple())

            # convert timezone information to UTC data
            if py_value.tzinfo is not None:
                try:
                    return py_value.astimezone(pytz.utc).replace(tzinfo=None)
                except ImportError:
                    raise orb.errors.DependencyNotFound('pytz')
            return py_value
        
        # save a dictionary
        elif type(py_value) == dict:
            return projex.rest.jsonify(py_value)
        
        # save a string
        elif type(py_value) in (str, unicode):
            return projex.text.decoded(py_value)
        
        # save a basic python value
        else:
            return py_value

    def toString(self, value):
        """
        Converts the inputted value to a string representation.
        
        :param      value | <variant>
        
        :return     <str>
        """
        return nstr(value)
