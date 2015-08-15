""" Defines an indexing system to use when looking up records. """

import logging
import projex.text

from xml.etree import ElementTree
from projex.lazymodule import lazy_import
from projex.text import nativestring as nstr


log = logging.getLogger(__name__)
orb = lazy_import('orb')
errors = lazy_import('orb.errors')


class Index(object):
    """ 
    Defines an indexed way to lookup information from a database.
    Creating an Index generates an object that works like a method, however
    has a preset query built into it, along with caching options.
    """

    def __init__(self,
                 name='',
                 columns=None,
                 unique=False,
                 order=None,
                 cached=False,
                 referenced=False):

        if columns is None:
            columns = []

        self.__name__ = name
        self._schema = None
        self._columnNames = columns
        self._unique = unique
        self._order = order
        self._local_cache = {}
        self._cache = {}
        self._cached = cached
        self._cacheTimeout = 0
        self._referenced = referenced

    def __call__(self, table, *values, **options):
        # make sure we have the right number of arguments
        if len(values) != len(self._columnNames):
            name = self.__name__
            columnCount = len(self._columnNames)
            valueCount = len(values)
            opts = (name, columnCount, valueCount)
            text = '%s() takes exactly %i arguments (%i given)' % opts
            raise TypeError(text)

        data = tuple(hash(value) for value in values)
        cache_key = (data,
                     hash(orb.LookupOptions(**options)),
                     options.get('db').name() if options.get('db') else '')
        cache = self.cache(table)

        if cache and cache_key in self._local_cache:
            return self._local_cache[cache_key]

        self._local_cache.pop(cache_key, None)

        # create the lookup query
        query = orb.Query()
        for i, col in enumerate(self._columnNames):
            value = values[i]
            column = table.schema().column(col)

            if (orb.Table.recordcheck(value) or orb.View.recordcheck(value)) and not value.isRecord():
                if self._unique:
                    return None

                return orb.RecordSet()

            if not column:
                name = table.schema().name()
                raise errors.ColumnNotFound(name, col)

            if column.isEncrypted():
                value = orb.system.encrypt(value)

            query &= orb.Query(col) == value

        # include additional where option selection
        if 'where' in options:
            options['where'] = query & options['where']
        else:
            options['where'] = query

        # selects the records from the database
        options['context'] = table.schema().context(self.name())

        if self._unique:
            results = table.selectFirst(**options)
        else:
            results = table.select(**options)

        # cache the results
        if cache and results is not None:
            self._local_cache[cache_key] = results
            cache.setValue(cache_key, True, timeout=self._cacheTimeout)

        return results

    def cache(self, table):
        """
        Returns the cache associated with this index for the given table.
        
        :return     <orb.TableCache> || None
        """
        if not self.cached():
            return None

        if table not in self._cache:
            self._cache[table] = table.tableCache()

        return self._cache[table]

    def cached(self):
        """
        Returns whether or not the results for this index should be cached.
        
        :return     <bool>
        """
        return self._cached

    def cacheTimeout(self):
        """
        Returns the number of seconds that this index will keep its cache
        before reloading.
        
        :return     <int> | seconds
        """
        return self._cacheTimeout

    def columnNames(self):
        """
        Returns the list of column names that this index will be expecting as \
        inputs when it is called.
        
        :return     [<str>, ..]
        """
        return self._columnNames

    def isReferenced(self):
        """
        Returns whether or not this
        """
        return self._referenced

    def name(self):
        """
        Returns the name of this index.
        
        :return     <str>
        """
        return self.__name__

    def schema(self):
        return self._schema

    def setCached(self, state):
        """
        Sets whether or not this index should cache the results of its query.
        
        :param      state | <bool>
        """
        self._cached = state

    def setCacheTimeout(self, seconds):
        """
        Sets the time in seconds that this index will hold onto a client
        side cache.  If the value is 0, then the cache will never expire, 
        otherwise it will update after N seconds.
        
        :param     seconds | <int>
        """
        self._cacheTimeout = seconds

    def setColumnNames(self, columnNames):
        """
        Sets the list of the column names that this index will use when \
        looking of the records.
        
        :param      columnNames | [<str>, ..]
        """
        self._columnNames = columnNames

    def setOrder(self, order):
        """
        Sets the order information for this index for how to sort and \
        organize the looked up data.
        
        :param      order   | [(<str> field, <str> direction), ..]
        """
        self._order = order

    def setName(self, name):
        """
        Sets the name for this index to this index.
        
        :param      name    | <str>
        """
        self.__name__ = nstr(name)

    def setUnique(self, state):
        """
        Sets whether or not this index should find only a unique record.
        
        :param      state | <bool>
        """
        self._unique = state

    def setSchema(self, schema):
        self._schema = schema

    def unique(self):
        """
        Returns whether or not the results that this index expects should be \
        a unique record, or multiple records.
        
        :return     <bool>
        """
        return self._unique

    def toolTip(self, context='index'):
        if self.unique():
            tip = '''\
<b>{schema}.{name} <small>({schema} || None)</small></b>
<pre>
>>> # lookup record by index
>>> {schema}.{getter}({columns})
&lt;{schema}&gt;
</pre>
'''
        else:
            tip = '''\
<b>{schema}.{name} <small>(RecordSet([{schema}, ..]))</small></b>
<pre>
>>> # lookup records by index
>>> {schema}.{getter}({columns})
&lt;orb.RecordSet([&lt;{schema}&gt;, ..])&gt;
</pre>
'''

        return tip.format(name=self.name(),
                          getter=self.name(),
                          schema=self.schema().name(),
                          record=projex.text.underscore(self.schema().name()),
                          columns=', '.join([projex.text.underscore(c) for c in self.columnNames()]))

    def toXml(self, xparent):
        """
        Saves the index data for this column to XML.
        
        :param      xparent     | <xml.etree.ElementTree.Element>
        
        :return     <xml.etree.ElementTree.Element>
        """
        xindex = ElementTree.SubElement(xparent, 'index')
        xindex.set('name', self.name())

        if self.unique():
            xindex.set('unique', 'True')

        if self.cached():
            xindex.set('cached', nstr(self.cached()))
            xindex.set('cacheTimeout', nstr(self._cacheTimeout))

        for name in self.columnNames():
            ElementTree.SubElement(xindex, 'column').text = name

        return xindex

    def validate(self, record, values):
        """
        Validates whether or not this index's requirements are satisfied by the inputted record and
        values.  If this index fails validation, a ValidationError will be raised.

        :param      record | subclass of <orb.Table>
                    values | {<orb.Column>: <variant>, ..}

        :return     <bool>
        """
        schema = record.schema()
        try:
            column_values = [values[schema.column(name)] for name in self.columnNames()]
        except StandardError:
            msg = 'Missing some columns ({0}) from {1}.{2}.'.format(', '.join(self.columnNames()),
                                                                    record.schema().name(),
                                                                    self.name())
            raise errors.IndexValidationError(self, msg=msg)

        # ensure a unique record is preserved
        if self.unique():
            lookup = getattr(record, self.name())
            other = lookup(*column_values)
            if other and other != record:
                msg = 'A record already exists with the same {0} combination.'.format(', '.join(self.columnNames()))
                raise errors.IndexValidationError(self, msg=msg)

        return True

    @staticmethod
    def fromXml(xindex, referenced=False):
        """
        Generates an index method descriptor from xml data.
        
        :param      xindex  | <xml.etree.Element>
        
        :return     <Index> || None
        """
        index = Index(referenced=referenced)
        index.setName(xindex.get('name', ''))
        index.setUnique(xindex.get('unique') == 'True')
        index.setCached(xindex.get('cached') == 'True')
        index.setCacheTimeout(int(xindex.get('cacheTimeout',
                                             xindex.get('cachedExpires', index._cacheTimeout))))

        xcolumns = xindex.findall('column')
        if xcolumns:
            columns = [xcolumn.text for xcolumn in xcolumns]
        else:
            columns = xindex.get('columns', '').split(',')

        index.setColumnNames(columns)

        return index

