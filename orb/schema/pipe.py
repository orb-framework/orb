""" Defines an piping system to use when accessing multi-to-multi records. """

import projex.text

from orb import errors
from projex.lazymodule import lazy_import
from projex.text import nativestring as nstr
from xml.etree import ElementTree

orb = lazy_import('orb')


class Pipe(object):
    """ 
    Defines a piped way to lookup information from a database.
    Creating an Pipe generates an object that works like a method, however
    has a preset query built into it allowing multi-to-multi connections
    """

    def __init__(self, name, **options):
        self.__name__ = name
        self._schema = options.get('schema', None)
        self._pipeReference = options.get('pipeReference', options.get('through', ''))
        self._pipeTable = None
        self._sourceColumn = options.get('sourceColumn', options.get('source', ''))
        self._targetReference = options.get('targetReference', '')
        self._targetTable = None
        self._targetColumn = options.get('targetColumn', options.get('target', ''))
        self._cache = {}
        self._cached = options.get('cached', False)
        self._cacheTimeout = options.get('cacheTimeout', 0)
        self._referenced = options.get('referenced', False)
        self._unique = options.get('unique', False)

    def __call__(self, record, **options):
        # return a blank piperecordset
        if not record.isRecord():
            return orb.PipeRecordSet([], record)

        reload = options.get('reload', False)
        pipeTable = self.pipeReferenceModel()
        targetTable = self.targetReferenceModel()
        if None in (pipeTable, targetTable):
            return orb.PipeRecordSet([], record)

        # update the caching information
        pipe_cache = self.cache(pipeTable)
        target_cache = self.cache(targetTable)
        cache_key = (record.id() if record else 0,
                     hash(orb.LookupOptions(**options)),
                     options.get('db', record.database()).name())

        # ensure neither the pipe nor target table have timeout their caches
        preload_cache = getattr(record, '_Model__preload_cache', {})
        if not reload and self.__name__ in preload_cache:
            out = preload_cache[self.__name__]
            out.updateOptions(**options)
            return out

        preload_cache.pop(self.__name__, None)

        # create the query for the pipe
        sub_q = orb.Query(pipeTable, self._sourceColumn) == record
        rset = pipeTable.select(columns=[self._targetColumn], where=sub_q)
        q = orb.Query(targetTable).in_(rset)

        if 'where' in options:
            options['where'] = q & options['where']
        else:
            options['where'] = q

        # generate the new record set
        options['context'] = record.schema().context(self.name())

        lookup = orb.LookupOptions(**options)
        context = record.contextOptions(**options)
        records = targetTable.select(lookup=lookup, options=context)

        # map this recordset to a pipe set (if the options generate a pipe set)
        if isinstance(records, orb.RecordSet):
            rset_cls = record.schema().context(self.__name__).get('RecordSet', orb.PipeRecordSet)
            rset = rset_cls(records,
                            record,
                            pipeTable,
                            self._sourceColumn,
                            self._targetColumn)
            rset.setLookupOptions(lookup)
            rset.setContextOptions(context)

            preload_cache[self.__name__] = rset
            setattr(record, '_Model__preload_cache', preload_cache)

            if pipe_cache:
                pipe_cache.setValue(cache_key, True, timeout=self._cacheTimeout)
            if target_cache:
                target_cache.setValue(cache_key, True, timeout=self._cacheTimeout)
        else:
            rset = records

        if self._unique:
            if isinstance(rset, orb.RecordSet):
                return rset.first()
            else:
                try:
                    return rset[0]
                except IndexError:
                    return None
        return rset

    def cache(self, table, force=False):
        """
        Returns the cache for the inputted table.
        
        :param      table | <subclass of orb.Table>
        
        :return     <orb.TableCache> || None
        """
        try:
            return self._cache[table]
        except KeyError:
            if force or self.cached():
                cache = table.tableCache() or orb.TableCache(table, orb.system.cache(), timeout=self._cacheTimeout)
                self._cache[table] = cache
                return cache
            else:
                return None

    def cached(self):
        """
        Returns whether or not the results for this index should be cached.
        
        :return     <bool>
        """
        return self._cached

    def cacheTimeout(self):
        """
        Returns the time in seconds to store the cached results for this pipe.
        
        :return     <int>
        """
        return self._cacheTimeout

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

    def preload(self, record, data, options, type='records'):
        """
        Preloads the inputted record and result values.

        :param      record       | <orb.Table>
                    data         | [<dict>, ..]
                    lookup       | <orb.LookupOptions>
                    options      | <orb.DatabaseOptions>
                    type         | <str>
        """
        target_model = self.targetReferenceModel()
        pipe_model = self.pipeReferenceModel()

        # define the pipe cached value
        preload_cache = getattr(record, '_Model__preload_cache', {})
        pset = preload_cache.get(self.__name__)
        if pset is None:
            pset = orb.PipeRecordSet(orb.RecordSet(),
                                     record,
                                     pipe_model,
                                     self._sourceColumn,
                                     self._targetColumn)

            preload_cache[self.__name__] = pset
            setattr(record, '_Model__preload_cache', preload_cache)

        # update teh cache for the dataset
        if type == 'ids':
            pset.cache('ids', data)
        elif type == 'count':
            pset.cache('count', data)
        elif type == 'first':
            pset.cache('first', target_model(__values=data, options=options) if data else None)
        elif type == 'last':
            pset.cache('last', target_model(__values=data, options=options) if data else None)
        elif type == 'records':
            pset.cache('records', [target_model(__values=record, options=options) for record in data or []])

    def pipeReference(self):
        return self._pipeReference

    def pipeReferenceModel(self):
        if self._pipeTable is None:
            pipeTable = orb.system.model(self._pipeReference)
            if pipeTable and not self._targetReference:
                col = pipeTable.schema().column(self._targetColumn)
                self._targetReference = col.reference()

            self._pipeTable = pipeTable
            if not self._pipeTable:
                raise errors.TableNotFound(self._pipeReference)

        return self._pipeTable

    def setCached(self, state):
        """
        Sets whether or not this index should cache the results of its query.
        
        :param      state | <bool>
        """
        self._cached = state

    def setCacheTimeout(self, seconds):
        """
        Sets the time in seconds to store the cached results for this pipe
        set.
        
        :param      seconds | <int>
        """
        self._cacheTimeout = seconds

    def setUnique(self, unique):
        self._unique = unique

    def setName(self, name):
        """
        Sets the name for this index to this index.
        
        :param      name    | <str>
        """
        self.__name__ = nstr(name)

    def setPipeReference(self, reference):
        self._pipeReference = reference

    def setSourceColumn(self, column):
        self._sourceColumn = column

    def setTargetColumn(self, column):
        self._targetColumn = column

    def setTargetReference(self, reference):
        self._targetReference = reference

    def schema(self):
        return self._schema

    def setSchema(self, schema):
        self._schema = schema

    def sourceColumn(self):
        return self._sourceColumn

    def sourceReferenceModel(self):
        model = self.pipeReferenceModel()
        column = model.schema().column(self.sourceColumn())
        return column.referenceModel()

    def targetColumn(self):
        return self._targetColumn

    def targetReference(self):
        if not self._targetReference:
            try:
                pipe = orb.system.schema(self.pipeReference())
                col = pipe.column(self.targetColumn())
                self._targetReference = col.reference()
            except AttributeError:
                pass

        return self._targetReference

    def targetReferenceModel(self):
        if self._targetTable is None:
            self._targetTable = orb.system.model(self.targetReference())
            if not self._targetTable:
                raise errors.TableNotFound(self.targetReference())
        return self._targetTable

    def toolTip(self, context='pipe'):
        tip = '''\
<b>{schema}.{name} <small>(RecordSet([{target_model}, ..]))</small></b>
<pre>
>>> # piped through {pipe} model
>>> {record} = {schema}()
>>> {record}.{getter}()
&lt;orb.PipeRecordSet([&lt;{target_model}&gt;, ..])&gt;

>>> # modify the joined records
>>> {target_record} = {target_model}.all().first()
>>> {record}.{getter}().addRecord({target_record})
>>> {record}.{getter}().removeRecord({target_record})
</pre>
'''
        return tip.format(name=self.name(),
                          getter=self.name(),
                          schema=self.schema().name(),
                          record=projex.text.underscore(self.schema().name()),
                          pipe=self.pipeReference(),
                          source=self.sourceColumn(),
                          target=self.targetColumn(),
                          target_model=self.targetReference(),
                          target_record=projex.text.underscore(self.targetReference()))

    def toXml(self, xparent):
        """
        Saves the index data for this column to XML.
        
        :param      xparent     | <xml.etree.ElementTree.Element>
        
        :return     <xml.etree.ElementTree.Element>
        """
        xpipe = ElementTree.SubElement(xparent, 'pipe')
        xpipe.set('name', self.name())

        if self._unique:
            xpipe.set('unique', 'True')

        if self.cached():
            xpipe.set('cached', nstr(self.cached()))
            xpipe.set('timeout', nstr(self._cacheTimeout))

        ElementTree.SubElement(xpipe, 'through').text = self._pipeReference
        ElementTree.SubElement(xpipe, 'source').text = self._sourceColumn
        ElementTree.SubElement(xpipe, 'target').text = self._targetColumn

        if self._targetReference:
            ElementTree.SubElement(xpipe, 'table').text = self._targetReference

        return xpipe

    def unique(self):
        return self._unique

    @staticmethod
    def fromXml(xpipe, referenced=False):
        """
        Generates an index method descriptor from xml data.
        
        :param      xindex  | <xml.etree.Element>
        
        :return     <Index> || None
        """
        pipe = Pipe(xpipe.get('name', ''), referenced=referenced)
        pipe.setUnique(xpipe.get('unique') == 'True')
        pipe.setCached(xpipe.get('cached') == 'True')
        pipe.setCacheTimeout(int(xpipe.get('timeout', xpipe.get('expires', pipe._cacheTimeout))))

        try:
            pipe.setPipeReference(xpipe.find('through').text)
        except AttributeError:
            pipe.setPipeReference(xpipe.get('pipeReference', ''))

        try:
            pipe.setSourceColumn(xpipe.find('source').text)
        except AttributeError:
            pipe.setSourceColumn(xpipe.get('sourceColumn', ''))

        try:
            pipe.setTargetReference(xpipe.find('table').text)
        except AttributeError:
            pipe.setTargetReference(xpipe.get('targetReference', ''))

        try:
            pipe.setTargetColumn(xpipe.find('target').text)
        except AttributeError:
            pipe.setTargetColumn(xpipe.get('targetColumn', ''))

        return pipe

