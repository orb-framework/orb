#!/usr/bin/python

""" 
Defines the main Table class that will be used when developing
database classes.
"""

# define authorship information
__authors__         = ['Eric Hulser']
__author__          = ','.join(__authors__)
__credits__         = []
__copyright__       = 'Copyright (c) 2011, Projex Software'
__license__         = 'LGPL'

# maintanence information
__maintainer__      = 'Projex Software'
__email__           = 'team@projexsoftware.com'

#------------------------------------------------------------------------------

import datetime
import orb

from new import instancemethod

TEMP_REVERSE_LOOKUPS = {}
GETTER_DOCS = """\
Gets the value for the {name} column.
{optdocs}

This method was auto-generated from ORB.

:param      {optparams}

:return     {returns}
"""

GETTER_I18N_DOCS = """\

:internationalization   You can provide the optional locale parameter to get
                        the locale-specific value for this column.  If no
                        locale is provided, the current global locale
                        defined in the [[Orb]] instance will be used.
"""

GETTER_FKEY_DOCS = """\

:references             Foreign key references will be inflated to their
                        API class type.  If you want simply the key value
                        from the database instead of a record, then you
                        can pass inflated = False.  This
                        can improve overhead when working with large amounts
                        of records at one time.
"""

#----------------------------------------------------------------------

SETTER_DOCS = """\
Sets the value for the {name} column to the inputed value.  This will only
affect the value of the API object instance, and '''not''' the value in
the database.  To commit this value to the database, use the [[Table::commit]]
method.  This method will return a boolean whether or not a change in the
value acutally occurred.

:param      {param}  | {returns}
{optparams}

:return     <bool> | changed
"""

SETTER_I18N_DOCS = """\
:internationalization   If this column is translatable, you can provide the
                        optional locale parameter to set the locale-specific
                        value for this column.  If no locale is provided, the
                        current global locale defined in the [[Orb]] instance
                        will be used.
"""


#------------------------------------------------------------------------------

class gettermethod(object):
    """ Creates a method for tables to use as a field accessor. """
    
    def __init__(self, **kwds):
        """
        Defines the getter method that will be used when accessing
        information about a column on a database record.  This
        class should only be used by the TableBase class when
        generating column methods on a model.
        """
        self.__dict__.update(kwds)
        self.columnName     = kwds.get('columnName', '')
        
        translatable   = kwds.get('translatable', False)
        inflatable     = kwds.get('inflatable', False)
        
        args = []
        optdocs = []
        optparams = []
        
        if translatable:
            args.append('locale=None')
            optdocs.append(GETTER_I18N_DOCS)
            optparams.append('            locale    | None || <str> locale code')
        
        if inflatable:
            args.append('inflated=True')
            optdocs.append(GETTER_FKEY_DOCS)
            optparams.append('            inflated    | <bool>')
        
        default = '<variant>  (see {0} for details)'.format(self.columnName)
        returns = kwds.get('returns', default)
        
        self.func_name  = kwds['__name__']
        self.func_args  = '({0})'.format(', '.join(args))
        self.func_doc   = GETTER_DOCS.format(name=self.columnName,
                                             optdocs='\n'.join(optdocs),
                                             optparams='\n'.join(optparams),
                                             returns=returns)
        
        self.__dict__['__doc__'] = self.func_doc

    def __call__(self, record, **kwds):
        """
        Calls the getter lookup method for the database record.
        
        :param      record      <Table>
        """
        inflated = kwds.get('inflated', kwds.get('autoInflate', True))
        
        val = record.recordValue(self.columnName,
                                  locale=kwds.get('locale', None),
                                  default=kwds.get('default', None),
                                  autoInflate=inflated,
                                  useMethod=False)
        
        if not inflated and orb.Table.recordcheck(val):
            return val.primaryKey()
        return val

#------------------------------------------------------------------------------

class settermethod(object):
    """ Defines a method for setting database fields on a Table instance. """
    
    def __init__(self, **kwds):
        """
        Defines the setter method that will be used when accessing
        information about a column on a database record.  This
        class should only be used by the TableBase class when
        generating column methods on a model
        """
        self.__dict__.update(kwds)
        self.columnName = kwds.get('columnName', '')
        
        if kwds.get('inflatable'):
            args = ['record_or_key']
        else:
            args = ['value']
        optdocs = []
        optparams = []
        
        if kwds.get('translatable'):
            args.append('locale=None')
            optdocs.append(SETTER_I18N_DOCS)
            optparams.append('            locale    | None || <str>')
        
        default = '<variant>  (see {0} for details)'.format(self.columnName)
        returns = kwds.get('returns', default)
        
        self.func_name  = kwds['__name__']
        self.func_args  = '({0})'.format(', '.join(args))
        self.func_doc   = SETTER_DOCS.format(name=self.columnName,
                                             param=args[0],
                                             optdocs='\n'.join(optdocs),
                                             optparams='\n'.join(optparams),
                                             returns=returns)
        
        self.__dict__['__doc__']    = self.func_doc
    
    def __call__(self, record, value, **kwds):
        """
        Calls the setter method for the inputed database record.
        
        :param      record      <Table>
                    value       <variant>
        """
        kwds['useMethod'] = False
        return record.setRecordValue(self.columnName, value, **kwds)

#----------------------------------------------------------------------

class reverselookupmethod(object):
    """ Defines a reverse lookup method for lookup up relations. """
    
    def __init__(self, **kwds):
        """
        Defines the getter method that will be used when accessing
        information about a column on a database record.  This
        class should only be used by the TableBase class when
        generating column methods on a model.
        """
        self.__dict__.update(kwds)
        
        self._cache       = {}
        self.reference    = kwds.get('reference', '')
        self.referenceDb  = kwds.get('referenceDatabase', None)
        self.columnName   = kwds.get('columnName', '')
        self.unique       = kwds.get('unique', False)
        self.cached       = kwds.get('cached', False)
        self.cacheExpires = kwds.get('cacheExpires', 0)
        self.func_name    = kwds['__name__']
        self.func_args    = '()'
        self.func_doc     = 'Auto-generated Orb reverse lookup method'
        
        self.__dict__['__doc__']    = self.func_doc
    
    def __call__(self, record, **kwds):
        """
        Calls the getter lookup method for the database record.
        
        :param      record      <Table>
        """
        reload = kwds.pop('reload', False)
        
        # remove any invalid query lookups
        if 'where' in kwds and orb.Query.testNull(kwds['where']):
            kwds.pop('where')
        
        # lookup the records with a specific model
        table = kwds.pop('model', None)
        if not table:
            table = record.polymorphicModel(self.reference)
            
            if not table:
                table = orb.system.model(self.reference, database=self.referenceDb)
            
            if not table:
                if self.unique:
                    return None
                return orb.RecordSet()
        
        # return from the cache when specified
        cache = self.cache(table)
        cache_key = (id(record),
                     hash(orb.LookupOptions(**kwds)),
                     id(record.database()))
        
        if cache and not cache.isExpired(cache_key):
            return cache.value(cache_key)
        
        # make sure this is a valid record
        if not record.isRecord():
            if self.unique:
                return None
            return orb.RecordSet()
        
        if 'where' in kwds:
            where = kwds['where']
            kwds['where'] = (orb.Query(self.columnName) == record) & where
        else:
            kwds['where'] = orb.Query(self.columnName) == record
        
        if not 'order' in kwds and table.schema().defaultOrder():
            kwds['order'] = table.schema().defaultOrder()
        
        # make sure we stay within the same database
        kwds['db'] = record.database()
        
        if self.unique:
            output = table.selectFirst(**kwds)
        else:
            output = table.select(**kwds)
        
        if cache and output is not None:
            cache.setValue(cache_key, output)
        return output
    
    def cache(self, table):
        """
        Returns the cache for this table.
        
        :param      table | <subclass of orb.Table>
        
        :return     <orb.TableCache> || None
        """
        if not self.cached:
            return None
        elif table in self._cache:
            return self._cache[table]
        else:
            cache = orb.TableCache(table, self.cacheExpires)
            self._cache[table] = cache
            return cache

#------------------------------------------------------------------------------

class TableBase(type):
    """ 
    Defines the table Meta class that will be used to dynamically generate
    Table class types.
    """
    def __new__(mcs, name, bases, attrs):
        """
        Manages the creation of database model classes, reading
        through the creation attributes and generating table
        schemas based on the inputed information.  This class
        never needs to be expressly defined, as any class that
        inherits from the Table class will be passed through this
        as a constructor.
        
        :param      mcs         <TableBase>
        :param      name        <str>
        :param      bases       <tuple> (<object> base,)
        :param      attrs       <dict> properties
        
        :return     <type>
        """
        # ignore initial class
        db_ignore = attrs.pop('__db_ignore__', False)
        if db_ignore:
            return super(TableBase, mcs).__new__(mcs, name, bases, attrs)
        
        # collect database and schema information
        db_data     = {}
        db_data['__db_name__']      = name
        db_data['__db_columns__']   = []
        db_data['__db_indexes__']   = []
        db_data['__db_pipes__']     = []
        db_data['__db_schema__']    = None
        
        for key, value in attrs.items():
            if key.startswith('__db_'):
                db_data[key] = attrs.pop(key)
                keys_found = True
        
        # make sure we have are creating a new database table class
        parents = [base for base in bases if isinstance(base, TableBase)]
        
        # set the database name for this table
        if parents:
            db_data.setdefault('__db__', parents[0].__db__)
        else:
            db_data.setdefault('__db__', None)
        
        # merge inherited information
        for parent in parents:
            for key, value in parent.__dict__.items():
                # skip non-db values
                if not key.startswith('__db_'):
                    continue
                
                db_data.setdefault(key, value)
        
        # determine if this is a definition, or a specific schema
        new_class   = super(TableBase, mcs).__new__(mcs, name, bases, attrs)
        schema      = db_data.get('__db_schema__')
        
        if schema:
            new_columns = db_data.get('__db_columns__', [])
            cur_columns = schema.columns(recurse=False, includeProxies=False)
            columns     = cur_columns + new_columns
            indexes     = schema.indexes() + db_data.get('__db_indexes__', [])
            pipes       = schema.pipes() + db_data.get('__db_pipes__', [])
            tableName   = schema.tableName()
            
            schemaName  = schema.name()
            schema.setColumns(columns)
            schema.setIndexes(indexes)
            schema.setPipes(pipes)
            
        else:
            db           = db_data.get('__db__',            None)
            abstract     = db_data.get('__db_abstract__',   False)
            columns      = db_data.get('__db_columns__',    [])
            indexes      = db_data.get('__db_indexes__',    [])
            pipes        = db_data.get('__db_pipes__',      [])
            schemaName   = db_data.get('__db_name__',       name)
            schemaGroup  = db_data.get('__db_group__',      'Default')
            tableName    = db_data.get('__db_tablename__',  '')
            inherits     = db_data.get('__db_inherits__',   '')
            autoPrimary  = db_data.get('__db_autoprimary__', True)
            
            if not '__db_inherits__' in db_data:
                if parents and parents[0].schema():
                    inherits = parents[0].schema().name()
            
            # create the table schema
            schema  = orb.TableSchema()
            schema.setDatabase(db)
            schema.setAutoPrimary(autoPrimary)
            schema.setName(schemaName)
            schema.setGroupName(schemaGroup)
            schema.setTableName(tableName)
            schema.setAbstract(abstract)
            schema.setColumns(columns)
            schema.setIndexes(indexes)
            schema.setPipes(pipes)
            schema.setInherits(inherits)
            schema.setModel(new_class)
            
            orb.system.registerSchema(schema)
        
        db_data['__db_schema__'] = schema
        
        # add the db values to the class
        for key, value in db_data.items():
            setattr(new_class, key, value)
        
        # create class methods for the index instances
        for index in indexes:
            iname = index.name()
            if not hasattr(new_class, iname):
                setattr(new_class, index.name(), classmethod(index))
        
        # create instance methods for the pipe instances
        for pipe in pipes:
            pname = pipe.name()
            if not hasattr(new_class, pname):
                pipemethod = instancemethod(pipe, None, new_class)
                setattr(new_class, pname, pipemethod)
        
        # pylint: disable-msg=W0212
        for column in columns:
            colname = column.name()
            
            # create getter method
            gname = column.getterName()
            if gname and not hasattr(new_class, gname):
                gmethod = gettermethod(columnName=colname,
                                       translatable=column.isTranslatable(),
                                       inflatable=column.isReference(),
                                       returns=column.returnType(),
                                       __name__ = gname )
                                        
                getter  = instancemethod(gmethod, None, new_class)
                setattr(new_class, gname, getter)
            
            # create setter method
            sname = column.setterName()
            if sname and not (column.isReadOnly() or hasattr(new_class, sname)):
                smethod = settermethod(columnName=colname,
                                       translatable=column.isTranslatable(),
                                       inflatable=column.isReference(),
                                       returns=column.returnType(),
                                       __name__=sname)
                setter  = instancemethod(smethod, None, new_class)
                setattr(new_class, sname, setter)
            
            # create an index if necessary
            iname = column.indexName()
            if column.indexed() and iname and not hasattr(new_class, iname):
                index = orb.Index(iname,
                                  [column.name()],
                                  unique=column.unique())
                index.setCached(column.indexCached())
                index.setCachedExpires(column.indexCachedExpires())
                index.__name__ = iname
                imethod = classmethod(index)
                setattr(new_class, iname, imethod)
            
            # create a reverse lookup
            if column.isReversed() and column.schema().name() == schemaName:
                rev_name   = column.reversedName()
                rev_cached = column.reversedCached()
                ref_name   = column.reference()
                ref_model  = column.referenceModel()
                rev_cacheExpires = column.reversedCacheExpires()
                
                # create the lookup method
                lookup = reverselookupmethod(columnName   = column.name(),
                                             reference    = schemaName,
                                             unique       = column.unique(),
                                             cached       = rev_cached,
                                             cacheExpires = rev_cacheExpires,
                                             __name__     = rev_name)
                
                # ensure we're assigning it to the proper base module
                while ref_model and \
                      ref_model.__module__ != 'orb.schema.dynamic' and \
                      ref_model.__bases__ and \
                      ref_model.__bases__[0] != orb.Table:
                    ref_model = ref_model.__bases__[0]
                
                # assign to an existing model
                if ref_model:
                    ilookup = instancemethod(lookup, None, ref_model)
                    setattr(ref_model, rev_name, ilookup)
                else:
                    TEMP_REVERSE_LOOKUPS.setdefault(ref_name, [])
                    TEMP_REVERSE_LOOKUPS[ref_name].append((rev_name, lookup))
        
        # assign any cached reverse lookups to this model
        if schemaName in TEMP_REVERSE_LOOKUPS:
            lookups = TEMP_REVERSE_LOOKUPS.pop(schemaName)
            for rev_name, lookup in lookups:
                ilookup = instancemethod(lookup, None, new_class)
                setattr(new_class, rev_name, ilookup)
                
        return new_class