"""
Defines the main View class that will be used when developing
database classes.
"""

# ------------------------------------------------------------------------------

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
Sets the value for the {name} column to the inputted value.  This will only
affect the value of the API object instance, and '''not''' the value in
the database.  To commit this value to the database, use the [[View::commit]]
method.  This method will return a boolean whether or not a change in the
value actually occurred.

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


# ------------------------------------------------------------------------------

class gettermethod(object):
    """ Creates a method for views to use as a field accessor. """

    def __init__(self, **kwds):
        """
        Defines the getter method that will be used when accessing
        information about a column on a database record.  This
        class should only be used by the MetaView class when
        generating column methods on a model.
        """
        self.__dict__.update(kwds)
        self.columnName = kwds.get('columnName', '')

        translatable = kwds.get('translatable', False)
        inflatable = kwds.get('inflatable', False)

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

        self.func_name = kwds['__name__']
        self.func_args = '({0})'.format(', '.join(args))
        self.func_doc = GETTER_DOCS.format(name=self.columnName,
                                           optdocs='\n'.join(optdocs),
                                           optparams='\n'.join(optparams),
                                           returns=returns)

        self.__dict__['__doc__'] = self.func_doc

    def __call__(self, record, **options):
        """
        Calls the getter lookup method for the database record.

        :param      record      <View>
        """
        default = options.pop('default', None)
        options['context'] = record.schema().context(self.func_name)
        context = record.contextOptions(**options)
        val = record.recordValue(self.columnName,
                                 locale=context.locale,
                                 default=default,
                                 inflated=context.inflated,
                                 useMethod=False)

        if not context.inflated and orb.Table.recordcheck(val):
            return val.primaryKey()
        return val


#------------------------------------------------------------------------------

class settermethod(object):
    """ Defines a method for setting database fields on a View instance. """

    def __init__(self, **kwds):
        """
        Defines the setter method that will be used when accessing
        information about a column on a database record.  This
        class should only be used by the MetaView class when
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

        self.func_name = kwds['__name__']
        self.func_args = '({0})'.format(', '.join(args))
        self.func_doc = SETTER_DOCS.format(name=self.columnName,
                                           param=args[0],
                                           optdocs='\n'.join(optdocs),
                                           optparams='\n'.join(optparams),
                                           returns=returns)

        self.__dict__['__doc__'] = self.func_doc

    def __call__(self, record, value, **kwds):
        """
        Calls the setter method for the inputted database record.

        :param      record      <View>
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
        class should only be used by the MetaView class when
        generating column methods on a model.
        """
        self.__dict__.update(kwds)
        self.__lookup__ = True

        self._cache = {}
        self.reference = kwds.get('reference', '')
        self.referenceDb = kwds.get('referenceDatabase', None)
        self.columnName = kwds.get('columnName', '')
        self.unique = kwds.get('unique', False)
        self.cached = kwds.get('cached', False)
        self.cacheTimeout = kwds.get('cacheTimeout', 0)
        self.func_name = kwds['__name__']
        self.func_args = '()'
        self.func_doc = 'Auto-generated Orb reverse lookup method'

        self.__dict__['__doc__'] = self.func_doc

    def __call__(self, record, **options):
        """
        Calls the getter lookup method for the database record.

        :param      record      <View>
        """
        reload = options.pop('reload', False)

        # remove any invalid query lookups
        if 'where' in options and orb.Query.testNull(options['where']):
            options.pop('where')

        # lookup the records with a specific model
        options.setdefault('locale', record.recordLocale())
        view = options.get('view') or self.viewFor(record)
        if not view:
            return None if self.unique else orb.RecordSet()

        # return from the cache when specified
        cache = self.cache(view)
        cache_key = (record.id(),
                     hash(orb.LookupOptions(**options)),
                     record.database().name())

        if not reload and cache_key in self._local_cache:
            out = self._local_cache[cache_key]
            out.updateOptions(**options)
            return out

        self._local_cache.pop(cache_key, None)

        # make sure this is a valid record
        if not record.isRecord():
            if self.unique:
                return None
            return orb.RecordSet()

        # generate the reverse lookup query
        reverse_q = orb.Query(self.columnName) == record

        options['where'] = reverse_q & options.get('where')
        options['db'] = record.database()
        options['context'] = record.schema().context(self.func_name)

        lookup = orb.LookupOptions(**options)
        context = record.contextOptions(**options)

        if self.unique:
            output = view.selectFirst(lookup=lookup, options=context)
        else:
            output = view.select(lookup=lookup, options=context)

        if isinstance(output, orb.RecordSet):
            output.setSource(record)
            output.setSourceColumn(self.columnName)

        if cache and output is not None:
            self._local_cache[cache_key] = output
            cache.setValue(cache_key, True, timeout=self.cacheTimeout)

        return output

    def cache(self, view, force=False):
        """
        Returns the cache for this view.

        :param      view | <subclass of orb.View>

        :return     <orb.ViewCache> || None
        """
        try:
            return self._cache[view]
        except KeyError:
            if force or self.cached:
                cache = view.viewCache() or orb.ViewCache(view, view.schema().cache(), timeout=self.cacheTimeout)
                self._cache[view] = cache
                return cache
            return None

    def preload(self, record, data, options, type='records'):
        """
        Preload a list of records from the database.

        :param      record  | <orb.View>
                    data    | [<dict>, ..]
                    lookup  | <orb.LookupOptions> || None
                    options | <orb.ContextOptions> || None
        """
        view = self.viewFor(record)

        preload_cache = getattr(record, '_Model__preload_cache', {})
        rset = preload_cache.get(self.func_name)
        if rset is None:
            rset = orb.RecordSet()
            preload_cache[self.func_name] = rset
            setattr(record, '_Model__preload_cache', preload_cache)

        if type == 'ids':
            rset.cache('ids', data)
        elif type == 'count':
            rset.cache('count', data)
        elif type == 'first':
            rset.cache('first', view(__values=data, options=options) if data else None)
        elif type == 'last':
            rset.cache('last', view(__values=data, options=options) if data else None)
        else:
            rset.cache('records', [view(__values=record, options=options) for record in data or []])

    def viewFor(self, record):
        """
        Returns the view for the inputted record.

        :return     <orb.View>
        """
        return record.polymorphicModel(self.reference) or orb.system.model(self.reference, database=self.referenceDb)

# -----------------------------------------------------------------------------


class MetaView(type):
    """
    Defines the view Meta class that will be used to dynamically generate
    View class types.
    """

    def __new__(mcs, name, bases, attrs):
        """
        Manages the creation of database model classes, reading
        through the creation attributes and generating view
        schemas based on the inputted information.  This class
        never needs to be expressly defined, as any class that
        inherits from the View class will be passed through this
        as a constructor.

        :param      mcs         <MetaView>
        :param      name        <str>
        :param      bases       <tuple> (<object> base,)
        :param      attrs       <dict> properties

        :return     <type>
        """
        # ignore initial class
        db_ignore = attrs.pop('__db_ignore__', False)
        if db_ignore:
            return super(MetaView, mcs).__new__(mcs, name, bases, attrs)

        base_views = [base for base in bases if isinstance(base, MetaView)]
        base_data = {key: value for base_view in base_views
                     for key, value in base_view.__dict__.items() if key.startswith('__db_')}

        # define the default database information
        db_data = {
            '__db__': None,
            '__db_group__': 'Default',
            '__db_name__': name,
            '__db_viewname__': '',
            '__db_columns__': [],
            '__db_cached__': False,
            '__db_indexes__': [],
            '__db_pipes__': [],
            '__db_contexts__': {},
            '__db_views__': {},
            '__db_cache__': {},
            '__db_schema__': None,
            '__db_inherits__': None,
            '__db_abstract__': False,
            '__db_archived__': False,
            '__db_static__': False,
        }
        # override with any inherited data
        db_data.update(base_data)

        # override with any custom data
        db_data.update({key: value for key, value in attrs.items() if key.startswith('__db_')})

        if not db_data['__db_inherits__'] and base_views and base_views[0].schema():
            db_data['__db_inherits__'] = base_views[0].schema().name()

        # create a new model for this view
        return mcs.createModel(mcs, name, bases, attrs, db_data)

    @staticmethod
    def createModel(mcs, name, bases, attrs, db_data):
        """
        Create a new view model.
        """
        new_model = super(MetaView, mcs).__new__(mcs, name, bases, attrs)
        schema = db_data['__db_schema__']

        if schema:
            db_data['__db_name__'] = schema.name()
            db_data['__db_viewname__'] = schema.viewName()

            if db_data['__db_columns__']:
                columns = schema.columns(recurse=False) + db_data['__db_columns__']
                schema.setColumns(columns)
            if db_data['__db_indexes__']:
                indexes = schema.indexes() + db_data['__db_indexes__']
                schema.setIndexes(indexes)
            if db_data['__db_pipes__']:
                pipes = schema.pipes() + db_data['__db_pipes__']
                schema.setPipes(pipes)
            if db_data['__db_contexts__']:
                contexts = dict(schema.contexts())
                contexts.update(db_data['__db_contexts__'])
                schema.setContexts(contexts)
            if db_data['__db_views__']:
                for name, view in db_data['__db_views__'].items():
                    schema.setView(name, view)
        else:
            # create the view schema
            schema = orb.ViewSchema()
            schema.setDatabase(db_data['__db__'])
            schema.setAutoPrimary(False)
            schema.setName(db_data['__db_name__'] or name)
            schema.setGroupName(db_data['__db_group__'])
            schema.setDbName(db_data['__db_viewname__'])
            schema.setAbstract(db_data['__db_abstract__'])
            schema.setColumns(db_data['__db_columns__'])
            schema.setIndexes(db_data['__db_indexes__'])
            schema.setPipes(db_data['__db_pipes__'])
            schema.setInherits(db_data['__db_inherits__'])
            schema.setArchived(db_data['__db_archived__'])
            schema.setContexts(db_data['__db_contexts__'])
            schema.setStatic(db_data['__db_static__'])
            schema.setCacheEnabled(db_data['__db_cache__'].get('enabled', db_data['__db_cache__'].get('preload')))
            schema.setPreloadCache(db_data['__db_cache__'].get('preload'))
            schema.setCacheTimeout(db_data['__db_cache__'].get('timeout', 0))

            for name, view in db_data['__db_views__'].items():
                schema.setView(name, view)

            schema.setModel(new_model)

            orb.system.registerSchema(schema)

        db_data['__db_schema__'] = schema

        # add the db values to the class
        for key, value in db_data.items():
            setattr(new_model, key, value)

        # create class methods for the index instances
        for index in schema.indexes():
            iname = index.name()
            if not hasattr(new_model, iname):
                setattr(new_model, index.name(), classmethod(index))

        # create instance methods for the pipe instances
        for pipe in schema.pipes():
            pname = pipe.name()
            if not hasattr(new_model, pname):
                pipemethod = instancemethod(pipe, None, new_model)
                setattr(new_model, pname, pipemethod)

        # pylint: disable-msg=W0212
        columns = schema.columns(recurse=False)
        for column in columns:
            colname = column.name()

            # create getter method
            gname = column.getterName()
            if gname and not hasattr(new_model, gname):
                gmethod = gettermethod(columnName=colname,
                                       translatable=column.isTranslatable(),
                                       inflatable=column.isReference(),
                                       returns=column.returnType(),
                                       __name__=gname)

                getter = instancemethod(gmethod, None, new_model)
                setattr(new_model, gname, getter)

            # create setter method
            sname = column.setterName()
            if sname and not (column.isReadOnly() or hasattr(new_model, sname)):
                smethod = settermethod(columnName=colname,
                                       translatable=column.isTranslatable(),
                                       inflatable=column.isReference(),
                                       returns=column.returnType(),
                                       __name__=sname)
                setter = instancemethod(smethod, None, new_model)
                setattr(new_model, sname, setter)

            # create an index if necessary
            iname = column.indexName()
            if column.indexed() and iname and not hasattr(new_model, iname):
                index = orb.Index(iname,
                                  [column.name()],
                                  unique=column.unique())
                index.setCached(column.indexCached())
                index.setCacheTimeout(column.indexCacheTimeout())
                index.__name__ = iname
                imethod = classmethod(index)
                setattr(new_model, iname, imethod)

            # create a reverse lookup
            if column.isReversed() and column.schema().name() == db_data['__db_name__']:
                rev_name = column.reversedName()
                rev_cached = column.reversedCached()
                ref_name = column.reference()
                ref_model = column.referenceModel()
                rev_cacheTimeout = column.reversedCacheTimeout()

                # create the lookup method
                lookup = reverselookupmethod(columnName=column.name(),
                                             reference=db_data['__db_name__'],
                                             unique=column.unique(),
                                             cached=rev_cached,
                                             cacheTimeout=rev_cacheTimeout,
                                             __name__=rev_name)

                # ensure we're assigning it to the proper base module
                while ref_model and ref_model.__module__ != 'orb.schema.dynamic' and \
                        ref_model.__bases__ and ref_model.__bases__[0] == orb.View:
                    ref_model = ref_model.__bases__[0]

                # assign to an existing model
                if ref_model:
                    ilookup = instancemethod(lookup, None, ref_model)
                    setattr(ref_model, rev_name, ilookup)
                else:
                    TEMP_REVERSE_LOOKUPS.setdefault(ref_name, [])
                    TEMP_REVERSE_LOOKUPS[ref_name].append((rev_name, lookup))

        # assign any cached reverse lookups to this model
        lookups = TEMP_REVERSE_LOOKUPS.pop(db_data['__db_name__'], [])
        for rev_name, lookup in lookups:
            ilookup = instancemethod(lookup, None, new_model)
            setattr(new_model, rev_name, ilookup)

        return new_model