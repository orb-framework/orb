#!/usr/bin/python

""" Defines an overall management class for all environments, databases, 
    and schemas. """

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
import glob
import logging
import os
import projex.security
import projex.text
import weakref

from collections import OrderedDict
from projex.callbacks import CallbackSet
from projex.lazymodule import LazyModule
from projex.text import nativestring as nstr
from xml.etree import ElementTree
from  xml.parsers.expat import ExpatError

from .settings import Settings

log = logging.getLogger(__name__)
orb = LazyModule('orb')
pytz = LazyModule('pytz')
tzlocal = LazyModule('tzlocal')


class Manager(object):
    _instance = None
    
    def __init__(self):
        # system wide options
        self._environment = None    # current environment
        self._database = None       # current database
        self._tableclass = None     # base table class (default: orb.Table)
        self._namespace = ''        # current namespace
        self._cacheExpired = datetime.datetime.now()
        self._searchEngine = orb.SearchEngine()
        
        # orb file loading/merging
        self._filename = ''         # current filename (*.orb file)
        self._referenceFiles = []   # referenced files (*.orb files)
        self._merging = False       # wether or not we're actively merging
        
        # settings instance
        self._settings = Settings()
        
        # language options
        self._language = os.environ.get('ORB_LANGUAGE', 'en_US')
        self._timezone = None
        
        # registry
        self._callbacks     = CallbackSet()
        self._environments  = {}
        self._groups        = {}
        self._databases     = {}
        self._schemas       = {}
        self._properties    = {}
        self._customEngines = {}
    
    def baseTableType(self):
        """
        Returns the base table type that all other tables will inherit from.
        By default, the orb.Table instance will be used, however, the developer
        can provide their own base table using the setBaseTableType method.
        
        :return     <subclass of Table>
        """
        if not self._tableclass:
            return orb.Table
        return self._tableclass
    
    def clear(self):
        """
        Clears out all the current data from this orb instance.
        """
        self._environment = None
        self._database = None
        
        # close any active connections
        for db in self._databases.values():
            db.disconnect()
        
        # close any active environments
        for env in self._environments.values():
            env.clear()
        
        self._filename = ''
        self._referenceFiles = []
        self._groups.clear()
        self._environments.clear()
        self._databases.clear()
        self._schemas.clear()
        self._properties.clear()
    
    def clearCache(self):
        """
        Force clears all the cached data from the various schemas.
        """
        for schema in self.schemas():
            model = schema.model()
            if not (model and schema.isCacheEnabled()):
                continue
            model.recordCache().clear()
    
    def clearCallbacks(self, callbackType=None):
        """
        Clears out the callbacks globally or for the given callback type.
        
        :param      callbackType | <orb.CallbackType>
        """
        self._callbacks.clear(callbackType)
    
    def customEngines(self, databaseType):
        """
        Returns a list of the custom engines for the database type.
        
        :param      databaseType | <str>
        
        :return     [(<ColumnType> typ, <subclass of CommandEngine> eng), ..]
        """
        try:
            return self._customEngines[databaseType].items()
        except KeyError, AttributeError:
            return []
    
    def database(self, name=None, environment=None):
        """
        Returns the database for this manager based on the inputed name. \
        If no name is supplied, then the currently active database is \
        returned.  If the environment variable is specified then the \
        database lookup will occur in the specific environment, otherwise \
        the active environment is used.
        
        :usage      |>>> import orb
                    |>>> orb.system.database() # returns active database
                    |>>> orb.system.database('User') # returns the User db
                    |>>> orb.system.database('User', 'Debug') # from Debug
        
        :param      name | <str> || None
                    environment | <str> || <orb.environment.Environment> || None
        
        :return     <orb.database.Database> || None
        """
        # if no name is given, then return the current database
        if not name:
            return self._database
        
        # otherwise, if the environment is provided, then use the environments
        # database for the given name
        if environment is None:
            environment = self._environment
        elif not isinstance(environment, orb.Environment):
            environment = self.environment(environment)
        
        if environment:
            db = environment.database(name)
        else:
            db = None
        
        if not db:
            db = self._databases.get(nstr(name))
        
        return db
    
    def databases(self, recursive=False):
        """
        Returns the databases for this system.  If the recursive flag is \
        set, then all databases defined by all environments will also be \
        returned.
        
        :return     [<orb.database.Database>, ..]
        """
        output = self._databases.values()
        
        if recursive:
            for env in self.environments():
                output += env.databases()
        
        return output
    
    def databaseSchemas(self, db):
        """
        Returns a list of schemas that are mapped to the inputed database.
        
        :param      db | <orb.database.Database>
        """
        is_curr = db == self._database
        
        out = []
        for schema in self._schemas.values():
            if not schema.databaseName() and is_curr or \
               schema.databaseName() == db.name():
                out.append(schema)
        
        return out
    
    def environment(self, name=None):
        """
        Returns the environment for this manager based on the inputed name. \
        If no name is supplied, then the currently active environment is \
        returned.
        
        :param      name | <str> || None
        
        :return     <orb.environment.Environment> || None
        """
        if name:
            return self._environments.get(nstr(name))
        return self._environment
    
    def environments(self):
        """
        Returns a list of all the environments that are used by this orb \
        instance.
        
        :return     [<orb.environment.Environment>, ..]
        """
        return self._environments.values()
    
    def findRelatedColumns(self, schema):
        """
        Looks up all the related columns and tables for the inputed table \
        schema.
        
        :param      schema | <orb.tableschema.TableSchema>
        """
        names           = [schema.name()] + schema.inheritsRecursive()
        related_columns = []
        
        for table_schema in self._schemas.values():
            for column in table_schema.columns():
                if column in related_columns:
                    continue
                    
                if column.reference() in names:
                    related_columns.append(column)
        
        return related_columns
    
    def findRelations(self, schema):
        """
        Looks up all the related columns and tables for the inputed table \
        schema.
        
        :param      schema | <orb.tableschema.TableSchema>
        """
        names       = [schema.name()] + schema.inheritsRecursive()
        relations   = []
        processed   = []
        
        for table_schema in self._schemas.values():
            rel_cols = []
            
            for column in table_schema.columns():
                if column in processed:
                    continue
                    
                if column.reference() in names:
                    rel_cols.append(column)
                    processed.append(column)
            
            if rel_cols:
                relations.append((table_schema.model(), rel_cols))
        
        return relations
    
    def filename(self):
        """
        Returns the filename linked with this orb manager.  This property will \
        be set by the load and save methods.
        
        :return     <str>
        """
        return self._filename
    
    def group(self, name, autoAdd=False, database=None):
        """
        Returns a group based on the inputed name.
        
        :param      name     | <str>
                    autoAdd  | <bool>
                    database | <str> || None
        
        :return     <orb.TableGroup> || None
        """
        use_generic = False
        if database is None:
            use_generic = True
            curr_db = self.database()
            if curr_db:
                database = curr_db.name()
            else:
                database = ''
        
        # look for direct access
        key = (database, nstr(name))
        if key in self._groups:
            return self._groups[key]
        
        # look for generic access
        if use_generic:
            for key, group in self._groups.items():
                if key[1] == name:
                    return group
        
        if autoAdd:
            grp = orb.TableGroup(nstr(name))
            grp.setDatabaseName(database)
            grp.setOrder(len(self._groups))
            self._groups[(database, grp.name())] = grp
            return grp
            
        return None
    
    def groups(self, database=None):
        """
        Returns a list of all the registered groups for this orb instance.
        
        :return     [<orb.TableGroup>, ..]
        """
        if database is None:
            out = self._groups.values()
            out.sort(key = lambda x: x.order())
            return out
        
        return [grp for key, grp in self._groups.items() if key[0] == database]
    
    def inheritedModels(self, model):
        """
        Returns any models that inherit from the inputed moddel.
        
        :return     [<orb.tableschema.Table>, ..]
        """
        out = []
        for schema in self._schemas.values():
            smodel = schema.model()
            
            if model == smodel:
                continue
            
            if smodel and issubclass(smodel, model):
                out.append(smodel)
        return out
    
    def isCacheExpired(self, cachetime):
        """
        Returns whether or not the cache is expired against the global
        cache time.
        
        :param      cachetime | <datetime.datetime>
        """
        return cachetime < self._cacheExpired
    
    def isCachingEnabled(self):
        """
        Returns whether or not global caching will exist for the system.
        
        :return     <bool>
        """
        return self.settings().isCachingEnabled()
    
    def language(self):
        """
        Returns the current language that the system is going to be in.
        
        :return     <str>
        """
        return self._language

    def load(self, filename = '', includeReferences=False):
        """
        Loads the settings for this orb manager from the inputed xml file.
        
        :param      filename | <str>
        
        :return     <bool> | success
        """
        if not filename:
            filename = self.filename()
        
        if not (filename and os.path.exists(filename)):
            log.error('Invalid ORB file: %s' % filename)
            return False
        
        self.clear()
        if not self.merge(filename, includeReferences=includeReferences):
            return False
            
        self._filename = nstr(filename)
        return True
    
    def loadModels(self,
                   scope,
                   groupName=None,
                   autoGenerate=True,
                   schemas=None,
                   database=None):
        """
        Loads the models from the orb system into the inputed scope.
        
        :param      scope        | <dict>
                    groupName    | <str> || None
                    autoGenerate | <bool>
                    schemas      | [<orb.TableSchema>, ..] || None
                    database     | <str> || None
        """
        # ensure we have a valid scope to load
        if scope is None:
            return []
        
        # ensure we have schemas to load
        if schemas is None:
            schemas = self.schemas(database)
        
        # load models for the schemas
        added = []
        for schema in schemas:
            # ensure the desired group name is what we are loading
            if groupName is not None and schema.groupName() != groupName:
                continue
            
            model = schema.model(autoGenerate=autoGenerate)
            scope[model.__name__] = model
            added.append(schema.name())
        
        return added
    
    def maxCacheTimeout(self):
        """
        Returns the maximum cache timeout allowed (in minutes) for a cache.
        
        :return     <int>
        """
        return self.settings().maxCacheTimeout()
    
    def markCacheExpired(self):
        """
        Marks the current time as the time the global cache system expired.
        """
        self._cacheExpired = datetime.datetime.now()
    
    def merge(self,
              filename_or_xml,
              includeReferences=False,
              referenced=False,
              dereference=False,
              database=None ):
        """
        Merges the inputed ORB file to the schema.
        
        :param      filename_or_xml   | <str> || <xml.etree.ElementTree.Element>
                    includeReferences | <bool>
                    referenced        | <bool> | flags the schemas as being referenced
                    database          | <str> || None
        
        :return     [<orb.TableSchema>, ..]
        """
        if dereference:
            referenced = False
        
        if not isinstance(filename_or_xml, ElementTree.Element):
            filename = nstr(filename_or_xml)
            xorb = None
        else:
            filename = ''
            xorb = filename_or_xml
        
        output   = []
        
        # load a directory of ORB files
        if filename and os.path.isdir(filename):
            for orbfile in glob.glob(os.path.join(filename, '*.orb')):
                output += self.merge(orbfile,
                                     includeReferences,
                                     referenced,
                                     dereference=dereference,
                                     database=database)
            return output
        
        # ensure the orb file exists
        if xorb is None and not os.path.exists(filename):
            return output
        
        # load a reference system
        if filename and referenced:
            # only load reference files once
            if filename in self._referenceFiles:
                return []
            
            self._referenceFiles.append(filename)
        
        if filename and xorb is None:
            try:
                xorb = ElementTree.parse(filename).getroot()
            except (ExpatError, ElementTree.ParseError):
                xorb = None
        
            # check for encrypted files
            if xorb is None:
                f = open(filename, 'r')
                data = f.read()
                f.close()
                
                # try unencrypting the data
                unencrypted = projex.security.decrypt(data, useBase64=True)
                try:
                    xorb = ElementTree.fromstring(unencrypted)
                
                except (ExpatError, ElementTree.ParseError):
                    log.exception('Failed to load ORB file: %s' % filename)
                    return []
        
        output = []
        
        # load references
        if includeReferences:
            xrefs = xorb.find('references')
            if xrefs is not None:
                for xref in xrefs:
                    ref_path = xref.get('path').replace('\\', '/')
                    ref_path = os.path.join(filename, ref_path)
                    ref_path = os.path.normpath(ref_path)
                    ref_path = os.path.abspath(ref_path)
                    output += self.merge(ref_path,
                                         includeReferences=True,
                                         referenced=True,
                                         dereference=dereference,
                                         database=database)
        
        # load properties
        xprops = xorb.find('properties')
        if xprops is not None:
            for xprop in xprops:
                self.setProperty(xprop.get('key'), xprop.get('value'))
            
        # load environments
        xenvs = xorb.find('environments')
        if xenvs is not None:
            for xenv in xenvs:
                env = orb.Environment.fromXml(xenv, referenced)
                self.registerEnvironment(env, env.isDefault())
        
        # load databases
        xdbs = xorb.find('databases')
        if xdbs is not None:
            for xdb in xdbs:
                db = orb.Database.fromXml(xdb, referenced)
                self.registerDatabase(db, db.isDefault())
                
        # load schemas
        xgroups = xorb.find('groups')
        if xgroups is not None:
            for xgroup in xgroups:
                grp, schemas = orb.TableGroup.fromXml(xgroup,
                                                      referenced,
                                                      database=database)
                if not grp:
                    continue
                
                self.registerGroup(grp)
                output += schemas
        
        return output
    
    def model(self, name, autoGenerate=False, database=None):
        """
        Looks up a model class from the inputed name.
        
        :param      name | <str>
                    autoGenerate | <bool>
        
        :return     <subclass of Table> || <orb.Table> || None
        """
        schema = self.schema(name, database)
        # define a model off an existing schema
        if schema:
            return schema.model(autoGenerate=autoGenerate)
        
        # generate a blank table
        elif autoGenerate:
            log.warning('Could not find a schema for %s' % name)
            return orb.Table
        
        return None
    
    def models(self, database=None):
        """
        Returns a list of all the models that have been defined within the
        manager's scope.
        
        :return     [<subclass of orb.Table>, ..]
        """
        models = [schema.model() for schema in self.schemas(database)]
        return filter(lambda m: m is not None, models)
    
    def namespace(self):
        """
        Returns the current namespace that the system should be operating in.
        
        :return     <str>
        """
        return self._namespace
    
    def property(self, propname, default=''):
        """
        Returns the property value for this manager from the given name.  \
        If no property is set, then the default value will be returned.
        
        :return     <str>
        """
        return self._properties.get(propname, nstr(default))
    
    def runCallback(self, callbackType, *args):
        """
        Runs the callbacks for this system of the given callback type.
        
        :param      callbackType | <orb.CallbackType>
                    *args        | arguments to be supplied to registered
        """
        self._callbacks.emit(callbackType, *args)
            
    def registerCallback(self, callbackType, callback):
        """
        Registers the inputed method as a callback for the given type.
        Callbacks get thrown at various times through the ORB system to allow
        other APIs to hook into information changes that happen.  The 
        callback type will be defined as one of the values from the
        CallbackType enum in the common module, and the specific arguments
        that will be called will change on a per-callback basis.  The callback
        itself will be registered as a weak-reference so that if it gets
        collected externally, the internal cache will not break - however this
        means you will need to make sure your method is persistent as long as
        you want the callback to be run.
        
        :param      callbackType | <orb.CallbackType>
                    callback     | <method> || <function>
        """
        self._callbacks.connect(callbackType, callback)
        
    def registerDatabase(self, database, active=True):
        """
        Registers a particular database with this environment.
        
        :param      database | <orb.database.Database>
                    active   | <bool>
        """
        self._databases[database.name()] = database
        if active or not self._database:
            self._database = database
    
    def registerEnvironment(self, environment, active=False):
        """
        Registers a particular environment with this environment.
        
        :param      database | <orb.environment.Environment>
                    active | <bool>
        """
        self._environments[environment.name()] = environment
        
        # set the active environment
        if active or not self._environment:
            self.setEnvironment(environment)
    
    def registerGroup(self, group, database=None):
        """
        Registers the inputed orb group to the system.
        
        :param      group | <orb.TableGroup>
        """
        if database is None:
            database = group.databaseName()
        
        group.setOrder(len(self._groups))
        self._groups[(database, group.name())] = group
        for schema in group.schemas():
            self._schemas[(database, nstr(schema.name()))] = schema
    
    def registerSchema(self, schema, database=None):
        """
        Registers the inputed schema with the environment.
        
        :param      schema | <orb.tableschema.TableSchema>
        """
        if database is None:
            database = schema.databaseName()
        
        grp = self.group(schema.groupName(), autoAdd=True, database=database)
        grp.addSchema(schema)
        
        self._schemas[(database, schema.name())] = schema
    
    def save(self, encrypted=False):
        """
        Saves the current orb structure out to a file.  The filename will be \
        based on the currently set name.
        
        :param      encrypted | <bool>
        
        :sa     saveAs
        
        :return     <bool>
        """
        return self.saveAs(self.filename(), encrypted = encrypted)
    
    def saveAs(self, filename, encrypted=False):
        """
        Saves the current orb structure out to the inputed file.
        
        :param      filename | <str>
                    encrypted | <bool>
        
        :return     <bool> | success
        """
        if not filename:
            return False
        
        filename = nstr(filename)
        xorb = ElementTree.Element('orb')
        xorb.set('version', orb.__version__)
        
        # save out references
        xrefs = ElementTree.SubElement(xorb, 'references')
        for ref_file in self._referenceFiles:
            rel_path = os.path.relpath(ref_file, filename)
            xref = ElementTree.SubElement(xrefs, 'reference')
            xref.set('path', rel_path)
        
        # save out properties
        xprops = ElementTree.SubElement(xorb, 'properties')
        for key, value in self._properties.items():
            xprop = ElementTree.SubElement(xprops, 'property')
            xprop.set('key', key)
            xprop.set('value', value)
        
        # save out the environments
        xenvs = ElementTree.SubElement(xorb, 'environments')
        for env in self.environments():
            if not env.isReferenced():
                env.toXml(xenvs)
        
        # save out the global databases
        xdbs = ElementTree.SubElement(xorb, 'databases')
        for db in self.databases():
            if not db.isReferenced():
                db.toXml(xdbs)
        
        # save out the groups
        xgroups = ElementTree.SubElement(xorb, 'groups')
        for grp in self.groups():
            grp.toXml(xgroups)
        
        projex.text.xmlindent(xorb)
        data = ElementTree.tostring(xorb)
        
        if encrypted:
            data = projex.security.encrypt(data, useBase64=True)
        
        f = open(filename, 'w')
        f.write(data)
        f.close()
        
        return True
    
    def schema(self, name, database=None):
        """
        Looks up the registered schemas for the inputed schema name.
        
        :parma      name     | <str>
                    database | <str> || None
        
        :return     <orb.tableschema.TableSchema> || None
        """
        use_generic = False
        if database is None:
            use_generic = True
            curr_db = self.database()
            if curr_db:
                database = curr_db.name()
            else:
                database = ''
        
        # look for direct access
        key = (database, nstr(name))
        if key in self._schemas:
            return self._schemas[key]
        
        # look for generic access
        if use_generic:
            for key, schema in self._schemas.items():
                if key[1] == name:
                    return schema
        
        return None
    
    def schemas(self, database=None):
        """
        Returns a list of all the schemas for this instance.
        
        :return     [<orb.tableschema.TableSchema>, ..]
        """
        if database is None:
            return self._schemas.values()
        
        return [schema for key, schema in self._schemas.items() \
                if key[0] == database]
    
    def searchEngine(self):
        """
        Returns the search engine that will be used for this system.
        
        :return     <orb.SearchEngine>
        """
        return self._searchEngine
    
    def searchThesaurus(self):
        """
        Returns the search thesaurus associated with this manager's instance.
        This will help drive synonyms that users will use while searching
        the database.  Thesauruses can be defined at a per class level,
        and at the API as a whole.
        
        :return     <orb.SearchThesaurus>
        """
        return self._searchEngine.thesaurus()
    
    def settings(self):
        """
        Returns the settings instance associated with this manager.
        
        :return     <orb.Settings>
        """
        return self._settings
    
    def setBaseTableType(self, tableType):
        """
        Sets the base table type that all other tables will inherit from.
        By default, the orb.Table instance will be used, however, the developer
        can provide their own base table using the setBaseTableType method.
        
        :param      tableType | <subclass of Table> || None
        """
        self._tableclass = tableType
    
    def setCachingEnabled(self, state):
        """
        Sets globally whether or not to allow caching.
        
        :param      state | <bool>
        """
        self.settings().setCachingEnabled(state)
    
    def setCustomEngine(self, databaseType, columnType, engineClass):
        """
        Returns a list of the custom engines for the database type.
        
        :param      databaseType | <str>
                    columnType   | <orb.ColumnType>
                    engineClass  | <subclass of CommandEngine>
        """
        self._customEngines.setdefault(databaseType, {})
        self._customEngines[databaseType][columnType] = engineClass
    
    def setDatabase(self, database, environment=None):
        """
        Sets the active database to the inputed database.
        
        :param      database | <str> || <orb.database.Database> || None
                    environment | <str> || <orb.environment.Environment> || None
        """
        if not isinstance(database, orb.Database):
            database = self.database(database, environment)
            
        self._database = database
    
    def setLanguage(self, lang_code):
        """
        Sets the language that the orb file will be using.
        
        :param      lang_code | <str>
        """
        if lang_code:
            self._language = lang_code

    def setEnvironment(self, environment):
        """
        Sets the active environment to the inputed environment.
        
        :param      environment | <str> || <orb.environment.Environment> || None
        """
        if not isinstance(environment, orb.Environment):
            environment = self.environment(environment)
        
        self._environment = environment
        self.setDatabase(environment.defaultDatabase())
    
    def setMaxCacheTimeout(self, minutes):
        """
        Sets the maximum cache timeout allowed (in minutes) for a cache.
        
        :param      minutes | <int>
        """
        self.settings().setMaxCacheTimeout(minutes)
    
    def setModel(self, name, model, database=None):
        """
        Sets the model class for the inputed schema to the given model.
        
        :param      name    | <str>
                    model   | <subclass of Table>
        
        :return     <bool> | success
        """
        if database is None:
            try:
                database = model.databaseName()
            except AttributeError:
                database = ''
        
        schema = model.schema()
    
        # replace out the old schema model with the new one
        # for any other classes that have already been generated
        old_model = schema.model()
        
        # define a method to lookup the base model
        def find_model(check, base):
            if not check:
                return None
            if base in check.__bases__:
                return check
            for base_model in check.__bases__:
                output = find_model(base_model, base)
                if output:
                    return output
            return None
        
        # look for the old base model
        for other_model in self.models():
            if other_model == old_model:
                continue
            
            # replace the sub-class models
            if other_model and issubclass(other_model, old_model):
                # retrieve the root class that inherits from our old one
                other_model = find_model(other_model, old_model)
                new_bases   = list(other_model.__bases__)
                index       = new_bases.index(old_model)
                new_bases.remove(old_model)
                new_bases.insert(index, model)
                other_model.__bases__ = tuple(new_bases)
                
                log.info('Replaced %s base with %s', 
                            other_model, model)
        
        schema.setModel(model)
        return True
    
    def setNamespace(self, namespace):
        """
        Sets the namespace that will be used for this system.
        
        :param      namespace | <str>
        """
        self._namespace = namespace
    
    def setProperty(self, property, value):
        """
        Sets the custom property to the inputed value.
        
        :param      property | <str>
                    value    | <str>
        """
        self._properties[nstr(property)] = nstr(value)
    
    def setSearchEngine(self, engine):
        """
        Sets the search engine that will be used for this system.
        
        :param      engine | <orb.SearchEngine>
        """
        self._searchEngine = engine
    
    def setSearchThesaurus(self, thesaurus):
        """
        Returns the search thesaurus associated with this manager's instance.
        This will help drive synonyms that users will use while searching
        the database.  Thesauruses can be defined at a per class level,
        and at the API as a whole.
        
        :param          thesaurus | <orb.SearchThesaurus>
        """
        self._searchEngine.setThesaurus(thesaurus)
        self.markCacheExpired()
    
    def setTimezone(self, timezone):
        """
        Sets the timezone for the system to the inptued zone.  This will
        affect how the date time information for UTC data will be returned
        and formatted from the database.
        
        Timezone support requires the pytz package to be installed.
        
        :sa     http://pytz.sourceforge.net/
                https://pypi.python.org/pypi/tzlocal
        
        :param      timezone | <pytz.tzfile>
        """
        self._timezone = timezone
    
    def timezone(self):
        """
        Returns the timezone for the system.  This will affect how the
        date time information will be returned and formatted from the database.
        
        Timezone support requires the pytz package to be installed.  For
        auto-default timezone based on computer settings, you also need to
        make sure that the tzlocal package is installed.
        
        :sa     http://pytz.sourceforge.net/
                https://pypi.python.org/pypi/tzlocal
        
        :return     <pytz.tzfile> || None
        """
        if self._timezone is None:
            default = self.settings().defaultTimezone()
            if default:
                try:
                    self._timezone = pytz.timezone(default)
                except ImportError:
                    log.error('pytz must be installed for timezone support.')
            else:
                try:
                    self._timezone = tzlocal.get_localzone()
                except ImportError:
                    log.error('tzlocal must be installed for local zone support.')
        
        return self._timezone
    
    def unregisterCallback(self, callbackType, callback):
        """
        Unegisters the inputed method as a callback for the given type.
        Callbacks get thrown at various times through the ORB system to allow
        other APIs to hook into information changes that happen.  The 
        callback type will be defined as one of the values from the
        CallbackType enum in the common module, and the specific arguments
        that will be called will change on a per-callback basis.  The callback
        itself will be registered as a weak-reference so that if it gets
        collected externally, the internal cache will not break - however this
        means you will need to make sure your method is persistent as long as
        you want the callback to be run.
        
        :param      callbackType | <orb.CallbackType>
                    callback     | <method> || <function>
        """
        return self._callbacks.disconnect(callbackType, callback)
    
    def unregisterDatabase(self, database):
        """
        Un-registers a particular database with this environment.
        
        :param      database | <orb.database.Database>
        """
        if database.name() in self._databases:
            database.disconnect()
            self._databases.pop(database.name())
    
    def unregisterGroup(self, group, database=None):
        """
        Un-registers the inputed orb group to the system.
        
        :param      group | <orb.TableGroup>
        """
        if database is None:
            database = group.databaseName()
        
        key = (database, group.name())
        if not key in self._groups:
            return
        
        self._groups.pop(key)
        for schema in group.schemas():
            self._schemas.pop((database, nstr(schema.name())), None)
        
    def unregisterEnvironment(self, environment):
        """
        Un-registers a particular environment with this environment.
        
        :param      database | <orb.environment.Environment>
        """
        if environment.name() in self._environments:
            self._environments.pop(environment.name())
    
    def unregisterSchema(self, schema, database=None):
        """
        Un-registers the inputed schema with the environment.
        
        :param      schema | <orb.tableschema.TableSchema>
        """
        if database is None:
            database = schema.databaseName()
        
        key = (database, schema.name())
        if key in self._schemas:
            grp = self.group(schema.groupName(), database=database)
            grp.removeSchema(schema)
            
            self._schemas.pop(key)
    
    @staticmethod
    def databaseTypes():
        """
        Returns a list of all the database types (Connection backends) that are
        available for the system.
        
        :return     [<str>, ..]
        """
        return orb.Connection.addons().keys()
    
    @staticmethod
    def instance():
        """
        Returns the global instance of the Manager.
        
        :return     <orb.Manager>
        """
        if not Manager._instance:
            Manager._instance = Manager()
            
        return Manager._instance
    
    @staticmethod
    def printHierarchy(obj):
        """
        Prints the heirarchy for the inputed class to show the inheritance
        information.
        
        :param      obj | <class>
        """
        bases = []
        def collect_bases(subcls, indent=''):
            out = [(indent + subcls.__name__, subcls.__module__)]
            for base in subcls.__bases__:
                out += collect_bases(base, indent + '-')
            return out
        
        bases = collect_bases(obj)
        for base in bases:
            print '{:<40s}{:>70s}'.format(*base)
    
    @staticmethod
    def quickinit(filename, scope=None):
        """
        Loads the settings for the orb system from the inputed .orb filename. \
        If the inputed scope variable is passed in, then the scope will be \
        updated with the models from the system.
        
        :param      filename | <str>
                    scope | <bool>
        
        :return     <bool> | success
        """
        mgr = Manager.instance()
        
        # clear the current information
        mgr.clear()
        
        # load the new information
        if not mgr.load(filename, includeReferences=True):
            return False
        
        # update the scope with the latest data
        mgr.loadModels(scope)
        return True
