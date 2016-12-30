""" Defines an overall management class for all databases and schemas. """

import logging

from projex.lazymodule import lazy_import
from ..settings import Settings

log = logging.getLogger(__name__)
orb = lazy_import('orb')
pytz = lazy_import('pytz')

from .security import Security


class System(object):
    def __init__(self):
        self.__current_db = None
        self.__databases = {}
        self.__schemas = {}
        self.__settings = Settings()
        self.__security = Security(self.__settings.security_key)

    def activate(self, db):
        """
        Sets the currently active database instance.

        :param db:  <orb.Database> || None
        """
        self.__current_db = db

    def database(self, code=''):
        """
        Returns the database for this manager based on the inputted name. \
        If no name is supplied, then the currently active database is \
        returned.
        
        :usage      |>>> import orb
                    |>>> orb.system.database() # returns active database
                    |>>> orb.system.database('User') # returns the User db
                    |>>> orb.system.database('User', 'Debug') # from Debug
        
        :param      name | <str> || None

        :return     <orb.database.Database> || None
        """
        return self.__databases.get(code) or self.__current_db

    def databases(self):
        """
        Returns the databases for this system.
        
        :return     {<str> name: <orb.Database>, ..}
        """
        return self.__databases

    def init(self, scope):
        """
        Loads the models from the orb system into the inputted scope.
        
        :param      scope        | <dict>
                    autoGenerate | <bool>
                    schemas      | [<orb.TableSchema>, ..] || None
                    database     | <str> || None
        """
        schemas = self.schemas().values()
        for schema in schemas:
            scope[schema.name()] = schema.model()

    def register(self, obj, force=False):
        """
        Registers a particular database.
        
        :param      obj     | <orb.Database> || <orb.Schema>
        """
        if isinstance(obj, orb.Database):
            scope = self.__databases
            key = obj.code()
        elif isinstance(obj, orb.Schema):
            scope = self.__schemas
            key = obj.name()
        else:
            raise orb.errors.OrbError('Unknown object to register: {0}'.format(obj))

        try:
            existing = self.__schemas[obj.name()]
        except KeyError:
            pass
        else:
            if existing != obj and not force:
                raise orb.errors.DuplicateEntryFound('{0} is already a registered {1}.'.format(key, typ))

        scope[key] = obj
        return True

    def model(self, code, autoGenerate=False):
        return self.models(autoGenerate=autoGenerate).get(code)

    def models(self, base=None, database='', autoGenerate=False):
        output = {}
        for schema in self.__schemas.values():
            model = schema.model(autoGenerate=autoGenerate)
            if (model and
                (base is None or (issubclass(model, base) and model != base)) and
                (not database or not model.schema().database() or database == model.schema().database())):
                output[schema.name()] = model
        return output

    def schema(self, code):
        """
        Looks up the registered schemas for the inputted schema name.
        
        :param      name     | <str>
                    database | <str> || None
        
        :return     <orb.tableschema.TableSchema> || None
        """
        return self.__schemas.get(code)

    def schemas(self):
        """
        Returns a list of all the schemas for this instance.
        
        :return     {<str> code: <orb.Schema>, ..}
        """
        return self.__schemas

    def settings(self):
        """
        Returns the settings instance associated with this manager.
        
        :return     <orb.Settings>
        """
        return self.__settings

    def security(self):
        return self.__security

    def setSecurity(self, security):
        self.__security = security

    def unregister(self, obj=None):
        """
        Unregisters the object from the system.  If None is supplied, then
        all objects will be unregistered

        :param obj: <str> or <orb.Database> or <orb.Schema> or None
        """
        if obj is None:
            self.__databases.clear()
            self.__schemas.clear()
        elif isinstance(obj, orb.Schema):
            self.__schemas.pop(obj.name(), None)
        elif isinstance(obj, orb.Database):
            if obj == self.__current_db:
                self.__current_db = None
            self.__databases.pop(obj.name(), None)
        else:
            self.__current_db = None
            self.__schemas.pop(obj, None)
            self.__databases.pop(obj, None)