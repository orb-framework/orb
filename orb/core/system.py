""" Defines an overall management class for all databases and schemas. """

import logging

from projex.lazymodule import lazy_import
from .settings import Settings

log = logging.getLogger(__name__)
orb = lazy_import('orb')
pytz = lazy_import('pytz')
tzlocal = lazy_import('tzlocal')


class System(object):
    def __init__(self):
        self.__databases = {}
        self.__schemas = {}
        self.__settings = Settings()
        self.__locale = None

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
        return self.__databases.get(code or self.__currentDatabase)

    def databases(self):
        """
        Returns the databases for this system.
        
        :return     {<str> name: <orb.Database>, ..}
        """
        return self.__databases

    def locale(self, context=None):
        """
        Returns the current locale that the system is going to be in.

        :param      context | <orb.ContextOptions>

        :return     <str>
        """
        if callable(self.__locale):
            return self.__locale(context)
        else:
            return self.__locale

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

    def register(self, obj):
        """
        Registers a particular database.
        
        :param      obj     | <orb.Database> || <orb.Schema>
        """
        if isinstance(obj, orb.Database):
            self.__databases[obj.code()] = obj
        elif isinstance(obj, orb.Schema):
            self.__schemas[obj.code()] = obj

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
        return self._settings

    def setLocale(self, locale):
        """
        Sets the locale that the orb file will be using.
        
        :param      locale | <str>
        """
        if locale:
            self.__locale = locale

