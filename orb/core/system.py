"""
Defines an overall management class for all databases and schemas.
"""

import demandimport

from ..decorators import deprecated
from ..settings import Settings

with demandimport.enabled():
    import orb


class System(object):
    """
    Class utilized for managing system state within an ORB application.
    """
    def __init__(self, **settings):
        # define private properties
        self.__active_database = None
        self.__databases = {}
        self.__schemas = {}

        # define public properties
        self.settings = Settings(**settings)

    def activate(self, db):
        """
        Activates the given database instance.

        :param db:  <orb.Database> or None
        """
        self.__active_database = db

    def add_models_to_scope(self, scope, group=None):
        """
        Adds all of the models within this system to the given scope.  An
        optional keyword can be supplied to filter the models that are
        added to a scope down to a particular group.

        :param scope: <dict>
        :param group: <str> or None
        """
        schemas = self.schemas().values()
        for schema in schemas:
            if group is None or schema.group() == group:
                scope[schema.name()] = schema.model()

    def database(self, code=None):
        """
        Returns the database whose code matches the given string, or the active database if
        no code is given.

        :usage:

            import orb
            orb.system.database()  # returns active database
            orb.system.database('my-database')  # returns the 'my-database' database

        :param code: <str> or None (default)

        :return: <orb.Database> or None
        """
        if code is not None:
            try:
                return self.__databases[code]
            except KeyError:
                raise orb.errors.DatabaseNotFound()
        else:
            return self.__active_database

    def databases(self):
        """
        Returns all of the registered databases associated with this system.
        
        :return: {<str> code: <orb.Database>, ..}
        """
        return self.__databases

    def model(self, code, auto_generate=False):
        """
        Returns the model class for the given code.  The optional keyword parameter
        `auto_generate` is used for systems that build schemas and need models to
        be generated before use.

        :param code: <str>
        :param auto_generate: <bool>

        :return: subclass of <orb.Model> or None
        """
        models = self.models(auto_generate=auto_generate)
        return models.get(code)

    def models(self, base=None, database='', auto_generate=False):
        """
        Returns a collection of model classes found within this system.

        To filter sub-classes based on model, the optional `base` keyword can be provided.
        When used, only models that inherit from that base class will be returned.

        To filter models based on database associations, the `database` keyword can be specified.

        Systems that build from schemas and require model's to be generated can load models using
        the `auto_generate` keyword.

        :param base: subclass of <orb.Model> or None
        :param database: <str>
        :param auto_generate: <bool>

        :return: {<str> code: subclass of <orb.Model>, ..}
        """
        output = {}
        for schema in self.__schemas.values():
            model = schema.model(auto_generate=auto_generate)
            if not model:
                continue

            # filter based on subclassed model types
            elif base is not None and (model == base or not issubclass(model, base)):
                continue

            # filter based on database associations
            elif database and model.schema().database() and model.schema().database() != database:
                continue

            else:
                output[schema.name()] = model

        return output

    def register(self, obj, force=False):
        """
        Registers an object into this system.  The object could be a database
        or schema.  The default behavior of this method is to raise a DuplicateEntryFound
        error if an oject is registered with the same name, but if you set `force` equal
        to True it will override the pre-existing registered object.

        :param obj: <orb.Database> or <orb.Schema>
        :param force: <bool>
        """
        if isinstance(obj, orb.Database):
            return self.register_database(obj, force=force)
        elif isinstance(obj, orb.Schema):
            return self.register_schema(obj, force=force)
        else:
            raise orb.errors.OrbError('Unknown object being registered')

    def register_database(self, db, force=False):
        """
        Registers a database to the system.  If you would like to override an
        existing registered database, use `force=True`

        :param db: <orb.Database>
        :param force: <bool>
        """
        if db.code() in self.__databases and not force:
            raise orb.errors.DuplicateEntryFound('{0} is already a registered database'.format(db.code()))
        else:
            self.__databases[db.code()] = db

    def register_schema(self, schema, force=False):
        """
        Registers a schema to the system.  If you would like to override an
        existing registered schema, use `force=True`

        :param schema: <orb.Schema>
        :param force: <bool>
        """
        if schema.name() in self.__databases and not force:
            raise orb.errors.DuplicateEntryFound('{0} is already a registered schema'.format(schema.name()))
        else:
            self.__schemas[schema.name()] = schema

    def schema(self, code):
        """
        Returns a schema by it's code name.

        :param code: <str>

        :return: <orb.Schema> or None
        """
        return self.__schemas.get(code)

    def schemas(self, group=None, database=None):
        """
        Returns a collection of all the schemas associated with this system.

        :param group: <str> or None
        :param database: <str> or None
        
        :return     {<str> code: <orb.Schema>, ..}
        """
        if group or database:
            return {
                schema.name(): schema
                for schema in self.__schemas.values()
                if ((group is None or schema.group() == group) and
                    (database is None or schema.database() == database))
            }
        else:
            return self.__schemas

    def unregister(self, obj):
        """
        Unregisters an object from this system.

        :param obj: <orb.Database> or <orb.Schema>
        """
        if isinstance(obj, orb.Database):
            self.unregister_database(obj)
        elif isinstance(obj, orb.Schema):
            self.unregister_schema(obj)
        else:
            raise orb.errors.OrbError('Unknown object to unregister')

    def unregister_database(self, db):
        """
        Unregisters the given database from the system.

        :param db: <orb.Database>
        """
        self.__databases.pop(db.code(), None)

    def unregister_schema(self, schema):
        """
        Unregisters the given schema from the system.

        :param schema: <orb.Schema>
        """
        self.__schemas.pop(schema.name(), None)

    @deprecated
    def init(self, scope):
        """ Using the `add_models_to_scope` method instead, more verbose. """
        return self.add_models_to_scope(scope)