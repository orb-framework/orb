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

    def add_models_to_scope(self, scope, group=None, auto_generate=False):
        """
        Adds all of the models within this system to the given scope.  An
        optional keyword can be supplied to filter the models that are
        added to a scope down to a particular group.

        :param scope: <dict>
        :param group: <str> or None
        """
        models = self.models(group=group, auto_generate=auto_generate)
        scope.update(models)

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
        if code:
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
        model = models.get(code)
        if model is None:
            raise orb.errors.ModelNotFound(code)
        else:
            return model

    def models(self, base=None, group=None, database='', auto_generate=False):
        """
        Returns a collection of model classes found within this system.

        To filter sub-classes based on model, the optional `base` keyword can be provided.
        When used, only models that inherit from that base class will be returned.

        To filter models based on database associations, the `database` keyword can be specified.

        Systems that build from schemas and require model's to be generated can load models using
        the `auto_generate` keyword.

        :param base: subclass of <orb.Model> or None
        :param group: <str> or None
        :param database: <str>
        :param auto_generate: <bool>

        :return: {<str> code: subclass of <orb.Model>, ..}
        """
        output = {}
        for schema in self.__schemas.values():
            # filter based on group association
            if group is not None and schema.group() != group:
                continue

            # filter based on database associations
            if database and schema.database() and database != schema.database():
                continue

            model = schema.model(auto_generate=auto_generate)
            if not model:
                continue

            # filter based on subclassed model types
            elif base is not None and (not issubclass(model, base) or model is base):
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
            try:
                is_model = issubclass(obj, orb.Model)
            except StandardError:
                raise orb.errors.OrbError('Unknown object being registered')
            else:
                return self.register_schema(obj.schema(), force=force)

    def register_database(self, db, force=False):
        """
        Registers a database to the system.  If you would like to override an
        existing registered database, use `force=True`

        :param db: <orb.Database>
        :param force: <bool>
        """
        existing = self.__databases.get(db.code())
        if existing and existing is not db and not force:
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
        existing = self.__schemas.get(schema.name())
        if existing and existing is not schema and not force:
            raise orb.errors.DuplicateEntryFound('{0} is already a registered schema'.format(schema.name()))
        else:
            self.__schemas[schema.name()] = schema

    def schema(self, code):
        """
        Returns a schema by it's code name.

        :param code: <str>

        :return: <orb.Schema> or None
        """
        try:
            return self.__schemas[code]
        except KeyError:
            raise orb.errors.ModelNotFound(code)

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

    def unregister(self, obj=None):
        """
        Unregisters an object from this system.

        :param obj: <orb.Database> or <orb.Schema>
        """
        if obj is None:
            self.__active_database = None
            self.__databases.clear()
            self.__schemas.clear()
        elif isinstance(obj, orb.Database):
            self.unregister_database(obj)
        elif isinstance(obj, orb.Schema):
            self.unregister_schema(obj)
        else:
            try:
                is_model = issubclass(obj, orb.Model)
            except StandardError:
                is_model = False

            if not is_model:
                raise orb.errors.OrbError('Unknown object to unregister')
            else:
                self.unregister_schema(obj.schema())

    def unregister_database(self, db):
        """
        Unregisters the given database from the system.

        :param db: <orb.Database>
        """
        self.__databases.pop(db.code(), None)
        if db == self.__active_database:
            self.__active_database = db

    def unregister_schema(self, schema):
        """
        Unregisters the given schema from the system.

        :param schema: <orb.Schema>
        """
        print(self.__schemas.keys())
        self.__schemas.pop(schema.name(), None)
        print(self.__schemas.keys())

    @deprecated
    def init(self, scope):
        """ Using the `add_models_to_scope` method instead, more verbose. """
        return self.add_models_to_scope(scope)