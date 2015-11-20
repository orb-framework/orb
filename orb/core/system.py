""" Defines an overall management class for all databases and schemas. """

import logging

from projex.lazymodule import lazy_import
from ..settings import Settings

log = logging.getLogger(__name__)
orb = lazy_import('orb')
pytz = lazy_import('pytz')
tzlocal = lazy_import('tzlocal')

from .security import Security


class System(object):
    def __init__(self):
        self.__current_db = None
        self.__databases = {}
        self.__schemas = {}
        self.__settings = Settings()
        self.__locale = None
        self.__syntax = None
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

    def locale(self, context=None):
        """
        Returns the current locale that the system is going to be in.

        :param      context | <orb.Context>

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

    def generateModel(self, schema):
        if self._model:
            return self._model

        # generate the base models
        if self.inherits():
            inherits = self.inherits()
            inherited = orb.system.schema(inherits)

            if not inherited:
                raise orb.errors.ModelNotFound(inherits)
            else:
                base = inherited.model(autoGenerate=True)

            if base:
                bases = [base]
            else:
                bases = [orb.system.baseTableType()]
        else:
            bases = [orb.system.baseTableType()]

        # generate the attributes
        attrs = {'__db_schema__': self, '__module__': 'orb.schema.dynamic'}
        grp = self.group()
        prefix = ''
        if grp:
            prefix = grp.modelPrefix()

        # generate archive layer
        if self.isArchived():
            # create the archive column
            archive_columns = []
            colname = projex.text.camelHump(self.name())

            # create a duplicate of the existing columns, disabling translations since we'll store
            # a single record per change
            found_locale = False
            for column in self.columns(recurse=False):
                if column.name() == 'id':
                    continue

                new_column = column.copy()
                new_column.setTranslatable(False)
                new_column.setUnique(False)
                archive_columns.append(new_column)
                if column.name() == 'locale':
                    found_locale = True

            archive_columns += [
                # primary key for the archives is a reference to the article
                orb.Column(orb.ColumnType.ForeignKey,
                           colname,
                           fieldName='{0}_archived_id'.format(projex.text.underscore(self.name())),
                           required=True,
                           reference=self.name(),
                           reversed=True,
                           reversedName='archives'),

                # and its version
                orb.Column(orb.ColumnType.Integer,
                           'archiveNumber',
                           required=True),

                # created the archive at method
                orb.Column(orb.ColumnType.DatetimeWithTimezone,
                           'archivedAt',
                           default='now')
            ]
            archive_indexes = [
                orb.Index('byRecordAndVersion', [colname, 'archiveNumber'], unique=True)
            ]

            # store data per locale
            if not found_locale:
                archive_columns.append(orb.Column(orb.ColumnType.String,
                                                  'locale',
                                                  fieldName='locale',
                                                  required=True,
                                                  maxLength=5))

            # create the new archive schema
            archive_name = '{0}Archive'.format(self.name())
            archive_schema = orb.Schema()
            archive_schema.setDatabase(self.code())
            archive_schema.setName(archive_name)
            archive_schema.setDbName('{0}_archives'.format(projex.text.underscore(self.name())))
            archive_schema.setColumns(archive_columns)
            archive_schema.setIndexes(archive_indexes)
            archive_schema.setAutoLocalize(self.autoLocalize())
            archive_schema.setArchived(False)
            archive_schema.setDefaultOrder([('archiveNumber', 'asc')])

            # define the class properties
            class_data = {
                '__module__': 'orb.schema.dynamic',
                '__db_schema__': archive_schema
            }

            model = MetaTable(archive_name, tuple(bases), class_data)
            archive_schema.setModel(model)
            self.setArchiveModel(model)
            orb.system.registerSchema(archive_schema)

            setattr(dynamic, archive_name, model)

        # finally, create the new model
        cls = MetaTable(prefix + self.name(), tuple(bases), attrs)
        setattr(dynamic, cls.__name__, cls)
        return cls

    def register(self, obj):
        """
        Registers a particular database.
        
        :param      obj     | <orb.Database> || <orb.Schema>
        """
        if isinstance(obj, orb.Database):
            self.__databases[obj.code()] = obj
        elif isinstance(obj, orb.Schema):
            self.__schemas[obj.name()] = obj

    def model(self, code, autoGenerate=True):
        return self.models(autoGenerate=autoGenerate).get(code)

    def models(self, base=None, database='', autoGenerate=True):
        output = {}
        for schema in self.__schemas.values():
            model = schema.model(autoGenerate=autoGenerate)
            if (model and
                (base is None or issubclass(model, base)) and
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
        return self._settings

    def setLocale(self, locale):
        """
        Sets the locale that the orb file will be using.
        
        :param      locale | <str>
        """
        if locale:
            self.__locale = locale

    def security(self):
        return self.__security

    def setSecurity(self, security):
        self.__security = security

    def setSyntax(self, syntax):
        if not isinstance(syntax, orb.Syntax):
            syntax = orb.Syntax.byName(syntax)
            if syntax:
                self.__syntax = syntax()
        else:
            self.__syntax = syntax

    def syntax(self):
        """
        Returns the syntax that is being used for this system.

        :return:    <orb.Syntax>
        """
        if self.__syntax is None:
            self.__syntax = orb.Syntax.byName(self.__settings.syntax)()
        return self.__syntax
