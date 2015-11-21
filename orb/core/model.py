"""
Defines the main Table class that will be used when developing
database classes.
"""

import logging
import projex.rest
import projex.security
import projex.text

from projex.locks import ReadLocker, ReadWriteLock, WriteLocker
from projex.lazymodule import lazy_import
from projex.text import nativestring as nstr
from projex.addon import AddonManager
from projex import funcutil

from .metamodel import MetaModel

log = logging.getLogger(__name__)
orb = lazy_import('orb')
errors = lazy_import('orb.errors')


class Model(object):
    """
    Defines the base class type that all database records should inherit from.
    """
    # define the table meta class
    __metaclass__ = MetaModel
    __orb__ = {'bypass': True}

    def __getitem__(self, key):
        if self.has(key):
            return self.get(key)
        else:
            raise KeyError

    def __json__(self, *args):
        """
        Iterates this object for its values.  This will return the field names from the
        database rather than the API names.  If you want the API names, you should use
        the recordValues method.

        :sa         recordValues

        :return     <iter>
        """
        # additional options
        context = self.context()

        # hide private columns
        schema = self.schema()
        columns = [schema.column(x) for x in context.columns] if context.columns else schema.columns().values()
        columns = [x for x in columns if x and not x.testFlag(x.Flags.Private)]

        # simple json conversion
        output = self.collect(key='field', columns=columns, inflated=False)

        # expand any references we need
        expand_tree = context.expandtree()
        if expand_tree:
            for key, subtree in expand_tree.items():
                try:
                    getter = getattr(self, key)
                except AttributeError:
                    continue
                else:
                    value = getter(inflated=True, expand=subtree, returning=context.returning)
                    if isinstance(value, orb.Model):
                        output[key] = value.__json__()
                    else:
                        output[key] = value

        # don't include the column names
        if context.returning == 'values':
            output = [output[column.field()] for column in context.schemaColumns(self.schema())]
            if len(output) == 1:
                output = output[0]

        if context.format == 'text':
            return projex.rest.jsonify(output)
        else:
            return output

    def __iter__(self):
        """
        Iterates this object for its values.  This will return the field names from the
        database rather than the API names.  If you want the API names, you should use
        the recordValues method.

        :sa         recordValues

        :return     <iter>
        """
        for col in self.schema().columns().values():
            yield col.field(), self.get(col, inflated=False)

    def __format__(self, spec):
        """
        Formats this record based on the inputted format_spec.  If no spec
        is supplied, this is the same as calling str(record).

        :param      spec | <str>

        :return     <str>
        """
        if not spec:
            return projex.text.nativestring(self)
        elif spec == 'id':
            return projex.text.nativestring(self.id())
        elif self.has(spec):
            return projex.text.nativestring(self.get(spec))
        else:
            return super(Model, self).__format__(spec)

    def __eq__(self, other):
        """
        Checks to see if the two records are equal to each other
        by comparing their primary key information.

        :param      other       <variant>

        :return     <bool>
        """
        return id(self) == id(other) or (isinstance(other, Model) and hash(self) == hash(other))

    def __ne__(self, other):
        """
        Returns whether or not this object is not equal to the other object.

        :param      other | <variant>

        :return     <bool>
        """
        return not self.__eq__(other)

    def __hash__(self):
        """
        Creates a hash key for this instance based on its primary key info.

        :return     <int>
        """
        if not self.isRecord():
            return super(Model, self).__hash__()
        else:
            return hash((self.__class__, self.id()))

    def __cmp__(self, other):
        """
        Compares one record to another.

        :param      other | <variant>

        :return     -1 || 0 || 1
        """
        try:
            my_str = '{0}({1})'.format(type(self).__name__, self.id())
            other_str = '{0}({1})'.format(type(self).__name__, other.id())
            return cmp(my_str, other_str)
        except StandardError:
            return -1

    def __init__(self,
                 context=None,
                 *record,
                 **values):
        """
        Initializes a database record for the table class.  A
        table model can be initialized in a few ways.  Passing
        no arguments will create a fresh record that does not
        exist in the database.  Providing keyword arguments will
        map to this table's schema column name information,
        setting default values for the record.  Supplying an
        argument will be the records unique primary key, and
        trigger a lookup from the database for the record directly.

        :param      *args       <tuple> primary key
        :param      **kwds      <dict>  column default values
        """
        self.__dataLock = ReadWriteLock()
        self.__values = {}
        self.__loaded = set()
        self.__context = context or orb.Context()

        # restore the pickled state for this object
        if isinstance(record, dict):
            values = record
            record = []

        # restore the database update values
        if 'loadEvent' in values:
            self.onLoad(values.pop('loadEvent'))

        # initialize a new record if no record is provided
        elif not record:
            self.init()

        # otherwise, fetch the record from the database
        else:
            if len(record) == 1 and isinstance(record[0], Model):
                record = record.id()
                event = orb.events.LoadEvent(data=dict(record[0]))
                self.onLoad(event)
            elif len(record) == 1:
                record = record[0]  # don't use tuples unless multiple ID columns are used

            data = self.fetch(record, inflated=False, context=context)
            if data:
                event = orb.events.LoadEvent(data=data)
                self.onLoad(event)
            else:
                raise errors.RecordNotFound(self, record)

        # after loading everything else, update the values for this model
        self.update({k: v for k, v in values.items() if self.schema().column(k)})

    # --------------------------------------------------------------------
    #                       EVENT HANDLERS
    # --------------------------------------------------------------------

    def onChange(self, event):
        pass

    def onLoad(self, event):
        context = self.context()
        schema = self.schema()
        dbname = schema.dbname()
        loaders = []
        clean = {}

        for col, value in event.data.items():
            try:
                model_dbname, col_name = col.split('.')
            except ValueError:
                col_name = col
                model_dbname = dbname

            # make sure the value we're setting is specific to this model
            column = schema.column(col_name)
            if model_dbname != dbname:
                continue

            # prefer to use record objects
            elif column in clean and isinstance(clean[column], Model):
                continue

            # look for preloaded reverse lookups and pipes
            elif not column:
                try:
                    loader = getattr(self, col_name).preload
                except AttributeError:
                    continue
                else:
                    loaders.append((loader, value))
                    continue

            # extract the value from the database
            else:
                value = column.extract(value, context=context)

            clean[column] = value

        # update the local values
        with WriteLocker(self.__dataLock):
            for col, val in clean.items():
                default = val if not isinstance(val, dict) else val.copy()
                self.__values[col] = (default, val)
                self.__loaded.add(col)

        # load pre-loaded information from database
        for loader, value in loaders:
            for preload_type, preload_value in value.items():
                loader(self, preload_value, context, type=preload_type)

    def onDelete(self, event):
        with WriteLocker(self.__dataLock):
            self.__loaded.clear()

    def onPreSave(self, event):
        pass

    def onPostSave(self, event):
        pass

    @classmethod
    def onSync(cls, event):
        pass

    # ---------------------------------------------------------------------
    #                       PUBLIC METHODS
    # ---------------------------------------------------------------------
    def changes(self, columns=None, recurse=True, flags=0, inflated=False):
        """
        Returns a dictionary of changes that have been made
        to the data from this record.

        :return     { <orb.Column>: ( <variant> old, <variant> new), .. }
        """
        pre_check = {}
        is_record = self.isRecord()
        schema = self.schema()
        columns = [schema.column(c) for c in columns] if columns else \
                   schema.columns(recurse=recurse, flags=flags).values()

        with ReadLocker(self.__dataLock):
            for col in columns:
                old, curr = self.__values.get(col, (None, None))
                if col.testFlag(col.Flags.ReadOnly):
                    continue
                elif is_record:
                    old = None

                check_old = col.restore(old, False)
                check_curr = col.restore(curr, False)

                if check_old != check_curr:
                    pre_check[col] = (check_old, check_curr)

        return {col: (col.restore(old, inflated), col.restore(curr, inflated))
                for col, (old, curr) in pre_check.items()}

    def collect(self,
                columns=None,
                recurse=True,
                flags=0,
                mapper=None,
                key='name',
                **get_options):
        """
        Returns a dictionary grouping the columns and their
        current values.  If the inflated value is set to True,
        then you will receive any foreign keys as inflated classes (if you
        have any values that are already inflated in your class, then you
        will still get back the class and not the primary key value).  Setting
        the mapper option will map the value by calling the mapper method.

        :param      useFieldNames | <bool>
                    inflated | <bool>
                    mapper | <callable> || None

        :return     { <str> key: <variant>, .. }
        """
        output = {}
        schema = self.schema()
        all_columns = schema.columns(recurse=recurse, flags=flags).values()
        req_columns = [schema.column(col) for col in columns] if columns else None

        for column in all_columns:
            if req_columns is not None and column not in req_columns:
                continue

            if key == 'column':
                val_key = column
            else:
                try:
                    val_key = getattr(column, key)()
                except AttributeError:
                    raise errors.OrbError('Invalid key used in data collection.  Must be name, field, or column.')

            value = self.get(column, **get_options)
            if mapper:
                value = mapper(value)
            output[val_key] = value

        return output

    def context(self, **options):
        """
        Returns the lookup options for this record.  This will track the options that were
        used when looking this record up from the database.

        :return     <orb.LookupOptions>
        """
        output = self.__context or orb.Context()
        output.update(options)
        return output

    def define(self, column, value, overwrite=False):
        """
        Defines the value for this column.  This will set both the current and
        default value for the column, vs. set which only updates the current value.

        :param column: <orb.Column> || <str>
        :param value: <variant>
        :param overwrite: <bool>

        :return: <bool> changed
        """
        col = self.schema().column(column)
        if not col:
            raise orb.errors.ColumnNotFound(column)

        with WriteLocker(self.__dataLock):
            if overwrite or not col in self.__values:
                self.__values[col] = (value, value)
                return True
            else:
                return False

    def delete(self, **context):
        """
        Removes this record from the database.  If the dryRun \
        flag is specified then the command will be logged and \
        not executed.

        :note       From version 0.6.0 on, this method now accepts a mutable
                    keyword dictionary of values.  You can supply any member
                    value for either the <orb.LookupOptions> or
                    <orb.Context>, as well as the keyword 'lookup' to
                    an instance of <orb.LookupOptions> and 'options' for
                    an instance of the <orb.Context>

        :return     <int>
        """
        if not self.isRecord():
            return 0

        event = orb.events.DeleteEvent(context=context)
        self.onDelete(event)
        if event.preventDefault:
            return 0

        context = self.context(**context)
        conn = context.db.connection()
        _, count = conn.delete([self], context)

        # clear out the old values
        if count == 1:
            col = self.schema().column(self.schema().idColumn())
            with WriteLocker(self.__dataLock):
                self.__values[col] = (None, None)

        return count

    def get(self, column, default=None, useMethod=True, **context):
        """
        Returns the value for the column for this record.

        :param      column      | <orb.Column> || <str>
                    default     | <variant>
                    inflated    | <bool>

        :return     <variant>
        """
        context = self.context(**context)

        # normalize the given column
        col = self.schema().column(column)
        if not col:
            raise errors.ColumnNotFound(self.schema().name(), column)

        # call the getter method fot this record if one exists
        if useMethod:
            method = getattr(type(self), col.getter(), None)
            if method is not None and type(method.im_func).__name__ != 'orb_getter_method':
                keywords = list(funcutil.extract_keywords(method))
                kwds = {}

                if 'locale' in keywords:
                    kwds['locale'] = context.locale
                if 'default' in keywords:
                    kwds['default'] = default
                if 'inflated' in keywords:
                    kwds['inflated'] = context.inflated

                return method(self, **kwds)

        # grab the current value
        with ReadLocker(self.__dataLock):
            _, value = self.__values[col]

        # return a reference when desired
        return col.restore(value, context)

    def init(self):
        """
        Initializes the default values for this record.
        """
        for column in self.schema().columns().values():
            if not column.testFlag(column.Flags.Virtual):
                value = column.default()
                if column.testFlag(column.Flags.Translatable):
                    value = {self.__context.locale: value}
                elif column.testFlag(column.Flags.Polymorphic):
                    value = type(self).__name__

                self.define(column, value)

    def id(self, **context):
        return self.get(self.schema().idColumn(), useMethod=False, **context)

    def isModified(self):
        """
        Returns whether or not any data has been modified for
        this object.

        :return     <bool>
        """
        return not self.isRecord() or len(self.changes()) > 0

    def isRecord(self, db=None):
        """
        Returns whether or not this database table record exists
        in the database.

        :return     <bool>
        """
        if db in (None, self.context().db):
            col = self.schema().column(self.schema().idColumn())
            with ReadLocker(self.__dataLock):
                if self.__values[col][1] is None:
                    return False
                return True
        else:
            return None

    def save(self, **context):
        """
        Commits the current change set information to the database,
        or inserts this object as a new record into the database.
        This method will only update the database if the record
        has any local changes to it, otherwise, no commit will
        take place.  If the dryRun flag is set, then the SQL
        will be logged but not executed.

        :note       From version 0.6.0 on, this method now accepts a mutable
                    keyword dictionary of values.  You can supply any member
                    value for either the <orb.LookupOptions> or
                    <orb.Context>, 'options' for
                    an instance of the <orb.Context>

        :return     <bool> success
        """
        # check to see if we have any modifications to store
        if not (self.isModified() and self.validate()):
            return False

        # create the commit options
        context = self.context(**context)

        # create the pre-commit event
        event = orb.events.SaveEvent(context=context)
        self.onPreSave(event)
        if event.preventDefault:
            return event.result

        conn = context.db.connection()
        if not self.isRecord():
            records, _ = conn.insert([self], context)
            if records:
                event = orb.events.LoadEvent(records[0])
                self.onLoad(event)
        else:
            conn.update([self], context)

        # mark all the data as committed
        cols = [self.schema().column(c) for c in context.columns or []]
        with WriteLocker(self.__dataLock):
            for col, (_, value) in self.__values.items():
                if not cols or col in cols:
                    self.__values[col] = (value, value)

        # create post-commit event
        event = orb.events.SaveEvent(context=context)
        self.onPostSave(event)
        return True

    def set(self, column, value, useMethod=True, **context):
        """
        Sets the value for this record at the inputted column
        name.  If the columnName provided doesn't exist within
        the schema, then the ColumnNotFound error will be
        raised.

        :param      columnName      | <str>
                    value           | <variant>

        :return     <bool> changed
        """
        col = self.schema().column(column)
        if col is None:
            raise errors.ColumnNotFound(self.schema().name(), column)
        elif col.testFlag(col.Flags.ReadOnly):
            raise errors.ColumnReadOnly(column)

        # ensure the value is good
        col.validate(value)

        context = self.context(**context)
        if useMethod:
            try:
                method = getattr(self.__class__, column.setter())
            except AttributeError:
                pass
            else:
                if type(method.im_func).__name__ != 'orb_setter_method':
                    keywords = list(funcutil.extract_keywords(method))
                    if 'locale' in keywords:
                        return method(self, value, locale=context.locale)
                    else:
                        return method(self, value)

        with WriteLocker(self.__dataLock):
            orig, curr = self.__values[col]
            value = col.store(value, context)

            # update the context based on the locale value
            if col.testFlag(col.Flags.Translatable):
                new_value = curr.copy()
                new_value.update(value)

            change = curr != value
            if change:
                self.__values[col] = (orig, value)

        # broadcast the change event
        if change:
            event = orb.events.ChangeEvent(column=col, old=curr, value=value)
            self.onChange(event)
            if event.preventDefault:
                with WriteLocker(self.__dataLock):
                    orig, _ = self.__values[col]
                    self.__values[col] = (orig, curr)
                return False
            else:
                return change
        else:
            return False

    def setId(self, value, **context):
        return self.get(self.schema().idColumn(), value, useMethod=False, **context)

    def update(self, values, **context):
        # update from value dictionary
        for col, value in values.items():
            self.set(col, value)
        return len(values)

    def validate(self):
        """
        Validates the current record object to make sure it is ok to commit to the database.  If
        the optional override dictionary is passed in, then it will use the given values vs. the one
        stored with this record object which can be useful to check to see if the record will be valid before
        it is committed.

        :param      overrides | <dict>

        :return     <bool>
        """
        values = self.collect(key='column')
        for col, value in values.items():
            if not col.validate(value):
                return False

        for index in self.schema().indexes().values():
            if not index.validate(self, values):
                return False

        return True

    #----------------------------------------------------------------------
    #                           CLASS METHODS
    #----------------------------------------------------------------------

    @classmethod
    def all(cls, **options):
        """
        Returns a record set containing all records for this table class.  This
        is a convenience method to the <orb.Table>.select method.

        :param      **options | <orb.LookupOptions> & <orb.Context>

        :return     <orb.Collection>
        """
        return cls.select(**options)

    @classmethod
    def baseQuery(cls, **context):
        """
        Returns the default query value for the inputted class.  The default
        table query can be used to globally control queries run through a
        Table's API to always contain a default.  Common cases are when
        filtering out inactive results or user based results.

        :return     <orb.Query> || None
        """
        return getattr(cls, '_%s__baseQuery' % cls.__name__, None)

    @classmethod
    def create(cls, values, **context):
        """
        Shortcut for creating a new record for this table.

        :param     values | <dict>

        :return    <orb.Table>
        """
        # extract additional options for return
        expand = [item for item in values.pop('expand', '').split(',') if item]
        context.setdefault('expand', expand)

        # create the new record
        record = cls(context=orb.Context(**context))
        record.update(**values)
        record.save()
        return record

    @classmethod
    def ensureExists(cls, **kwds):
        """
        Defines a new record for the given class based on the
        inputted set of keywords.  If a record already exists for
        the query, the first found record is returned, otherwise
        a new record is created and returned.

        :param      **kwds | columns & values
        """
        # require at least some arguments to be set
        if not kwds:
            return cls()

        # lookup the record from the database
        q = orb.Query()

        for key, value in kwds.items():
            column = cls.schema().column(key)
            if not column:
                raise orb.errors.ColumnNotFound(cls.schema().name(), key)

            if (isinstance(column, orb.AbstractStringColumn) and
                not column.testFlag(column.Flags.CaseSensitive) and
                isinstance(value, (str, unicode))):
                q &= orb.Query(key).lower() == value.lower()
            else:
                q &= orb.Query(key) == value

        record = cls.select(where=q).first()
        if not record:
            record = cls(**kwds)
            record.save()

        return record

    @classmethod
    def fetch(cls, key, **context):
        context.setdefault('where', orb.Query(cls) == key)
        context['limit'] = 1
        return cls.select(**context).first()

    @classmethod
    def inflate(cls, values, **context):
        """
        Returns a new record instance for the given class with the values
        defined from the database.

        :param      cls     | <subclass of orb.Table>
                    values  | <dict> values

        :return     <orb.Table>
        """
        context = orb.Context(**context)

        # inflate values from the database into the given class type
        if isinstance(values, Model):
            record = values
            values = dict(values)
        else:
            record = None

        schema = cls.schema()
        polymorphs = schema.columns(flags=orb.Column.Flags.Polymorphic)
        column = polymorphs[0] if polymorphs else None

        # attempt to expand the class to its defined polymorphic type
        if column and column.field() in values:
            morph_cls_name = values.get(column.name(), values.get(column.field()))
            morph_cls = orb.system.model(morph_cls_name)
            if morph_cls and morph_cls != cls:
                try:
                    record = morph_cls(values['id'], context=context)
                except KeyError:
                    raise orb.errors.RecordNotFound(morph_cls, values.get('id'))
            elif not morph_cls:
                raise orb.errors.ModelNotFound(morph_cls_name)

        if record is None:
            event = orb.events.LoadEvent(values)
            record = cls(loadEvent=event, context=context)

        return record

    @classmethod
    def schema(cls):
        """  Returns the class object's schema information. """
        return getattr(cls, '_{0}__schema'.format(cls.__name__), None)

    @classmethod
    def select(cls, **context):
        """
        Selects records for the class based on the inputted \
        options.  If no db is specified, then the current \
        global database will be used.  If the inflated flag is specified, then \
        the results will be inflated to class instances.

        If the flag is left as None, then results will be auto-inflated if no
        columns were supplied.  If columns were supplied, then the results will
        not be inflated by default.

        If the groupBy flag is specified, then the groupBy columns will be added
        to the beginning of the ordered search (to ensure proper paging).  See
        the Table.groupRecords methods for more details.

        :note       From version 0.6.0 on, this method now accepts a mutable
                    keyword dictionary of values.  You can supply any member
                    value for either the <orb.LookupOptions> or
                    <orb.Context>, as well as the keyword 'lookup' to
                    an instance of <orb.LookupOptions> and 'context' for
                    an instance of the <orb.Context>

        :return     [ <cls>, .. ] || { <variant> grp: <variant> result, .. }
        """
        rset_type = getattr(cls, 'Collection', orb.Collection)
        return rset_type(model=cls, **context)

    @classmethod
    def setBaseQuery(cls, query):
        """
        Sets the default table query value.  This method can be used to control
        all queries for a given table by setting global where inclusions.

        :param      query | <orb.Query> || None
        """
        setattr(cls, '_%s__baseQuery' % cls.__name__, query)

