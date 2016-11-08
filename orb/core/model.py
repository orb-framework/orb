"""
Defines the main Table class that will be used when developing
database classes.
"""

import blinker
import demandimport
import logging

from collections import defaultdict

from .access_control import AuthorizationPolicy
from ..utils import json2
from ..utils.text import nativestring
from ..utils.locks import ReadLocker, ReadWriteLock, WriteLocker
from ..utils import funcutil
from ..decorators import deprecated

from .metamodel import MetaModel
from .search import SearchEngine

with demandimport.enabled():
    import orb

log = logging.getLogger(__name__)


class ModelMixin(object):
    """ Namespace placeholder that will define additional model properties through the meta system """
    pass


class Model(object):
    """
    Defines the base class type that all database records should inherit from.
    """
    # define the table meta class
    __metaclass__ = MetaModel
    __model__ = False
    __search_engine__ = 'basic'
    __auth__ = AuthorizationPolicy()

    # signals
    about_to_sync = blinker.Signal()
    synced = blinker.Signal()

    def __init__(self,
                 record=None,
                 delayed=False,
                 **context):
        """
        Initializes a database record for the table class.  A
        table model can be initialized in a few ways.  Passing
        no arguments will create a fresh record that does not
        exist in the database.  Providing keyword arguments will
        map to this table's schema column name information,
        setting default values for the record.  Supplying an
        argument will be the records unique primary key, and
        trigger a lookup from the database for the record directly.

        :param record: <variant> id or <dict> values or <orb.Model> instance or None
        :param delayed: <bool> delays when the lookup for the record occurs
        :param context: <orb.Context> descriptor
        """
        # define custom properties
        self.__lock = ReadWriteLock()
        self.__attributes = {}
        self.__base_attributes = {}
        self.__cache = defaultdict(dict)
        self.__context = orb.Context(**context)
        self.__preload = {}
        self.__loaded = False

        # setup context
        default_namespace = self.schema().namespace()
        if default_namespace and not (self.__context.namespace or self.__context.force_namespace):
            self.__context.namespace = default_namespace

        # initialize record from values
        if type(record) is dict:
            defaults = dict(self.record_defaults(ignore=record.keys()), context=self.__context)
            defaults.update(record)
            self.update(defaults)

        # initialize a record by copying another
        elif type(record) is type(self):
            self.update(dict(record))

        # initialize a record by its id
        elif record is not None:
            # initialize by casting from a base class
            if isinstance(record, orb.Model):
                if not issubclass(type(self), type(record)):
                    raise orb.errors.RecordNotFound(schema=self.schema(), column=type(record).__name__)
                else:
                    record_id = record.id()

            # initialize from id
            else:
                record_id = record

            # set the id value for this instance
            id_column = self.schema().id_column()
            self.set(id_column, record_id)

            # if this record initialization is not delayed, then read from the
            # backend immediately
            if not delayed:
                self.read()

        # initialize defaults for the record
        else:
            self.update(dict(self.record_defaults(context=self.__context)))

    def __len__(self):
        """
        Returns the length of the dictionary that will be returned when
        doing a dict() on this instance.

        :return: <int>
        """
        return len(self.schema().columns())

    def __getitem__(self, key):
        """
        Retrieves the value of the given column from the record.  If there is no
        valid value found a KeyError is raised.

        :param key: <str>

        :return: <variant>
        """
        try:
            return self.get(key)
        except Exception:
            raise KeyError

    def __setitem__(self, key, value):
        """
        Sets the value of a given column for this record.  If there is no valid
        column found that matches the key, then a KeyError will be raised.

        :param key: <str>
        :param value: <variant>
        """
        try:
            return self.set(key, value)
        except Exception:
            raise KeyError

    def __json__(self):
        """
        Renders this object as a JSON compatible dictionary.  If the context for
        this record's output is "text" then it will do a dumps on the resulting
        data, otherwise the dictionary will be returned.

        :return: <dict> or <str>
        """
        # additional options
        context = self.context()

        # don't include the column names
        if context.returning == 'values':
            schema_fields = {c.field() for c in context.schema_columns(self.schema())}
            output = tuple(value for field, value in self if field in schema_fields)
            if len(output) == 1:
                output = output[0]
        else:
            output = dict(self)

        return json2.dumps(output) if context.format == 'text' else output

    def __iter__(self):
        """
        Iterates this object for its values.  This will return the field names from the
        database rather than the API names.  If you want the API names, you should use
        the recordValues method.

        :sa         recordValues

        :return     <iter>
        """
        schema = self.schema()
        context = self.context()
        expand_tree = context.expandtree(type(self))

        # ensure the data is loaded
        self.read(refresh=False)

        # iterate columns
        if context.columns:
            columns = [schema.column(x) for x in context.columns]
        else:
            columns = schema.columns(flags=~orb.Column.Flags.RequiresExpand).values()

        for column in columns:
            self.iter_column(column, tree=expand_tree, context=context)

        # iterate expanded objects
        if expand_tree:
            for attribute, value in self.iter_expanded(expand_tree, context=context):
                yield attribute, value

    def iter_column(self, column, tree=None, context=None):
        """
        Iterates over the values from this model for a given column.  If the
        column is a reference column, and it's name is included in the expanded
        tree, then this method will yield both it's raw reference value, and
        it's expanded value.

        :param column: <orb.Column>
        :param tree: <dict>
        :param context: <orb.Context>

        :return: <generator>
        """
        tree = tree or {}
        # ignore tree columns
        if column.test_flag(column.Flags.RequiresExpand):
            return

        # ignore permission denied columns
        elif not self.__auth__.can_read_column(column, context=context):
            return

        # fetch raw data for virtual columns not associated with views
        elif column.test_flag(column.Flags.Virtual) and not isinstance(self, orb.View):
            yield column.alias(), self.get(column, context=context)

        # fetch custom data for the column
        elif column.gettermethod() is not None:
            yield column.alias(), self.get(column, context=context)

        # fetch raw data for the column
        else:
            try:
                value = self.__attributes[column.name()]
            except KeyError:
                pass
            else:
                # normalize the value from the cache
                if column.test_flag(orb.Column.Flags.I18n) and type(value) == dict:
                    value = value.get(context.locale)

                # yield basic column value
                if not isinstance(column, orb.ReferenceColumn):
                    yield column.alias(), value

                # yield reference column
                else:
                    # yield the basic id by default
                    yield column.alias(), value if not isinstance(value, orb.Model) else value.id()

                    # yield tree value (if requested)
                    try:
                        subtree = tree.pop(column.name())
                    except KeyError:
                        pass
                    else:
                        reference = self.get(column,
                                             expand=subtree,
                                             returning=context.returning,
                                             scope=context.scope)
                        yield column.name(), dict(reference) if isinstance(reference, orb.Model) else reference

    def iter_expanded(self, tree, context=None):
        """
        Iterates over the expansion tree yielding results
        for the schema object.

        :param tree: <dict>
        :param context: <orb.Context>

        :return: <generator>
        """
        for attribute, subtree in tree.items():
            try:
                value = self.get(
                    attribute,
                    expand=subtree,
                    returning=context.returning,
                    scope=context.scope
                )
            except orb.errors.ColumnNotFound:
                continue
            else:
                yield attribute, dict(value) if isinstance(value, orb.Model) else value

    def _add_preloaded_data(self, cache):
        for key, value in cache.items():
            self.__preload[key] = value

    def _load(self, event):
        """
        Processes a load event by setting the properties of this record
        to the data restored from the database.

        :param event: <orb.events.LoadEvent>
        """
        if not event.data:
            return

        context = self.context()
        schema = self.schema()
        dbname = schema.dbname()
        clean = {}

        for col, value in event.data.items():
            try:
                model_dbname, col_name = col.split('.')
            except ValueError:
                col_name = col
                model_dbname = dbname

            # make sure the value we're setting is specific to this model
            try:
                column = schema.column(col_name)
            except orb.errors.ColumnNotFound:
                column = None

            if model_dbname != dbname or (column in clean and isinstance(clean[column], Model)):
                continue

            # look for preloaded reverse lookups and pipes
            elif not column:
                self.__preload[col_name] = value

            # extract the value from the database
            else:
                engine = column.get_engine()
                value = engine.get_api_value(column, 'default', value, context=context)
                clean[column] = value

        # update the local values
        with WriteLocker(self.__lock):
            for col, val in clean.items():
                default = val if not isinstance(val, dict) else val.copy()
                self.__attributes[col.name()] = (default, val)
                self.__loaded.add(col)

        if self.processEvent(event):
            self.onLoad(event)

    # --------------------------------------------------------------------
    #                       EVENT HANDLERS
    # --------------------------------------------------------------------

    def onChange(self, event):
        pass

    def onLoad(self, event):
        pass

    def onDelete(self, event):
        pass

    def onInit(self, event):
        """
        Initializes the default values for this record.
        """
        pass

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
        output = {}
        is_record = self.is_record()
        schema = self.schema()
        columns = [schema.column(c) for c in columns] if columns else \
                   schema.columns(recurse=recurse, flags=flags).values()

        context = self.context(inflated=inflated)
        with ReadLocker(self.__lock):
            for col in columns:
                old, curr = self.__attributes.get(col.name(), (None, None))
                if col.test_flag(col.Flags.ReadOnly):
                    continue
                elif not is_record:
                    old = None

                check_old = col.restore(old, context)
                check_curr = col.restore(curr, context)
                try:
                    different = check_old != check_curr
                except StandardError:
                    different = True

                if different:
                    output[col] = (check_old, check_curr)

        return output

    def collect(self, name, use_method=False, **context):
        collector = self.schema().collector(name)
        if not collector:
            raise orb.errors.ColumnNotFound(schema=self.schema(), column=name)
        else:
            return collector(self, use_method=use_method, **context)

    def context(self, **context):
        """
        Returns the lookup options for this record.  This will track the options that were
        used when looking this record up from the database.

        :return     <orb.LookupOptions>
        """
        output = orb.Context(context=self.__context) if self.__context is not None else orb.Context()
        output.update(context)
        return output

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
        if not self.is_record():
            return 0

        event = orb.events.DeleteEvent(record=self, context=context)
        if self.processEvent(event):
            self.onDelete(event)

        if event.preventDefault:
            return 0

        self.read(refresh=False)

        with WriteLocker(self.__lock):
            self.__loaded.clear()

        context = self.context(**context)
        conn = context.db.connection()
        _, count = conn.delete([self], context)

        # clear out the old values
        if count == 1:
            col = self.schema().column(self.schema().id_column())
            with WriteLocker(self.__lock):
                self.__attributes[col.name()] = (None, None)

        return count

    def get(self, column, use_method=True, **context):
        """
        Returns the value for the column for this record.

        :param      column      | <orb.Column> || <str>
                    default     | <variant>
                    inflated    | <bool>

        :return     <variant>
        """

        # look for shortcuts (dot-noted path)
        if isinstance(column, (str, unicode)) and '.' in column:
            # create the sub context
            base_context = context.copy()
            base_context['inflated'] = True

            # generate the expansion to avoid unnecessary lookups
            parts = column.split('.')

            # include the target if it is a reference
            if isinstance(self.schema().column(column), orb.ReferenceColumn):
                expand_path_index = None

            # otherwise, just get to the end column
            else:
                expand_path_index = -1

            value = self
            for i, part in enumerate(parts[:-1]):
                sub_context = base_context.copy()
                expand_path = '.'.join(parts[i+1:expand_path_index])

                try:
                    sub_expand = sub_context['expand']
                except KeyError:
                    sub_context['expand'] = expand_path
                else:
                    if isinstance(sub_expand, basestring):
                        sub_context['expand'] += ',{0}'.format(expand_path)
                    elif isinstance(sub_expand, list):
                        sub_expand.append(expand_path)
                    elif isinstance(sub_expand, dict):
                        curr = {}
                        for x in xrange(len(parts) - 1 + (expand_path_index or 0), i, -1):
                            curr = {parts[x]: curr}
                        sub_expand.update(curr)

                value = value.get(part, use_method=use_method, **sub_context)
                if value is None:
                    return None

            return value.get(parts[-1], use_method=use_method, **context)

        # otherwise, lookup a column
        else:
            my_context = self.context()
            sub_context = my_context.sub_context(**context)

            # normalize the given column
            if isinstance(column, orb.Column):
                col = column
            else:
                col = self.schema().column(column, raise_=False)

                if not col:
                    if isinstance(column, orb.Collector):
                        collector = column
                    else:
                        collector = self.schema().collector(column)

                    if collector:
                        try:
                            return self.__cache[collector][sub_context]
                        except KeyError:
                            records = collector(self, use_method=use_method, context=sub_context)
                            self.__cache[collector][sub_context] = records
                            return records
                    else:
                        raise orb.errors.ColumnNotFound(schema=self.schema(), column=column)

            # don't inflate if the requested value is a field
            if sub_context.inflated is None and isinstance(col, orb.ReferenceColumn):
                sub_context.inflated = column != col.field()

            # lookup the shortuct value vs. the local one (bypass for views
            # since they define tables for shortcuts)
            if col.shortcut() and not isinstance(self, orb.View):
                return self.get(col.shortcut(), **context)

            # call the getter method fot this record if one exists
            elif use_method and col.gettermethod():
                return col.gettermethod()(self, context=sub_context)

            else:
                self.read(refresh=False)

                # grab the current value
                with ReadLocker(self.__lock):
                    old_value, value = self.__attributes.get(col.name(), (None, None))

                # return a reference when desired
                out_value = col.restore(value, sub_context)
                if isinstance(out_value, orb.Model) and not isinstance(value, orb.Model):
                    with WriteLocker(self.__lock):
                        self.__attributes[col.name()] = (old_value, out_value)

                return out_value

    def id(self, **context):
        column = self.schema().id_column()
        use_method = column.name() != 'id'
        return self.get(column, use_method=use_method, **context)

    def init(self, ignore=None):
        columns = self.schema().columns().values()
        ignore = [self.schema().column(c) for c in ignore]
        with WriteLocker(self.__lock):
            for column in columns:
                if column in ignore:
                    continue
                elif column.name() not in self.__attributes and not column.test_flag(column.Flags.Virtual):
                    value = column.default()
                    if column.test_flag(column.Flags.I18n):
                        value = {self.__context.locale: value}
                    elif column.test_flag(column.Flags.Polymorphic):
                        value = type(self).__name__

                    self.__attributes[column.name()] = (value, value)

        event = orb.events.InitEvent(record=self)
        if self.processEvent(event):
            self.onInit(event)

    def isModified(self):
        """
        Returns whether or not any data has been modified for
        this object.

        :return     <bool>
        """
        return not self.is_record() or len(self.changes()) > 0

    def is_record(self, db=None):
        """
        Returns whether or not this database table record exists
        in the database.

        :return     <bool>
        """
        if db is not None:
            same_db = db == self.context().db

        if db is None or same_db:
            col = self.schema().id_column()
            with ReadLocker(self.__lock):
                return (col in self.__loaded) and (self.__attributes[col.name()][0] is not None)
        else:
            return None

    def mark_loaded(self, *columns):
        """
        Marks the given columns as representing valid loaded data
        from the database.
        """
        schema = self.schema()

        columns = {schema.column(col) for col in columns}
        column_names = {col.name() for col in columns}

        with WriteLocker(self.__lock):
            for key, (old_value, new_value) in self.__attributes.items():
                if key in column_names:
                    self.__attributes[key] = (new_value, new_value)

            self.__loaded.update(columns)

    def preloaded_data(self, name):
        """
        Returns raw backend data that has been preloaded for
        the given data name.

        :param name: <str>

        :return: <dict>
        """
        return self.__preload.get(name) or {}

    def save(self, values=None, after=None, before=None, **context):
        """
        Commits the current change set information to the database,
        or inserts this object as a new record into the database.
        This method will only update the database if the record
        has any local changes to it, otherwise, no commit will
        take place.  If the dryRun flag is set, then the SQL
        will be logged but not executed.

        :param values: None or dictionary of values to update before save
        :param after: <orb.Model> || None (optional)
                      if provided, this save call will be delayed
                      until after the given record has been saved,
                      triggering a PostSaveEvent callback
        :param before: <orb.Model> || None (optional)
                      if provided, this save call will be delayed
                      until before the given record is about to be
                      saved, triggering a PreSaveEvent callback


        :note       From version 0.6.0 on, this method now accepts a mutable
                    keyword dictionary of values.  You can supply any member
                    value for either the <orb.LookupOptions> or
                    <orb.Context>, 'options' for
                    an instance of the <orb.Context>

        :return     <bool> success
        """
        # specify that this save call should be performed after the save of
        # another record, useful for chaining events
        if after is not None:
            callback = orb.events.Callback(self.save, values=values, **context)
            after.addCallback(orb.events.PostSaveEvent, callback, record=after, once=True)
            return callback

        # specify that this save call should be performed before the save
        # of another record, useful for chaining events
        elif before is not None:
            callback = orb.events.Callback(self.save, values=values, **context)
            after.addCallback(orb.events.PreSaveEvent, callback, record=after, once=True)
            return callback

        if values is not None:
            self.update(values, **context)

        # create the commit options
        context = self.context(**context)
        new_record = not self.is_record()

        # create the pre-commit event
        changes = self.changes(columns=context.columns)
        event = orb.events.PreSaveEvent(record=self, context=context, newRecord=new_record, changes=changes)
        if self.processEvent(event):
            self.onPreSave(event)

        if event.preventDefault:
            return event.result

        # check to see if we have any modifications to store
        if not (self.isModified() and self.validate()):
            return False

        conn = context.db.connection()
        if not self.is_record():
            records, _ = conn.insert([self], context)
            if records:
                event = orb.events.LoadEvent(record=self, data=records[0])
                self._load(event)
        else:
            conn.update([self], context)

        # mark all the data as committed
        cols = [self.schema().column(c).name() for c in context.columns or []]
        with WriteLocker(self.__lock):
            for col_name, (_, value) in self.__attributes.items():
                if not cols or col_name in cols:
                    self.__attributes[col_name] = (value, value)

        # create post-commit event
        event = orb.events.PostSaveEvent(record=self, context=context, newRecord=new_record, changes=changes)
        if self.processEvent(event):
            self.onPostSave(event)
        return True

    def set(self, column, value, use_method=True, **context):
        """
        Sets the value for this record at the inputted column
        name.  If the column name provided doesn't exist within
        the schema, then the ColumnNotFound error will be
        raised.

        :param      column      | <str>
                    value           | <variant>

        :return     <bool> changed
        """
        col = self.schema().column(column, raise_=False)

        if col is None:
            # allow setting of collections as well
            collector = self.schema().collector(column)
            if collector:
                my_context = self.context()
                sub_context = my_context.sub_context(**context)

                method = collector.settermethod()
                if method and use_method:
                    return method(self, value, context=sub_context)
                else:
                    records = self.get(collector.name(), context=sub_context)
                    records.update(value,
                                   use_method=use_method,
                                   context=sub_context)

                    # remove any preloaded values from the collector
                    self.__preload.pop(collector.name(), None)

                    return records
            else:
                raise orb.errors.ColumnNotFound(schema=self.schema(), column=column)

        elif col.test_flag(col.Flags.ReadOnly):
            raise orb.errors.ColumnReadOnly(schema=self.schema(), column=column)

        context = self.context(**context)
        if use_method:
            method = col.settermethod()
            if method:
                keywords = list(funcutil.extract_keywords(method))
                if 'locale' in keywords:
                    return method(self, value, locale=context.locale)
                else:
                    return method(self, value)

        self.read(refresh=False)

        with WriteLocker(self.__lock):
            orig, curr = self.__attributes.get(col.name(), (None, None))
            value = col.store(value, context)

            # update the context based on the locale value
            if col.test_flag(col.Flags.I18n) and isinstance(curr, dict) and isinstance(value, dict):
                new_value = curr.copy()
                new_value.update(value)
                value = new_value

            try:
                change = curr != value
            except TypeError:
                change = True

            if change:
                self.__attributes[col.name()] = (orig, value)

        # broadcast the change event
        if change:
            if col.test_flag(col.Flags.I18n) and context.locale != 'all':
                old_value = curr.get(context.locale) if isinstance(curr, dict) else curr
                new_value = value.get(context.locale) if isinstance(value, dict) else value
            else:
                old_value = curr
                new_value = value

            event = orb.events.ChangeEvent(record=self, column=col, old=old_value, value=new_value)
            if self.processEvent(event):
                self.onChange(event)
            if event.preventDefault:
                with WriteLocker(self.__lock):
                    orig, _ = self.__attributes.get(col.name(), (None, None))
                    self.__attributes[col.name()] = (orig, curr)
                return False
            else:
                return change
        else:
            return False

    def setContext(self, context):
        if isinstance(context, dict):
            self.__context = orb.Context(**context)
            return True
        elif isinstance(context, orb.Context):
            self.__context = context
            return True
        else:
            return False

    def setId(self, value, **context):
        return self.set(self.schema().id_column(), value, use_method=False, **context)

    def update(self, values, **context):
        """
        Updates the model with the given dictionary of values.

        :param values: <dict>
        :param context: <orb.Context>

        :return: <int>
        """
        schema = self.schema()
        column_updates = {}
        other_updates = {}
        for key, value in values.items():
            try:
                column_updates[schema.column(key)] = value
            except orb.errors.ColumnNotFound:
                other_updates[key] = value

        # update the columns in order
        for col in sorted(column_updates.keys(), key=lambda x: x.order()):
            self.set(col, column_updates[col], **context)

        # update the other values
        for key, value in other_updates.items():
            try:
                self.set(key, value, **context)
            except orb.errors.ColumnValidationError:
                pass

        return len(values)

    def validate(self, columns=None):
        """
        Validates the current record object to make sure it is ok to commit to the database.  If
        the optional override dictionary is passed in, then it will use the given values vs. the one
        stored with this record object which can be useful to check to see if the record will be valid before
        it is committed.

        :param      overrides | <dict>

        :return     <bool>
        """
        schema = self.schema()
        if not columns:
            ignore_flags = orb.Column.Flags.Virtual | orb.Column.Flags.ReadOnly
            columns = schema.columns(flags=~ignore_flags).values()
            use_indexes = True
        else:
            use_indexes = False

        # validate the column values
        values = self.values(key='column', columns=columns)
        for col, value in values.items():
            if not col.validate(value):
                return False

        # valide the index values
        if use_indexes:
            for index in self.schema().indexes().values():
                if not index.validate(self, values):
                    return False

        return True

    def values(self,
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

        for column in columns or schema.columns(recurse=recurse, flags=flags).values():
            column = column if isinstance(column, orb.Column) else schema.column(column)
            try:
                val_key = column if key == 'column' else getattr(column, key)()
            except AttributeError:
                raise orb.errors.OrbError(
                    'Invalid key used in data collection.  Must be name, field, or column.'
                )
            else:
                value = self.get(column, **get_options)
                output[val_key] = mapper(value) if mapper else value

        return output

    #----------------------------------------------------------------------
    #                           CLASS METHODS
    #----------------------------------------------------------------------

    @classmethod
    def addCallback(cls, eventType, func, record=None, once=False):
        """
        Adds a callback method to the class.  When an event of the given type is triggered, any registered
        callback will be executed.

        :param  eventType: <str>
        :param  func: <callable>
        """
        callbacks = cls.callbacks()
        callbacks.setdefault(eventType, [])
        callbacks[eventType].append((func, record, once))

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
    def callbacks(cls, eventType=None):
        """
        Returns a list of callback methods that can be invoked whenever an event is processed.

        :return: {subclass of <Event>: <list>, ..}
        """
        key = '_{0}__callbacks'.format(cls.__name__)
        try:
            callbacks = getattr(cls, key)
        except AttributeError:
            callbacks = {}
            setattr(cls, key, callbacks)

        return callbacks.get(eventType, []) if eventType is not None else callbacks

    @classmethod
    def create(cls, values, **context):
        """
        Shortcut for creating a new record for this table.

        :param     values | <dict>

        :return    <orb.Table>
        """
        schema = cls.schema()
        model = cls

        # check for creating inherited classes from a sub class
        polymorphic_columns = schema.columns(flags=orb.Column.Flags.Polymorphic)
        if polymorphic_columns:
            polymorphic_column = polymorphic_columns.values()[0]
            schema_name = values.get(polymorphic_column.name(), schema.name())
            if schema_name and schema_name != schema.name():
                schema = orb.system.schema(schema_name)
                if not schema:
                    raise orb.errors.ModelNotFound(schema=schema_name)
                else:
                    model = schema.model()

        column_values = {}
        collector_values = {}

        for key, value in values.items():
            obj = schema.collector(key) or schema.column(key)
            if isinstance(obj, orb.Collector):
                collector_values[key] = value
            else:
                column_values[key] = value

        # create the new record with column values (values stored on this record)
        record = model(context=orb.Context(**context))
        record.update(column_values)
        record.save()

        # save any collector values after the model is generated (values stored on other records)
        record.update(collector_values)

        return record

    @classmethod
    def ensureExists(cls, values, defaults=None, **context):
        """
        Defines a new record for the given class based on the
        inputted set of keywords.  If a record already exists for
        the query, the first found record is returned, otherwise
        a new record is created and returned.

        :param      values | <dict>
        """
        # require at least some arguments to be set
        if not values:
            return cls()

        # lookup the record from the database
        q = orb.Query()

        for key, value in values.items():
            column = cls.schema().column(key)
            if not column:
                raise orb.errors.ColumnNotFound(schema=cls.schema(), column=key)
            elif column.test_flag(column.Flags.Virtual):
                continue

            if (isinstance(column, orb.StringColumn) and
                not column.test_flag(column.Flags.CaseSensitive) and
                not column.test_flag(column.Flags.I18n) and
                isinstance(value, (str, unicode))):
                q &= orb.Query(key).lower() == value.lower()
            else:
                q &= orb.Query(key) == value

        record = cls.select(where=q).first()
        if record is None:
            record = cls(context=orb.Context(**context))
            record.update(values)
            record.update(defaults or {})
            record.save()

        return record

    @classmethod
    def processEvent(cls, event):
        """
        Processes the given event by dispatching it to any waiting callbacks.

        :param event: <orb.Event>
        """
        callbacks = cls.callbacks(type(event))
        keep_going = True
        remove_callbacks = []

        for callback, record, once in callbacks:
            if record is not None and record != event.record:
                continue

            callback(event)
            if once:
                remove_callbacks.append((callback, record))

            if event.preventDefault:
                keep_going = False
                break

        for callback, record in remove_callbacks:
            cls.removeCallback(type(event), callback, record=record)

        return keep_going

    @classmethod
    def fetch(cls, key, **context):
        """
        Looks up a record based on the given key.  This will use the
        default id field, as well as any keyable properties if the
        given key is a string.

        :param key: <variant>
        :param context: <orb.Context>
        :return: <orb.Model> || None
        """
        # include any keyable columns for lookup
        if isinstance(key, basestring) and not key.isdigit():
            keyable_columns = cls.schema().columns(flags=orb.Column.Flags.Keyable)
            if keyable_columns:
                base_q = orb.Query()
                for col in keyable_columns:
                    base_q |= orb.Query(col) == key
                context.setdefault('where', base_q)
            else:
                context.setdefault('where', orb.Query(cls) == key)
        else:
            context.setdefault('where', orb.Query(cls) == key)

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
        polymorphs = schema.columns(flags=orb.Column.Flags.Polymorphic).values()
        column = polymorphs[0] if polymorphs else None

        # attempt to expand the class to its defined polymorphic type
        if column and column.field() in values:
            morph_cls_name = values.get(column.name(), values.get(column.field()))
            morph_cls = orb.system.model(morph_cls_name)
            id_col = schema.id_column().name()
            if morph_cls and morph_cls != cls:
                try:
                    record = morph_cls(values[id_col], context=context)
                except KeyError:
                    raise orb.errors.RecordNotFound(schema=morph_cls.schema(),
                                                    column=values.get(id_col))

        if record is None:
            event = orb.events.LoadEvent(record=record, data=values)
            record = cls(loadEvent=event, context=context)

        return record

    def read(self, refresh=False):
        if not self.__loaded or refresh:
            self.__loaded = True
            record_id = self.id()

            if record_id is None:
                raise orb.errors.RecordNotFound(schema=self.schema(), column=record_id)

            data = self.fetch(record_id, returning='data', context=self.__context)
            if not data:
                raise orb.errors.RecordNotFound(schema=self.schema(),
                                            column=record_id)

            event = orb.events.LoadEvent(record=self, data=data)
            self._load(event)

    @classmethod
    def record_defaults(cls, ignore=None, ignore_flags=None, context=None):
        """
        Returns the default values for a record of this class type.  You can provide
        a list of strings or columns to ignore when generating the dictionary, as well
        as a flag definition of columns to exclude.  If flags is None, then Virtual and
        ReadOnly columns will not be included.

        :param ignore: None or [<str> column, ..]
        :param ignore_flags: None or <orb.Column.Flags>

        :return: <generator>
        """
        context = context or orb.Context()
        ignore_flags = ignore_flags or (orb.Column.Flags.Virtual | orb.Column.Flags.ReadOnly)
        schema = cls.schema()
        ignore_columns = {schema.column(c) for c in ignore or []}
        for column in schema.columns(flags=~ignore_flags).values():
            if column in ignore_columns:
                continue
            elif column.test_flag(column.Flags.I18n):
                yield column.name(), {context.locale: column.default()}
            elif column.test_flag(column.Flags.Polymorphic):
                yield column.name(), cls.__name__
            else:
                yield column.name(), column.default()


    @classmethod
    def removeCallback(cls, eventType, func, record=None):
        """
        Removes a callback from the model's event callbacks.

        :param  eventType: <str>
        :param  func: <callable>
        """
        callbacks = cls.callbacks()
        callbacks.setdefault(eventType, [])
        for i in xrange(len(callbacks[eventType])):
            my_func, my_record, _ = callbacks[eventType][i]
            if func == my_func and record == my_record:
                del callbacks[eventType][i]
                break

    @classmethod
    def search(cls, terms, **context):
        if isinstance(cls.__search_engine__, (str, unicode)):
            engine = SearchEngine.byName(cls.__search_engine__)
            if not engine:
                raise orb.errors.SearchEngineNotFound(cls.__search_engine__)
        else:
            engine = cls.__search_engine__
        return engine.search(cls, terms, **context)

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
        return rset_type(**context).bind_model(cls)

    @classmethod
    def setBaseQuery(cls, query):
        """
        Sets the default table query value.  This method can be used to control
        all queries for a given table by setting global where inclusions.

        :param      query | <orb.Query> || None
        """
        setattr(cls, '_%s__baseQuery' % cls.__name__, query)

    @classmethod
    def schema(cls):
        """
        Returns the schema that is associated with this model, if any is defined.

        :return: <orb.Schema> or None
        """
        return getattr(cls, '_{0}__schema'.format(cls.__name__), None)


class Table(Model):
    """ Defines specific database Table model base class """
    __model__ = False


class View(Model):
    """ Defines specific database View model base class """
    __model__ = False

    def delete(self, **context):
        """
        Re-implements the delete method from `orb.Model`.  Database
        views are read-only and this will raise a runtime error if
        called.

        :param context: <orb.Context> descriptor
        """
        raise orb.errors.OrbError('View models are read-only.')

    def save(self, **context):
        """
        Re-implements the save method from `orb.Model`.  Database
        views are read-only and this will raise a runtime error if
        called.

        :param context: <orb.Context> descriptor
        """
        raise orb.errors.OrbError('View models are read-only.')


