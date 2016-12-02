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
from ..utils.locks import ReadLocker, ReadWriteLock, WriteLocker

from .collection import Collection
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
    # define class properties
    __metaclass__ = MetaModel
    __auth__ = AuthorizationPolicy()
    __base_query__ = None
    __collection_type__ = Collection
    __model__ = False
    __search_engine__ = None
    __schema__ = None

    # signals
    about_to_save = blinker.Signal()
    about_to_sync = blinker.Signal()
    changed = blinker.Signal()
    deleted = blinker.Signal()
    saved = blinker.Signal()
    synced = blinker.Signal()

    # built-ins
    # --------------------

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
        self.__collections = defaultdict(dict)
        self.__context = self.schema().context(**context)
        self.__loaded = set()
        self.__preload = {}

        # initialize record from values
        if type(record) is dict:
            self.update(record)
            ignored = list(self.__attributes.keys()) + record.keys()
            defaults = dict(self.__class__.iter_defaults(ignore=ignored, context=self.__context))
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
            self.update(dict(self.__class__.iter_defaults(context=self.__context)))

    def __eq__(self, other):
        """
        Checks to see if one model is equal to the other.  This is done by comparing
        the instance against each other, then comparing to see if each model is of
        the same type, shares the same id, and has no local changes.

        :return: <bool>
        """
        if self is other:
            return True
        elif type(self) != type(other):
            return False
        else:
            my_id = self.id()
            other_id = other.id()

            try:
                my_db = self.context().db
            except orb.errors.DatabaseNotFound:
                my_db = None

            try:
                other_db = other.context().db
            except orb.errors.DatabaseNotFound:
                other_db = None

            if my_db != other_db:
                return False
            elif my_id is None or my_id != other_id:
                return False
            elif self.is_modified():
                return False
            elif other.is_modified():
                return False
            else:
                return True

    def __ne__(self, other):
        """
        Checks to see if one model is equal to the other.  This is done by comparing
        the instance against each other, then comparing to see if each model is of
        the same type, shares the same id, and has no local changes.

        :return: <bool>
        """
        if self is other:
            return False  # pragma: no cover
        elif type(self) != type(other):
            return True
        else:
            my_id = self.id()
            other_id = other.id()

            try:
                my_db = self.context().db
            except orb.errors.DatabaseNotFound:
                my_db = None

            try:
                other_db = other.context().db
            except orb.errors.DatabaseNotFound:
                other_db = None

            if my_db != other_db:
                return True
            elif my_id is None or my_id != other_id:
                return True
            elif self.is_modified():
                return True
            elif other.is_modified():
                return True
            else:
                return False

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
        schema = self.schema()
        context = self.context()
        output = dict(self)

        # don't include fields
        if context.returning == 'values':
            columns = context.schema_columns(self.schema()) or schema.columns().values()
            output = tuple(output[column.alias()]
                           for column in columns
                           if column.alias() in output)

            # don't return tuple for single column requests
            if len(output) == 1:
                output = output[0]

        return json2.dumps(output) if context.format == 'text' else output

    def __iter__(self):
        """
        Iterates this object for its values.

        :return     <generator>
        """
        return self.iter_record(returning='data')

    # instance methods
    # --------------------

    def changes(self, **context):
        """
        Collects all the changes that have been made to the data of this record
        compared to the base attributes.  The default return type for this
        method will be raw values (`returning='values'`) to avoid unnecessary
        model inflation.  You can change that to `returning='records'` if you
        want full record objects out of references.

        :param context: <orb.Context> descriptor

        :return: {<orb.Column>: (<variant> old_value, <variant> new_value), ..}
        """
        return dict(self.iter_changes(**context))

    def context(self, **context):
        """
        Returns the context for this record.  It will join together any custom options with the
        internal context defined for this record.

        :param context: <orb.Context> descriptor

        :return: <orb.Context>
        """
        output = orb.Context(context=self.__context)
        output.update(context)
        return output

    def delete(self, **context):
        """
        Runs the deletion logic for this record from the backend.

        :return: <bool> deleted
        """
        # cannot delete a record that does not exist
        if not self.is_record():
            return False

        # create the deletion event and process it
        orb_context = self.context(**context)
        event = orb.events.DeleteEvent(record=self, context=orb_context)
        self.on_delete(event)
        if event.prevent_default:
            return False

        # ensure that the data for this record has been loaded
        conn = orb_context.db.connection()
        _, count = conn.delete([self], orb_context)

        # clear out the old values
        if count:
            self.mark_unloaded()

        return count > 0

    def get(self, key, use_method=True, **context):
        """
        Returns the value of a column or collector for this record.  If
        the `use_method` is True, then it will try to use any defined
        `gettermethod`'s for the column or collector.  Set that to False
        if you would like to ignore the gettermethod function and use
        the default logic.

        :param column: <str> or <orb.Column>
        :param use_method: <bool> (default: True)
        :param context: <orb.Context> descriptor

        :return: <variant>
        """
        # get a column value
        schema = self.schema()
        column = schema.column(key, raise_=False)
        collector = schema.collector(key)

        # get a shortcut value
        if isinstance(key, (str, unicode)) and '.' in key:
            return self.get_shortcut(key, use_method=use_method, **context)

        # get an attribute value
        elif column is not None:
            # for reference columns, if the field or alias is the key, then return the
            # raw values, otherwise return the record instances
            if isinstance(column, orb.ReferenceColumn):
                context.setdefault('returning', 'values' if key in (column.field(), column.alias()) else 'records')

            return self.get_attribute(column, use_method=use_method, **context)

        # get a collection value
        elif collector is not None:
            return self.get_collection(collector, use_method=use_method, **context)

        # raise an error if could not find key
        else:
            raise orb.errors.ColumnNotFound(schema=schema, column=key)

    def get_attribute(self, column, use_method=True, **context):
        """
        Returns the attribute value of the given column for this record.  If the `use_method` flag
        is set to True, then if the column has a custom `gettermethod` defined, it will use that, otherwise
        it will retrieve the raw value from this record.

        :param column: <orb.Column>
        :param use_method: <bool>
        :param context: <orb.Context> descriptor

        :return: <variant>
        """
        orb_context = self.context()
        sub_context = orb_context.sub_context(**context)

        # expand through a shortcut manually (unless the model is a View type, where the backend
        # will pre-merge shortcuts)
        if column.shortcut() and not isinstance(self, orb.View):
            return self.get_shortcut(column.shortcut(), use_method=use_method, **context)

        # use the getter method if available and the use_method is flagged as True
        getter = column.gettermethod()
        if getter is not None and use_method:
            return getter(self, context=sub_context)

        # grab the current value
        with ReadLocker(self.__lock):
            value = self.__attributes[column.name()]

        # read the value from the backend if not defined
        if value is None and not self.is_loaded(columns=[column]):
            self.read(refresh=False)
            with ReadLocker(self.__lock):
                value = self.__attributes[column.name()]

        # restore the value, which could include inflating references
        # if the value changes based on the restoration, then we should store that newly created
        # value for next time
        output = column.restore(value, context=sub_context)
        if isinstance(output, orb.Model) and not isinstance(value, orb.Model):
            with WriteLocker(self.__lock):
                self.__attributes[column.name()] = output

        return output

    def get_collection(self, collector, use_method=True, **context):
        """
        Returns the collection for the given collector for this record.  If the `use_method` flag
        is set to True, then if the collector has a custom `gettermethod` defined, it will use that, otherwise
        it will retrieve the raw value from this record.

        :param collector: <orb.Collector>
        :param use_method: <bool>
        :param context: <orb.Context> descriptor

        :return: <orb.Collection> or None
        """
        orb_context = self.context()
        sub_context = orb_context.sub_context(**context)

        # return the cached collection
        try:
            with ReadLocker(self.__lock):
                return self.__collections[collector.name()][sub_context]

        # otherwise, lookup the collection and cache it
        except KeyError:
            collection = collector.collect(self, use_method=use_method, context=sub_context)
            with WriteLocker(self.__lock):
                self.__collections[collector.name()][sub_context] = collection
            return collection

    def get_shortcut(self, shortcut, use_method=True, **context):
        """
        Traverses a shortcut and retreives the value for the end field.

        :param shortcut: <str>
        :param use_method: <bool>
        :param context: <orb.Context> descriptor

        :return: <variant>
        """
        schema = self.schema()

        # make sure that there is an end column for this shortcut
        # (if not found, this method will raise the `orb.errors.ColumnNotFound` error)
        column = schema.column(shortcut)
        expand_path_index = -1 if isinstance(column, orb.ReferenceColumn) else None

        # create the sub context
        base_context = context.copy()
        base_context['returning'] = 'records'

        # generate the expansion to avoid unnecessary lookups
        parts = shortcut.split('.')

        value = self
        for i, part in enumerate(parts[:-1]):
            sub_context = base_context.copy()
            expand_path = '.'.join(parts[i + 1:expand_path_index])

            # when getting a reference from the backend, include the full expansion
            # request for this lookup to ensure the minimum number of backend lookups
            # are executed
            try:
                sub_expand = sub_context['expand']
            except KeyError:
                sub_context['expand'] = expand_path
            else:
                # include expansion via comma separated list
                if isinstance(sub_expand, (str, unicode)):
                    sub_context['expand'] += ',{0}'.format(expand_path)

                # include expansion via addition to list of expansion requests
                elif isinstance(sub_expand, list):
                    sub_expand.append(expand_path)

                # include expansion as a nested trie of expansion requests
                elif isinstance(sub_expand, dict):
                    curr = {}
                    for x in range(len(parts) - 1 + (expand_path_index or 0), i, -1):
                        curr = {parts[x]: curr}
                    sub_expand.update(curr)

            # gets the value of the part from the database
            value = value.get(part, use_method=use_method, **sub_context)
            if value is None:
                return None
        else:
            return value.get(parts[-1], use_method=use_method, **context)

    def id(self, use_method=True, **context):
        """
        Returns the ID for this record.  This will lookup the schema's `id_column` and return
        the attribute value for it.

        :param use_method: <bool>
        :param context: <orb.Context> descriptor

        :return: <variant>
        """
        column = self.schema().id_column()
        getter = column.gettermethod()
        if getter and use_method:
            return getter(self, **context)
        else:
            with ReadLocker(self.__lock):
                return self.__attributes.get(column.name())

    def is_loaded(self, **context):
        """
        Checks to see if the record is loaded.  If you want to check if
        particular columns are loaded, you can provide the `columns` key
        to the context.

        :param context: <orb.Context> descriptor

        :return: <bool>
        """
        orb_context = self.context(**context)

        # look for particular columns
        with ReadLocker(self.__lock):
            for column in orb_context.schema_columns(self.schema()):
                if not column.name() in self.__loaded:
                    return False

            # look for any columns
            else:
                return len(self.__loaded) > 0

    def is_modified(self, **context):
        """
        Returns whether or not any data has been modified for
        this object.

        :return     <bool>
        """
        with ReadLocker(self.__lock):
            return self.__base_attributes != self.__attributes

    def is_record(self, **context):
        """
        Returns whether or not this database table record exists
        in the database.

        :return     <bool>
        """
        my_context = self.context()
        other_context = orb.Context(**context)

        try:
            my_db = my_context.db
        except orb.errors.DatabaseNotFound:
            my_db = None

        try:
            other_db = other_context.db
        except orb.errors.DatabaseNotFound:
            other_db = None

        # ensure that we're looking in the same context, and that there is a backend
        # for this context
        if other_db is None or my_db == other_db:
            column = self.schema().id_column()
            with ReadLocker(self.__lock):
                base_id = self.__base_attributes.get(column.name())
                curr_id = self.__attributes.get(column.name())
                return base_id is not None and curr_id == base_id
        else:
            return False

    def iter_attributes(self, columns, tree=None, context=None):
        """
        Iterates over the values from this model for a given column.  If the
        column is a reference column, and it's name is included in the expanded
        tree, then this method will yield both it's raw reference value, and
        it's expanded value.

        :param columns: [<orb.Column>, ..]
        :param tree: <dict>
        :param context: <orb.Context>

        :return: <generator>
        """
        tree = tree or {}
        context = context or orb.Context()

        for column in columns:
            # ignore tree columns
            if column.test_flag(column.Flags.RequiresExpand) and column.name() not in tree:
                continue

            # ignore permission denied columns
            elif not self.__auth__.can_read_column(column, context=context):
                continue

            # fetch custom data for the column
            elif column.gettermethod() is not None:
                yield column.alias(), self.get(column, context=context)

            # fetch raw data for the column
            else:
                value = self.__attributes[column.name()]

                # normalize the value from the cache
                if column.test_flag(orb.Column.Flags.I18n) and type(value) == dict and context.locale != 'all':
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
                        reference = self.get_attribute(column,
                                                       expand=subtree,
                                                       returning=context.returning,
                                                       scope=context.scope)
                        yield column.name(), reference

    def iter_changes(self, **context):
        """
        Returns an iterator over all of the changes for this record's attributes
        compared to the base attributes.  The default return type for this
        method will be raw values (`returning='values'`) to avoid unnecessary
        model inflation.  You can change that to `returning='records'` if you
        want full record objects out of references.

        :param context: <orb.Context> descriptor

        :return: <generator>
        """
        context.setdefault('returning', 'values')

        orb_context = self.context()
        sub_context = orb_context.sub_context(**context)
        schema = self.schema()
        columns = orb_context.schema_columns(schema) or schema.columns().values()

        with ReadLocker(self.__lock):
            for col in columns:
                if col.test_flag(col.Flags.ReadOnly):
                    continue
                else:
                    current_value = self.__attributes.get(col.name())
                    base_value = self.__base_attributes.get(col.name())

                    current_value = col.restore(current_value, context=sub_context)
                    base_value = col.restore(base_value, context=sub_context)

                    # can sometimes get offset aware issues, or unicode comparison
                    # issues.  if you can't compare the two values without hitting
                    # an error, then it can be assumed that they are different
                    try:
                        different = base_value != current_value
                    except Exception:  # pragma: no cover
                        different = True

                    if different:
                        yield col, (base_value, current_value)

    def iter_expand_tree(self, tree, context=None):
        """
        Iterates over the expansion tree yielding results
        for the schema object.

        :param tree: <dict>
        :param context: <orb.Context>

        :return: <generator>
        """
        context = context or orb.Context()

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
                yield attribute, value

    def iter_record(self, **context):
        """
        Iterates the properties for this record.

        :param context: <orb.Context>

        :return: <generator>
        """
        schema = self.schema()
        context = self.context(**context)
        expand_tree = context.expandtree(type(self))

        # collect the columns that will be iterated over
        columns = context.schema_columns(schema)
        if not columns:
            columns = schema.columns(flags=~orb.Column.Flags.RequiresExpand).values()

        # iterate the columns
        for attribute, value in self.iter_attributes(columns, tree=expand_tree, context=context):
            yield attribute, value

        # iterate the expansion preferences
        for attribute, value in self.iter_expand_tree(expand_tree, context=context):
            yield attribute, value

    def mark_loaded(self, columns=None):
        """
        Marks columns as loaded by updating the stored base attributes with
        the current attributes.  If no columns are provided, then the currently
        set attributes will be used as the columns to mark as loaded.

        :param columns: [<str>, ..] or [<orb.Column>, ..] or None
        """
        schema = self.schema()
        column_names = [schema.column(c).name() for c in columns] if columns else self.__attributes.keys()

        with WriteLocker(self.__lock):
            self.__base_attributes.update({k: v for k, v in self.__attributes.items() if k in column_names})
            self.__loaded.update(column_names)

    def mark_unloaded(self, columns=None):
        """
        Marks the columns as unloaded by clearing the stored base attributes.
        If no columns are provided, then all loaded data will be cleared.

        :param columns: [<str>, ..] or [<orb.Column>, ..] or None
        """
        if not columns:
            with WriteLocker(self.__lock):
                self.__base_attributes.clear()
                self.__loaded.clear()
        else:
            schema = self.schema()
            column_names = [schema.column(c).name() for c in columns]
            with WriteLocker(self.__lock):
                for column_name in column_names:
                    self.__base_attributes.pop(column_name, None)
                    try:
                        self.__loaded.remove(column_name)
                    except KeyError:  # pragma: no cover
                        pass

    def on_change(self, event):
        """
        Called before a record's attributes are changed.    The default
        behavior is to send the `changed` signal to any receivers.

        :param event: <orb.ChangeEvent>
        """
        Model.changed.send(type(self), event=event)
        if not event.prevent_default:
            Model.changed.send(self, event=event)

    def on_delete(self, event):
        """
        Called before a record is deleted from a backend store.  The default
        behavior is to send the `deleted` signal to any receivers.

        :param event: <orb.DeleteEvent>
        """
        Model.deleted.send(type(self), event=event)
        if not event.prevent_default:
            Model.deleted.send(self, event=event)

    def on_pre_save(self, event):
        """
        Called right before a record is being saved to the backend store.  The
        default behavior is to send the `about_to_save` signal to any receivers.

        :param event: <orb.SaveEvent>
        """
        Model.about_to_save.send(type(self), event=event)
        if not event.prevent_default:
            Model.about_to_save.send(self, event=event)

    def on_post_save(self, event):
        """
        Called right after a record has successfully saved to the backend store.  The
        default behavior is to send the `saved` signal to any receivers.

        :param event: <orb.SaveEvent>
        """
        Model.saved.send(type(self), event=event)
        if not event.prevent_default:
            Model.saved.send(self, event=event)

    def parse(self, raw_data):
        """
        Parses raw data from a backend to update this record's attributes.

        :param raw_data: <dict>
        """
        attributes = {}
        orb_context = self.context()
        schema = self.schema()
        base_table_name = schema.dbname()
        for raw_field, raw_value in raw_data.items():
            try:
                table_name, alias = raw_field.split('.')
            except ValueError:
                table_name = base_table_name
                alias = raw_field

            # make sure the value we're setting is specific to this model
            column = schema.column(alias, raise_=False)

            if column is None:
                self.__preload[alias] = raw_value

            # ignore data that is not related to this model
            elif table_name != base_table_name:
                continue

            # ignore data if we already have a model for the information (since that
            # will contain any fields or related data intrinsically)
            elif column.name() in attributes and isinstance(attributes[column.name()], orb.Model):
                continue

            # extract the value from the database
            else:
                value = column.database_restore(raw_value, context=orb_context)
                attributes[column.name()] = value

        # update the local values
        with WriteLocker(self.__lock):
            self.__loaded.update(attributes.keys())
            self.__attributes.update(attributes)
            self.__base_attributes.update({k: v.copy() if type(v) == dict else v
                                           for k, v in attributes.items()})

    def preload_data(self, cache):
        """
        Preloads information from a cache that will be accessed later.  This is used when adding references
        or collections from a single query to the model.

        :param cache: {<str> name: <variant> value, ..}
        """
        for key, value in cache.items():
            self.__preload[key] = value

    def preloaded_data(self, name):
        """
        Returns raw backend data that has been preloaded for
        the given data name.

        :param name: <str>

        :return: <variant>
        """
        return self.__preload.get(name)

    def read(self, refresh=True):
        """
        Reads data from the backend.  If the `refresh` flag is True then this will actively re-sync this
        record with it's value from the backend.  If the flag is False, then it will only load data if it
        has not previously been loaded.

        :param refresh: <bool>

        :return: <bool> loaded
        """
        refresh = refresh or not self.is_loaded()
        schema = self.schema()
        record_id = self.id()

        if refresh and record_id is not None:
            raw_data = self.__class__.fetch(record_id, returning='data', context=self.__context)
            if not raw_data:
                raise orb.errors.RecordNotFound(schema=schema, column=record_id)
            else:
                self.parse(raw_data)
                return True
        else:
            return False

    def reset(self, columns=None):
        """
        Resets this record's values to the last loaded ones.  If no columns are provided, then the currently
        set attributes will be used as the columns to mark as loaded.

        :param columns: [<str>, ..] or [<orb.Column>, ..] or None
        """
        schema = self.schema()
        column_names = [schema.column(c).name() for c in columns] if columns else self.__base_attributes.keys()

        with WriteLocker(self.__lock):
            self.__attributes.update({k: v for k, v in self.__base_attributes.items() if k in column_names})

    def save(self, values=None, after=None, before=None, **context):
        """
        Saves the current changes to the backend.  This method will either create
        a new record if one does not already exist, or update an existing backend
        record.  This method will only have any effect on the backend if there
        are changes.  If the `values` keyword is supplied, then the values provided
        will be updated on this model prior to save, and only the columns within those values
        will be saved.  You can optionally provide the `columns` context keyword to limit
        which columns to save.

        If you provide the `before` keyword, then the call to this save function will be delayed
        until the `about_to_save` signal is emitted by the `before` record supplied.

        If you provide the `after` keyword, then the call to this save function will be delayed
        until the `saved` signal is emitted by the `after` record supplied.

        :param values: {<str> or <orb.Column> column: <variant> value, ..} or None
        :param after: <orb.Model> or None
        :param before: <orb.Model> or None
        :param context: <orb.Context> descriptor

        :return: <bool>
        """
        # shortcut to the save after logic
        if after is not None:
            return self.save_after(after, values=values, **context) is not None

        # shortcut to the save before logic
        elif before is not None:
            return self.save_before(before, values=values, **context) is not None

        # execute the save function
        else:
            # update the model with any value specific changes pre-save,
            # setting up the columns to be used during save
            if values is not None:
                self.update(values, **context)
                context.setdefault('columns', values.keys())

            # generate the context options
            orb_context = self.context(**context)
            new_record = not self.is_record(**context)

            change_context = context.copy()
            change_context['returning'] = 'values'
            changes = self.changes(**change_context)

            # generate the pre-save event, which can be
            # used to actually create changes so we should
            # run this even if there are no active changes
            # on the record yet
            event = orb.events.SaveEvent(record=self,
                                         context=orb_context,
                                         new_record=new_record,
                                         changes=changes)

            self.on_pre_save(event)
            if event.prevent_default:
                return False

            # at this point validate that we have changes, and that
            # those changes are ok
            if not (self.is_modified(**context) and self.validate()):
                return False

            # execute the backend save
            conn = orb_context.db.connection()
            if new_record:
                raw_records, _ = conn.insert([self], orb_context)
                try:
                    raw_record = raw_records[0]
                except IndexError:
                    return False
                else:
                    self.parse(raw_record)
            else:
                conn.update([self], orb_context)

            # notify about the post save event
            self.on_post_save(event)
            return True

    def save_after(self, record, values=None, **context):
        """
        Delays the save for this instance until after the given record's `saved` signal has been triggered.

        :sa: save, save_after

        :param record: <orb.Model>
        :param values: <dict>
        :param context: <orb.Context> descriptor

        :return: <orb.Callback>
        """
        context['values'] = values
        callback = orb.Callback(self.save,
                                kwargs=context,
                                signal=Model.saved,
                                sender=record,
                                single_shot=True)
        return callback

    def save_before(self, record, values=None, **context):
        """
        Delays the save for this instance until after the given record's `about_to_save` signal has been
        triggered.

        :sa: save, save_before

        :param record: <orb.Model>
        :param values: <dict>
        :param context: <orb.Context> descriptor

        :return: <orb.Callback>
        """
        context['values'] = values
        callback = orb.Callback(self.save,
                                kwargs=context,
                                signal=Model.about_to_save,
                                sender=record,
                                single_shot=True)
        return callback

    def set(self, key, value, use_method=True, silent=False, **context):
        """
        Sets the value for the given key for this record.  If the `use_method`
        flag is set then the settermethod will be used for the schema object,
        otherwise it will update the raw cache of this record.

        :param key: <str> or <orb.Column> or <orb.Collector>
        :param value: <variant>
        :param use_method: <bool> (default: True)
        :param silent: <bool> (default: False)
        :param context: <orb.Context> descriptor

        :return: <bool>
        """
        schema = self.schema()
        column = schema.column(key, raise_=False)

        # save a column attribute for this record
        if column:
            return self.set_attribute(column, value, use_method=use_method, silent=silent, **context)

        # save a collector's collection for this record
        collector = schema.collector(key)
        if collector:
            return self.set_collection(collector, value, use_method=use_method, **context)

        # raise an error
        raise orb.errors.ColumnNotFound(schema=schema, column=key)

    def set_attribute(self, column, value, use_method=True, silent=False, **context):
        """
        Sets the attribute for this record's column to the given value.  If the `use_method`
        flag is True, then any custom setter will be called for this column.  If the `silent`
        flag is set to True then the `on_change` event callback will not be executed.

        :param column: <orb.Column>
        :param value: <orb.Collection>
        :param use_method: <bool> (default: True)
        :param silent: <bool> (default: False)
        :param context: <orb.Context> descriptor

        :return: (<variant> old value, <variant> new value) or None
        """
        orb_context = self.context()
        sub_context = orb_context.sub_context(**context)

        # use custom setter logic
        setter = column.settermethod()
        if use_method and setter:
            return setter(self, value, context=sub_context)
        else:
            value = column.store(value, context=sub_context)
            with WriteLocker(self.__lock):
                orig_value = self.__attributes.get(column.name())

                if column.test_flag(column.Flags.I18n) and orig_value is not None:
                    orig_value.update(value)
                else:
                    self.__attributes[column.name()] = value

            # broadcast the change through the system
            try:
                changed = orig_value != value
            except TypeError:  # pragma: no cover
                changed = True

            if changed:
                # determine what to emit as the change value for i18n
                if column.test_flag(column.Flags.I18n) and sub_context.locale != 'all':
                    orig_value = orig_value.get(sub_context.locale) if orig_value else None
                    value = value.get(sub_context.locale)

                # create the change event for this attribute
                if not silent:
                    changes = {column: (orig_value, value)}
                    event = orb.events.ChangeEvent(record=self, changes=changes)
                    self.on_change(event)

                return orig_value, value
            else:
                return None

    def set_collection(self, collector, collection, use_method=True, **context):
        """
        Sets the collection for this record's collection to the given records.  If the `use_method`
        flag is True, then any custom setter will be called for this collector.

        :param collector: <orb.Collector>
        :param collection: <orb.Collection>
        :param use_method: <bool>
        :param context: <orb.Context> descriptor

        :return: <variant>
        """
        orb_context = self.context()
        sub_context = orb_context.sub_context(**context)

        method = collector.settermethod()

        # use custom setter calls
        if method and use_method:
            return method(self, collection, context=sub_context)

        # update existing collection
        else:
            try:
                with ReadLocker(self.__lock):
                    base_collection = self.__collections[collector.name()][sub_context]
            except KeyError:
                with WriteLocker(self.__lock):
                    base_collection = collector.collection(self)
                    self.__collections[collector.name()][sub_context] = base_collection

            base_collection.update(collection, use_method=use_method, context=sub_context)

            # remove any preloaded values from the collector
            # to ensure that there is no conflict later -- setting the collection
            # will override any preloaded data
            self.__preload.pop(collector.name(), None)

            return base_collection

    def set_context(self, context):
        if isinstance(context, dict):
            self.__context = orb.Context(**context)
            return True
        elif isinstance(context, orb.Context):
            self.__context = context
            return True
        else:
            return False

    def set_id(self, value, use_method=True, **context):
        """
        Shortcut for setting the ID of this record by setting the `id_column` for this
        record's attribute set.

        :param value: <variant>

        :return: <bool> changed
        """
        id_column = self.schema().id_column()
        setter = id_column.settermethod()
        if setter and use_method:
            return setter(self, value, **context)
        else:
            with WriteLocker(self.__lock):
                self.__attributes[id_column.name()] = value
            return True

    def update(self, values, **context):
        """
        Updates this record with the given values.  This will update columns and
        collectors values based on the given data set.  At the end, if there were
        any changes, a ChangeEvent will be fired for this record.

        :param values: <dict>
        :param context: <orb.Context>

        :return: <int> number of changes
        """
        schema = self.schema()

        # have to organize updates in order to update in the proper sequence
        attributes = {}
        collections = {}
        unknown = {}
        changes = {}
        for key, value in values.items():
            # update a column
            column = schema.column(key, raise_=None)
            if column is not None:
                attributes[column] = value
                continue

            # update a collector
            collector = schema.collector(key)
            if collector is not None:
                collections[collector] = value
                continue

        # update the attributes in order
        for column in sorted(attributes.keys(), key=lambda x: x.order()):
            changed = self.set_attribute(column,
                                         attributes[column],
                                         silent=True,
                                         **context)
            if changed:
                changes[column] = changed

        # update the collections
        for collector, value in collections.items():
            self.set_collection(collector, value, **context)

        return len(changes)

    def validate(self, **context):
        """
        Validates the current record object to make sure that the current
        values that are associated with it are ready to be saved to the
        backend, and validate the rules for the columns and collectors
        that the schema defines for this model.  If this method succeeds,
        it will return True, if it fails a ValidationError is raised.

        :param context: <orb.Context>

        :return: <bool>
        """
        schema = self.schema()
        orb_context = self.context(**context)
        columns = orb_context.schema_columns(schema)
        if not columns:
            ignore_flags = orb.Column.Flags.Virtual | orb.Column.Flags.ReadOnly
            columns = schema.columns(flags=~ignore_flags).values()
            validate_indexes = True
        else:
            validate_indexes = False

        # validate the column values
        for column in columns:
            with ReadLocker(self.__lock):
                value = self.__attributes.get(column.name())

            # will raise a ValidationError if this check does not pass
            column.validate(value)

        # valide the index values
        if validate_indexes:
            for index in schema.indexes().values():
                with ReadLocker(self.__lock):
                    values = {column: self.__attributes.get(column.name())
                              for column in index.schema_columns(schema)}

                # validates the index against the values for that it uses,
                # will raise a ValidationError if it does not pass
                index.validate(values)

        return True

    # class methods
    # --------------------

    @classmethod
    def all(cls, **context):
        """
        Returns all of the records for this model.  Functionally the same
        as the `select` method, but will force the context's limit to None.
        This will not actually fetch the results from the backend, but will
        generate a collection with the information on how to go collect
        in the future.

        :param context: <orb.Context> descriptor

        :return: <orb.Collection>
        """
        context['limit'] = None
        return cls.select(**context)

    @classmethod
    def get_base_query(cls, **context):
        """
        Generates and returns the base query for this model definition.  By
        default it will return the `__base_query__` class property, if
        defined, but can be re-implemented to include additional logic.

        :return: <orb.Query> or None
        """
        return cls.__base_query__

    @classmethod
    def get_collection_type(cls, **context):
        """
        Returns the collection class type for this model class.  By default
        the standard <orb.Collection> will be returned, but if you would
        like to re-define the base collection class for your model, you can
        either override the `__collection_type__` property or re-implement
        this method to define context specific collection types.

        :param context: <orb.Context> descriptor

        :return: subclass of <orb.Context>
        """
        return cls.__collection_type__

    @classmethod
    def get_search_engine(cls, **context):
        """
        Returns the search engine that is associated with this model.

        :return: <orb.SearchEngine>
        """
        if cls.__search_engine__ is None:
            cls.__search_engine__ = SearchEngine.factory('simple')

        return cls.__search_engine__

    @classmethod
    def create(cls, values, **context):
        """
        Creates a new record for this model class type with the given values.
        This will generate a new model and insert it into the backend,
        returning the newly generated record.

        :param values: {<str> or <orb.Column> or <orb.Collector> key: <variant> value, ..}
        :param context: <orb.Context> descriptor

        :return: <orb.Model>
        """
        schema = cls.schema()
        system = schema.system()
        model = cls

        # check for creating polymorphic models
        pcols = schema.columns(flags=orb.Column.Flags.Polymorphic).values()
        if pcols:
            pcol = pcols[0]
            model_name = values.get(pcol,
                                    values.get(pcol.name(),
                                               values.get(pcol.field(),
                                                          values.get(pcol.alias()))))

            # create from the proper polymorphic model
            # if not found, a ModelNotFound error will be raised
            if model_name and model_name != schema.name():
                model = system.model(model_name)
                schema = model.schema()

        attributes = {}
        collections = {}

        for key, value in values.items():
            collector = schema.collector(key)
            if collector:
                collections[collector] = value
            else:
                attributes[schema.column(key)] = value

        # create a new record with the given attributes,
        # save it, and then update the collections (which
        # require the record to exist in the backend)
        # afterwards
        record = model(attributes, **context)
        record.save()

        for key, value in collections.items():
            record.set_collection(key, value)

        return record

    @classmethod
    def ensure_exists(cls, required, defaults=None, **context):
        """
        Ensures that a record exists for the required values.  This
        method will first query for an exact match for the required
        fields, and if found, return the database record.  If not
        found, a new record will be created with the required values
        as well as the default values.

        :note: This method will NOT update the backend record with
               defaults if it already exists.  It will only set
               default values on a new record.

        :param required: {<str> or <orb.Column> column: <variant> value, ..}
        :param defaults: {<str> or <orb.Column> column: <variant> value, ..} or None
        :param context: <orb.Context> descriptor

        :return: <orb.Model>
        """
        if not required:
            raise orb.errors.OrbError('No values provided')

        # lookup the record from the database
        schema = cls.schema()
        q = orb.Query()

        for key, value in required.items():
            column = schema.column(key)

            # check for non-case sensitive columns
            if isinstance(value, (str, unicode)) and not column.test_flag(column.Flags.CaseSensitive):
                q &= orb.Query(key).lower() == value.lower()

            # check for exact matches
            else:
                q &= orb.Query(key) == value

        # lookup the record from the database
        sub_context = context.copy()
        sub_context['where'] = q & context.get('where')
        record = cls.select(**sub_context).first()

        # if not found, generate a new record with the required values and default values
        if record is None:
            values = required.copy()
            values.update(defaults or {})

            # create the new record
            record = cls.create(values, **context)

        return record

    @classmethod
    def fetch(cls, key, **context):
        """
        Looks up a record based on the given key.  This will use the
        default id field, as well as any keyable properties if the
        given key is a string.

        :param key: <variant>
        :param context: <orb.Context>

        :return: <orb.Model> or None
        """
        # include any keyable columns for lookup
        if isinstance(key, (str, unicode)) and not key.isdigit():
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
    def iter_defaults(cls, ignore=None, ignore_flags=None, context=None):
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
    def on_pre_sync(cls, event):
        """
        Called after a model has been synced to a backend store.    The default
        behavior is to send the `synced` signal to any receivers.

        :param event: <orb.SyncEvent>
        """
        Model.about_to_sync.send(cls, event=event)

    @classmethod
    def on_sync(cls, event):
        """
        Called after a model has been synced to a backend store.    The default
        behavior is to send the `synced` signal to any receivers.

        :param event: <orb.SyncEvent>
        """
        Model.synced.send(cls, event=event)

    @classmethod
    def restore_record(cls, raw_record, **context):
        """
        Returns a new record instance for the given class with the values
        defined from the database.

        :param raw_record: <dict> database record
        :param context: <orb.Context> descriptor

        :return: <orb.Model>
        """
        if isinstance(raw_record, orb.Model):
            return raw_record
        else:
            schema = cls.schema()
            schema_context = schema.context(**context)
            pcols = schema.columns(flags=orb.Column.Flags.Polymorphic).values()
            pcol = pcols[0] if pcols else None

            # attempt to expand the class to its defined polymorphic type
            if pcol and pcol.alias() in raw_record:
                model_name = raw_record[pcol.alias()]
                model = schema_context.system.model(model_name)
                schema_context = model.schema().context(context=schema_context)
            else:
                model = cls

            # create the record instance from the database
            record = model(context=schema_context)
            record.parse(raw_record)
            record.mark_loaded(model.schema().columns())
            return record

    @classmethod
    def schema(cls):
        """
        Returns the schema that is associated with this model, if any is defined.

        :return: <orb.Schema> or None
        """
        return cls.__schema__

    @classmethod
    def search(cls, terms, **context):
        """
        Searches through the records in this model for the given query terms.

        :param terms: <str>
        :param context: <orb.Context> descriptor

        :return: <orb.Collection>
        """
        return cls.select(**context).search(terms)

    @classmethod
    def select(cls, **context):
        """
        Selects records from the backend based on the given context.

        :param context: <orb.Context> descriptor

        :return: <orb.Collection>
        """
        rset_type = cls.get_collection_type(**context)
        return rset_type(**context).bind_model(cls)


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


