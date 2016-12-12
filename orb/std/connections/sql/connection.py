import datetime
import demandimport
import orb.errors
import os
import inflection
import logging

from jinja2 import Environment

from orb.core.pooled_connection import PooledConnection

with demandimport.enabled():
    import orb


log = logging.getLogger(__name__)


def raise_error(error_cls, msg=''):
    """
    Raises a given error, used within the jinja templates.

    :param error_cls: <Exception>
    :param msg: <str>
    """
    cls = getattr(orb.errors, error_cls, RuntimeError)
    raise cls(msg)


class SQLConnection(PooledConnection):
    __default_namespace__ = ''
    __templates__ = None

    def alter_model(self, model, context, add=None, remove=None, owner=''):
        """
        Re-implements orb.Connection.alter_model to update the backend model
        definition with the new information.

        :param model: subclass of <orb.Model>
        :param context: <orb.Context>
        :param add: {'fields': [<orb.Column>, ..], 'indexes': [<orb.Index>, ..]} or None
        :param remove: {'fields': [<orb.Column>, ..], 'indexes': [<orb.Index>, ..]} or None
        :param owner: <str>
        """
        if issubclass(model, orb.View):
            raise NotImplementedError('Cannot alter a view')
        else:
            cmd, data = self.render_alter_table(model,
                                                context=context,
                                                add=add,
                                                remove=remove,
                                                owner=owner)

            # execute the commands
            self.execute(cmd, data=data, write_access=True)
            return True

    def count(self, model, context):
        """
        Returns the count of records that will be loaded for the inputted
        information.

        :param model: subclass of <orb.Model>
        :param context: <orb.Context>

        :return: <int>
        """
        cmd = self.render('select_count.sql.jinja', {'model': model, 'context': context})
        records, _ = self.execute(cmd)
        return sum(record['count'] for record in records)

    def create_model(self, model, context, owner='', include_references=True):
        """
        Implements the `orb.Connection.create_model` abstract method.

        Creates a new model in the database based on the given context.

        :param model: subclass of <orb.Model>
        :param context: <orb.Context>

        :return: <bool> success
        """
        if issubclass(model, orb.View):
            cmd, data = self.render_create_view(model,
                                                context=context,
                                                owner=owner)
        else:
            cmd, data = self.render_create_table(model,
                                                 context=context,
                                                 owner=owner,
                                                 include_references=include_references)

        self.execute(cmd, data)
        return True

    def create_namespace(self, namespace, context):
        """
        Creates a new SQL namespace.

        :param namespace: <str>
        :param context: <orb.Context>
        """
        cmd = self.render('create_namespace.sql.jinja', {'namespace': namespace})
        self.execute(cmd, write_access=True)
        return True

    def current_schema(self, context):
        """
        Implements the `Connection.current_schema` abstract method.  This
        will return a dictionary that represents the current backend
        schema information.

        :param context: <orb.Context>

        :return: <dict>
        """
        return {}

    def delete(self, records, context):
        """
        Deletes records from the database.

        :param records: <orb.Collection>
        :param context: <orb.Context>

        :return: <int> number of rows removed
        """
        if isinstance(records, orb.Collection):
            cmd, data = self.render_delete_collection(records, context=context)
        else:
            cmd, data = self.render_delete_records(records, context=context)

        return self.execute(cmd, data, write_access=True)

    def execute(self,
                cmd,
                data=None,
                returning=True,
                mapper=dict,
                write_access=False,
                context=None):
        """
        Executes the given command(s) on the backend.

        :param cmd: <unicode> or [<unicode>, ..]
        :param data: <dict> or None
        :param returning: <bool>
        :param mapper: <callable>
        :param write_access: <bool>
        :param context: <orb.Context> or None
        """
        if isinstance(cmd, (str, unicode)):
            cmds = [cmd.strip()]
        elif isinstance(cmd, (list, tuple, set)):
            cmds = [x.strip() for x in cmd if x.strip()]
        else:
            raise RuntimeError('Invalid command')

        if not cmds:
            raise orb.errors.EmptyCommand()
        else:
            data = data or {}
            data.setdefault('locale', (context or orb.Context()).locale)
            start = datetime.datetime.now()

            with self.pool().current_connection(write_access=write_access) as conn:
                for cmd in cmds:
                    try:
                        results, row_count = self.execute_native_command(conn,
                                                                         cmd,
                                                                         data=data,
                                                                         returning=returning,
                                                                         mapper=mapper)

                    # always raise interruption errors as these need to be handled
                    # from a thread properly.  interruptions occur when the
                    # user cancels out a request
                    except orb.errors.Interruption:
                        raise

                    # log any additional errors
                    except Exception as err:
                        delta = datetime.datetime.now() - start
                        log.exception('Query Failed')
                        log.error(u'{0}\n\n{1}'.format(cmd, err))
                        log.error(u'query took: {0}'.format(delta))
                        raise

            delta = (datetime.datetime.now() - start).total_seconds()
            if delta * 1000 < 3000:
                lvl = logging.DEBUG
            elif delta * 1000 < 6000:
                lvl = logging.WARNING
            else:
                lvl = logging.CRITICAL

            log.log(lvl, u'query took: {0}'.format(delta))
            return results, row_count


    def insert(self, records, context):
        """
        Inserts new records into the datbaase.

        :param records: <orb.Collection>
        :param context: <orb.Context>

        :return: <dict> changes
        """
        cmd, data = self.render('insert.sql.jinja', records=records, context=context)
        return self.execute(cmd, data=data, write_access=True)

    def process_column(self, column, context):
        """
        Processes the SQL data for the given column.

        :param column: <orb.Column>
        :param context: <orb.Context>

        :return: <dict>
        """
        column_data = {
            'field': column.field(),
            'alias': column.field(),
            'is_string': isinstance(column, orb.StringColumn),
            'type': self.get_column_type(column, context),
            'sequence': '{0}_{1}_seq'.format(column.schema().dbname(), column.field()),
            'flags': {orb.Column.Flags(flag): True for flag in column.iter_flags()}
        }
        return column_data

    def process_index(self, index, context):
        """
        Processes the SQL data for the given index.

        :param index: <orb.Index>
        :param context: <orb.Context>

        :return: <dict>
        """
        index_data = {
            'name': index.dbname(),
            'columns': [self.process_column(c, context) for c in index.schema_columns()],
            'flags': {orb.Index.Flags(flag): True for flag in index.iter_flags()}
        }
        return index_data

    def process_model(self, model, context, aliases=None):
        """
        Processes the SQL data for the given model.

        :param model: subclass of <orb.Model>
        :param context: <orb.Context>
        :param aliases: <dict> or None

        :return: <dict>
        """
        aliases = aliases or {}
        inherits_model = model.schema().inherits_model()
        inherits_data = self.process_model(inherits_model, context) if inherits_model else {}
        model_data = {
            'namespace': model.schema().namespace(context=context) or self.get_default_namespace(),
            'name': model.schema().dbname(),
            'alias': aliases.get(model) or model.schema().alias(),
            'force_alias': model in aliases,
            'inherits': inherits_data
        }
        return model_data

    def process_query(self, model, query, context, aliases=None, fields=None):
        """
        Converts the query object to SQL template properties.

        Args:
            model: subclass of <orb.Model>
            query: <orb.Query>
            context: <orb.Context>
            aliases: <dict> or None
            fields: <dict> or None

        Returns:
            <dict> template options, <dict> data

        """
        column = query.column(model=model)
        value = query.value()
        value_id = u'{0}_{1}'.format(column.field(), os.urandom(4).encode('hex'))
        value_key = u'%({0})s'.format(value_id)

        # convert the value to something the database can use
        db_value, db_value_data = self.process_value(column,
                                                     query.op(),
                                                     value,
                                                     context,
                                                     aliases=aliases,
                                                     fields=fields)

        data = {value_id: db_value}
        data.update(db_value_data)

        column_data = self.process_column(column, context)
        query_field, query_data = self.render_query_field(model,
                                                          column,
                                                          query,
                                                          context=context,
                                                          aliases=aliases)

        data.update(query_data)
        kw = {
            'field': query_field,
            'column': column_data,
            'op': self.get_query_op(column, query.op(), query.case_sensitive()),
            'op_name': orb.Query.Op(query.op()),
            'case_sensitive': query.case_sensitive(),
            'inverted': query.is_inverted(),
            'value': {
                'id': value_id,
                'key': value_key,
                'variable': db_value
            }
        }
        return kw, data

    def process_value(self, column, op, value, context=None, aliases=None, fields=None):
        """
        Processes the column's database value to prepare it for use in the database
        context.

        :param column: <orb.Column>
        :param op: <orb.Query.Op>
        :param value: <variant>
        :param context: <orb.Context>
        :param aliases: {<orb.Model>: <str>, ..} or None
        :param fields: {<orb.Column>: <str>, ..} or None

        :return: <variant> db value, <dict> data
        """
        db_value = column.store(value, context=context)

        # process a query value
        if isinstance(db_value, (orb.Query, orb.QueryCompound)):
            val_model = db_value.model()
            val_column = db_value.column()
            return self.render_query_field(val_model,
                                           val_column,
                                           db_value,
                                           context=context, aliases=aliases)

        # process a collection value
        elif isinstance(db_value, orb.Collection):
            val_model = db_value.model().id_column()
            context = db_value.context()
            if not context.columns:
                context.columns = [val_model.schema().id_column()]
                context.distinct = True

            return self.render_select(val_model, context)

        # adjust the value as needed
        elif op in (orb.Query.Op.Contains, orb.Query.Op.DoesNotContain):
            return u'%{0}%'.format(db_value), {}

        elif op in (orb.Query.Op.Startswith, orb.Query.Op.DoesNotStartwith):
            return u'{0}%'.format(db_value), {}

        elif op in (orb.Query.Op.Endswith, orb.Query.Op.DoesNotEndwith):
            return u'%{0}'.format(db_value), {}

        else:
            return db_value, {}

    def render_alter_table(self, table, context=None, add=None, remove=None, owner=''):
        """
        Re-implements orb.Connection.alter_model to update the backend model
        definition with the new information.

        :param model: subclass of <orb.Model>
        :param context: <orb.Context>
        :param add: {'fields': [<orb.Column>, ..], 'indexes': [<orb.Index>, ..]} or None
        :param remove: {'fields': [<orb.Column>, ..], 'indexes': [<orb.Index>, ..]} or None
        :param owner: <str>

        :return: <unicode> command, <dict> data
        """
        add = add or {'fields': [], 'indexes': []}
        remove = remove or {'fields': [], 'indexes': []}

        add_cols = []
        add_indexes = []
        add_i18n = []

        # add columns
        for column in add['fields']:
            if column.test_flag(column.Flags.I18n):
                add_i18n.append(self.process_column(column, context))
            else:
                add_cols.append(self.process_column(column, context))

        # add indexes
        for index in add['indexes']:
            add_indexes.append(self.process_index(index, context))

        kw = {
            'model': self.process_model(table, context),
            'add_columns': add_cols,
            'add_indexes': add_indexes,
            'add_i18n_columns': add_i18n
        }

        cmd = self.render('alter_table.sql.jinja', kw)
        return cmd, {}

    def render_create_table(self, table, context=None, owner='', include_references=True):
        """
        Renders the CREATE TABLE SQL from the `create_table.sql.jinja` template for
        this SQL connection type.

        :param table: <orb.Model>
        :param context: <orb.Context>
        :param owner: <str>
        :param include_references: <bool>

        :return: <str> sql command, <dict> sql data
        """
        context = context or orb.Context()

        columns = table.schema().columns(recurse=False).values()
        columns.sort(key=lambda x: x.field())

        id_column = table.schema().id_column()
        id_column_data = self.process_column(id_column, context)

        add_cols = []
        add_i18n = []

        # process the columns
        for col in columns:
            if col == id_column:
                continue
            elif not include_references and isinstance(col, orb.ReferenceColumn):
                continue
            elif col.test_flag(col.Flags.Virtual):
                continue
            elif col.test_flag(col.Flags.I18n):
                add_i18n.append(self.process_column(col, context))
            else:
                add_cols.append(self.process_column(col, context))

        kw = {
            'model': self.process_model(table, context),
            'id_column': id_column_data,
            'add_columns': add_cols,
            'add_i18n_columns': add_i18n
        }

        cmd = self.render('create_table.sql.jinja', kw)
        return cmd, {}

    def render_create_view(self, view, context=None, owner=''):
        return self.render('create_view.sql.jinja', {}), {}

    def render_delete_collection(self, collection, context=None):
        """
        Renders the DELETE SQL for this connection type.

        :param collection: <orb.Collection>
        :param context: <orb.Context>

        :return: <unicode> command, <dict> data
        """
        if collection.is_null():
            return '', {}
        elif collection.is_loaded():
            return self.render_delete_records(list(collection), context=context)
        else:
            model = collection.model()

            # cannot delete views
            if issubclass(model, orb.View):
                raise NotImplementedError('View models are read-only')

            collection_context = collection.context(context=context)

            if collection_context.where is not None:
                try:
                    where, data = self.render_query(model, collection_context.where, collection_context)

                # if the query ends up generating a null selection, then
                # there will be no results to delete, so no text is necessary
                except orb.errors.QueryIsNull:
                    return '', {}
            else:
                where = ''
                data = {}

            kw = {
                'model': self.process_model(model, context),
                'where': where
            }
            cmd = self.render('delete.sql.jinja', kw)
            return cmd, data

    def render_delete_records(self, records, context=None):
        """
        Renders the DELETE SQL for this connection type.

        :param records: [<orb.Model>, ..]
        :param context: <orb.Context>

        :return: <unicode> command, <dict> data
        """
        if not records:
            return '', {}
        else:
            models = {}

            # pre-compute the records into each model
            for record in records:
                record_id = record.id()
                if not record_id:
                    continue
                else:
                    models.setdefault(type(record), []).append(record.id())

            cmds = []
            data = {}
            for model, ids in models.items():
                # cannot delete views
                if issubclass(model, orb.View):
                    raise NotImplementedError('View models are read-only')

                q = orb.Query(model).in_(ids)

                where, where_data = self.render_query(model, q, context)
                kw = {
                    'model': self.process_model(model, context),
                    'where': where
                }
                cmd = self.render('delete.sql.jinja', kw)

                cmds.append(cmd)
                data.update(where_data)

            return u'\n'.join(cmds), data

    def render_query_field(self, model, column, query, context=None, aliases=None):
        """
        Renders a new field for the given model / column.

        :param model: subclass of <orb.Model>
        :param column: <orb.Column>
        :param query: <orb.Query>
        :param context: <orb.Context> or None
        :param aliases: <dict> or None
        """
        aliases = aliases or {}

        # generate the parts
        if model in aliases:
            parts = [self.wraps(aliases[model]), self.wraps(column.field())]
        else:
            parts = [self.wraps(model.schema().namespace(context=context) or self.get_default_namespace()),
                     self.wraps(model.schema().dbname()),
                     self.wraps(column.field())]

        # render the query field
        query_field = '.'.join(parts)

        # wrap the field in functions
        for func_op in query.functions():
            query_field = self.wrap_query_function(column, query_field, func_op)

        # wrap the field in math
        data = {}
        for math_op, value in query.math():
            # perform math on a query object
            if isinstance(value, orb.Query):
                qvalue, qvalue_data = self.render_query_field(value.model(default=model),
                                                              value.column(model=model),
                                                              value,
                                                              context=context,
                                                              aliases=aliases)
                data.update(qvalue_data)
            else:
                value_id = os.urandom(8).encode('hex')
                qvalue = u'%({0})s'.format(value_id)
                data[value_id] = value

            query_field = self.wrap_query_math(column, query_field, math_op, qvalue)

        return query_field, data

    def render_query(self, model, query, context=None, use_filter=True, aliases=None, fields=None):
        """
        Process the SQL data for the given query object.

        :param model: subclass of <orb.Model>
        :param query: <orb.Query> or <orb.QueryCompound>
        :param context: <orb.Context>

        :return: <unicode> command, <dict> data
        """
        query = query.expand(model=model, use_filter=use_filter)

        # validate that the query exists
        if query is None:
            return u'', {}

        # for QueryCompound instances, use the `render_query_compound` method
        elif isinstance(query, orb.QueryCompound):
            return self.render_query_compound(model,
                                              query,
                                              context=context,
                                              aliases=aliases,
                                              fields=fields)

        else:
            return self.render_query_column(model,
                                            query,
                                            context=context,
                                            aliases=aliases,
                                            fields=fields)

    def render_query_compound(self,
                              model,
                              query_compound,
                              context=None,
                              aliases=None,
                              fields=None):
        """
        Processes the SQL data for the given query compound object.

        :param model: subclass of <orb.Model>
        :param query_compound: <orb.QueryCompound>
        :param context: <orb.Context>

        :return: <unicode> command, <dict> data
        """
        context = context or orb.Context()
        data = {}
        sub_queries = []
        for sub_q in query_compound:
            sub_cmd, sub_data = self.render_query(model,
                                                  sub_q,
                                                  context=context,
                                                  aliases=aliases,
                                                  fields=fields)
            sub_queries.append(sub_cmd)
            data.update(sub_data)

        kw = {
            'queries': sub_queries,
            'op': self.get_query_compound_op(query_compound.op())
        }
        cmd = self.render('query_compound.sql.jinja', kw)
        return cmd, data

    def render_query_column(self,
                            model,
                            query,
                            context=None,
                            aliases=None,
                            fields=None):
        """
        Renders the SQL command for the given query instance.

        :param model: subclass of <orb.Model>
        :param query: <orb.Query>
        :param context: <orb.Context>
        :param aliases: <dict> or None
        :param fields: <dict> or None

        :return: <unicode> command, <dict> data
        """
        db_query, data = self.process_query(model,
                                            query,
                                            context=context,
                                            aliases=aliases,
                                            fields=fields)
        kw = {
            'query': db_query
        }
        cmd = self.render('query.sql.jinja', kw)
        return cmd, data

    def select(self, model, context):
        pass

    def update(self, records, context):
        pass

    @classmethod
    def get_column_type(cls, column, context=None):
        """
        Returns the sql string for the column's type.

        :param column: <orb.Column>
        :param context: <orb.Column> or None

        :return: <str>
        """
        key = '_{0}__type_mapping'.format(cls.__name__)
        mapping = getattr(cls, key, {})
        context = context or orb.Context()
        column_type = type(column)
        bases = [column_type] + list(column_type.get_base_types())

        # go through all types for this column to
        # find the best match, allowing for subclassed
        # columns to share the same type mapping
        for typ in bases:
            try:
                map = mapping[typ]
            except KeyError:
                continue
            else:
                if callable(map):
                    return map(column, context)
                else:
                    return map
        else:
            raise orb.errors.ColumnTypeNotFound(column_type)

    @classmethod
    def get_default_namespace(cls):
        """
        Returns the default namespace for this connection class instance.

        :return: <str>
        """
        return cls.__default_namespace__

    @classmethod
    def get_query_op(cls, column, op, case_sensitive=False):
        """
        Returns the SQL specific rendering for the operator.

        Args:
            column: <orb.Column>
            op: <orb.Query.Op>

        Returns:
            <unicode>
        """
        key = '_{0}__query_op_mapping'.format(cls.__name__)
        try:
            mapping = getattr(cls, key)[op]
        except (AttributeError, KeyError):
            return inflection.titleize(orb.Query.Op(op)).upper()
        else:
            if callable(mapping):
                return mapping(column, op, case_sensitive)
            else:
                return mapping

    @classmethod
    def get_query_compound_op(cls, op):
        """
        Returns the SQL specific rendering for the compound operator.

        :param op: <orb.QueryCompound.Op>

        :return: <unicode>
        """
        key = '_{0}__qcompound_op_mapping'.format(cls.__name__)
        try:
            mapping = getattr(cls, key)[op]
        except (AttributeError, KeyError):
            return orb.QueryCompound.Op(op).upper()
        else:
            if callable(mapping):
                return mapping(op)
            else:
                return mapping

    @classmethod
    def get_templates(cls):
        """
        Returns the templates environment for this sql connection.

        :return: <jinja2.Environment>
        """
        key = '_{0}__templates'.format(cls.__name__)
        try:
            return getattr(cls, key)
        except AttributeError:
            env = Environment(loader=cls.__templates__)
            env.globals['raise'] = raise_error
            env.filters['wraps'] = cls.wraps
            setattr(cls, key, env)
            return env

    @classmethod
    def render(cls, statement_name, keywords):
        """
        Renders the SQL statement for the given template name and given keywords.

        Args:
            statement_name: <str>
            keywords: <dict>

        Raises:
            <RuntimeError> if template is not found

        Return:
            <str> command
        """
        template = cls.get_templates().get_template(statement_name)
        if template:
            cmd = template.render(keywords).strip()
            log.debug(cmd)
            return cmd
        else:
            raise RuntimeError('{0} template not found'.format(statement_name))

    @classmethod
    def register_function_mapping(cls, query_function_op, map):
        """
        Sets the query function mapping type for this connection to the given
        map.  This can be a string or a callable function.  If a string is
        provided, then it should be a format accepting a single argument.  If
        it is a callable function, then it should accept the source text and
        return a new text.

        :param query_function_op: <orb.Query.Function>
        :param map: <str> or <callable>
        """
        key = '_{0}__function_mapping'.format(cls.__name__)
        try:
            mapping = getattr(cls, key)
        except AttributeError:
            setattr(cls, key, {query_function_op: map})
        else:
            mapping[query_function_op] = map

    @classmethod
    def register_math_mapping(cls, query_math_op, map):
        """
        Sets the query function mapping type for this connection to the given
        map.  This can be a string or a callable function.  If a string is
        provided, then it should be a format accepting a single argument.  If
        it is a callable function, then it should accept the source text and
        return a new text.

        :param query_math_op: <orb.Query.Math>
        :param map: <str> or <callable>
        """
        key = '_{0}__math_mapping'.format(cls.__name__)
        try:
            mapping = getattr(cls, key)
        except AttributeError:
            setattr(cls, key, {query_math_op: map})
        else:
            mapping[query_math_op] = map

    @classmethod
    def register_query_op_mapping(cls, op, map):
        """
            Sets the query op mapping type for this connection to the given
            map.  This can be a string or a callable function.  If a string is
            provided, then it should be a format accepting a single argument.  If
            it is a callable function, then it should accept the source text and
            return a new text.

            :param op: <orb.Query.Op>
            :param map: <str> or <callable>
            """
        key = '_{0}__query_op_mapping'.format(cls.__name__)
        try:
            mapping = getattr(cls, key)
        except AttributeError:
            setattr(cls, key, {op: map})
        else:
            mapping[op] = map

    @classmethod
    def register_query_compound_op_mapping(cls, op, map):
        """
            Sets the query compound op mapping type for this connection to the given
            map.  This can be a string or a callable function.  If a string is
            provided, then it should be a format accepting a single argument.  If
            it is a callable function, then it should accept the source text and
            return a new text.

            :param op: <orb.Query.Op>
            :param map: <str> or <callable>
            """
        key = '_{0}__qcompound_mapping'.format(cls.__name__)
        try:
            mapping = getattr(cls, key)
        except AttributeError:
            setattr(cls, key, {op: map})
        else:
            mapping[op] = map

    @classmethod
    def register_type_mapping(cls, column_type, map):
        """
        Sets the column mapping type for this connection to the given
        map.  This can be a string or a callable function.  If a callable
        function is provided, then it should accept a single argument (column
        instance) and return a string value.

        :param column_type: <orb.Column>
        :param map: <str> or <callable>
        """
        key = '_{0}__type_mapping'.format(cls.__name__)
        try:
            mapping = getattr(cls, key)
        except AttributeError:
            setattr(cls, key, {column_type: map})
        else:
            mapping[column_type] = map

    @classmethod
    def wraps(cls, text):
        """
        Wraps a string based with characters for this backend.

        :param text: <str> or <unicode>

        :return: <unicode>
        """
        return u'"{0}"'.format(text)

    @classmethod
    def wrap_query_function(cls, column, field, query_function_op):
        """
        Wraps the field with the given query function.

        :param field: <str> or <unicode>
        :param query_function_op: <orb.Query.Function>
        :param context: <orb.Context>

        :return: <unicode>
        """
        key = '_{0}__function_mapping'.format(cls.__name__)
        try:
            mapping = getattr(cls, key)[query_function_op]
        except (AttributeError, KeyError):
            log.warning('No {0} map for {1}'.format(orb.Query.Function(query_function_op), cls.__name__))
            return field
        else:
            if callable(mapping):
                return mapping(column, query_function_op, field)
            else:
                return mapping.format(field)

    @classmethod
    def wrap_query_math(cls, column, field, query_math_op, value):
        """
        Wraps the field with the given query function.

        :param column: <orb.Column>
        :param field: <str> or <unicode>
        :param query_math_op: <orb.Query.Math>
        :param context: <orb.Context>

        :return: <unicode>
        """
        key = '_{0}__math_mapping'.format(cls.__name__)
        try:
            mapping = getattr(cls, key)[query_math_op]
        except (AttributeError, KeyError):
            log.warning('No {0} map for {1}'.format(orb.Query.Function(query_math_op), cls.__name__))
            return field
        else:
            if callable(mapping):
                return mapping(column, field, query_math_op, value)
            else:
                return mapping.format(field, value)