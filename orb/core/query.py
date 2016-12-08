"""
Defines the global query building syntax for generating db
agnostic queries quickly and easily.
"""

import copy
import demandimport
import logging

from ..decorators import deprecated
from ..utils.enum import enum

with demandimport.enabled():
    import orb


log = logging.getLogger(__name__)


class State(object):
    """ Simple class for maintaining unique query states throughout the system """
    def __init__(self, text):
        self.text = text

    def __eq__(self, other):
        return isinstance(other, State) and other.text == self.text

    def __str__(self):
        return 'Query.State({0})'.format(self.text)

    def __unicode__(self):
        return u'Query.State({0})'.format(self.text)

    def __hash__(self):
        return hash((State, self.text))


class Query(object):
    """ 
    Defines the central class for the abstract query markup language.
    """

    Op = enum(
        Is=1,
        IsNot=2,

        LessThan=3,
        LessThanOrEqual=4,
        Before=5,
        GreaterThan=6,
        GreaterThanOrEqual=7,
        After=8,
        Between=9,

        Contains=10,
        DoesNotContain=11,
        Startswith=12,
        DoesNotStartwith=13,
        Endswith=14,
        DoesNotEndwith=15,
        Matches=17,
        DoesNotMatch=18,

        IsIn=19,
        IsNotIn=20
    )

    OpNegation = {
        Op.Is: Op.IsNot,
        Op.IsNot: Op.Is,
        Op.LessThan: Op.GreaterThanOrEqual,
        Op.LessThanOrEqual: Op.GreaterThan,
        Op.Before: Op.After,
        Op.GreaterThan: Op.LessThanOrEqual,
        Op.GreaterThanOrEqual: Op.LessThan,
        Op.After: Op.Before,
        Op.Contains: Op.DoesNotContain,
        Op.DoesNotContain: Op.Contains,
        Op.Startswith: Op.DoesNotStartwith,
        Op.Endswith: Op.DoesNotEndwith,
        Op.Matches: Op.DoesNotMatch,
        Op.DoesNotMatch: Op.Matches,
        Op.IsIn: Op.IsNotIn,
        Op.IsNotIn: Op.IsIn,
        Op.DoesNotStartwith: Op.Startswith,
        Op.DoesNotEndwith: Op.Endswith
    }

    Math = enum(
        Add=1,
        Subtract=2,
        Multiply=3,
        Divide=4,
        And=5,
        Or=6
    )

    Function = enum(
        Lower=1,
        Upper=2,
        Abs=3,
        AsString=4
    )

    # define query states
    UNDEFINED = State('UNDEFINED')
    NOT_EMPTY = State('NOT_EMPTY')
    EMPTY = State('EMPTY')
    ALL = State('ALL')

    def __init__(self, *args, **kw):
        """
        Constructor.

        :param args: variable args options -

            * blank (no initialization)
            * <str> schema object name
            * <orb.Column>
            * subclass of <orb.Model>
            * subclass of <orb.Model> source model, <str> schema object name
            * (subclass of <orb.Model> source model, <str> schema object name)

        :param kw: keyword options -

            * op: <Query.Op> or <str> (default: Query.Op.Is)
            * value: <variant> (default: None)
            * case_sensitive: <bool> (default: False)
            * inverted: <bool> (default: False)
            * functions: [<Query.Function>, ..] (default: [])
            * math: [<Query.Math>, ..] (default: [])

        """
        super(Query, self).__init__()

        # ensure we have the proper arguments
        if len(args) > 2:
            raise RuntimeError('Invalid Query arguments')

        # check for Query() initialization
        elif len(args) == 0:
            model = schema_object = None

        # check for Query(model, schema_object) initialization
        elif len(args) == 2:
            model, schema_object = args

        # check for Query(varg) initialization
        else:
            arg = args[0]

            # check for Query((model, schema_object)) initialization
            if isinstance(arg, tuple):
                model, schema_object = arg

            # check for Query(orb.Column()) initialization
            elif isinstance(arg, (orb.Column, orb.Collector)):
                model = arg.schema().model()
                schema_object = arg

            # check for Query('schema_object') initialization
            elif isinstance(arg, (str, unicode)):
                model = None
                schema_object = arg

            # check for Query(orb.Model) initialization
            else:
                try:
                    if issubclass(arg, orb.Model):
                        model = arg
                        schema_object = arg.schema().id_column()
                    else:
                        raise RuntimeError('Invalid Query arguments')
                except Exception:
                    raise RuntimeError('Invalid Query arguments')

        # initialize the operation
        op = kw.pop('op', Query.Op.Is)
        if isinstance(op, (str, unicode)):
            op = Query.Op(op)

        # set custom properties
        self.__model = model
        self.__schema_object = schema_object
        self.__op = op
        self.__value = kw.pop('value', Query.UNDEFINED)
        self.__case_sensitive = kw.pop('case_sensitive', False)
        self.__inverted = kw.pop('inverted', False)
        self.__functions = kw.pop('functions', [])
        self.__math = kw.pop('math', [])

        # ensure that we have not provided additional keywords
        if kw:
            raise RuntimeError('Unknown query keywords: {0}'.format(','.join(kw.keys())))

    def __hash__(self):
        """
        Hashes the query to be able to compare against others.  You can't
        use the `==` operator because it is overloaded to build query values.

        :usage:

            q = orb.Query('name') == 'testing'
            b = orb.Query('name') == 'testing'

            equal = hash(a) == hash(b)

        :return: <int>
        """
        if isinstance(self.__value, (list, set)):
            val_hash = hash(tuple(self.__value))
        else:
            try:
                val_hash = hash(self.__value)
            except TypeError:
                val_hash = hash(unicode(self.__value))

        object_name = self.object_name()

        return hash((
            self.__model,
            object_name,
            self.__op,
            self.__case_sensitive,
            val_hash,
            self.__inverted,
            tuple(self.__functions),
            tuple(self.__math)
        ))

    # python 2.x
    def __nonzero__(self):
        return not self.is_null()

    # python 3.x
    def __bool__(self):
        return not self.is_null()

    def __json__(self):
        """
        Converts this query to a JSON serializable dictionary.

        :return: <dict>
        """
        value = self.value()
        if hasattr(value, '__json__'):
            value = value.__json__()

        jdata = {
            'type': 'query',
            'model': self.__model.schema().name() if self.__model else '',
            'column': self.object_name(),   # TODO : deprecate
            'op': self.Op(self.__op),
            'case_sensitive': self.__case_sensitive,
            'functions': [self.Function(func) for func in self.__functions],
            'math': [{'op': self.Math(math_op), 'value': math_value} for (math_op, math_value) in self.__math],
            'inverted': self.__inverted,
            'value': value
        }
        return jdata

    def __contains__(self, schema_object):
        """
        Returns whether or not the schema_object is used within this query.

        :param schema_object: <str> or <orb.Column> or <orb.Collector>

        :return: <bool>
        """
        return self.has(schema_object)

    # operators

    def __add__(self, value):
        """
        Addition operator for query object.  This will create a new query with the
        Math.Add operator applied to the given value for the new query.
        
        :usage:
        
            a = (orb.Query('offset') + 10)

        :param value: <variant>
        
        :return: <orb.Query>
        """
        out = self.copy()
        out.append_math_op(Query.Math.Add, value)
        return out

    def __abs__(self):
        """
        Absolute operator for query object.  This will create a new query with
        the Function.Abs operator applied to the the new query.

        :usage:

            a = abs(orb.Query('difference'))

        :return: <orb.Query>
        """
        out = self.copy()
        out.append_function_op(Query.Function.Abs)
        return out

    def __and__(self, other):
        """
        And operator for query object.  Depending on the value type provided,
        one of two things will happen.  If `other` is a `Query` or `QueryCompound`
        object, then this query will be combined with the other to generate
        a new `QueryCoumpound` instance.  If `other` is any other value type,
        then a new `Query` object will be created with the `Query.Math.And` operator
        for the given value added to it.

        :usage:

            q = orb.Query('offset') & 3
            a = (orb.Query('offset') > 1) & (orb.Query('offset') < 3)

        :param other: <orb.Query> or <orb.QueryCompound> or variant

        :return: <orb.QueryCompound> or <orb.Query>
        """
        if other is None:
            return self.copy()
        elif isinstance(other, (Query, orb.QueryCompound)):
            return self.and_(other)
        else:
            out = self.copy()
            out.append_math_op(Query.Math.And, other)
            return out

    def __div__(self, value):
        """
        Divide operator for query object.  This will create a new query with the
        Math.Divide operator applied to the given value for the new query.

        :usage:

            a = (orb.Query('offset') / 10)

        :param value: <variant>

        :return: <orb.Query>
        """
        out = self.copy()
        out.append_math_op(Query.Math.Divide, value)
        return out

    def __eq__(self, other):
        """
        Equals operator for query object.  Calls the `is_` method of the Query object.

        :warning:   DO NOT use this method as a comparison between two queries.
                    You should compare their hash values instead.

        :usage:

            q = orb.Query('first_name') == 'jdoe'

        :param other: <variant>

        :return: <orb.Query>
        """
        return self.is_(other)

    def __gt__(self, other):
        """
        Greater than operator.  Calls the `greater_than` method of the Query object.

        :usage:

            q = orb.Query('value') > 10

        :param other: <variant>
        
        :return: <orb.Query>
        """
        return self.greater_than(other)

    def __ge__(self, other):
        """
        Greater than or equal to operator.  Calls the `greater_than_or_equal` method
        of the Query object.

        :usage:

            q = orb.Query('value') >= 10

        :param other: <variant>
        
        :return: <orb.Query>
        """
        return self.greater_than_or_equal(other)

    def __lt__(self, other):
        """
        Less than operator.  Calls the `less_than` method of the Query object.

        :usage:

            q = orb.Query('value') < 10

        :param other: <variant>

        :return: <orb.Query>
        """
        return self.less_than(other)

    def __le__(self, other):
        """
        Less than or equal to operator.  Calls the `less_than_or_equal` method of
        the Query object.

        :usage:

            q = orb.Query('value') <= 10

        :param other: <variant>

        :return: <orb.Query>
        """
        return self.less_than_or_equal(other)

    def __mul__(self, value):
        """
        Multiply operator for query object.  This will create a new query with the
        Math.Multiply operator applied to the given value for the new query.

        :usage:

            a = (orb.Query('offset') * 10)

        :param value: <variant>

        :return: <orb.Query>
        """
        out = self.copy()
        out.append_math_op(Query.Math.Multiply, value)
        return out

    def __ne__(self, other):
        """
        Not equals operator for query object.  Calls the `is_not` method of the Query object.

        :warning:   DO NOT use this method as a comparison between two queries.
                    You should compare their hash values instead.

        :usage:

            q = orb.Query('first_name') != 'jdoe'

        :param other: <variant>

        :return: <orb.Query>
        """
        return self.is_not(other)

    def __or__(self, other):
        """
        Or operator for query object.  Depending on the value type provided,
        one of two things will happen.  If `other` is a `Query` or `QueryCompound`
        object, then this query will be combined with the other to generate
        a new `QueryCoumpound` instance.  If `other` is any other value type,
        then a new `Query` object will be created with the `Query.Math.And` operator
        for the given value added to it.

        :usage:

            q = orb.Query('offset') | 3
            a = (orb.Query('offset') > 1) | (orb.Query('offset') < 3)

        :param other: <orb.Query> or <orb.QueryCompound> or variant

        :return: <orb.QueryCompound> or <orb.Query>
        """
        if other is None:
            return self.copy()
        elif isinstance(other, (Query, orb.QueryCompound)):
            return self.or_(other)
        else:
            out = self.copy()
            out.append_math_op(Query.Math.Or, other)
            return out

    def __sub__(self, value):
        """
        Subtraction operator for query object.  This will create a new query with the
        Math.Subtract operator applied to the given value for the new query.

        :usage:

            a = (orb.Query('offset') - 10)

        :param value: <variant>

        :return: <orb.Query>
        """
        out = self.copy()
        out.append_math_op(Query.Math.Subtract, value)
        return out

    # public methods
    def append_function_op(self, func_op):
        """
        Adds the given function to the stack for this query.

        :param: <orb.Query.Function>
        """
        self.__functions.append(func_op)

    def append_math_op(self, math_op, value):
        """
        Appends a new math operator to the math associated with this query.

        :param math_op: <Query.Math>
        :param value: <variant>
        """
        self.__math.append((math_op, value))

    def after(self, value):
        """
        Creates a copy of this query with the operator set to `Query.Op.After` and
        the value set to the given input.

        :usage:

            q = orb.Query('created_at').after(datetime.date(2000, 1, 1)

        :param value: <variant>

        :return: <orb.Query>
        """
        return self.copy(op=Query.Op.After, value=value)

    def and_(self, other):
        """
        Joins this query together with the given other query.  If this
        query is null, then the other a copy of the other query is returned.  If
        the other query is null, then a copy of this query is returned.  If
        neither query is null, then a <orb.QueryCompound> is returned with the
        operator set to And.

        :usage:

            q = orb.Query('first_name') == 'john'
            b = orb.Query('last_name') == 'doe'
            c = a.and_(b)

        :param other: <orb.Query> or <orb.QueryCompound>

        :return: <orb.Query> or <orb.QueryCompound>
        """
        if not isinstance(other, (orb.Query, orb.QueryCompound)):
            return self.copy()
        elif other.is_null():
            return self.copy()
        elif self.is_null():
            return other.copy()
        else:
            return orb.QueryCompound(self, other, op=orb.QueryCompound.Op.And)

    def as_string(self):
        """
        Creates a new query with the Function.AsString operator applied to the
        the new query.

        :usage:

            q = orb.Query('offset').as_string()

        :return: <orb.Query>
        """
        q = self.copy()
        q.append_function_op(Query.Function.AsString)
        return q

    def before(self, value):
        """
        Creates a copy of this query with the operator set to `Query.Op.Before` and
        the value set to the given input.

        :usage:

            q = orb.Query('created_at').before(datetime.date.today())

        :param value: <variant>

        :return: <orb.Query>
        """
        return self.copy(op=Query.Op.Before, value=value)

    def between(self, minimum, maximum):
        """
        Creates a copy of this query with the operator set to `Query.Op.Between` and
        the value set to the given input.

        :usage:

            q = orb.Query('threshold').between(1, 5)

        :param minimum: <variant>
        :param maximum: <variant>

        :return: <orb.Query>
        """
        return self.copy(op=Query.Op.Between, value=(minimum, maximum))

    def case_sensitive(self):
        """
        Returns whether or not this query item will be case
        sensitive.  This will be used with string lookup items.
        
        :return: <bool>
        """
        return self.__case_sensitive

    def collector(self, model=None):
        """
        Returns the collector that this query is referencing, if any.  If this
        query is directly associated to a model, then supplying one will not
        have an affect, otherwise, the schema of the given model will be used
        to look up the collector by name.

        :param model: subclass of <orb.Model> or None

        :return: <orb.Collector> or None
        """
        model = self.model(default=model)
        if not model:
            return None
        elif isinstance(self.__schema_object, orb.Column):
            return None
        else:
            return model.schema().collector(self.__schema_object)

    def column(self, model=None):
        """
        Returns the column that this query is referencing, if any.  If this
        query is directly associated to a model, then supplying one will not make
        a difference.  If the query column is a string, then the given model
        will provide the schema for the returned column.

        :param model: subclass of <orb.Model> or None

        :return: <orb.Column> or None
        """
        model = self.model(default=model)
        if not model:
            return None
        elif isinstance(self.__schema_object, orb.Collector):
            return None
        else:
            return model.schema().column(self.__schema_object, raise_=False)

    @deprecated
    def columns(self, model=None):
        """
        Deprecated.  Please use <orb.Query.schema_objects> instead.

        :return     <generator>(<orb.Column> or <orb.Collector>)
        """
        return self.schema_objects(model=model)

    @deprecated
    def column_name(self):
        """
        Deprecated.  Please use `orb.Query.object_name()` instead.
        """
        return self.object_name()

    def contains(self, value, case_sensitive=False):
        """
        Creates a copy of this query with the operator set to `Query.Op.Contains` and
        the value set to the given input.

        :usage:

            q = orb.Query('name').contains('in')

        :param value: <variant>
        :param case_sensitive: <bool>

        :return: <orb.Query>
        """
        return self.copy(op=orb.Query.Op.Contains,
                         value=value,
                         case_sensitive=case_sensitive)

    def copy(self, **kw):
        """
        Returns a duplicate of this instance.

        :return: <orb.Query>
        """
        kw.setdefault('op', self.__op)
        kw.setdefault('case_sensitive', self.__case_sensitive)
        kw.setdefault('value', copy.copy(self.__value))
        kw.setdefault('inverted', self.__inverted)
        kw.setdefault('functions', copy.copy(self.__functions))
        kw.setdefault('math', copy.copy(self.__math))
        kw.setdefault('model', self.__model)
        kw.setdefault('schema_object', self.__schema_object)

        return type(self)((kw.pop('model'), kw.pop('schema_object')), **kw)

    def does_not_contain(self, value, case_sensitive=False):
        """
        Creates a copy of this query with the operator set to `Query.Op.DoesNotContain` and
        the value set to the given input.

        :usage:

            q = orb.Query('name').does_not_contain('in')

        :param value: <variant>
        :param case_sensitive: <bool>

        :return: <orb.Query>
        """
        return self.copy(op=orb.Query.Op.DoesNotContain,
                         value=value,
                         case_sensitive=case_sensitive)

    def does_not_match(self, value, case_sensitive=True):
        """
        Creates a copy of this query with the operator set to `Query.Op.DoesNotMatch` and
        the value set to the given input.

        :usage:

            q = orb.Query('name').does_not_match('in')

        :param value: <variant>
        :param case_sensitive: <bool>

        :return: <orb.Query>
        """
        return self.copy(op=orb.Query.Op.DoesNotMatch,
                         value=value,
                         case_sensitive=case_sensitive)

    def endswith(self, value, case_sensitive=True):
        """
        Creates a copy of this query with the operator set to `Query.Op.Endswith` and
        the value set to the given input.

        :usage:

            q = orb.Query('name').endswith('me')

        :param value: <variant>
        :param case_sensitive: <bool>

        :return: <orb.Query>
        """
        return self.copy(op=orb.Query.Op.Endswith, case_sensitive=case_sensitive, value=value)

    def expand(self, model=None, use_filter=True):
        """
        Expands the shortcuts associated with this query.  This will generate a new
        query or query compound that represents the shortcut defined in this query.

        :usage:

            q = orb.Query('user.username') == 'jdoe'
            new_q = q.expand()

        :param model: subclass of <orb.Model> or None
        :param use_filter: <bool> (default: True)

        :return: <orb.Query> or <orb.QueryCompound>
        """
        model = self.model(default=model)
        if model is None:
            raise orb.errors.QueryInvalid('Could not traverse query expand')

        schema = model.schema()
        parts = self.object_name().split('.')
        schema_object = schema.column(parts[0], raise_=False) or schema.collector(parts[0])
        filter = schema_object.filtermethod() if schema_object else None

        # if there is a filter that should be used, then use it
        if use_filter and callable(filter):
            new_q = filter(model, self)
            if new_q:
                return new_q.expand(model, use_filter=False)
            else:
                return orb.Query()

        else:
            # if there is a shortcut that should be used, then use it
            if isinstance(schema_object, orb.Column) and schema_object.shortcut():
                parts = schema_object.shortcut().split('.')
                schema_object = schema.column(parts[0], raise_=False)

            # if there is nothing to expand, exit out
            if len(parts) == 1:
                return self

            # expand a collector
            elif isinstance(schema_object, orb.Collector):
                return orb.Query(model).in_(schema_object.collect_expand(self, parts))

            # expand a reference
            elif isinstance(schema_object, orb.ReferenceColumn):
                reference_model = schema_object.reference_model()
                reference_q = self.copy(model=reference_model,
                                        schema_object='.'.join(parts[1:]))
                references = reference_model.select(columns=[reference_model.schema().id_column()],
                                                    where=reference_q)
                return orb.Query(model, parts[0]).in_(references)

            # unable to expand otherwise
            else:
                raise orb.errors.QueryInvalid('Could not traverse query expand')

    def functions(self):
        """
        Returns a list of the functions that are associated with this query.
        This will modify the lookup column for the given function type in order.
        
        :return     [<Query.Function>, ..]
        """
        return self.__functions

    def greater_than(self, value):
        """
        Creates a copy of this query with the operator set to `Query.Op.GreaterThan` and
        the value set to the given input.

        :usage:

            q = orb.Query('value').greater_than(10)

        :param value: <variant>
        
        :return: <orb.Query>
        """
        return self.copy(op=Query.Op.GreaterThan, value=value)

    def greater_than_or_equal(self, value):
        """
        Creates a copy of this query with the operator set to `Query.Op.GreaterThanOrEqual`
        and the value set to the given input.

        :usage:

            q = orb.Query('value').greater_than_or_equal(10)

        :param value: <variant>

        :return: <orb.Query>
        """
        return self.copy(op=Query.Op.GreaterThanOrEqual, value=value)

    def has(self, schema_object):
        """
        Tests to see if the given schema object is represented by this query.

        :param schema_object: <str> or <orb.Column> or <orb.Collector>

        :return: <bool>
        """
        if isinstance(schema_object, (orb.Column, orb.Collector)):
            try:
                my_object = self.schema_object()
            except Exception:
                my_object = None

            if my_object is None:
                return schema_object.name() == self.object_name()
            else:
                return my_object == schema_object
        else:
            return schema_object == self.object_name()

    def inverted(self):
        """
        Returns an inverted copy of this query.

        :return: <orb.Query>
        """
        return self.copy(inverted=not self.is_inverted())

    def is_(self, value):
        """
        Creates a copy of this query object with the `Is` operator set and
        the `value` set to the given input.

        :usage:

            q = orb.Query('test').is_(1)

        :param value: <variant>

        :return: <orb.Query>
        """
        return self.copy(op=Query.Op.Is, value=value)

    def is_inverted(self):
        """
        Returns whether or not the value and column data should be inverted during query.

        :return: <bool>
        """
        return self.__inverted

    def is_not(self, value):
        """
        Creates a copy of this query object with the `IsNot` operator set and
        the `value` set to the given input.

        :usage:

            q = orb.Query('test').is_not(1)

        :param value: <variant>

        :return: <orb.Query>
        """
        return self.copy(op=Query.Op.IsNot, value=value)

    def is_null(self):
        """
        Return whether or not this query contains any information.
        
        :return: <bool>
        """
        return (self.__schema_object is None or
                self.__op is None or
                self.__value is Query.UNDEFINED)

    def in_(self, value):
        """
        Creates a copy of this query object with the `IsIn` operator set and
        the `value` set to the given input.

        :usage:

            q = orb.Query('test').in_([1, 2])

        :param value: <variant>

        :return: <orb.Query>
        """
        if isinstance(value, orb.Collection):
           pass
        elif not isinstance(value, (set, list, tuple)):
            value = (value,)
        else:
            value = tuple(value)

        return self.copy(op=Query.Op.IsIn, value=value)

    def math(self):
        """
        Returns the mathematical operations that are being performed for
        this query object.
        
        :return: [(<Query.Math>, <variant>), ..]
        """
        return self.__math

    def not_in(self, value):
        """
        Creates a copy of this query object with the `IsNotIn` operator set and
        the `value` set to the given input.

        :usage:

            q = orb.Query('test').not_in([1, 2])

        :param value: <variant>

        :return: <orb.Query>
        """
        if isinstance(value, orb.Collection):
            pass
        elif not isinstance(value, (set, list, tuple)):
            value = (value,)
        else:
            value = tuple(value)

        return self.copy(op=Query.Op.IsNotIn, value=value)

    def less_than(self, value):
        """
        Creates a copy of this query with the operator set to `Query.Op.LessThan` and
        the value set to the given input.

        :usage:

            q = orb.Query('value').less_than(10)

        :param value: <variant>

        :return: <orb.Query>
        """
        return self.copy(op=Query.Op.LessThan, value=value)

    def less_than_or_equal(self, value):
        """
        Creates a copy of this query with the operator set to `Query.Op.LessThanOrEqual`
        and the value set to the given input.

        :usage:

            q = orb.Query('value').less_than_or_equal(10)

        :param value: <variant>

        :return: <orb.Query>
        """
        return self.copy(op=Query.Op.LessThanOrEqual, value=value)

    def lower(self):
        """
        Returns a new query for this instance with Query.Function.Lower as
        a function option.
        
        :return     <Query>
        """
        q = self.copy()
        q.append_function_op(Query.Function.Lower)
        return q

    def matches(self, value, case_sensitive=True):
        """
        Sets the operator type to Query.Op.Matches and sets \
        the value to the inputted regex expression.  This method will only work \
        on text based fields.
        
        :param      value       <str>
        
        :return     <Query>
        
        :usage      |>>> from orb import Query as Q
                    |>>> query = Q('test').matches('^\d+-\w+$')
                    |>>> print query
                    |test matches ^\d+-\w+$
        """
        return self.copy(op=Query.Op.Matches, value=value, case_sensitive=case_sensitive)

    def model(self, default=None):
        """
        Returns the model associated with this query instance.  If no model is explicitly
        linked as a member, then the given model will be returned.

        :param default: subclass of <orb.Model>

        :return: subclass of <orb.Model> or None
        """
        return self.__model or default

    def negated(self):
        """
        Inverts the query.  This will create a copy of this Query
        by flipping the operator for the query to it's corresponding inversion
        op.

        :return: <orb.Query>
        """
        new_op = self.get_negated_op(self.__op)
        return self.copy(op=new_op)

    def op(self):
        """
        Returns the operator type assigned to this query
        instance.
        
        :return     <Query.Op>
        """
        return self.__op

    def or_(self, other):
        """
        Or operator for query object.  Depending on the value type provided,
        one of two things will happen.  If `other` is a `Query` or `QueryCompound`
        object, then this query will be combined with the other to generate
        a new `QueryCoumpound` instance.  If `other` is any other value type,
        then a new `Query` object will be created with the `Query.Math.And` operator
        for the given value added to it.

        :usage:

            q = orb.Query('offset') | 3
            a = (orb.Query('offset') > 1) | (orb.Query('offset') < 3)

        :param other: <orb.Query> or <orb.QueryCompound> or variant

        :return: <orb.QueryCompound> or <orb.Query>
        """
        if not isinstance(other, (orb.Query, orb.QueryCompound)):
            return self.copy()
        elif other.is_null():
            return self.copy()
        elif self.is_null():
            return other.copy()
        else:
            return orb.QueryCompound(self, other, op=orb.QueryCompound.Op.Or)

    def object_name(self):
        """
        Returns the schema object's name that this query instance is
        looking up.

        :return: <str>
        """
        if isinstance(self.__schema_object, (orb.Column, orb.Collector)):
            return self.__schema_object.name()
        else:
            return self.__schema_object

    def schema_object(self, model=None):
        """
        Returns the schema object this query is referencing.  This will
        return either an `orb.Column`, `orb.Collector` or raise a `ColumnNotFound` error.
        If there is a model provided, it will only be used if no model is explicitly defined
        for this query instance.

        :param model: subclass of <orb.Model>

        :return: <orb.Column> or <orb.Collector>
        """
        model = self.model(default=model)
        if not model:
            raise orb.errors.ModelNotFound(msg='No model provided to the query')
        else:
            schema = model.schema()
            return schema.collector(self.__schema_object) or schema.column(self.__schema_object)

    def schema_objects(self, model=None):
        """
        Returns a generator that looks up all the schema objects associated with this query.

        :param model: suclass of <orb.Model> or None

        :return: <generator>
        """
        try:
            obj = self.schema_object(model=model)
        except Exception:
            pass
        else:
            yield obj

        if isinstance(self.__value, (orb.Query, orb.QueryCompound)):
            check = [self.__value]
        elif isinstance(self.__value, (list, set, tuple)):
            check = self.__value
        else:
            return

        # check for sub-objects
        for val in check:
            if isinstance(val, (orb.Query, orb.QueryCompound)):
                for sub_obj in val.schema_objects(model=model):
                    yield sub_obj

    def startswith(self, value, case_sensitive=True):
        """
        Creates a copy of this query with the operator set to `Query.Op.Startswith` and
        the value set to the given input.

        :usage:

            q = orb.Query('name').startswith('me')

        :param value: <variant>
        :param case_sensitive: <bool>

        :return: <orb.Query>
        """
        return self.copy(op=Query.Op.Startswith, case_sensitive=case_sensitive, value=value)

    def upper(self):
        """
        Returns this query with the Upper function added to its list.
        
        :return: <orb.Query>
        """
        q = self.copy()
        q.append_function_op(Query.Function.Upper)
        return q

    def value(self):
        """
        Returns the value for this query instance
        
        :return: <variant>
        """
        if isinstance(self.__value, State):
            return None
        else:
            return self.__value

    @classmethod
    def get_negated_op(cls, op):
        """
        Returns the inverted operation for the given op.  By default,
        this will look at the classes OpNegation dictionary.

        :param op: <orb.Query.Op>

        :return: <orb.Query.Op>
        """
        return cls.OpNegation.get(op, op)

    @staticmethod
    def build(data):
        """
        Creates a new query from a dictionary.

        :param data: <dict>

        :return: <orb.Query> or <orb.QueryCompound>
        """
        out = orb.Query()
        for obj, value in data.items():
            out &= orb.Query(obj) == value
        return out

    @staticmethod
    def load(jdata, **context):
        """
        Loads a new query object from a dictionary.

        :param jdata: <dict>

        :return: <orb.Query> or <orb.QueryCompound>
        """
        orb_context = orb.Context(**context)

        # load a compound
        if jdata['type'] == 'compound':
            queries = [orb.Query.load(sub_q) for sub_q in jdata['queries']]
            return orb.QueryCompound(*queries, op=jdata['op'])

        # load a query
        else:
            system = orb_context.system
            try:
                model_name = jdata['model']
            except KeyError:
                model = None
            else:
                model = system.model(model_name) if model_name else None

            funcs = [orb.Query.Function(f) for f in jdata.get('functions') or []]
            math = [orb.Query.Math(v['op'], v['value']) for v in jdata.get('math') or []]
            args = (model, jdata.get('column') or jdata.get('schema_object'))
            kw = {
                'op': jdata.get('op', 'Is'),
                'value': jdata.get('value'),
                'inverted': jdata.get('inverted', False),
                'case_sensitive': jdata.get('case_sensitive', False),
                'functions': funcs,
                'math': math
            }
            return orb.Query(args, **kw)

