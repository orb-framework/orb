"""
Defines the global query building syntax for generating db
agnostic queries quickly and easily.
"""

import copy
import demandimport
import projex.regex
import projex.text

from ..utils.enum import enum

with demandimport.enabled():
    import orb


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

    NegatedOp = {
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
        Uppser=2,
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
            * <str> column name
            * <orb.Column>
            * subclass of <orb.Model>
            * subclass of <orb.Model> source model, <str> column name
            * (subclass of <orb.Model> source model, <str> column name)

        :param kw: keyword options -

            * op: <Query.Op> or <str> (default: Query.Op.Is)
            * value: <variant> (default: None)
            * case_sensitive: <bool> (default: False)
            * inverted: <bool> (default: False)
            * functions: [<Query.Function>, ..] (default: [])
            * math: [<Query.Math>, ..] (default: [])

        """
        # ensure we have the proper arguments
        if len(args) > 2:
            raise RuntimeError('Invalid Query arguments')

        # check for Query() initialization
        elif len(args) == 0:
            model = column = None

        # check for Query(model, column) initialization
        elif len(args) == 2:
            model, column = args
            column = model.schema().column(column) if model else column

        # check for Query(varg) initialization
        else:
            arg = args[0]

            # check for Query((model, column)) initialization
            if isinstance(arg, tuple):
                model, column = arg
                column = model.schema().column(column) if model else column

            # check for Query(orb.Column()) initialization
            elif isinstance(arg, orb.Column):
                model = arg.schema().model()
                column = arg

            # check for Query('column') initialization
            elif isinstance(arg, (str, unicode)):
                model = None
                column = arg

            # check for Query(orb.Model) initialization
            else:
                try:
                    if issubclass(arg, orb.Model):
                        model = arg
                        column = arg.schema().id_column()
                    else:
                        raise RuntimeError('Invalid Query arguments')
                except StandardError:
                    raise RuntimeError('Invalid Query arguments')

        # initialize the operation
        op = kw.pop('op', Query.Op.Is)
        if isinstance(op, (str, unicode)):
            op = Query.Op(op)

        # set custom properties
        self.__model = model
        self.__column = column
        self.__op = op
        self.__case_sensitive = kw.pop('case_sensitive', False)
        self.__value = kw.pop('value', Query.UNDEFINED)
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

            a = orb.Query('name') == 'testing'
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

        column_name = self.column_name()

        return hash((
            self.__model,
            column_name,
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
        value = self.value()
        if hasattr(value, '__json__'):
            value = value.__json__()

        jdata = {
            'type': 'query',
            'model': self.__model.schema().name() if self.__model else '',
            'column': self.column_name(),
            'op': self.Op(self.__op),
            'case_sensitive': self.__case_sensitive,
            'functions': [self.Function(func) for func in self.__functions],
            'math': [{'op': self.Math(op), 'value': value} for (op, value) in self.__math],
            'inverted': self.__inverted,
            'value': value
        }
        return jdata

    def __contains__(self, column):
        """
        Returns whether or not the column is used within this query.

        :param column: <str> or <orb.Column>

        :return: <bool>
        """
        if isinstance(column, orb.Column):
            my_column = self.column()
            if my_column is None:
                return column.name() == self.column_name()
            else:
                return my_column == column
        else:
            return column == self.column_name()

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

            a = orb.Query('offset') & 3
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

            a = orb.Query('first_name') == 'jdoe'

        :param other: <variant>

        :return: <orb.Query>
        """
        return self.is_(other)

    def __gt__(self, other):
        """
        Greater than operator.  Calls the `greater_than` method of the Query object.

        :usage:

            a = orb.Query('value') > 10

        :param other: <variant>
        
        :return: <orb.Query>
        """
        return self.greater_than(other)

    def __ge__(self, other):
        """
        Greater than or equal to operator.  Calls the `greater_than_or_equal` method
        of the Query object.

        :usage:

            a = orb.Query('value') >= 10

        :param other: <variant>
        
        :return: <orb.Query>
        """
        return self.greater_than_or_equal(other)

    def __lt__(self, other):
        """
        Less than operator.  Calls the `less_than` method of the Query object.

        :usage:

            a = orb.Query('value') < 10

        :param other: <variant>

        :return: <orb.Query>
        """
        return self.less_than(other)

    def __le__(self, other):
        """
        Allows the joining of values to the query by the <= 
        operator.  If another Query instance is passed in, then it 
        will do a standard comparison.
        
        :param      other       <variant>
        
        :return     <Query>
        
        :sa         less_than_or_equal
        
        :usage      |>>> from orb import Query as Q
                    |>>> query = Q('test') <= 1
                    |>>> print query
                    |test <= 1
        """
        return self.less_than_or_equal(other)

    def __mul__(self, value):
        """
        Multiplies the value with this query to the inputted query.
        
        :param      value | <variant>
        
        :return     <Query> self
        """
        out = self.copy()
        out.append_math_op(Query.Math.Multiply, value)
        return out

    def __ne__(self, other):
        """
        Allows joining of values to the query by the != operator.
        If another Query instance is passed in, then it will do a
        standard comparison.
        
        :param      other       <variant>
        
        :return     <Query>
        
        :sa         isNot
        
        :usage      |>>> from orb import Query as Q
                    |>>> query = Q('test') != 1
                    |>>> print query
                    |test is not 1
        """
        return self.isNot(other)

    def __invert__(self):
        """
        Negates the values within this query by using the - operator.
        
        :return     self
        
        :sa         negated
        
        :usage      |>>> from orb import Query as Q
                    |>>> query = Q('test') == 1
                    |>>> print -query
                    |test != 1
        """
        return self.negated()

    def __or__(self, other):
        """
        Creates a new compound query using the
        <orb.QueryCompound.Op.Or> type.
        
        :param      other   <orb.Query> || <orb.QueryCompound>
        
        :return     <orb.QueryCompound>
        
        :sa         or_
        
        :usage      |>>> from orb import Query as Q
                    |>>> query = (Q('test') != 1) | (Q('name') == 'Eric')
                    |>>> print query
                    |(test is not 1 or name is Eric)
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
        Subtracts the value from this query.
        
        :param      value | <variant>
        
        :return     <Query> self
        """
        out = self.copy()
        out.append_math_op(Query.Math.Subtract, value)
        return out

    # public methods
    def append_function_op(self, func):
        """
        Adds a new function for this query.
        
        :param      func | <Query.Function>
        """
        self.__functions.append(func)

    def append_math_op(self, math_op, value):
        """
        Appends a new math operator to the math associated with this query.

        :param math_op: <Query.Math>
        :param value: <variant>
        """
        self.__math.append((math_op, value))

    def after(self, value):
        """
        Sets the operator type to Query.Op.After and sets the value to 
        the amount that this query should be lower than.  This is functionally
        the same as doing the less_than operation, but is useful for visual
        queries for things like dates.
        
        :param      value   | <variant>
        
        :return     <Query>
        
        :usage      |>>> from orb import Query as Q
                    |>>> query = Q('dateStart').after(date.today())
                    |>>> print query
                    |dateStart after 2011-10-10
        """
        newq = self.copy()
        newq.set_op(Query.Op.After)
        newq.set_value(value)
        return newq

    def and_(self, other):
        """
        Creates a new compound query using the 
        <orb.QueryCompound.Op.And> type.
        
        :param      other   <Query> || <orb.QueryCompound>
        
        :return     <orb.QueryCompound>
        
        :sa         __and__
        
        :usage      |>>> from orb import Query as Q
                    |>>> query = (Q('test') != 1).and_((Q('name') == 'Eric')
                    |>>> print query
                    |(test is not 1 and name is Eric)
        """
        if not isinstance(other, (Query, orb.QueryCompound)) or other.is_null():
            return self.copy()
        elif not self:
            return other.copy()
        else:
            return orb.QueryCompound(self, other, op=orb.QueryCompound.Op.And)

    def asString(self):
        """
        Returns this query with an AsString function added to it.
        
        :return     <Query>
        """
        q = self.copy()
        q.append_function_op(Query.Function.AsString)
        return q

    def before(self, value):
        """
        Sets the operator type to Query.Op.Before and sets the value to 
        the amount that this query should be lower than.  This is functionally
        the same as doing the less_than operation, but is useful for visual
        queries for things like dates.
        
        :param      value   | <variant>
        
        :return     <Query>
        
        :usage      |>>> from orb import Query as Q
                    |>>> query = Q('dateStart').before(date.today())
                    |>>> print query
                    |dateStart before 2011-10-10
        """
        newq = self.copy()
        newq.set_op(Query.Op.Before)
        newq.set_value(value)
        return newq

    def between(self, low, high):
        """
        Sets the operator type to Query.Op.Between and sets the
        value to a tuple of the two inputted values.
        
        :param      low | <variant>
        :param      high | <variant>
        
        :return     <Query>
        
        :usage      |>>> from orb import Query as Q
                    |>>> query = Q('test').between(1,2)
                    |>>> print query
                    |test between [1,2]
        """
        newq = self.copy()
        newq.set_op(Query.Op.Between)
        newq.set_value((low, high))
        return newq

    def case_sensitive(self):
        """
        Returns whether or not this query item will be case
        sensitive.  This will be used with string lookup items.
        
        :return     <bool>
        """
        return self.__case_sensitive

    def column(self, model=None):
        """
        Returns the column instance for this query.
        
        :return     <orb.Column>
        """
        try:
            schema = (self.__model or model).schema()
        except AttributeError:
            return None
        else:
            return schema.column(self.__column)

    def collector(self, model=None):
        try:
            schema = (self.__model or model).schema()
        except AttributeError:
            return None
        else:
            return schema.collector(self.__column)

    def columns(self, model=None):
        """
        Returns a generator that loops through the columns that are associated with this query.
        
        :return     <generator>(orb.Column)
        """
        column = self.column(model=model)
        if column:
            yield column

        check = self.__value
        if not isinstance(check, (list, set, tuple)):
            check = (check,)

        for val in check:
            if isinstance(val, (Query, orb.QueryCompound)):
                for col in val.columns(model):
                    yield col

    def column_name(self):
        """
        Returns the column name that this query instance is
        looking up.
        
        :return     <str>
        """
        return self.__column if not isinstance(self.__column, orb.Column) else self.__column.name()

    def contains(self, value, case_sensitive=False):
        """
        Sets the operator type to Query.Op.Contains and sets the    
        value to the inputted value.  Use an astrix for wildcard
        characters.
        
        :param      value           <variant>
        :param      case_sensitive   <bool>
        
        :return     self    (useful for chaining)
        
        :usage      |>>> from orb import Query as Q
                    |>>> query = Q('comments').contains('test')
                    |>>> print query
                    |comments contains test
        """
        newq = self.copy()
        newq.set_op(Query.Op.Contains)
        newq.set_value(value)
        newq.setCaseSensitive(case_sensitive)
        return newq

    def copy(self, **kw):
        """
        Returns a duplicate of this instance.

        :return     <Query>
        """
        kw.setdefault('op', self.__op)
        kw.setdefault('case_sensitive', self.__case_sensitive)
        kw.setdefault('value', copy.copy(self.__value))
        kw.setdefault('inverted', self.__inverted)
        kw.setdefault('functions', copy.copy(self.__functions))
        kw.setdefault('math', copy.copy(self.__math))
        kw.setdefault('model', self.__model)
        kw.setdefault('column', self.__column)

        return type(self)((kw.pop('model'), kw.pop('column')), **kw)

    def doesNotContain(self, value):
        """
        Sets the operator type to Query.Op.DoesNotContain and sets the
        value to the inputted value.
        
        :param      value       <variant>
        
        :return     self    (useful for chaining)
        
        :usage      |>>> from orb import Query as Q
                    |>>> query = Q('comments').doesNotContain('test')
                    |>>> print query
                    |comments does_not_contain test
        """
        newq = self.copy()
        newq.set_op(Query.Op.DoesNotContain)
        newq.set_value(value)
        return newq

    def doesNotMatch(self, value, case_sensitive=True):
        """
        Sets the operator type to Query.Op.DoesNotMatch and sets the \
        value to the inputted value.
        
        :param      value       <variant>
        
        :return     self    (useful for chaining)
        
        :usage      |>>> from orb import Query as Q
                    |>>> query = Q('comments').doesNotMatch('test')
                    |>>> print query
                    |comments does_not_contain test
        """
        newq = self.copy()
        newq.set_op(Query.Op.DoesNotMatch)
        newq.set_value(value)
        newq.setCaseSensitive(case_sensitive)
        return newq

    def endswith(self, value):
        """
        Sets the operator type to Query.Op.Endswith and sets \
        the value to the inputted value.  This method will only work on text \
        based fields.
        
        :param      value       <str>
        
        :return     <Query>
        
        :usage      |>>> from orb import Query as Q
                    |>>> query = Q('test').endswith('blah')
                    |>>> print query
                    |test startswith blah
        """
        newq = self.copy()
        newq.set_op(Query.Op.Endswith)
        newq.set_value(value)
        return newq

    def expand(self, model=None, ignoreFilter=False):
        """
        Expands any shortcuts that were created for this query.  Shortcuts
        provide the user access to joined methods using the '.' accessor to
        access individual columns for referenced tables.
        
        :param      model | <orb.Model> || None
        
        :usage      |>>> from orb import Query as Q
                    |>>> # lookup the 'username' of foreign key 'user'
                    |>>> Q('user.username') == 'bob.smith'
        
        :return     <orb.Query> || <orb.QueryCompound>
        """
        model = self.__model or model
        if not model:
            raise orb.errors.QueryInvalid('Could not traverse: {0}'.format(self.__column))

        schema = model.schema()
        parts = [self.__column.name()] if isinstance(self.__column, orb.Column) else self.__column.split('.')

        # expand the current column
        lookup = schema.column(parts[0], raise_=False) or schema.collector(parts[0])

        if lookup:
            # utilize query filters to generate
            # a new filter based on this object
            query_filter = lookup.filtermethod()
            if callable(query_filter) and not ignoreFilter:
                new_q = query_filter(model, self)
                if new_q:
                    return new_q.expand(model, ignoreFilter=True)
                else:
                    return None

            # otherwise, check to see if the lookup
            # has a shortcut to look through
            elif isinstance(lookup, orb.Column) and lookup.shortcut():
                parts = lookup.shortcut().split('.')
                lookup = schema.column(parts[0], raise_=False)

        if len(parts) == 1:
            return self
        else:
            if isinstance(lookup, orb.Collector):
                return orb.Query(model).in_(lookup.collect_expand(self, parts))

            elif isinstance(lookup, orb.ReferenceColumn):
                rmodel = lookup.reference_model()
                sub_q = self.copy()
                sub_q._Query__column = '.'.join(parts[1:])
                sub_q._Query__model = rmodel
                records = rmodel.select(columns=[rmodel.schema().id_column()], where=sub_q)
                return orb.Query(model, parts[0]).in_(records)

            else:
                raise orb.errors.QueryInvalid('Could not traverse: {0}'.format(self.__column))

    def functions(self):
        """
        Returns a list of the functions that are associated with this query.
        This will modify the lookup column for the given function type in order.
        
        :return     [<Query.Function>, ..]
        """
        return self.__functions

    def is_(self, value):
        """
        Creates a copy of this query object with the `Is` operator set and
        the `value` set to the given input.

        :usage:

            a = orb.Query('test').is_(1)

        :param value: <variant>

        :return: <orb.Query>
        """
        return self.copy(op=Query.Op.Is, value=value)

    def is_inverted(self):
        """
        Returns whether or not the value and column data should be inverted during query.

        :return     <bool>
        """
        return self.__inverted

    def greater_than(self, value):
        """
        Creates a copy of this query with the operator set to `Query.Op.GreaterThan` and
        the value set to the given input.

        :usage:

            a = orb.Query('value').greater_than(10)

        :param value: <variant>
        
        :return: <orb.Query>
        """
        return self.copy(op=Query.Op.GreaterThan, value=value)

    def greater_than_or_equal(self, value):
        """
        Sets the operator type to Query.Op.GreaterThanOrEqual and 
        sets the value to the inputted value.
        
        :param      value       <variant>
        
        :return     <Query>
        
        :sa         __ge__
        
        :usage      |>>> from orb import Query as Q
                    |>>> query = Q('test').greater_than_or_equal(1)
                    |>>> print query
                    |test greater_than_or_equal 1
        """
        return self.copy(op=Query.Op.GreaterThanOrEqual, value=value)

    def has(self, column):
        if isinstance(column, orb.Column):
            if self.__column in (column.field(), column.name()):
                return True
            else:
                return False
        else:
            return self.__column == column

    def hasShortcuts(self):
        """
        Returns whether or not this widget has shortcuts.
        
        :return     <bool>
        """
        return '.' in self.column_name()

    def inverted(self):
        """
        Returns an inverted copy of this query.

        :return     <orb.Query>
        """
        out = self.copy()
        out.setInverted(not self.isInverted())
        return out

    def isNot(self, value):
        """
        Sets the operator type to Query.Op.IsNot and sets the
        value to the inputted value.
        
        :param      value       <variant>
        
        :return     <Query>
        
        :sa         __ne__
        
        :usage      |>>> from orb import Query as Q
                    |>>> query = Q('test').isNot(1)
                    |>>> print query
                    |test is not 1
        """
        newq = self.copy()
        newq.set_op(Query.Op.IsNot)
        newq.set_value(value)
        return newq

    def is_null(self):
        """
        Return whether or not this query contains any information.
        
        :return     <bool>
        """
        return (self.__column is None or
                self.__op is None or
                self.__value is Query.UNDEFINED)

    def in_(self, value):
        """
        Sets the operator type to Query.Op.IsIn and sets the value
        to the inputted value.
        
        :param      value       <variant>
        
        :return     <Query>
        
        :usage      |>>> from orb import Query as Q
                    |>>> query = Q('test').isIn([1,2])
                    |>>> print query
                    |test is_in [1,2]
        """
        newq = self.copy()
        newq.set_op(Query.Op.IsIn)

        if isinstance(value, orb.Collection):
            newq.set_value(value)
        elif not isinstance(value, (set, list, tuple)):
            newq.set_value((value,))
        else:
            newq.set_value(tuple(value))

        return newq

    def math(self):
        """
        Returns the mathematical operations that are being performed for
        this query object.
        
        :return     [(<Query.Math>, <variant>), ..]
        """
        return self.__math

    def notIn(self, value):
        """
        Sets the operator type to Query.Op.IsNotIn and sets the value
        to the inputted value.
        
        :param      value       <variant>
        
        :return     <Query>
        
        :usage      |>>> from orb import Query as Q
                    |>>> query = Q('test').not_in([1,2])
                    |>>> print query
                    |test is_not_in [1,2]
        """
        newq = self.copy()
        newq.set_op(Query.Op.IsNotIn)

        if isinstance(value, orb.Collection):
            newq.set_value(value)
        elif not isinstance(value, (set, list, tuple)):
            newq.set_value((value,))
        else:
            newq.set_value(tuple(value))

        return newq

    def less_than(self, value):
        """
        Sets the operator type to Query.Op.LessThan and sets the
        value to the inputted value.
        
        :param      value       <variant>
        
        :return     <Query>
        
        :sa         less_than
        
        :usage      |>>> from orb import Query as Q
                    |>>> query = Q('test').less_than(1)
                    |>>> print query
                    |test less_than 1
        """
        return self.copy(op=Query.Op.LessThan, value=value)

    def less_than_or_equal(self, value):
        """
        Sets the operator type to Query.Op.LessThanOrEqual and sets 
        the value to the inputted value.
        
        :param      value       <variant>
        
        :return     <Query>
        
        :sa         less_than_or_equal
        
        :usage      |>>> from orb import Query as Q
                    |>>> query = Q('test').less_than_or_equal(1)
                    |>>> print query
                    |test less_than_or_equal 1
        """
        newq = self.copy()
        newq.set_op(Query.Op.LessThanOrEqual)
        newq.set_value(value)
        return newq

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
        newq = self.copy()
        newq.set_op(Query.Op.Matches)
        newq.set_value(value)
        newq.setCaseSensitive(case_sensitive)
        return newq

    def model(self, model=None):
        return self.__model or model

    def negated(self):
        """
        Negates the current state for this query.
        
        :return     <self>
        """
        query = self.copy()
        op = self.op()
        query.set_op(self.NegatedOp.get(op, op))
        query.set_value(self.value())
        return query

    def op(self):
        """
        Returns the operator type assigned to this query
        instance.
        
        :return     <Query.Op>
        """
        return self.__op

    def or_(self, other):
        """
        Creates a new compound query using the 
        <orb.QueryCompound.Op.Or> type.
        
        :param      other   <Query> || <orb.QueryCompound>
        
        :return     <orb.QueryCompound>
        
        :sa         or_
        
        :usage      |>>> from orb import Query as Q
                    |>>> query = (Q('test') != 1).or_(Q('name') == 'Eric')
                    |>>> print query
                    |(test does_not_equal 1 or name is Eric)
        """
        if not isinstance(other, (Query, orb.QueryCompound)) or other.is_null():
            return self.copy()
        elif not self:
            return other.copy()
        else:
            return orb.QueryCompound(self, other, op=orb.QueryCompound.Op.Or)

    def setCaseSensitive(self, state):
        """
        Sets whether or not this query will be case sensitive.
        
        :param      state   <bool>
        """
        self.__case_sensitive = state

    def setColumn(self, column):
        self.__column = column

    def setInverted(self, state=True):
        """
        Sets whether or not this query is inverted.

        :param      state | <bool>
        """
        self.__inverted = state

    def setModel(self, model):
        self.__model = model

    def set_op(self, op):
        """
        Sets the operator type used for this query instance.
        
        :param      op          <Query.Op>
        """
        self.__op = op

    def set_value(self, value):
        """
        Sets the value that will be used for this query instance.
        
        :param      value       <variant>
        """
        self.__value = projex.text.decoded(value) if isinstance(value, (str, unicode)) else value

    def startswith(self, value):
        """
        Sets the operator type to Query.Op.Startswith and sets \
        the value to the inputted value.  This method will only work on text \
        based fields.
        
        :param      value       <str>
        
        :return     <Query>
        
        :usage      |>>> from orb import Query as Q
                    |>>> query = Q('test').startswith('blah')
                    |>>> print query
                    |test startswith blah
        """
        newq = self.copy()
        newq.set_op(Query.Op.Startswith)
        newq.set_value(value)
        return newq

    def upper(self):
        """
        Returns this query with the Upper function added to its list.
        
        :return     <Query>
        """
        q = self.copy()
        q.append_function_op(Query.Function.Upper)
        return q

    def value(self):
        """
        Returns the value for this query instance
        
        :return     <variant>
        """
        if isinstance(self.__value, State):
            return None
        else:
            return self.__value

    @staticmethod
    def build(data=None, **kwds):
        data = data or {}
        data.update(kwds)

        if not data:
            return None
        else:
            q = Query()
            for k, v in data.items():
                q &= Query(k) == v
            return q

    @staticmethod
    def fromJSON(jdata):
        """
        Creates a new Query object from the given JSON data.

        :param      jdata | <dict>

        :return     <orb.Query> || <orb.QueryCompound>
        """
        if jdata['type'] == 'compound':
            queries = [orb.Query.fromJSON(jquery) for jquery in jdata['queries']]
            out = orb.QueryCompound(*queries)
            out.set_op(orb.QueryCompound.Op(jdata['op']))
            return out
        else:
            if jdata.get('model'):
                model = orb.schema.model(jdata.get('model'))
                if not model:
                    raise orb.errors.ModelNotFound(schema=jdata.get('model'))
                else:
                    column = (model, jdata['column'])
            else:
                column = (jdata['column'],)

            query = orb.Query(*column)
            query.set_op(orb.Query.Op(jdata.get('op', 'Is')))
            query.setInverted(jdata.get('inverted', False))
            query.setCaseSensitive(jdata.get('case_sensitive', False))
            query.set_value(jdata.get('value'))

            # restore the function information
            for func in jdata.get('functions', []):
                query.append_function_op(orb.Query.Function(func))

            # restore the math information
            for entry in jdata.get('math', []):
                query.append_math_op(orb.Query.Math(entry.get('op')), entry.get('value'))
            return query

