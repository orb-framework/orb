"""
Defines the global query building syntax for generating db
agnostic queries quickly and easily.
"""

import copy
import datetime
import projex.regex
import projex.text
import re

from projex.enum import enum
from projex.lazymodule import lazy_import


orb = lazy_import('orb')


class Query(object):
    """ 
    Defines the central class for the abstract query markup language.
    """

    Op = enum(
        # equality operators
        'Is',
        'IsNot',

        # comparison operators
        'LessThan',
        'LessThanOrEqual',
        'Before',
        'GreaterThan',
        'GreaterThanOrEqual',
        'After',
        'Between',

        # string operators
        'Contains',
        'DoesNotContain',
        'Startswith',
        'Endswith',
        'Matches',
        'DoesNotMatch',

        # list operators
        'IsIn',
        'IsNotIn',

        #----------------------------------------------------------------------
        # added in 4.0
        #----------------------------------------------------------------------
        'DoesNotStartwith',
        'DoesNotEndwith'
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
        'Add',
        'Subtract',
        'Multiply',
        'Divide',
        'And',
        'Or'
    )

    Function = enum(
        'Lower',
        'Upper',
        'Abs',
        'AsString',
    )

    # additional option values to control query flow
    UNDEFINED = '__QUERY__UNDEFINED__'
    NOT_EMPTY = '__QUERY__NOT_EMPTY__'
    EMPTY = '__QUERY__EMPTY__'
    ALL = '__QUERY__ALL__'

    def __hash__(self):
        if isinstance(self.__value, (list, set)):
            val_hash = tuple(self.__value)
        else:
            try:
                val_hash = hash(self.__value)
            except TypeError:
                val_hash = hash(unicode(self.__value))

        return hash((
            self.__model,
            self.__column,
            self.__op,
            self.__caseSensitive,
            val_hash,
            self.__inverted,
            tuple(self.__functions),
            tuple(self.__math)
        ))

    # python 2.x
    def __nonzero__(self):
        return not self.isNull()

    # python 3.x
    def __bool__(self):
        return not self.isNull()

    def __json__(self):
        if hasattr(self.__value, '__json__'):
            value = self.__value.__json__()
        else:
            value = self.__value

        jdata = {
            'type': 'query',
            'model': self.__model.schema().name() if self.__model else '',
            'column': self.__column,
            'op': self.Op(self.__op),
            'caseSensitive': self.__caseSensitive,
            'functions': [self.Function(func) for func in self.__functions],
            'math': [{'op': self.Math(op), 'value': value} for (op, value) in self.__math],
            'inverted': self.__inverted,
            'value': value
        }
        return jdata

    def __init__(self, *column, **options):
        """
        Initializes the Query instance.  The only required variable
        is the column name, the rest can be manipulated after
        creation.  This class takes a variable set of information
        to initialize.  You can initialize a blank query object
        by supplying no arguments, which is useful when generating
        queries in a loop, or you can supply only a string column
        value for lookup (the table will auto-populate from the
        selection, or you can supply a model and column name (
        used in the join operation).
        
        :param      *args           <tuple>
                    
                    #. None
                    #. <str> columnName
                    #. <orb.Column>
                    #. <subclass of Table>
                    #. (<subclass of Table> table,<str> columnName)
                    
        :param      **options       <dict> options for the query.
        
                    *. op               <Query.Op>
                    *. value            <variant>
                    *. caseSensitive    <bool>
        
        """
        # initialized with (model, column)
        if len(column) == 2:
            self.__model, self.__column = column
        elif len(column) == 1:
            column = column[0]

            try:
                if issubclass(column, orb.Model):
                    self.__model = column
                    self.__column = column.schema().idColumn().name()
            except StandardError:
                if isinstance(column, orb.Column):
                    self.__model = column.schema().model()
                    self.__column = column.name()
                else:
                    self.__model = None
                    self.__column = column
        else:
            self.__model = None
            self.__column = None

        self.__op = options.get('op', Query.Op.Is)
        self.__caseSensitive = options.get('caseSensitive', False)
        self.__value = options.get('value', None)
        self.__inverted = options.get('inverted', False)
        self.__functions = options.get('functions', [])
        self.__math = options.get('math', [])

    def __contains__(self, column):
        """
        Returns whether or not the query defines the inputted column name.

        :param      value | <variant>

        :return     <bool>

        :usage      |>>> from orb import Query as Q
                    |>>> q = Q('testing') == True
                    |>>> 'testing' in q
                    |True
                    |>>> 'name' in q
                    |False
        """
        if isinstance(column, orb.Column):
            return self.__model == column.schema().model() and self.__column == column.name()
        else:
            return column == self.__column

    # operator methods
    def __add__(self, value):
        """
        Adds the inputted value to this query with arithmetic joiner.
        
        :param      value | <variant>
        
        :return     <Query> self
        """
        out = self.copy()
        out.addMath(Query.Math.Add, value)
        return out

    def __abs__(self):
        """
        Creates an absolute version of this query using the standard python
        absolute method.
        
        :return     <Query>
        """
        out = self.copy()
        out.addFunction(Query.Function.Abs)
        return out

    def __and__(self, other):
        """
        Creates a new compound query using the 
        <orb.QueryCompound.Op.And> type.
        
        :param      other   <Query> || <orb.QueryCompound>
        
        :return     <orb.QueryCompound>
        
        :sa         and_
        
        :usage      |>>> from orb import Query as Q
                    |>>> query = (Q('test') != 1) & (Q('name') == 'Eric')
                    |>>> print query
                    |(test is not 1 and name is Eric)
        """
        if other is None:
            return self.copy()
        elif isinstance(other, (Query, QueryCompound)):
            return self.and_(other)
        else:
            out = self.copy()
            out.addMath(Query.Math.And, other)
            return out

    def __cmp__(self, other):
        """
        Use the compare method to be able to see if two query items are
        the same vs. ==, since == is used to set the query's is value.
        
        :param      other       <variant>
        
        :return     <int> 1 | 0 | -1
        """
        if not isinstance(other, Query):
            return -1
        elif id(self) == id(other):
            return 0
        else:
            return 1

    def __div__(self, value):
        """
        Divides the inputted value for this query to the inputted query.
        
        :param      value | <variant>
        
        :return     <Query> self
        """
        out = self.copy()
        out.addMath(Query.Math.Divide, value)
        return out

    def __eq__(self, other):
        """
        Allows joining of values to the query by the == operator.
        If another Query instance is passed in, then it will do 
        a standard comparison.
        
        :param      other       <variant>
        
        :return     <Query>
        
        :sa         is
        
        :usage      |>>> from orb import *
                    |>>> query = Query('test') == 1 
                    |>>> print query
                    |test is 1
        """
        return self.is_(other)

    def __gt__(self, other):
        """
        Allows the joining of values to the query by the > 
        operator. If another Query instance is passed in, then it 
        will do a standard comparison.
        
        :param      other       <variant>
        
        :return     <Query>
        
        :sa         lessThan
        
        :usage      |>>> from orb import Query as Q
                    |>>> query = Q('test') > 1
                    |>>> print query
                    |test greater_than 1
        """
        return self.greaterThan(other)

    def __ge__(self, other):
        """
        Allows the joining of values to the query by the >= 
        operator.  If another Query instance is passed in, then it 
        will do a standard comparison.
        
        :param      other       <variant>
        
        :return     <Query>
        
        :sa         greaterThanOrEqual
        
        :usage      |>>> from orb import Query as Q
                    |>>> query = Q('test') >= 1
                    |>>> print query
                    |test <= 1
        """
        return self.greaterThanOrEqual(other)

    def __lt__(self, other):
        """
        Allows the joining of values to the query by the < 
        operator.  If another Query instance is passed in, then it 
        will do a standard comparison.
        
        :param      other       <variant>
        
        :return     <Query>
        
        :sa         lessThan
        
        :usage      |>>> from orb import Query as Q
                    |>>> query = Q('test') < 1
                    |>>> print query
                    |test less_than 1
        """
        return self.lessThan(other)

    def __le__(self, other):
        """
        Allows the joining of values to the query by the <= 
        operator.  If another Query instance is passed in, then it 
        will do a standard comparison.
        
        :param      other       <variant>
        
        :return     <Query>
        
        :sa         lessThanOrEqual
        
        :usage      |>>> from orb import Query as Q
                    |>>> query = Q('test') <= 1
                    |>>> print query
                    |test <= 1
        """
        return self.lessThanOrEqual(other)

    def __mul__(self, value):
        """
        Multiplies the value with this query to the inputted query.
        
        :param      value | <variant>
        
        :return     <Query> self
        """
        out = self.copy()
        out.addMath(Query.Math.Multiply, value)
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
        elif isinstance(other, (Query, QueryCompound)):
            return self.or_(other)
        else:
            out = self.copy()
            out.addMath(Query.Math.Or, other)
            return out

    def __sub__(self, value):
        """
        Subtracts the value from this query.
        
        :param      value | <variant>
        
        :return     <Query> self
        """
        out = self.copy()
        out.addMath(Query.Math.Subtract, value)
        return out

    # public methods
    def addFunction(self, func):
        """
        Adds a new function for this query.
        
        :param      func | <Query.Function>
        """
        self.__functions.append(func)

    def addMath(self, math, value):
        self.__math.append((math, value))

    def after(self, value):
        """
        Sets the operator type to Query.Op.After and sets the value to 
        the amount that this query should be lower than.  This is functionally
        the same as doing the lessThan operation, but is useful for visual
        queries for things like dates.
        
        :param      value   | <variant>
        
        :return     <Query>
        
        :usage      |>>> from orb import Query as Q
                    |>>> query = Q('dateStart').after(date.today())
                    |>>> print query
                    |dateStart after 2011-10-10
        """
        newq = self.copy()
        newq.setOp(Query.Op.After)
        newq.setValue(value)
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
        if not isinstance(other, (Query, QueryCompound)) or other.isNull():
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
        q.addFunction(Query.Function.AsString)
        return q

    def before(self, value):
        """
        Sets the operator type to Query.Op.Before and sets the value to 
        the amount that this query should be lower than.  This is functionally
        the same as doing the lessThan operation, but is useful for visual
        queries for things like dates.
        
        :param      value   | <variant>
        
        :return     <Query>
        
        :usage      |>>> from orb import Query as Q
                    |>>> query = Q('dateStart').before(date.today())
                    |>>> print query
                    |dateStart before 2011-10-10
        """
        newq = self.copy()
        newq.setOp(Query.Op.Before)
        newq.setValue(value)
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
        newq.setOp(Query.Op.Between)
        newq.setValue((low, high))
        return newq

    def caseSensitive(self):
        """
        Returns whether or not this query item will be case
        sensitive.  This will be used with string lookup items.
        
        :return     <bool>
        """
        return self.__caseSensitive

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
            if isinstance(val, (Query, QueryCompound)):
                for col in val.columns(model):
                    yield col

    def columnName(self):
        """
        Returns the column name that this query instance is
        looking up.
        
        :return     <str>
        """
        return self.__column

    def contains(self, value, caseSensitive=False):
        """
        Sets the operator type to Query.Op.Contains and sets the    
        value to the inputted value.  Use an astrix for wildcard
        characters.
        
        :param      value           <variant>
        :param      caseSensitive   <bool>
        
        :return     self    (useful for chaining)
        
        :usage      |>>> from orb import Query as Q
                    |>>> query = Q('comments').contains('test')
                    |>>> print query
                    |comments contains test
        """
        newq = self.copy()
        newq.setOp(Query.Op.Contains)
        newq.setValue(value)
        newq.setCaseSensitive(caseSensitive)
        return newq

    def copy(self):
        """
        Returns a duplicate of this instance.

        :return     <Query>
        """
        options = {
            'op': self.__op,
            'caseSensitive': self.__caseSensitive,
            'value': copy.copy(self.__value),
            'inverted': self.__inverted,
            'functions': copy.copy(self.__functions),
            'math': copy.copy(self.__math)
        }
        return orb.Query(self.__model, self.__column, **options)

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
        newq.setOp(Query.Op.DoesNotContain)
        newq.setValue(value)
        return newq

    def doesNotMatch(self, value, caseSensitive=True):
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
        newq.setOp(Query.Op.DoesNotMatch)
        newq.setValue(value)
        newq.setCaseSensitive(caseSensitive)
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
        newq.setOp(Query.Op.Endswith)
        newq.setValue(value)
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
        parts = self.__column.split('.')

        # expand the current column
        lookup = schema.column(parts[0], raise_=False) or schema.collector(parts[0])

        if lookup:
            # utilize query filters to generate
            # a new filter based on this object
            query_filter = lookup.queryFilterMethod()
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
                return orb.Query(model).in_(lookup.collectExpand(self, parts))

            elif isinstance(lookup, orb.ReferenceColumn):
                rmodel = lookup.referenceModel()
                sub_q = self.copy()
                sub_q._Query__column = '.'.join(parts[1:])
                sub_q._Query__model = rmodel
                records = rmodel.select(columns=[rmodel.schema().idColumn()], where=sub_q)
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
        Sets the operator type to Query.Op.Is and sets the
        value to the inputted value.
        
        :param      value       <variant>
        
        :return     <Query>
        
        :sa         __eq__
        
        :usage      |>>> from orb import Query as Q
                    |>>> query = Q('test').is_(1)
                    |>>> print query
                    |test is 1
        """
        newq = self.copy()
        newq.setOp(Query.Op.Is)
        newq.setValue(value)
        return newq

    def isInverted(self):
        """
        Returns whether or not the value and column data should be inverted during query.

        :return     <bool>
        """
        return self.__inverted

    def greaterThan(self, value):
        """
        Sets the operator type to Query.Op.GreaterThan and sets the
        value to the inputted value.
        
        :param      value       <variant>
        
        :return     <Query>
        
        :sa         __gt__
        
        :usage      |>>> from orb import Query as Q
                    |>>> query = Q('test').greaterThan(1)
                    |>>> print query
                    |test greater_than 1
        """
        newq = self.copy()
        newq.setOp(Query.Op.GreaterThan)
        newq.setValue(value)
        return newq

    def greaterThanOrEqual(self, value):
        """
        Sets the operator type to Query.Op.GreaterThanOrEqual and 
        sets the value to the inputted value.
        
        :param      value       <variant>
        
        :return     <Query>
        
        :sa         __ge__
        
        :usage      |>>> from orb import Query as Q
                    |>>> query = Q('test').greaterThanOrEqual(1)
                    |>>> print query
                    |test greater_than_or_equal 1
        """
        newq = self.copy()
        newq.setOp(Query.Op.GreaterThanOrEqual)
        newq.setValue(value)
        return newq

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
        return '.' in self.columnName()

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
        newq.setOp(Query.Op.IsNot)
        newq.setValue(value)
        return newq

    def isNull(self):
        """
        Return whether or not this query contains any information.
        
        :return     <bool>
        """
        return self.__column is None

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
        newq.setOp(Query.Op.IsIn)

        if isinstance(value, orb.Collection):
            newq.setValue(value)
        elif not isinstance(value, (set, list, tuple)):
            newq.setValue((value,))
        else:
            newq.setValue(tuple(value))

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
        newq.setOp(Query.Op.IsNotIn)

        if isinstance(value, orb.Collection):
            newq.setValue(value)
        elif not isinstance(value, (set, list, tuple)):
            newq.setValue((value,))
        else:
            newq.setValue(tuple(value))

        return newq

    def lessThan(self, value):
        """
        Sets the operator type to Query.Op.LessThan and sets the
        value to the inputted value.
        
        :param      value       <variant>
        
        :return     <Query>
        
        :sa         lessThan
        
        :usage      |>>> from orb import Query as Q
                    |>>> query = Q('test').lessThan(1)
                    |>>> print query
                    |test less_than 1
        """
        newq = self.copy()
        newq.setOp(Query.Op.LessThan)
        newq.setValue(value)
        return newq

    def lessThanOrEqual(self, value):
        """
        Sets the operator type to Query.Op.LessThanOrEqual and sets 
        the value to the inputted value.
        
        :param      value       <variant>
        
        :return     <Query>
        
        :sa         lessThanOrEqual
        
        :usage      |>>> from orb import Query as Q
                    |>>> query = Q('test').lessThanOrEqual(1)
                    |>>> print query
                    |test less_than_or_equal 1
        """
        newq = self.copy()
        newq.setOp(Query.Op.LessThanOrEqual)
        newq.setValue(value)
        return newq

    def lower(self):
        """
        Returns a new query for this instance with Query.Function.Lower as
        a function option.
        
        :return     <Query>
        """
        q = self.copy()
        q.addFunction(Query.Function.Lower)
        return q

    def matches(self, value, caseSensitive=True):
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
        newq.setOp(Query.Op.Matches)
        newq.setValue(value)
        newq.setCaseSensitive(caseSensitive)
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
        query.setOp(self.NegatedOp.get(op, op))
        query.setValue(self.value())
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
        if not isinstance(other, (Query, QueryCompound)) or other.isNull():
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
        self.__caseSensitive = state

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

    def setOp(self, op):
        """
        Sets the operator type used for this query instance.
        
        :param      op          <Query.Op>
        """
        self.__op = op

    def setValue(self, value):
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
        newq.setOp(Query.Op.Startswith)
        newq.setValue(value)
        return newq

    def upper(self):
        """
        Returns this query with the Upper function added to its list.
        
        :return     <Query>
        """
        q = self.copy()
        q.addFunction(Query.Function.Upper)
        return q

    def value(self):
        """
        Returns the value for this query instance
        
        :return     <variant>
        """
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
            out.setOp(orb.QueryCompound.Op(jdata['op']))
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
            query.setOp(orb.Query.Op(jdata.get('op', 'Is')))
            query.setInverted(jdata.get('inverted', False))
            query.setCaseSensitive(jdata.get('caseSensitive', False))
            query.setValue(jdata.get('value'))

            # restore the function information
            for func in jdata.get('functions', []):
                query.addFunction(orb.Query.Function(func))

            # restore the math information
            for entry in jdata.get('math', []):
                query.addMath(orb.Query.Math(entry.get('op')), entry.get('value'))
            return query


class QueryCompound(object):
    """ Defines combinations of queries via either the AND or OR mechanism. """
    Op = enum(
        'And',
        'Or'
    )

    def __hash__(self):
        return hash((
            self.__op,
            hash(hash(q) for q in self.__queries)
        ))

    def __json__(self):
        data = {
            'type': 'compound',
            'queries': [q.__json__() for q in self.__queries],
            'op': self.Op(self.__op)
        }
        return data

    def __contains__(self, column):
        """
        Returns whether or not the query compound contains a query for the
        inputted column name.

        :param      value | <variant>

        :return     <bool>

        :usage      |>>> from orb import Query as Q
                    |>>> q = Q('testing') == True
                    |>>> 'testing' in q
                    |True
                    |>>> 'name' in q
                    |False
        """
        for query in self.__queries:
            if column in query:
                return True
        return False

    def __nonzero__(self):
        return not self.isNull()

    def __init__(self, *queries, **options):
        self.__queries = queries
        self.__op = options.get('op', QueryCompound.Op.And)

    def __iter__(self):
        for query in self.__queries:
            yield query

    def __and__(self, other):
        """
        Creates a new compound query using the
        QueryCompound.Op.And type.

        :param      other   <Query> || <QueryCompound>

        :return     <QueryCompound>

        :sa         and_

        :usage      |>>> from orb import Query as Q
                    |>>> query = (Q('test') != 1) & (Q('name') == 'Eric')
                    |>>> print query
                    |(test does_not_equal 1 and name is Eric)
        """
        return self.and_(other)

    def __neg__(self):
        """
        Negates the current state of the query.

        :sa     negate

        :return     self

        :usage      |>>> from orb import Query as Q
                    |>>> query = (Q('test') == 1) & (Q('name') == 'Eric')
                    |>>> print -query
                    |NOT (test is  1 and name is Eric)
        """
        return self.negated()

    def __or__(self, other):
        """
        Creates a new compound query using the
        QueryCompound.Op.Or type.

        :param      other   <Query> || <QueryCompound>

        :return     <QueryCompound>

        :sa         or_

        :usage      |>>> from orb import Query as Q
                    |>>> query = (Q('test') != 1) | (Q('name') == 'Eric')
                    |>>> print query
                    |(test isNot 1 or name is Eric)
        """
        return self.or_(other)

    def and_(self, other):
        """
        Creates a new compound query using the
        QueryCompound.Op.And type.

        :param      other   <Query> || <QueryCompound>

        :return     <QueryCompound>

        :sa         __and__

        :usage      |>>> from orb import Query as Q
                    |>>> query = (Q('test') != 1).and_((Q('name') == 'Eric')
                    |>>> print query
                    |(test isNot 1 and name is Eric)
        """
        if not isinstance(other, (Query, QueryCompound)) or other.isNull():
            return self.copy()
        elif self.isNull():
            return other.copy()
        else:
            # grow this if the operators are the same
            if self.__op == QueryCompound.Op.And:
                queries = list(self.__queries) + [other]
                return QueryCompound(*queries, op=QueryCompound.Op.And)
            else:
                return QueryCompound(self, other, op=QueryCompound.Op.And)

    def copy(self):
        """
        Returns a copy of this query compound.

        :return     <QueryCompound>
        """
        return type(self)(*self.__queries, op=self.__op)

    def columns(self, model=None):
        """
        Returns any columns used within this query.

        :return     [<orb.Column>, ..]
        """
        for query in self.__queries:
            for column in query.columns(model=model):
                yield column

    def expand(self, model=None, ignoreFilter=False):
        """
        Expands any shortcuts that were created for this query.  Shortcuts
        provide the user access to joined methods using the '.' accessor to
        access individual columns for referenced tables.

        :param      model | <orb.Model>

        :usage      |>>> from orb import Query as Q
                    |>>> # lookup the 'username' of foreign key 'user'
                    |>>> Q('user.username') == 'bob.smith'

        :return     <orb.Query> || <orb.QueryCompound>
        """
        queries = []
        current_records = None

        for query in self.__queries:
            sub_q = query.expand(model)
            if not sub_q:
                continue

            # chain together joins into sub-queries
            if ((isinstance(sub_q, orb.Query) and isinstance(sub_q.value(), orb.Query)) and
                 sub_q.value().model(model) != sub_q.model(model)):
                sub_model = sub_q.value().model(model)
                sub_col = sub_q.value().column()
                new_records = sub_model.select(columns=[sub_col])

                sub_q = sub_q.copy()
                sub_q.setOp(sub_q.Op.IsIn)
                sub_q.setValue(new_records)

                if current_records is not None and current_records.model() == sub_q.model(model):
                    new_records = new_records.refine(createNew=False, where=sub_q)
                else:
                    queries.append(sub_q)

                current_records = new_records

            # update the existing recordset in the chain
            elif (current_records is not None and
                 (
                    (isinstance(sub_q, orb.Query) and current_records.model() == query.model(model)) or
                    (isinstance(sub_q, orb.QueryCompound) and current_records.model() in sub_q.models(model))
                 )):
                current_records.refine(createNew=False, where=sub_q)

            # clear out the chain and move on to the next query set
            else:
                current_records = None
                queries.append(query)

        return QueryCompound(*queries, op=self.op())

    def has(self, column):
        for query in self.__queries:
            if query.has(column):
                return True
        else:
            return False

    def isNull(self):
        """
        Returns whether or not this join is empty or not.

        :return     <bool>
        """
        for query in self.__queries:
            if not query.isNull():
                return False
        else:
            return True

    def negated(self):
        """
        Negates this instance and returns it.

        :return     self
        """
        op = QueryCompound.Op.And if self.__op == QueryCompound.Op.Or else QueryCompound.Op.Or
        return QueryCompound(*self.__queries, op=op)

    def op(self):
        """
        Returns the operator type for this compound.

        :return     <QueryCompound.Op>
        """
        return self.__op

    def or_(self, other):
        """
        Creates a new compound query using the
        QueryCompound.Op.Or type.

        :param      other   <Query> || <QueryCompound>

        :return     <QueryCompound>

        :sa         or_

        :usage      |>>> from orb import Query as Q
                    |>>> query = (Q('test') != 1).or_(Q('name') == 'Eric')
                    |>>> print query
                    |(test isNot 1 or name is Eric)
        """
        if not isinstance(other, (Query, QueryCompound)) or other.isNull():
            return self.copy()
        elif self.isNull():
            return other.copy()
        else:
            # grow this if the operators are the same
            if self.__op == QueryCompound.Op.And:
                queries = list(self.__queries) + [other]
                return QueryCompound(*queries, op=QueryCompound.Op.Or)
            else:
                return QueryCompound(self, other, op=QueryCompound.Op.Or)

    def queries(self):
        """
        Returns the list of queries that are associated with
        this compound.

        :return     <list> [ <Query> || <QueryCompound>, .. ]
        """
        return self.__queries

    def setOp(self, op):
        """
        Sets the operator type that this compound that will be
        used when joining together its queries.

        :param      op      <QueryCompound.Op>
        """
        self.__op = op

    def models(self, model=None):
        """
        Returns the tables that this query is referencing.

        :return     [ <subclass of Table>, .. ]
        """
        for query in self.__queries:
            if isinstance(query, orb.Query):
                yield query.model(model)
            else:
                for model in query.models(model):
                    yield model
