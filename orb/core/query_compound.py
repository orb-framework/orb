import demandimport
import datetime
import re

from ..utils.enum import enum

with demandimport.enabled():
    import orb


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
        if not isinstance(other, (orb.Query, QueryCompound)) or other.isNull():
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

    def at(self, index):
        """
        Returns the query or compound at the given index for this compound.
        If the index is out of bounds, then a None value will be returned.

        :param index: <int>

        :return: <orb.Query> or <orb.QueryCompound> or None
        """
        try:
            return self.__queries[index]
        except IndexError:
            return None

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
                    new_records = new_records.refine(create_new=False, where=sub_q)
                else:
                    queries.append(sub_q)

                current_records = new_records

            # update the existing recordset in the chain
            elif (current_records is not None and
                 (
                    (isinstance(sub_q, orb.Query) and current_records.model() == query.model(model)) or
                    (isinstance(sub_q, orb.QueryCompound) and current_records.model() in sub_q.models(model))
                 )):
                current_records.refine(create_new=False, where=sub_q)

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
        if not isinstance(other, (orb.Query, QueryCompound)) or other.isNull():
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
