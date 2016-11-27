import demandimport

from ..decorators import deprecated
from ..utils.enum import enum

with demandimport.enabled():
    import orb


class QueryCompound(object):
    Op = enum(
        And=1,
        Or=2
    )

    def __init__(self, *queries, **kw):
        super(QueryCompound, self).__init__()

        # setup the query compound op
        op = kw.get('op', QueryCompound.Op.And)
        if isinstance(op, (str, unicode)):
            op = QueryCompound.Op(op)

        # define custom properties
        self.__queries = queries or list(kw.get('queries') or [])
        self.__op = op

    # python 2.x
    def __nonzero__(self):
        return not self.is_null()

    # python 3.x
    def __bool__(self):
        return not self.is_null()

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
        return self.and_(other)

    def __contains__(self, schema_object):
        """
        Returns whether or not the given schema object is found
        in this compound or any of it's queries.

        :param schema_object: <str> or <orb.Column> or <orb.QueryCompound>

        :return: <bool>
        """
        if isinstance(schema_object, (orb.Query, orb.QueryCompound)):
            return schema_object in self.__queries
        else:
            return self.has(schema_object)

    def __getitem__(self, index):
        """
        Returns the query at the given index for this instance.

        :param index: <int>
        """
        return self.__queries[index]

    def __hash__(self):
        """
        Hashes the compound to be able to compare against others.  You can't
        use the `==` operator because it is overloaded to build query values.

        :usage:

            q = orb.Query('name') == 'testing'
            b = orb.Query('name') == 'testing'
            c = orb.QueryCompound(a, b)
            d = orb.QueryCompound(a, b)

            hash(c) == hash(d)

        :return: <int>
        """
        return hash((
            self.__op,
            tuple(hash(q) for q in self.__queries)
        ))

    def __iter__(self):
        """
        Returns an iterator for this compound.  This will iterate
        over the queries in the instance.

        :return: <generator>
        """
        return iter(self.__queries)

    def __json__(self):
        """
        Converts this query to a JSON serializable dictionary.

        :return: <dict>
        """
        data = {
            'type': 'compound',
            'queries': [q.__json__() for q in self.__queries],
            'op': self.Op(self.__op)
        }
        return data

    def __len__(self):
        """
        Returns the length of this compound, which is the length of the
        queries in this compound.

        :return: <int>
        """
        return len(self.__queries)

    def __neg__(self):
        """
        Inverts the query.  This will create a copy of this Query
        by flipping the operator for the query to it's corresponding inversion
        op.

        :return: <orb.QueryCompound>
        """
        return self.negated()

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
        return self.or_(other)

    def and_(self, other):
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
        if not isinstance(other, (orb.Query, orb.QueryCompound)):
            return self.copy()
        elif other.is_null():
            return self.copy()
        elif self.is_null():
            return other.copy()
        else:
            # expand the core queries if the same operator is used
            if self.__op == orb.QueryCompound.Op.And:
                queries = list(self.__queries) + [other]

            # create a new set of core queries
            else:
                queries = [self, other]

            return QueryCompound(*queries, op=orb.QueryCompound.Op.And)

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

    def copy(self, **kw):
        """
        Returns a copy of this query compound.

        :return     <QueryCompound>
        """
        kw.setdefault('queries', self.__queries or [])
        kw.setdefault('op', self.__op)
        return type(self)(*kw.pop('queries'), **kw)

    @deprecated
    def columns(self, model=None):
        """
        Returns any columns used within this query.

        :return     [<orb.Column>, ..]
        """
        return self.schema_objects(model=model)

    def expand(self, model=None, use_filter=True):
        """
        Expands any shortcuts that were created for this query.  Shortcuts
        provide the user access to joined methods using the '.' accessor to
        access individual columns for referenced tables.

        :param model: subclass of <orb.Model> or None
        :param use_filter: <bool>

        :return: <orb.QueryCompound>
        """
        queries = []
        current_records = None

        for query in self.__queries:
            # generate an expanded subquery
            sub_q = query.expand(model=model, use_filter=use_filter)

            if not sub_q:  # pragma: no cover
                continue

            # chain together joins into sub-queries
            if (isinstance(sub_q, orb.Query) and
                isinstance(sub_q.value(), orb.Query) and
                sub_q.model(default=model) is not sub_q.value().model(default=model)):

                # create a new collection based on the sub-query
                value = sub_q.value()
                value_model = value.model(default=model)
                value_column = value.column(model=model)
                new_records = value_model.select(columns=[value_column])

                # create a new copy of the subquery with updated values
                sub_q = sub_q.copy(op=sub_q.Op.IsIn, value=new_records)

                # filter the existing records
                if current_records is not None and current_records.model() is sub_q.model(default=model):
                    new_records.refine(create_new=False, where=sub_q)
                else:
                    queries.append(sub_q)

                # cache the new records
                current_records = new_records

            # update the existing recordset in the chain
            elif (current_records is not None and
                 (
                    (isinstance(sub_q, orb.Query) and current_records.model() == query.model(default=model)) or
                    (isinstance(sub_q, orb.QueryCompound) and current_records.model() in list(sub_q.models(model)))
                 )):
                current_records.refine(create_new=False, where=sub_q)

            # clear out the chain and move on to the next query set
            else:  # pragma: no cover
                current_records = None
                queries.append(query)

        return QueryCompound(*queries, op=self.op())

    def has(self, schema_object):
        """
        Checks to see if the given schema object is represented
        in one of the queries within this compound.

        :param schema_object: <str> or <orb.Column> or <orb.Collector>

        :return: <bool>
        """
        for query in self.__queries:
            if query.has(schema_object):
                return True
        else:
            return False

    def is_null(self):
        """
        Returns whether or not this join is empty or not.

        :return: <bool>
        """
        for query in self.__queries:
            if not query.is_null():
                return False
        else:
            return True

    def models(self, default=None):
        """
        Returns the models that are being referenced by this
        compound.

        :param default: subclass of <orb.Model> or None

        :return: [subclass of <orb.Model>, ..]
        """
        for query in self.__queries:
            if isinstance(query, orb.Query):
                yield query.model(default=default)
            else:
                for model in query.models(default=default):
                    yield model

    def negated(self):
        """
        Negates the operator for this compound, turning an `And` joined
        query into an `Or` joined query, and vice versa.

        :return: <orb.QueryCompound>
        """
        op = QueryCompound.Op.And if self.__op == QueryCompound.Op.Or else QueryCompound.Op.Or
        return QueryCompound(*self.__queries, op=op)

    def op(self):
        """
        Returns the operator type for this compound.

        :return: <QueryCompound.Op>
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
            # expand the core queries if the same operator is used
            if self.__op == orb.QueryCompound.Op.Or:
                queries = list(self.__queries) + [other]

            # create a new set of core queries
            else:
                queries = [self, other]

            return QueryCompound(*queries, op=orb.QueryCompound.Op.Or)

    def queries(self):
        """
        Returns the list of queries that are associated with
        this compound.

        :return: [<orb.Query> or <orb.QueryCompound>, ..]
        """
        return list(self.__queries)

    def schema_objects(self, model=None):
        """
        Returns any associated schema objects used within this query.

        :param model: subclass of <orb.Model> or None

        :return     [<orb.Column> or <orb.Collector>, ..]
        """
        for query in self.__queries:
            for schema_object in query.schema_objects(model=model):
                yield schema_object
