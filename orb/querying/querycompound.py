"""
Defines the global query building syntax for generating db
agnostic queries quickly and easily.
"""

import logging
import projex.text

from projex.lazymodule import lazy_import
from projex.enum import enum
from projex.text import nativestring as nstr
from xml.etree import ElementTree
from xml.parsers.expat import ExpatError

log = logging.getLogger(__name__)
orb = lazy_import('orb')


class QueryCompound(object):
    """ Defines combinations of queries via either the AND or OR mechanism. """
    Op = enum(
        'And',
        'Or'
    )

    def __contains__(self, value):
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
        for query in self._queries:
            if value in query:
                return True
        return False

    def __nonzero__(self):
        return not self.isNull()

    def __str__(self):
        """
        Returns the string representation for this query instance
        
        :sa         toString
        """
        return self.toString()

    def __init__(self, *queries, **options):
        self._queries = queries
        self._op = options.get('op', QueryCompound.Op.And)
        self._name = nstr(options.get('name', ''))

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

    def __hash__(self):
        return hash(self.toXmlString())

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
        if other is None or other.isNull():
            return self

        elif self.isNull():
            return other

        # grow this objects list if the operator types are the same
        if self.operatorType() == QueryCompound.Op.And:
            queries = list(self._queries)
            queries.append(other)
            opts = {'op': QueryCompound.Op.And}

            return QueryCompound(*queries, **opts)

        # create a new compound
        return QueryCompound(self, other, op=QueryCompound.Op.And)

    def copy(self):
        """
        Returns a copy of this query compound.
        
        :return     <QueryCompound>
        """
        out = QueryCompound()
        out._queries = [q.copy() for q in self._queries]
        out._op = self._op
        return out

    def columns(self, schema=None):
        """
        Returns any columns used within this query.
        
        :return     [<orb.Column>, ..]
        """
        output = []
        for query in self.queries():
            output += query.columns(schema=schema)
        return list(set(output))

    def expandShortcuts(self, basetable=None):
        """
        Expands any shortcuts that were created for this query.  Shortcuts
        provide the user access to joined methods using the '.' accessor to
        access individual columns for referenced tables.
        
        :param      basetable | <orb.Table> || None
        
        :usage      |>>> from orb import Query as Q
                    |>>> # lookup the 'username' of foreign key 'user'
                    |>>> Q('user.username') == 'bob.smith'
        
        :return     <orb.Query> || <orb.QueryCompound>
        """
        output = self.copy()
        queries = []
        rset = None

        for query in output._queries:
            query = query.expandShortcuts(basetable)

            # chain together joins into sub-queries
            if isinstance(query, orb.Query) and \
               isinstance(query.value(), orb.Query) and \
               query.value().table(basetable) != query.table(basetable):
                columns = [query.value().columnName()] if query.value().columnName() else ['id']
                new_rset = query.value().table(basetable).select(columns=columns)
                query = query.copy()
                query.setOperatorType(query.Op.IsIn)
                query.setValue(new_rset)

                if rset is not None and rset.table() == query.table(basetable):
                    rset.setQuery(query & rset.query())
                else:
                    queries.append(query)

                rset = new_rset

            # update the existing recordset in the chain
            elif rset is not None and \
                    ((isinstance(query, orb.Query) and rset.table() == query.table(basetable)) or
                     (isinstance(query, orb.QueryCompound) and rset.table() in query.tables(basetable))):
                rset.setQuery(query & rset.query())

            # clear out the chain and move on to the next query set
            else:
                rset = None
                queries.append(query)

        output._queries = queries
        return output

    def findValue(self, column, instance=1):
        """
        Looks up the value for the inputted column name for the given instance.
        If the instance == 1, then this result will return the value and a
        0 instance count, otherwise it will decrement the instance for a
        matching column to indicate it was found, but not at the desired
        instance.
        
        :param      column   | <str>
                    instance | <int>
        
        :return     (<bool> success, <variant> value, <int> instance)
        """
        for query in self.queries():
            success, value, instance = query.findValue(column, instance)
            if success:
                return success, value, 0
        return False, None, instance

    def isNull(self):
        """
        Returns whether or not this join is empty or not.
        
        :return     <bool>
        """
        am_null = True
        for query in self._queries:
            if not query.isNull():
                am_null = False
                break

        return am_null

    def name(self):
        return self._name

    def negated(self):
        """
        Negates this instance and returns it.
        
        :return     self
        """
        qcompound = QueryCompound(*self._queries)
        qcompound._op = QueryCompound.Op.And if self._op == QueryCompound.Op.Or else QueryCompound.Op.Or
        return qcompound

    def operatorType(self):
        """
        Returns the operator type for this compound.
        
        :return     <QueryCompound.Op>
        """
        return self._op

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
        if other is None or other.isNull():
            return self

        elif self.isNull():
            return other

        # grow this objects list if the operator types are the same
        if self.operatorType() == QueryCompound.Op.Or:
            queries = list(self._queries)
            queries.append(other)
            opts = {'op': QueryCompound.Op.Or}

            return QueryCompound(*queries, **opts)

        return QueryCompound(self, other, op=QueryCompound.Op.Or)

    def queries(self):
        """
        Returns the list of queries that are associated with
        this compound.
        
        :return     <list> [ <Query> || <QueryCompound>, .. ]
        """
        return self._queries

    def removed(self, columnName):
        """
        Removes the query containing the inputted column name from this
        query set.
        
        :param      columnName | <str>
        
        :return     <QueryCompound>
        """
        out = self.copy()
        new_queries = []
        for query in out._queries:
            new_queries.append(query.removed(columnName))

        out._queries = new_queries
        return out

    def setName(self, name):
        self._name = nstr(name)

    def setOperatorType(self, op):
        """
        Sets the operator type that this compound that will be
        used when joining together its queries.
        
        :param      op      <QueryCompound.Op>
        """
        self._op = op

    def tables(self, base=None):
        """
        Returns the tables that this query is referencing.
        
        :return     [ <subclass of Table>, .. ]
        """
        output = []
        for query in self._queries:
            output += query.tables(base=base)

        return list(set(output))

    def toString(self):
        """
        Returns this query instance as a semi readable language
        query.
        
        :warning    This method will NOT return a valid SQL statement.  The
                    backend classes will determine how to convert the Query
                    instance to whatever lookup code they need it to be.
        
        :return     <str>
        """
        optypestr = QueryCompound.Op[self.operatorType()]
        op_type = ' %s ' % projex.text.underscore(optypestr)
        query = '(%s)' % op_type.join([q.toString() for q in self.queries()])
        return query

    def toDict(self):
        """
        Creates a dictionary representation of this query.
        
        :return     <dict>
        """
        output = {}

        if self.isNull():
            return output

        output['type'] = 'compound'
        output['name'] = self.name()
        output['op'] = self.operatorType()

        queries = []
        for query in self.queries():
            queries.append(query.toDict())

        output['queries'] = queries
        return output

    def toXml(self, xparent=None):
        """
        Returns this query as an XML value.
        
        :param      xparent | <xml.etree.ElementTree.Element> || None
        
        :return     <xml.etree.ElementTree.Element>
        """
        if self.isNull():
            return None

        if xparent is None:
            xquery = ElementTree.Element('compound')
        else:
            xquery = ElementTree.SubElement(xparent, 'compound')

        xquery.set('name', nstr(self.name()))
        xquery.set('op', nstr(self.operatorType()))

        for query in self.queries():
            query.toXml(xquery)

        return xquery

    def toXmlString(self, indented=False):
        """
        Returns this query as an XML string.
        
        :param      indented | <bool>
        
        :return     <str>
        """
        xml = self.toXml()
        if indented:
            projex.text.xmlindent(xml)

        return ElementTree.tostring(xml)

    def validate(self, record, table=None):
        """
        Validates the inputted record against this query compound.
        
        :param      record | <orb.Table>
        """
        op = self._op
        queries = self.queries()

        if not queries:
            return False

        for query in queries:
            valid = query.validate(record, table)

            if op == QueryCompound.Op.And and not valid:
                return False
            elif op == QueryCompound.Op.Or and valid:
                return True

        return op == QueryCompound.Op.And

    @staticmethod
    def build(compound, queries):
        """
        Builds a compound based on the inputted compound string.  This should
        look like: ((QUERY_1 and QUERY_2) or (QUERY_3 and QUERY_4)).  The 
        inputted query dictionary should correspond with the keys in the string.
        
        This method will be called as part of the Query.fromString method and 
        probably never really needs to be called otherwise.
        
        :param      compound | <str>
                    queries  | [<Query>, ..]
        
        :return     <Query> || <QueryCompound>
        """
        indexStack = []
        compounds = {}
        new_text = projex.text.decoded(compound)

        for index, char in enumerate(projex.text.decoded(compound)):
            # open a new compound
            if char == '(':
                indexStack.append(index)

            # close a compound
            elif char == ')' and indexStack:
                openIndex = indexStack.pop()
                match = compound[openIndex + 1:index]

                if not match:
                    continue

                # create the new compound
                new_compound = QueryCompound.build(match, queries)

                key = 'QCOMPOUND_%i' % (len(compounds) + 1)
                compounds[key] = new_compound

                new_text = new_text.replace('(' + match + ')', key)

        new_text = new_text.strip('()')
        query = orb.Query()
        last_op = 'and'
        for section in new_text.split():
            section = section.strip('()')

            # merge a compound
            if section in compounds:
                section_q = compounds[section]

            elif section in queries:
                section_q = queries[section]

            elif section in ('and', 'or'):
                last_op = section
                continue

            else:
                log.warning('Missing query section: %s', section)
                continue

            if query is None:
                query = section_q
            elif last_op == 'and':
                query &= section_q
            else:
                query |= section_q

        return query

    @staticmethod
    def fromDict(data):
        if data.get('type') != 'compound':
            return orb.Query.fromDict(data)

        compound = QueryCompound()
        compound.setName(data.get('name', ''))
        compound.setOperatorType(int(data.get('op', '1')))

        queries = []
        for subdata in data.get('queries', []):
            queries.append(orb.Query.fromDict(subdata))

        compound._queries = queries
        return compound

    @staticmethod
    def fromString(querystr):
        """
        Returns a new compound from the inputted query string.  This simply calls
        the Query.fromString method, as the two work the same.
        
        :param      querystr | <str>
        
        :return     <Query> || <QueryCompound> || None
        """
        return orb.Query.fromString(querystr)

    @staticmethod
    def fromXml(xquery):
        if xquery.tag == 'query':
            return orb.Query.fromXml(xquery)

        compound = QueryCompound()
        compound.setName(xquery.get('name', ''))
        compound.setOperatorType(int(xquery.get('op', '1')))

        queries = []
        for xsubquery in xquery:
            queries.append(orb.Query.fromXml(xsubquery))

        compound._queries = queries
        return compound

    @staticmethod
    def fromXmlString(xquery_str):
        """
        Returns a query from the XML string.
        
        :param      xquery_str | <str>
        
        :return     <orb.Query> || <orb.QueryCompound>
        """
        try:
            xml = ElementTree.fromstring(xquery_str)
        except ExpatError:
            return orb.Query()

        return orb.Query.fromXml(xml)

    @staticmethod
    def typecheck(obj):
        """
        Returns whether or not the inputted object is a QueryCompound object.
        
        :param      obj     <variant>
        
        :return     ,bool>
        """
        return isinstance(obj, QueryCompound)

