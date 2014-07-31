#!/usr/bin/python
""" 
Defines a searching algorithm for searching across multiple tables.
"""

# define authorship information
__authors__         = ['Eric Hulser']
__author__          = ','.join(__authors__)
__credits__         = []
__copyright__       = 'Copyright (c) 2011, Projex Software'
__license__         = 'LGPL'

# maintanence information 
__maintainer__      = 'Projex Software'
__email__           = 'team@projexsoftware.com'

import projex.text
import re

from projex.enum import enum
from projex.lazymodule import LazyModule
from projex.text import nativestring as nstr
from xml.etree import ElementTree

orb = LazyModule('orb')


class SearchResult(object):
    """
    Defines a search result for a given term.  This will store the record
    that was found, and the search rank that it received.
    """
    def __cmp__(self, other):
        if type(other) == SearchResult:
            return cmp(self.rank(), other.rank())
        return -1
    
    def __init__(self, record, rank):
        self._record = record
        self._rank = rank
    
    def rank(self):
        """
        Returns the rank that this search result was given for its query.
        """
    
    def record(self):
        """
        Returns the record that was found for this search result.
        
        :return     <orb.Table>
        """
        return self._record

#----------------------------------------------------------------------

class Search(object):
    """
    Defines a search class that will allow a user to search generically across
    multiple tables in a datbase.
    """
    def __init__(self, *tables):
        self._tables = set(tables)
        self._searchText = []
        self._searchResults = {}
    
    def addTable(self, table):
        """
        Adds a table to search against in the database.
        
        :param      table | <orb.Table>
        """
        self._tables.add(table)
    
    def removeTable(self, table):
        """
        Removes a table to search against from the database.
        
        :param      table | <orb.Table>
        """
        try:
            self._tables.remove(table)
        except:
            pass
    
    def searchResults(self, table=None):
        """
        Returns the search results for the given table, or all tables if
        no table is specified.
        
        :param      table | <orb.Table> || None
        """
        if table:
            return self._searchResults.get(table, [])
        else:
            output = []
            for value in self._searchResults.values():
                output += value
            return output
    
    def search(self, text=None, thesaurus=None):
        """
        Searches the database for the tables associated with this search
        instance for the given text term.  If no term is supplied, then
        the cached search term will be used instead.
        
        If the optional thesaurus value is supplied, then as the search is
        processing, the thesaurus will be called for all non-exact search
        terms.
        
        :param      text      | <str> || None
                    thesaurus | <callable> || None
        
        :return     [<SearchResult>, ..]
        """
        if text is None:
            text = self.searchText()
        else:
            self._searchText = nstr(text)
        
        exact_lookups = {}
        column_lookups = {}
        
        # lookup exact matches first
        for group in re.findall('"[^"]*"', text):
            key = 'EXACT_%i' % (len(exact_lookups) + 1)
            exact_lookups[key] = group
            text = text.replace(group, key)
        
        # lookup column matches first
        for group, column, value in re.findall('((\w+):([^\s]+))', text):
            column_lookups.setdefault(column, [])
            column_lookups[column] += value.split(',')
            text = text.replace(group, '')
        
        # stem out the remaining common words
        keywords = projex.text.stemmed(text)
        
        terms = []
        for keyword in keywords:
            # join together the exact terms
            if keyword in exact_lookups:
                terms.append((exact_lookups[word],))
            
            # ignore column specific terms
            if keyword in column_lookups:
                continue
            
            # lookup synonyms
            if thesaurus:
                word = re.search('\w+', keyword).group()
                words = []
                for synonym in thesaurus.synonyms(word):
                    words.append(keyword.replace(word, synonym))
            else:
                words = [keyword]
            
            terms.append(tuple(words))
        
        # search the records for each term
        self._searchResults.clear()
        for table in self.tables():
            q = orb.Query()
            
            # lookup column specific information
            for column_name, value in exact_lookups.items():
                column = table.schema().column(column)
                if not column:
                    continue
                
                q &= self.toQuery(table, column, value.split(','))
            
            # lookup keyword specific information
            for term in terms:
                term_q = orb.Query()
                for column in table.schema().searchableColumns():
                    term_q |= self.toQuery(table, column, term)
                q &= term_q
            
            self._searchResults[table] = table.select(where=q)
        
        return True
    
    def searchText(self):
        """
        Returns the search text that was used to generate this instances
        search results.
        
        :return     <str>
        """
        return self._searchText
    
    def toQuery(self, table, column, terms):
        """
        Converts the inputed list of terms to a searchable query.
        
        :param      table  | <orb.Table>
                    column | <orb.Column>
                    terms  | <str>
        
        :return     <orb.Query>
        """
        out = orb.Query()
        columnName = column.name()
        
        for term in terms:
            negate = False
            
            # determine if we need to negate the value
            if term.startswith('!'):
                negate = True
                term = term[1:]
            
            # use exact
            if term.startswith('"') and term.endswith('"'):
                term_q = orb.Query(table, columnName) == term.strip('"')
            
            # use endswith
            elif term.startswith('*') and not term.endswith('*'):
                term_q = orb.Query(table, columName).endswith(term)
            
            # use startswith
            elif term.endswith('*') and not term.startswith('*'):
                term_q = orb.Query(table, columName).startswith(term)
            
            # use values
            elif not column.isString():
                try:
                    term_q = orb.Query(table, columnName) == eval(term)
                except:
                    term_q = orb.Query(table, columnName) == term
            
            # use container
            else:
                term_q = orb.Query(table, columnName).contains(term)
            
            if negate:
                term_q = term_q.negated()
            
            out |= term_q
        
        return out
    
    def tables(self):
        """
        Returns a list of the tables that are assigned to this search instance.
        
        :return     [<orb.Table>, ..]
        """
        return list(self._tables)


