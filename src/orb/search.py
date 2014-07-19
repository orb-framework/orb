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

import re
from xml.etree import ElementTree

from projex.text import nativestring
from projex.enum import enum
import projex.text

from orb.common import ColumnType
from orb.query import Query as Q

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
            self._searchText = nativestring(text)
        
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
            q = Q()
            
            # lookup column specific information
            for column_name, value in exact_lookups.items():
                column = table.schema().column(column)
                if not column:
                    continue
                
                q &= self.toQuery(table, column, value.split(','))
            
            # lookup keyword specific information
            for term in terms:
                term_q = Q()
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
        out = Q()
        columnName = column.name()
        
        for term in terms:
            negate = False
            
            # determine if we need to negate the value
            if term.startswith('!'):
                negate = True
                term = term[1:]
            
            # use exact
            if term.startswith('"') and term.endswith('"'):
                term_q = Q(table, columnName) == term.strip('"')
            
            # use endswith
            elif term.startswith('*') and not term.endswith('*'):
                term_q = Q(table, columName).endswith(term)
            
            # use startswith
            elif term.endswith('*') and not term.startswith('*'):
                term_q = Q(table, columName).startswith(term)
            
            # use values
            elif not column.isString():
                try:
                    term_q = Q(table, columnName) == eval(term)
                except:
                    term_q = Q(table, columnName) == term
            
            # use container
            else:
                term_q = Q(table, columnName).contains(term)
            
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

#----------------------------------------------------------------------

class SearchThesaurus(object):
    """
    Defines a global thesaurus system for searching.  This will allow
    additional keyword lookups based on synonyms.  Thesuarus' can be
    define on a per API and per Table system.
    """
    Flags = enum('FindSingular', 'FindPlural', 'FindInherited')
    
    def __init__(self, wordsets=None, parent=None):
        self._wordsets = []
        self._phrases = set()
        self._parent = parent
        
        if wordsets is not None:
            self.update(wordsets)

    def add(self, word, synonym):
        """
        Adds a new synonym for a given word.
        
        :param      word    | <str>
                    synonym | <str>
        """
        word = nativestring(word)
        synonym = nativestring(synonym)
        
        # lookup wordsets 
        for wordset in self._wordsets:
            if word in wordset:
                wordset.add(synonym)
        
        # define new wordset
        self._wordsets.append(set(word, synonym))

    def addset(self, wordset):
        """
        Defines a wordset that will be used as a grouping of synonyms
        within the thesaurus.  A wordset is a comma separated list of
        synonyms.
        
        :param      wordset | <str> || <list> || <set>
        """
        if type(wordset) in (list, tuple, set):
            wordset = set(map(str, wordset))
        else:
            wordset = set(nativestring(wordset).split(','))

        self._wordsets.append(wordset)

    def addPhrase(self, pattern):
        """
        Phrases define groups of words that will create a singular
        search term within a search pattern.  For instance, "is not" will be 
        treated as single term, so instead of looking for "is" and "not", the
        phrase "is not" will be matched.  The inputed pattern can be a regular
        expression, or hard set of terms.
        
        :param      pattern | <str>
        """
        self._phrases.add(pattern)

    def clear(self):
        """
        Clears out the data for this thesaurus.
        """
        self._phrases = set()
        self._wordsets = []

    def expand(self, wordset, flags=Flags.all()):
        """
        Expands a given wordset based on the synonyms per word in the set.
        
        :param      wordset | <list> || <tuple> || <set>
        """
        output = set()
        for word in wordset:
            output = output.union(self.synonyms(word, flags))
        return output

    def parent(self):
        """
        Returns the parent thesaurus for this instance if any is defined.
        
        :return     <orb.SearchThesaurus>
        """
        return self._parent

    def phrases(self):
        """
        Returns a list of phrases for the searching pattern.
        
        :return     [<str>, ..]
        """
        out = set(self._phrases)
        if self.parent():
            out = out.union(self.parent().phrases())
        return out

    def load(self, xml):
        """
        Loads the thesaurus information from the inputed XML file.
        
        :param      filename | <str>
        """
        try:
            xroot = ElementTree.parse(xml).getroot()
        except:
            try:
                xroot = ElementTree.fromstring(xml)
            except:
                return False

        # load wordsets
        xwordsets = xroot.find('wordsets')
        if xwordsets is not None:
            for xwordset in xwordsets:
                self.addset(xwordset.text)
        
        # load patterns
        xphrases = xroot.find('phrases')
        if xphrases is not None:
            for xphrase in xphrases:
                self.addPhrase(xphrase.text)

        return True

    def remove(self, word, synonym):
        """
        Removes a given synonym from the inputed word in a wordset.
        
        :param      word    | <str>
                    synonym | <str>
        """
        word = nativestring(word)
        synonym = nativestring(synonym)
        
        for wordset in self._wordsets:
            if word in wordset:
                try:
                    wordset.remove(synonym)
                except KeyError:
                    pass
                return

    def removePhrase(self, pattern):
        """
        Removes the given phrasing pattern for this search term.
        
        :param      pattern | <str>
        """
        try:
            self._phrases.remove(pattern)
        except KeyError:
            pass

    def setParent(self, parent):
        """
        Sets the parent thesaurus for this instance if any is defined.
        
        :param      parent | <orb.SearchThesaurus> || None
        """
        self._parent = parent

    def splitterms(self, text):
        """
        Splits the inputed search text into search terms.  This will use the
        phrasing patterns within this thesaurus to determine groups of words.
        
        :param      text | <str>
        
        :return     [<str>, ..]
        """
        text = nativestring(text)
        repl = []
        
        # pre-process all phrases into their own groups
        for phrase in self.phrases():
            grp = re.search(phrase, text)
            while grp and grp.group():
                result = grp.group()
                text = text.replace(result, '`REGEXGRP{0}`'.format(len(repl)))
                repl.append(result)
                grp = re.search(phrase, text)
        
        # split the terms into their own words, grouped together with phrases
        output = []
        for term in text.split():
            term = term.strip()
            grp = re.match('`REGEXGRP(\d+)`(.*)', term)
            if grp:
                index, remain = grp.groups()
                term = repl[int(index)] + remain
            
            output.append(term)
        
        return output

    def synonyms(self, word, flags=Flags.all()):
        """
        Looks up the synonyms for the given word within this thesaurus
        system.
        
        :param      word | <str>
        
        :return     set(<str>, ..)
        """
        word = nativestring(word)
        output = {word}
        
        # find matching words
        for wordset in self._wordsets:
            if output.intersection(wordset):
                output = output.union(wordset)
        
        # include any merged values for each word set
        for w in list(output):
            # singularize words if desired
            if flags & SearchThesaurus.Flags.FindSingular:
                output.add(projex.text.singularize(w))
            
            # pluralize words if possible
            if flags & SearchThesaurus.Flags.FindPlural:
                output.add(projex.text.pluralize(w))
        
        # lookup inherited synonyms
        if self.parent() and flags & SearchThesaurus.Flags.FindInherited:
            output = output.union(self.parent().synonyms(word, flags=flags))
        
        return output
    
    def update(self, wordsets):
        """
        Updates the records for this thesaurus' wordsets with the inputed
        list of sets.
        
        :param      wordsets | [<str>, ..]
        """
        for wordset in wordsets:
            self.addset(wordset)

    def updatePhrases(self, phrases):
        """
        Updates the phrase sets for this thesaurus with the inputed list
        of phrases.
        
        :param      phrases | [<str>, ..]
        """
        for phrase in phrases:
            self.addPhrase(phrase)

