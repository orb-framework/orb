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

from projex.addon import AddonManager

from .terms import SearchTermGroup
from .thesaurus import SearchThesaurus
from .spelling import SpellingEngine

class SearchEngine(AddonManager):
    def __init__(self, *tables):
        self._tables = tables
        self._thesaurus = SearchThesaurus()
        self._spellingEngine = SpellingEngine()
    
    def addTable(self, table):
        """
        Adds a table to the search.
        
        :param      table | <orb.Table>
        """
        if not table in self._tables:
            self._tables.append(table)

    def autocorrect(self, phrase):
        """
        Autocorrects the inputed search phrase through the spelling engine.
        
        :param      phrase | <str>
        """
        spelling = self.spellingEngine()
        words = phrase.split()
        for i in range(len(words)):
            words[i] = spelling.autocorrect(words[i])
        return u' '.join(words)

    def hasTable(self, table):
        """
        Returns whether or not the inputed table is included with this search.
        
        :return     <bool>
        """
        return table in self._tables
    
    def parse(self, text):
        """
        Parses the inputed text into the search engine terms.
        
        :param      text | <unicode>
        
        :return     <orb.SearchParseResult>
        """
        return SearchTermGroup.fromString(text)
    
    def setParser(self, parser):
        """
        Sets the parser that will be utilized for this engine.
        
        :param      parser | <orb.SearchParser>
        """
        self._parser = parser

    def removeTable(self, table):
        """
        Removes the inputed table from this engine.
        
        :param      table | <orb.Table>
        """
        try:
            self._tables.remove(table)
        except ValueError:
            pass

    def spellingEngine(self):
        """
        Returns the spelling suggestion engine for this search engine.
        
        :return     <orb.SpellingEngine>
        """
        return self._spellingEngine

    def suggestions(self, phrase, locale=None, limit=10):
        """
        Returns the best guess suggestions for the inputed phrase.
        
        :param      phrase | <str> || <unicode>
        
        :return     [(<unicode> phrase, <int> ranking), ..]
        """
        spelling = self.spellingEngine()
        known = []
        words = phrase.split()
        for word in words[:-1]:
            known.append(spelling.autocorrect(word, locale))
        
        output = []
        for suggestion in spelling.suggestions(words[-1], locale, limit):
            output.append(u' '.join(known + [suggestion]))
        return output

    def setThesaurus(self, thesaurus):
        """
        Sets the search thesaurus that is associated with this engine.
        
        :param      thesaurus | <orb.SearchThesaurus>
        """
        self._thesaurus = thesaurus

    def thesaurus(self):
        """
        Returns the search thesaurus that is associated with this engine.
        
        :return     <orb.SearchThesaurus>
        """
        return self._thesaurus
