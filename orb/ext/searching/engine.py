"""
Defines a searching algorithm for searching across multiple tables.
"""

from projex.addon import AddonManager

from .terms import SearchTermGroup
from .thesaurus import SearchThesaurus
from .spelling import SpellingEngine


class SearchEngine(AddonManager):
    def __init__(self, *tables):
        self._tables = list(tables)
        self._parser = None
        self._thesaurus = SearchThesaurus()
        self._spellingEngine = SpellingEngine()
    
    def addTable(self, table):
        """
        Adds a table to the search.
        
        :param      table | <orb.Table>
        """
        if table not in self._tables:
            self._tables.append(table)

    def autocorrect(self, phrase):
        """
        Auto-correct the inputted search phrase through the spelling engine.
        
        :param      phrase | <str>
        """
        spelling = self.spellingEngine()
        words = phrase.split()
        for i in range(len(words)):
            words[i] = spelling.autocorrect(words[i])
        return u' '.join(words)

    def hasTable(self, table):
        """
        Returns whether or not the inputted table is included with this search.
        
        :return     <bool>
        """
        return table in self._tables
    
    def parse(self, text):
        """
        Parses the inputted text into the search engine terms.
        
        :param      text | <unicode>
        
        :return     <orb.SearchTermGroup>
        """
        return SearchTermGroup.fromString(text)

    def parseQuery(self, table, text):
        """
        Takes the given table and generates a Query object for the inputted text.

        :param      table | <orb.Table>
                    text  |  <str>

        :return     <orb.Query>
        """
        return self.parse(text).toQuery(table)

    def setParser(self, parser):
        """
        Sets the parser that will be utilized for this engine.
        
        :param      parser | <orb.SearchParser>
        """
        self._parser = parser

    def removeTable(self, table):
        """
        Removes the inputted table from this engine.
        
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
        Returns the best guess suggestions for the inputted phrase.
        
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
