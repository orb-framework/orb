"""
Defines a searching algorithm for searching across multiple tables.
"""

import re

from xml.etree import ElementTree
from projex.addon import AddonManager
from projex.enum import enum
from projex.text import nativestring as nstr


class SearchThesaurus(AddonManager):
    """
    Defines a global thesaurus system for searching.  This will allow
    additional keyword lookups based on synonyms.  Thesaurus can be
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
        word = nstr(word)
        synonym = nstr(synonym)
        
        # lookup wordsets 
        for wordset in self._wordsets:
            if word in wordset:
                wordset.add(synonym)
        
        # define new wordset
        self._wordsets.append({word, synonym})

    def addset(self, wordset):
        """
        Defines a wordset that will be used as a grouping of synonyms
        within the thesaurus.  A wordset is a comma separated list of
        synonyms.
        
        :param      wordset | <str> || <list> || <set>
        """
        if type(wordset) in (list, tuple, set):
            wordset = set([nstr(word) for word in wordset])
        else:
            wordset = set(nstr(wordset).split(','))

        self._wordsets.append(wordset)

    def addPhrase(self, pattern):
        """
        Phrases define groups of words that will create a singular
        search term within a search pattern.  For instance, "is not" will be 
        treated as single term, so instead of looking for "is" and "not", the
        phrase "is not" will be matched.  The inputted pattern can be a regular
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
        Loads the thesaurus information from the inputted XML file.
        
        :param      filename | <str>
        """
        try:
            xroot = ElementTree.parse(xml).getroot()
        except StandardError:
            try:
                xroot = ElementTree.fromstring(xml)
            except StandardError:
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
        Removes a given synonym from the inputted word in a wordset.
        
        :param      word    | <str>
                    synonym | <str>
        """
        word = nstr(word)
        synonym = nstr(synonym)
        
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
        Splits the inputted search text into search terms.  This will use the
        phrasing patterns within this thesaurus to determine groups of words.
        
        :param      text | <str>
        
        :return     [<str>, ..]
        """
        text = nstr(text)
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
        word = nstr(word)
        output = {word}
        
        # find matching words
        for wordset in self._wordsets:
            if output.intersection(wordset):
                output = output.union(wordset)
        
        # lookup inherited synonyms
        if self.parent() and flags & SearchThesaurus.Flags.FindInherited:
            output = output.union(self.parent().synonyms(word, flags=flags))
        
        return output
    
    def update(self, wordsets):
        """
        Updates the records for this thesaurus' wordsets with the inputted
        list of sets.
        
        :param      wordsets | [<str>, ..]
        """
        for wordset in wordsets:
            self.addset(wordset)

    def updatePhrases(self, phrases):
        """
        Updates the phrase sets for this thesaurus with the inputted list
        of phrases.
        
        :param      phrases | [<str>, ..]
        """
        for phrase in phrases:
            self.addPhrase(phrase)

