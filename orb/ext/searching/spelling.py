import collections
import os
import re

from projex.lazymodule import lazy_import

orb = lazy_import('orb')


class SpellingEngine(object):
    Alphabet = {
        'default': u'abcdefghijklmnopqrstuvwxyz',
        'en_US': u'abcdefghijklmnopqrstuvwxyz'
    }

    def __init__(self):
        self._rankings = {}

    def alphabet(self, locale=None):
        """
        Returns the alphabet for this spelling system.  This will be a
        language based set based on the language for this suggester.
        
        :param      locale | <str> || None
        """
        if locale is None:
            locale = orb.system.locale()

        default = SpellingEngine.Alphabet['default']
        return SpellingEngine.Alphabet.get(locale, default)

    def autocorrect(self, word, locale=None):
        """
        Returns the best guess for the inputted word for the given locale.
        
        :param      word     | <str> || <unicode>
                    locale   | <str> || None
        
        :return     <unicode> word
        """
        choices = self.knownWords([word], locale)
        if not choices:
            edits = self.knownEdits(word, locale)
            choices = self.knownWords(edits, locale) or edits or [word]

        rankings = self.rankings(locale)
        return max(choices, key=lambda x: x.startswith(word) * 10 ** 10 + rankings.get(x))

    def commonEdits(self, word, locale=None):
        """
        Returns the most common edits for the inputted word for a given
        locale.
        
        :param      word   | <str> || <unicode>
                    locale | <str> || <unicode> || None
        
        :return     {<unicode> word, ..}
        """
        alphabet = self.alphabet(locale)
        s = [(word[:i], word[i:]) for i in range(len(word) + 1)]
        deletes = [a + b[1:] for a, b in s if b]
        transposes = [a + b[1] + b[0] + b[2:] for a, b in s if len(b) > 1]
        replaces = [a + c + b[1:] for a, b in s for c in alphabet if b]
        inserts = [a + c + b for a, b in s for c in alphabet]
        return set(deletes + transposes + replaces + inserts)

    def createRankings(self, source, locale=None):
        """
        Sets the source text information for this locale.
        
        :param      words       | <str> || <unicode>
                    locale      | <str> || None
        """
        # collect all the words from the source
        words = re.findall('[a-z]+', source.lower())

        # define the counts per word for common tests
        rankings = collections.defaultdict(lambda: 1)
        for word in words:
            rankings[word] += 1

        return rankings

    def knownEdits(self, word, locale=None):
        """
        Returns the known words for the most common edits for the inputted word.
        
        :param      word   | <str> || <unicode>
                    locale | <str> || None
        
        :return     {<unicode> word, ..}
        """
        rankings = self.rankings(locale)
        return set(e2 for e1 in self.commonEdits(word, locale)
                   for e2 in self.commonEdits(e1) if e2 in rankings)

    def knownWords(self, words, locale=None):
        """
        Returns a set of the known words based on the inputted list of
        words compared against the locale's data set.
        
        :param      words       | [<str> || <unicode>, ..]
                    locale      | <str> || None
        
        :return     {<unicode> word, ..}
        """
        rankings = self.rankings(locale)
        return set(w for w in words if w in rankings)

    def ranking(self, word, locale=None):
        """
        Returns the ranking for the inputted word within the spelling engine.
        
        :param      word | <str>
        
        :return     <int>
        """
        rankings = self.rankings(locale)
        return rankings.get(word, 0)

    def rankings(self, locale=None):
        """
        Loads the source text for this spelling suggester based on the
        inputted locale source.
        
        :param      locale | <str> || None
        
        :return     {<unicode> word: <int> ranking, ..}
        """
        if locale is None:
            locale = orb.system.locale()

        try:
            return self._rankings[locale]
        except KeyError:
            filepath = os.path.join(os.path.dirname(__file__),
                                    'words',
                                    '{0}.txt'.format(locale))
            try:
                source = file(filepath).read()
            except OSError:
                return {}

            # set the words for this locale
            rankings = self.createRankings(source, locale)
            self._rankings[locale] = rankings
            return rankings

    def suggestions(self, word, locale=None, limit=10):
        """
        Returns a list of the best guesses for the inputted word for
        the given locale, along with their ranking.
        
        :param      word     | <str> || <unicode>
                    locale   | <str> || None
        
        :return     [<unicode> word, ..]
        """
        edits = list(self.knownEdits(word, locale))
        choices = self.knownWords([word] + edits, locale) or edits or [word]
        choices = list(choices)

        rankings = self.rankings(locale)
        choices.sort(key=lambda x: x.startswith(word) * 10 ** 10 + rankings.get(x))
        choices.reverse()
        return choices[:limit]

