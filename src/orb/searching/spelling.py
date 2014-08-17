import os, re, collections

from projex.lazymodule import lazy_import

orb = lazy_import('orb')

class SpellingEngine(object):
    Alphabet = {
        'default': u'abcdefghijklmnopqrstuvwxyz',
        'en_US': u'abcdefghijklmnopqrstuvwxyz'
    }
    
    def __init__(self):
        self._rankings = {}

    def alphabet(self, language=None):
        """
        Returns the alphabet for this spelling system.  This will be a
        language based set based on the language for this suggestor.
        
        :param      language | <str> || None
        """
        if language is None:
            language = orb.system.language()
        
        default = SpellingEngine.Alphabet['default']
        return SpellingEngine.Alphabet.get(language, default)

    def autocorrect(self, word, language=None):
        """
        Returns the best guess for the inputed word for the given language.
        
        :param      word     | <str> || <unicode>
                    language | <str> || None
        
        :return     <unicode> word
        """
        choices = self.knownWords([word], language)
        if not choices:
            edits = self.knownEdits(word, language)
            choices = self.knownWords(edits, language) or edits or [word]
        
        rankings = self.rankings(language)
        return max(choices, key=lambda x: x.startswith(word) * 10**10 + rankings.get(x))

    def commonEdits(self, word, language=None):
        """
        Returns the most common edits for the inputed word for a given
        language.
        
        :param      word | <str> || <unicode>
                    language | <str> || <unicode> || None
        
        :return     {<unicode> word, ..}
        """
        alphabet = self.alphabet(language)
        s = [(word[:i], word[i:]) for i in range(len(word) + 1)]
        deletes    = [a + b[1:] for a, b in s if b]
        transposes = [a + b[1] + b[0] + b[2:] for a, b in s if len(b)>1]
        replaces   = [a + c + b[1:] for a, b in s for c in alphabet if b]
        inserts    = [a + c + b for a, b in s for c in alphabet]
        return set(deletes + transposes + replaces + inserts)

    def createRankings(self, source, language=None):
        """
        Sets the source text information for this language.
        
        :param      words       | <str> || <unicode>
                    language    | <str> || None
        """
        if language is None:
            language = orb.system.language()
        
        # collect all the words from the source
        words = re.findall('[a-z]+', source.lower())
        
        # define the counts per word for common tests
        rankings = collections.defaultdict(lambda: 1)
        for word in words:
            rankings[word] += 1
        
        return rankings

    def knownEdits(self, word, language=None):
        """
        Returns the known words for the most common edits for the inputed word.
        
        :param      word | <str> || <unicode>
                    language | <str> || None
        
        :return     {<unicode> word, ..}
        """
        rankings = self.rankings(language)
        return set(e2 for e1 in self.commonEdits(word, language) \
                   for e2 in self.commonEdits(e1) if e2 in rankings)

    def knownWords(self, words, language=None):
        """
        Returns a set of the known words based on the inputed list of 
        words compared against the language's data set.
        
        :param      words       | [<str> || <unicode>, ..]
                    language    | <str> || None
        
        :return     {<unicode> word, ..}
        """
        rankings = self.rankings(language)
        return set(w for w in words if w in rankings)

    def ranking(self, word, language=None):
        """
        Returns the ranking for the inputed word within the spelling engine.
        
        :param      word | <str>
        
        :return     <int>
        """
        rankings = self.rankings(language)
        return rankings.get(word, 0)

    def rankings(self, language=None):
        """
        Loads the source text for this spelling suggestor based on the
        inputed language source.
        
        :param      language | <str> || None
        
        :return     {<unicode> word: <int> ranking, ..}
        """
        if language is None:
            language = orb.system.language()
        
        try:
            return self._rankings[language]
        except KeyError:
            filepath = os.path.join(os.path.dirname(__file__),
                                    'words',
                                    '{0}.txt'.format(language))
            try:
                source = file(filepath).read()
            except OSError:
                return {}
            
            # set the words for this language
            rankings = self.createRankings(source, language)
            self._rankings[language] = rankings
            return rankings

    def suggestions(self, word, language=None, limit=10):
        """
        Returns a list of the best guesses for the inputed word for 
        the given language, along with their ranking.
        
        :param      word     | <str> || <unicode>
                    language | <str> || None
        
        :return     [<unicode> word, ..]
        """
        edits = list(self.knownEdits(word, language))
        choices = self.knownWords([word] + edits, language) or edits or [word]
        choices = list(choices)
        
        rankings = self.rankings(language)
        choices.sort(key=lambda x: x.startswith(word) * 10**10 + rankings.get(x))
        choices.reverse()
        return choices[:limit]

