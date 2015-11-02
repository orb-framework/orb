"""
Defines a searching algorithm for searching across multiple tables.
"""
from projex.lazymodule import lazy_import

orb = lazy_import('orb')
pyparsing = lazy_import('pyparsing')


class SearchJoiner(object):
    def __init__(self, text):
        self._text = text

    def __str__(self):
        return self.toString()

    def toString(self):
        return self._text

# ---------------------------------------------------------------------


class SearchTerm(object):
    def __init__(self, engine, text, column=''):
        self._engine = engine
        self._text = text
        self._column = column

    def __str__(self):
        return self.toString()

    def append(self, word):
        if self._text:
            self._text += u' ' + word
        else:
            self._text = word

    def column(self):
        return self._column

    def isExact(self):
        """
        Returns whether or not this is an exact term to match.
        
        :return     <bool>
        """
        check = self._text.lstrip('-')
        return (check.startswith('"') and check.endswith('"')) or \
               (check.startswith("'") and check.endswith("'"))

    def isNegated(self):
        """
        Returns whether or not this search term is negated.
        
        :return     <bool>
        """
        return self._text.startswith('-')

    def setColumn(self, column):
        self._column = column

    def text(self):
        """
        Returns the text for this term.
        
        :return     <unicode>
        """
        return self._text.lstrip('-').strip('"').strip("'")

    def toQuery(self, table, column=''):
        column = column or self.column()
        col = table.schema().column(column)
        
        # if the column is not specified then return an or join
        # for all searchable columns
        if not col:
            search_columns = table.schema().searchableColumns()
            out = orb.Query()
            for search_column in search_columns:
                out |= self.toQuery(table, search_column)
            return out
        
        # otherwise, search the provided column
        else:
            # use a non-column based ':' search term
            if self.column() and col.name() != self.column():
                text = '{0}:'.format(self.column()) + self._text
            else:
                text = self._text

            negated = text.startswith('-')
            text = text.lstrip('-')
            exact = (text.startswith('"') and text.endswith("'")) or \
                    (text.startswith("'") and text.endswith('"'))
            
            text = text.strip('"').strip("'")
            text = text.replace(u'\\', u'\\\\')
            
            if self.column():
                expr = u'^{0}$'.format(text).replace('*', '\w+')
            elif exact:
                expr = text.replace('*', '\w+')
            else:
                parts = []
                for word in text.split():
                    words = self._engine.thesaurus().synonyms(word)
                    if len(words) > 1:
                        item = u'({0})'.format(u'|'.join(words))
                    else:
                        if '*' in word:
                            word = u'(^|\W){0}(\W|$)'.format(word.replace('*', '.*'))
                        item = word
                    parts.append(item)
                    
                expr = u'.*'.join(parts)
            
            if negated:
                return orb.Query(table, column).asString().doesNotMatch(expr, caseSensitive=False)
            else:
                return orb.Query(table, column).asString().matches(expr, caseSensitive=False)

    def toString(self):
        if self.column():
            return u'{0}:{1}'.format(self.column(), self._text)
        else:
            return self._text

#----------------------------------------------------------------------


class SearchTermGroup(object):
    def __init__(self, engine, words, column='', root=False):
        self._engine = engine
        self._terms = []
        self._column = column
        self._root = root
        
        last_column = ''
        for word in words:
            curr_column = last_column
            
            # a group of terms
            if type(word) == list:
                self._terms.append(SearchTermGroup(engine,
                                                   word,
                                                   column=curr_column)) 
            
            # lookup a specific column
            elif not (word.startswith('"') or word.startswith("'")) and word.count(':') == 1:
                if word.endswith(':'):
                    last_column = word.strip(':')
                else:
                    curr_column, text = word.split(':')
                    self._terms.append(SearchTerm(engine,
                                                  text,
                                                  column=curr_column))
            
            # load a joiner
            elif word in ('AND', 'OR'):
                self._terms.append(SearchJoiner(word))
            
            # load a term
            else:
                # update terms for the same column
                if self._terms and \
                   type(self._terms[-1]) == SearchTerm and \
                   self._terms[-1].column() == curr_column:
                    self._terms[-1].append(word)
                
                # otherwise, create a new term
                else:
                    self._terms.append(SearchTerm(engine,
                                                  word,
                                                  column=curr_column))
            
            # reset the last key
            if last_column == curr_column and curr_column:
                last_column = ''

    def __str__(self):
        return self.toString()

    def column(self):
        return self._column

    def setColumn(self, column):
        self._column = column

    def terms(self):
        """
        Returns the search terms for this group.
        
        :return     [<SearchTerm> || <SearchTermGroup>, ..]
        """
        return self._terms

    def toQuery(self, table, column=''):
        """
        Creates a query for the inputted table based on this search term
        information.
        
        :param      table | <orb.Table>
        
        :return     <orb.Query> || <orb.QueryCompound>
        """
        column = self.column() or column
        search_columns = table.schema().searchableColumns()
        
        out = orb.Query()
        op = orb.QueryCompound.Op.And
        for term in self.terms():
            # update the joining operator
            if type(term) == SearchJoiner:
                if str(term) == 'AND':
                    op = orb.QueryCompound.Op.And
                else:
                    op = orb.QueryCompound.Op.Or
                continue
            
            # generate the search term
            subq = None
            term_column = term.column() or column
            if term_column:
                subq = term.toQuery(table, term_column)
            
            # generate the search columns
            elif search_columns:
                subq = orb.Query()
                for search_column in search_columns:
                    subq |= term.toQuery(table, search_column)
            
            if op == orb.QueryCompound.Op.And:
                out &= subq
            else:
                out |= subq
            
        return out

    def toString(self):
        words = []
        for term in self._terms:
            words.append(str(term))
        
        if self.column():
            return u'{0}:({1})'.format(self.column(), u' '.join(words))
        elif not self._root:
            return u'({0})'.format(u' '.join(words))
        else:
            return u' '.join(words)

    @staticmethod
    def fromString(text, engine=None):
        """
        Parses the inputted text using the common searching syntax.  By default,
        the words will be separated and processed to their root, joined
        together as an AND join.  Override behaviors exist as well:
        
        :syntax     terms                   | general parsing
                    "<terms>"               | Exact match for terms
                    (<terms>) AND (<terms>) | AND join for two terms
                    (<terms>) OR (<terms>)  | OR join for two terms
                    -<terms>                | NOT search for terms
                    <terms>*                | startswith
                    *<terms>                | endswith
                    <column>:<terms>        | column IS specific term
                    <column>:-<terms>       | column IS NOT specific term
        
        :param      text | <unicode>
        
        :return     <SearchParseResult>
        """
        if engine is None:
            engine = orb.system.searchEngine()
        
        expr = pyparsing.nestedExpr('(', ')')
        words = expr.parseString(u'({0})'.format(text)).asList()
        return SearchTermGroup(engine, words[0], root=True)


