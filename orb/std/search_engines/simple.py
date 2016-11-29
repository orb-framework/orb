import orb
import re
import pyparsing

from collections import defaultdict
from orb.core.search import SearchEngine


class Node(list):
    def __eq__(self, other):
        return list.__eq__(self, other) and self.__class__ == other.__class__

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, list.__repr__(self))

    @classmethod
    def group(cls, expr):
        def group_action(s, l, t):
            try:
                lst = t[0].asList()
            except (IndexError, AttributeError) as e:
                lst = t
            return [cls(lst)]

        return pyparsing.Group(expr).setParseAction(group_action)


class TextNode(Node):
    def pattern(self, thesaurus, locale):
        return thesaurus.synonyms(self[0].replace('*', '.*'), locale=locale)


class ExactNode(Node):
    def pattern(self, thesaurus, locale):
        return re.escape(self[0])


class ComparisonNode(Node): pass

# --------------------

# define the simple parser
# define printable options
UNICODE_PRINTABLES = u''.join(unichr(c) for c in xrange(65536) if not unichr(c).isspace())

# lookup simple phrases words
TEXT_OP = TextNode.group(pyparsing.Word(UNICODE_PRINTABLES))
TEXT_OP.setResultsName('word')

# lookup exact matches
EXACT_OP = ExactNode.group(pyparsing.QuotedString('"', unquoteResults=True, escChar='\\'))
EXACT_OP.setResultsName('exact')

TERM_OP = EXACT_OP | TEXT_OP

# lookup comparisons (column values)
COMPARISON_NAME = pyparsing.Word(UNICODE_PRINTABLES, excludeChars=':')
COMPARISON_OP = ComparisonNode.group(COMPARISON_NAME + pyparsing.Literal(':') + TERM_OP)

# create the search operator
SIMPLE_PARSER = pyparsing.OneOrMore(COMPARISON_OP | TERM_OP)

# --------------------

SIMPLE_SYNONYMS = {
    'en_US': [
        ('cannot' , "can't"),
        ('is not', "isn't"),
        ('has not', "hasn't"),
        ('will not', "won't")
    ]
}


class SimpleSearchThesaurus(object):
    def __init__(self, synonyms=None):
        synonyms = synonyms or SIMPLE_SYNONYMS

        self.__synonyms = defaultdict(dict)
        for locale, pairings in synonyms.items():
            for pairing in pairings:
                expr = u'({0})'.format('|'.join(pairing))
                for word in pairing:
                    self.__synonyms[locale][word] = expr

    def synonyms(self, word, locale='en_US'):
        return self.__synonyms[locale].get(word, word)

SIMPLE_THESAURUS = SimpleSearchThesaurus()


class SimpleSearchEngine(SearchEngine):
    __factory__ = 'simple'

    def __init__(self, parser=None, thesaurus=None):
        super(SimpleSearchEngine, self).__init__()

        self.__parser = parser or SIMPLE_PARSER
        self.__thesaurus = thesaurus or SIMPLE_THESAURUS

    def search(self, model, terms, **context):
        search_context = context.get('context') or orb.Context(**context)
        locale = search_context.locale
        nodes = self.__parser.parseString(terms)

        # separate into 2 categories general (searchable by any column) and specific (user gave a column)
        general_nodes = [node for node in nodes if not isinstance(node, ComparisonNode)]
        comparison_nodes = [node for node in nodes if isinstance(node, ComparisonNode)]

        # build general search column matches
        q = orb.Query()
        if general_nodes:
            expr = u'.*\s{0}'
            pattern = u'(^|.*\s){0}'.format(general_nodes[0].pattern(self.__thesaurus, locale))
            pattern += ''.join(expr.format(node.pattern(self.__thesaurus, locale)) for node in general_nodes[1:])

            general_q = orb.Query()
            searchable_columns = model.schema().columns(flags=orb.Column.Flags.Searchable).values()

            # if there are no searchable columns, then there will be no search results
            # so just return an empty collection
            if not searchable_columns:
                log.warning('{0} has no searchable columns'.format(model.schema().name()))
                return orb.Collection()

            for column in searchable_columns:
                general_q |= orb.Query(column).as_string().matches(pattern, case_sensitive=False)
            q &= general_q

        # build comparison nodes
        if comparison_nodes:
            schema = model.schema()
            for node in comparison_nodes:
                column = schema.column(node[0])
                value_node = node[-1]
                value = value_node[0]
                q &= orb.Query(column) == value

        if not q.is_null():
            context['where'] = q & context.get('where')

        return model.select(**context)

