"""
Defines the base SQLStatement class used for rendering SQLStatement statements
out.
"""

import logging

from abc import abstractmethod
from projex.addon import AddonManager

log = logging.getLogger(__name__)


class SQLStatement(AddonManager):
    @abstractmethod
    def __call__(self):
        """
        Executes this statement with the inputted keywords to generate
        the context SQLStatement statement.

        :return     <str> SQLStatement statement
        """
        # this method will need to be implemented to render each individual template
        # based on its own needs

# define the default lengths
SQLStatement.registerAddon('Length::Color', 25)
SQLStatement.registerAddon('Length::String', 256)
SQLStatement.registerAddon('Length::Email', 256)
SQLStatement.registerAddon('Length::Password', 256)
SQLStatement.registerAddon('Length::Url', 500)
SQLStatement.registerAddon('Length::Filepath', 500)
SQLStatement.registerAddon('Length::Directory', 500)

# define the base flags
SQLStatement.registerAddon('Flag::Unique', u'UNIQUE')
SQLStatement.registerAddon('Flag::Required', u'NOT NULL')

# define the base operators
SQLStatement.registerAddon('Op::Is', u'=')
SQLStatement.registerAddon('Op::IsNot', u'!=')
SQLStatement.registerAddon('Op::LessThan', u'<')
SQLStatement.registerAddon('Op::Before', u'<')
SQLStatement.registerAddon('Op::LessThanOrEqual', u'<=')
SQLStatement.registerAddon('Op::GreaterThanOrEqual', u'>=')
SQLStatement.registerAddon('Op::GreaterThan', u'>')
SQLStatement.registerAddon('Op::After', u'>')
SQLStatement.registerAddon('Op::Matches', u'~*')
SQLStatement.registerAddon('Op::Matches::CaseSensitive', u'~')
SQLStatement.registerAddon('Op::DoesNotMatch', u'!~*')
SQLStatement.registerAddon('Op::DoesNotMatch::CaseSensitive', u'!~*')
SQLStatement.registerAddon('Op::Contains', u'ILIKE')
SQLStatement.registerAddon('Op::Contains::CaseSensitive', u'LIKE')
SQLStatement.registerAddon('Op::Startswith', u'ILIKE')
SQLStatement.registerAddon('Op::Startswith::CaseSensitive', u'LIKE')
SQLStatement.registerAddon('Op::Endswith', u'ILIKE')
SQLStatement.registerAddon('Op::Endswith::CaseSensitive', u'LIKE')
SQLStatement.registerAddon('Op::DoesNotContain', u'NOT ILIKE')
SQLStatement.registerAddon('Op::DoesNotContain::CaseSensitive', u'NOT LIKE')
SQLStatement.registerAddon('Op::DoesNotStartwith', u'NOT ILIKE')
SQLStatement.registerAddon('Op::DoesNotStartwith::CaseSensitive', u'NOT LIKE')
SQLStatement.registerAddon('Op::DoesNotEndwith', u'NOT ILIKE')
SQLStatement.registerAddon('Op::DoesNotEndwith::CaseSensitive', u'NOT LIKE')
SQLStatement.registerAddon('Op::IsIn', u'IN')
SQLStatement.registerAddon('Op::IsNotIn', u'NOT IN')

# define the base functions
SQLStatement.registerAddon('Func::Lower', u'lower({0})')
SQLStatement.registerAddon('Func::Upper', u'upper({0})')
SQLStatement.registerAddon('Func::Abs', u'abs({0})')
SQLStatement.registerAddon('Func::AsString', u'{0}::varchar')

# define the base math operators
SQLStatement.registerAddon('Math::Add', u'+')
SQLStatement.registerAddon('Math::Subtract', u'-')
SQLStatement.registerAddon('Math::Multiply', u'*')
SQLStatement.registerAddon('Math::Divide', u'/')
SQLStatement.registerAddon('Math::And', u'&')
SQLStatement.registerAddon('Math::Or', u'|')

SQLStatement.registerAddon('Math::Add::String', u'||')
SQLStatement.registerAddon('Math::Add::Text', u'||')