"""
Defines the Access Control system for ORB.
"""

class AuthorizationPolicy(object):
    def can_read_column(self, column, context=None):
        """
        Returns whether or not a column can be read for a given context.  By default
        Private columns will be ignored.

        :param column: <orb.Column>
        :param context: <orb.Context> or None
        """
        return not column.test_flag(column.Flags.Private)