import re

from orb import errors

class AbstractColumnValidator(object):
    def validate(self, column, value):
        """
        Validates the inputed value for this instance.

        :param      column | <orb.Column>
                    value | <variant>

        :return     <bool> | is valid
        """
        raise NotImplemented

#-----------------------------------------------------------

class AbstractRecordValidator(object):
    def validate(self, record, values):
        """
        Validates the record against the inputed dictionary of column
        values.

        :param      record | <orb.Table>
                    values | {<orb.Column>: <value>, ..}

        :return     <bool>
        """
        raise NotImplemented

#-----------------------------------------------------------

class RegexValidator(AbstractColumnValidator):
    def __init__(self):
        super(RegexValidator, self).__init__()

        # define extra properties
        self._expression = ''
        self._help = ''

    def expression(self):
        """
        Returns the regular expression for the inputed text.

        :return     <str>
        """
        return self._expression

    def help(self):
        """
        Returns the help expression that will be shown to when this validator fails.

        :return     <str>
        """
        return self._help

    def setExpression(self, expression):
        """
        Sets the regular expression to the inputed text.

        :param      expression | <str>
        """
        self._expression = expression

    def setHelp(self, help):
        """
        Defines the help text that will be raised when processing this validator.

        :param      help | <str>
        """
        self._help = help

    def validate(self, column, value):
        """
        Validates the inputed value against this validator's expression.

        :param      value | <variant>

        :return     <bool> | is valid
        """
        try:
            valid = re.match(self.expression(), value) is not None
        except StandardError:
            msg = 'Invalid validator expression: {0}'.format(self.expression())
            raise errors.ColumnValidationError(column, msg=msg)
        else:
            if not valid:
                msg = 'Invalid value for {0}, needs to match {1}'.format(column.name(), self.expression())
                raise errors.ColumnValidationError(column, msg=self.help() or msg)
            return True

#-----------------------------------------------------------

class RequiredValidator(AbstractColumnValidator):
    def validate(self, column, value):
        """
        Varifies that the inputed value is not a NULL value.

        :param      value | <variant>

        :return     <bool>
        """
        if type(value) != bool and not bool(value):
            msg = '{0} is required.'.format(column.name())
            raise errors.ColumnValidationError(column, msg=msg)
        return True