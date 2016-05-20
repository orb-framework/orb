import re

from orb import errors


class AbstractColumnValidator(object):
    def validate(self, column, value):
        """
        Validates the inputted value for this instance.

        :param      column | <orb.Column>
                    value | <variant>

        :return     <bool> | is valid
        """
        raise NotImplemented


# -----------------------------------------------------------

class AbstractRecordValidator(object):
    # noinspection PyMethodMayBeStatic
    def validate(self, record, values):
        """
        Validates the record against the inputted dictionary of column
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
        Returns the regular expression for the inputted text.

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
        Sets the regular expression to the inputted text.

        :param      expression | <str>
        """
        self._expression = expression

    def setHelp(self, text):
        """
        Defines the help text that will be raised when processing this validator.

        :param      text | <str>
        """
        self._help = text

    def validate(self, column, value):
        """
        Validates the inputted value against this validator expression.

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
        Verifies that the inputted value is not a NULL value.

        :param      value | <variant>

        :return     <bool>
        """
        if value is None or value == '':
            msg = '{0} is required.'.format(column.name())
            raise errors.ColumnValidationError(column, msg=msg)
        return True
