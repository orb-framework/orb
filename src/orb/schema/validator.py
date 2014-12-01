import re

from projex.decorators import abstractmethod

class AbstractValidator(object):
    def __init__(self):
        self._help = ''

    def help(self):
        """
        Returns the help information for the user when the validation error occurs.

        :return     <str>
        """
        return self._help

    def setHelp(self, text):
        """
        Sets the help information for the user when the validation error occurs.

        :param      text | <str>
        """
        self._help = text

    @abstractmethod()
    def validate(self, value):
        """
        Validates the inputed value for this instance.

        :param      value | <variant>

        :return     <bool> | is valid
        """
        return False

#-----------------------------------------------------------

class RegexValidator(AbstractValidator):
    def __init__(self):
        super(RegexValidator, self).__init__()

        # define extra properties
        self._expression = ''

    def expression(self):
        """
        Returns the regular expression for the inputed text.

        :return     <str>
        """
        return self._expression

    def setExpression(self, expression):
        """
        Sets the regular expression to the inputed text.

        :param      expression | <str>
        """
        self._expression = expression

    def validate(self, value):
        """
        Validates the inputed value against this validator's expression.

        :param      value | <variant>

        :return     <bool> | is valid
        """
        try:
            return re.match(self.expression(), value) is not None
        except StandardError:
            return False

#-----------------------------------------------------------

class NotNullValidator(AbstractValidator):
    def __init__(self):
        super(NotNullValidator, self).__init__()

        # define the help information
        self.setHelp('{context} is required.')

    def validate(self, value):
        """
        Varifies that the inputed value is not a NULL value.

        :param      value | <variant>

        :return     <bool>
        """
        # for boolean values, only a None value will fail
        if type(value) == bool:
            return True

        # otherwise, ensure the value "exists"
        else:
            return bool(value)