from projex.addon import AddonManager
from projex.decorators import abstractmethod


class Syntax(AddonManager):
    @abstractmethod('Syntax', 'Generate a getter name based on the given column name')
    def display(self, name):
        pass

    @abstractmethod('Syntax', 'Generate a getter name based on the given column name')
    def field(self, name, reference=False):
        pass

    @abstractmethod('Syntax', 'Generate a getter name based on the given column name')
    def getter(self, name):
        pass

    @abstractmethod()
    def indexdb(self, schema, name):
        pass

    @abstractmethod('Syntax', 'Generate a setter name based on the given column name')
    def schemadb(self, name):
        pass

    @abstractmethod('Syntax', 'Generate a setter name based on the given column name')
    def setter(self, name):
        pass

