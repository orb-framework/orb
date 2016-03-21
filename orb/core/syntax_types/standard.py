import projex.text
from ..syntax import Syntax


class StandardSyntax(Syntax):
    def display(self, name):
        return projex.text.pretty(name)

    def name(self, text):
        underscored = projex.text.underscore(text)
        if underscored.startswith('set_'):
            underscored = underscored[4:]
        return underscored

    def field(self, name, reference=False):
        base = projex.text.underscore(name)
        if reference:
            base += '_id'
        return base

    def getterName(self, name):
        return projex.text.camelHump(name)

    def indexdb(self, schema, name):
        return '{0}_{1}_idx'.format(schema.dbname(), projex.text.underscore(name))

    def schemadb(self, name):
        return projex.text.pluralize(projex.text.underscore(name))

    def setterName(self, name):
        setter = projex.text.camelHump(name)
        return 'set' + setter[0].upper() + setter[1:]

Syntax.registerAddon('standard', StandardSyntax)