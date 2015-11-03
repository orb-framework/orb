from ..model import Model


class Table(Model):
    __orb__ = {'bypass': True}

Model.registerAddon('Table', Table)