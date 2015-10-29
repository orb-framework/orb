from ..column import Column


class JoinColumn(Column):
    def __init__(self, joiner=None, **kwds):
        super(JoinColumn, self).__init__(**kwds)

        # set standard properties
        self.setFlag(Column.Flags.ReadOnly)
        self.setFlag(Column.Flags.Field, False)

        # define custom properties
        self._joiner = joiner

    def joiner(self):
        return self._joiner

    def setJoiner(self, joiner):
        """
        Sets the joiner query for this column to the inputted query.

        :param      query | (<orb.Column>, <orb.Query>) || <callable> || None
        """
        self._joiner = joiner


# register the column type
Column.registerAddon('Join', JoinColumn)