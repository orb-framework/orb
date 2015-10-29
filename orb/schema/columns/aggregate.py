from ..column import Column


class AggregateColumn(Column):
    def __init__(self, aggregator=None, **kwds):
        super(AggregateColumn, self).__init__(**kwds)

        # set default properties
        self.setFlag(Column.Flags.Field, False)

        # define custom properties
        self._aggregator = aggregator

    def aggregate(self):
        """
        Returns the query aggregate that is associated with this column.

        :return     <orb.QueryAggregate> || None
        """
        if self._aggregator:
            return self._aggregator.generate(self)
        return None

    def aggregator(self):
        """
        Returns the aggregation instance associated with this column.  Unlike
        the <aggregate> function, this method will return the class instance
        versus the resulting <orb.QueryAggregate>.

        :return     <orb.ColumnAggregator> || None
        """
        return self._aggregator


Column.registerAddon('Aggregate', AggregateColumn)