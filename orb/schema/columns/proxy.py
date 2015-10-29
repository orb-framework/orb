from ..column import Column


class ProxyColumn(Column):
    """
    Defines a way to define methods as fake columns for a model.

    Usage
    ---

        import orb

        class Comment(orb.Table):
            num_attachments = orb.ProxyColumn()

            @num_attachments.getter
            def calculate_num_attachments(self):
                return 0

    """
    def __init__(self, gettermethod=None, settermethod=None, **kwds):
        super(ProxyColumn, self).__init__(**kwds)

        # set default properties
        self.setFlag(Column.Flags.Field, False)

        # define custom properties
        self._gettermethod = gettermethod
        self._settermethod = settermethod

    def getter(self, func):
        self._getter = func
        return func

    def setter(self, func):
        self._setter = func
        return func


Column.registerAddon('Proxy', ProxyColumn)