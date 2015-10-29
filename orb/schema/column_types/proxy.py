from ..column import Column, VirtualColumn


class ProxyColumn(VirtualColumn):
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

        # set standard properties
        self.setFlag(Column.Flags.Queryable, False)

        # define custom properties
        self.__gettermethod = gettermethod
        self.__settermethod = settermethod

    def getter(self, func):
        self.__getter = func
        return func

    def setter(self, func):
        self.__setter = func
        return func


Column.registerAddon('Proxy', ProxyColumn)