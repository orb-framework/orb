from ..column import Column, VirtualColumn

class ShortcutColumn(VirtualColumn):
    def __init__(self, shortcut='', **kwds):
        super(ShortcutColumn, self).__init__(**kwds)

        # set standard properties
        self.setFlag(Column.Flags.ReadOnly)

        # define custom properties
        self.__shortcut = shortcut

    def shortcut(self):
        return self.__shortcut

    def setShortcut(self, shortcut):
        self.__shortcut = shortcut

Column.registerAddon('Shortcut', ShortcutColumn)