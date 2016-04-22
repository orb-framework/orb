from ..column import Column

class ShortcutColumn(Column):
    def __init__(self, shortcut='', **kwds):
        super(ShortcutColumn, self).__init__(**kwds)

        # set standard properties
        self.setFlag(Column.Flags.ReadOnly)

        # define custom properties
        self.__shortcut = shortcut

    def copy(self):
        out = super(ShortcutColumn, self).copy()
        out.setShortcut(self.__shortcut)
        return out

    def shortcut(self):
        return self.__shortcut

    def setShortcut(self, shortcut):
        self.__shortcut = shortcut

Column.registerAddon('Shortcut', ShortcutColumn)