from ..column import Column

class ShortcutColumn(Column):
    def __init__(self, shortcut='', **kwds):
        super(ShortcutColumn, self).__init__(**kwds)

        # set default properties
        self.setFlag(Column.Flags.Field, False)

        # define custom properties
        self._shortcut = shortcut

    def shortcut(self):
        return self._shortcut

    def setShortcut(self, shortcut):
        self._shortcut = shortcut

Column.registerAddon('Shortcut', ShortcutColumn)