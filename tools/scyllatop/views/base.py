import time
import urwid


class Base(object):
    def __init__(self):
        self._items = []
        self._widgets = {}
        self._box = urwid.ListBox(urwid.SimpleFocusListWalker([]))

    def widget(self):
        return self._box

    def writeStatusLine(self, measurements):
        line = '*** time: {0}| {1} measurements ***'.format(time.asctime(), len(measurements))
        self._items = [line]

    def refresh(self):
        for i, text in enumerate(self._items):
            if i not in self._widgets:
                self._widgets[i] = urwid.Button(text)
                self._box.body.append(self._widgets[i])
            else:
                self._widgets[i].set_label(text)

    def clearScreen(self):
        self._items = []
        return

    def writeLine(self, thing):
        self._items.append(str(thing))
