import urwid


class UserInput(object):
    def __init__(self):
        self._viewMap = None
        self._mainLoop = None

    def setMap(self, ** viewMap):
        self._viewMap = viewMap

    def setLoop(self, loop):
        self._mainLoop = loop

    def __call__(self, keypress):
        if keypress in ('q', 'Q'):
            raise urwid.ExitMainLoop()
        if keypress.upper() not in self._viewMap:
            return

        view = self._viewMap[keypress.upper()]
        self._mainLoop.widget = view.widget()
