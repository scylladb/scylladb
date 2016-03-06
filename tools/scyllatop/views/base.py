import time
import curses
import curses.panel
import logging


class Base(object):
    def __init__(self, window):
        lines, columns = window.getmaxyx()
        self._window = curses.newwin(lines, columns)
        self._panel = curses.panel.new_panel(self._window)

    def writeStatusLine(self, measurements):
        line = 'time: {0}| {1} measurements, at most {2} visible'.format(time.asctime(), len(measurements), self.availableLines())
        columns = self.dimensions()['columns']
        line = line[:columns]
        self._window.addstr(0, 0, line.ljust(columns), curses.A_REVERSE)

    def availableLines(self):
        STATUS_LINE = 1
        return self.dimensions()['lines'] - STATUS_LINE

    def refresh(self):
        curses.panel.update_panels()
        curses.doupdate()

    def onTop(self):
        logging.info('put {0} view on top'.format(self.__class__.__name__))
        self._panel.top()
        curses.panel.update_panels()
        curses.doupdate()

    def clearScreen(self):
        self._window.erase()
        self._window.move(0, 0)

    def writeLine(self, thing, line):
        columns = self.dimensions()['columns']
        lines = self.dimensions()['lines']
        if line == lines - 1:
            output = str(thing)[:columns - 1]
        else:
            output = str(thing)
        self._window.addstr(line, 0, output)

    def dimensions(self):
        lines, columns = self._window.getmaxyx()
        return {'lines': lines, 'columns': columns}
