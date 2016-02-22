import threading
import logging


class UserInput(threading.Thread):
    def __init__(self, liveData, screen, simpleView, meansView):
        self._liveData = liveData
        self._screen = screen
        self._simpleView = simpleView
        self._meansView = meansView
        threading.Thread.__init__(self)
        self.daemon = True
        self.start()

    def run(self):
        while True:
            keypress = self._screen.getch()
            logging.debug('key pressed {0}'.format(keypress))
            if keypress == ord('m'):
                self._meansView.onTop()
            if keypress == ord('s'):
                self._simpleView.onTop()
            if keypress == ord('q'):
                logging.info('quitting on user request')
                self._liveData.stop()
                return
