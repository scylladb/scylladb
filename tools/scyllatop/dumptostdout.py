import livedata
import views.stdout


class _FakeLoop:
    def __init__(self, liveData, iterations):
        self._iterations = iterations
        self._liveData = liveData

    def draw_screen(self):
        if self._iterations is None:
            return

        self._iterations -= 1
        if self._iterations == 0:
            self._liveData.stop()


def dumpToStdout(metricPatterns, interval, collectd, iterations):
    stdout = views.stdout.Stdout()
    liveData = livedata.LiveData(metricPatterns, interval, collectd)
    liveData.addView(stdout)

    loop = _FakeLoop(liveData, iterations)
    liveData.go(loop)
