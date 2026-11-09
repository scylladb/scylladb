#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Port of pytest-dev/pytest-xdist#1395: stop all workers after their current test once the run must stop.

When the controller decides to stop (e.g. --maxfail is reached), xdist 3.8.0
only queues a shutdown after each worker's queued tests.  With
--dist=worksteal every test is queued up front, so the workers run nearly
the whole suite before the session stops.

The controller now also tells every worker to stop right away, and the
worker sets session.shouldstop, so it leaves its loop after the test in
progress.  The stop decision and the failure count stay xdist's own.

TODO: remove this module (and test/pylib_test/test_xdist_stop.py) once a
pytest-xdist release with pytest-dev/pytest-xdist#1395 is pinned in
test/pyproject.toml.
"""

import contextlib

import pytest
from xdist.dsession import DSession


_triggershutdown = DSession.triggershutdown


def triggershutdown(self: DSession) -> None:
    # The stock shutdown only queues a stop marker after each worker's queued tests.
    if not self.shuttingdown and self.shouldstop:
        for node in self.sched.nodes:
            with contextlib.suppress(OSError):
                node.sendcommand("stop_now", shouldstop=self.shouldstop)
    _triggershutdown(self)


@pytest.hookimpl(trylast=True)
def pytest_configure(config: pytest.Config) -> None:
    if not hasattr(config, "workerinput"):
        DSession.triggershutdown = triggershutdown
        return
    # xdist runs its remote.py through execnet's remote_exec, so the worker's
    # WorkerInteractor class can't be imported: patch the registered instance.
    interactor = next(p for p in config.pluginmanager.get_plugins() if type(p).__name__ == "WorkerInteractor")
    handle_command = interactor.handle_command

    def handle_stop_now(command: object) -> None:
        if isinstance(command, tuple) and command[0] == "stop_now":
            # The worker loop checks this after each test.
            interactor.session.shouldstop = command[1]["shouldstop"]
        else:
            handle_command(command)

    interactor.handle_command = handle_stop_now
