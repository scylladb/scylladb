#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""The test logs: the session log of each process, the per-test failure logs
and the failed_test/ copies of the server logs.  runner.py calls these from
its hooks."""

from __future__ import annotations

import logging
import os
import pathlib
import shutil
import urllib.parse
from itertools import chain
from typing import TYPE_CHECKING

import pytest

from test import HOST_ID

if TYPE_CHECKING:
    from test.pylib.scylla_cluster import ScyllaCluster

PYTEST_LOG_FOLDER = "pytest_log"
PYTEST_TESTS_LOGS_FOLDER = "pytest_tests_logs"

PYTEST_LOG_FILE = pytest.StashKey[str]()

FAILED_TEST_DIR = "failed_test"


def make_failed_test_dir(config: pytest.Config, build_mode: str, test_name: str) -> pathlib.Path:
    """Create and return `<mode>/failed_test/<test>` (uploaded on failure).

    The test name is translated so `[]` don't confuse the artifact upload.
    """
    path = (pathlib.Path(config.getoption("--tmpdir")).absolute()
            / build_mode / FAILED_TEST_DIR
            / test_name.translate(str.maketrans('[]', '()')))
    path.mkdir(parents=True, exist_ok=True)
    return path


def record_failed_test_artifacts(config: pytest.Config,
                                 properties: list[tuple[str, object]],
                                 failed_test_dir_path: pathlib.Path,
                                 longreprtext: str,
                                 when: str) -> None:
    """Common failed-test bookkeeping: append the phase traceback to
    stacktrace.txt and record the clickable TEST_LOGS / PYTEST_LOG links.

    Fires once per failed phase, so the traceback is always appended while the
    links are recorded only once (idempotent).
    """
    with open(failed_test_dir_path / "stacktrace.txt", "a") as f:
        f.write(f"----- {when} -----\n{longreprtext}\n")

    artifacts_dir_url = config.getoption("artifacts_dir_url", None)
    if artifacts_dir_url is None or any(name == "TEST_LOGS" for name, _ in properties):
        return

    def _artifact_link(path: pathlib.Path | str) -> str:
        # URL-encoded so test names with ::/[]/() don't 404; the junit plugin
        # linkifies http(s) values.
        posix = pathlib.Path(path).as_posix()
        return urllib.parse.urljoin(artifacts_dir_url + "/", urllib.parse.quote(posix[posix.find("testlog"):]))

    # TEST_LOGS -> failed_test folder (server logs + traceback);
    # PYTEST_LOG -> this worker's session log.
    properties.append(("TEST_LOGS", _artifact_link(failed_test_dir_path) + "/"))
    if PYTEST_LOG_FILE in config.stash:
        properties.append(("PYTEST_LOG", _artifact_link(config.stash[PYTEST_LOG_FILE])))


def setup_session_log(config: pytest.Config) -> None:
    log_file_format = config.getini("log_file_format") or config.getini("log_format") or "%(asctime)s %(levelname)s %(name)s> %(message)s"
    log_file_level = config.getini("log_file_level") or config.getini("log_level") or "INFO"

    pytest_log_dir = pathlib.Path(config.getoption("--tmpdir")).absolute() / PYTEST_LOG_FOLDER
    worker_id = os.environ.get("PYTEST_XDIST_WORKER")
    # If this is an xdist worker, set up logging to a separate file for this worker. Otherwise, set up logging for the main process.
    if worker_id is not None:
        config.stash[PYTEST_LOG_FILE] = f"{pytest_log_dir}/pytest_{worker_id}_{HOST_ID}.log"
    else:
        # For the main process, we want to clean up old logs before the run, so we create the log directory and remove any existing log files.
        pytest_log_dir.mkdir(parents=True, exist_ok=True)
        if not config.getoption("--save-log-on-success"):
            for file in pytest_log_dir.glob("*"):
                # This will help in case framework tests are executed with test.py event if it's the wrong way to run them.
                # test_no_bare_skip_markers_in_collection uses a subprocess to run a collection that has lead to race
                # condition, especially with repeat.
                file.unlink(missing_ok=True)

        config.stash[PYTEST_LOG_FILE] = f"{pytest_log_dir}/pytest_main_{HOST_ID}.log"

    # Explicitly configure the root logger to write exclusively to a file.
    # logging.basicConfig() is a no-op when the root logger already has handlers
    # (e.g. added by pytest or any early import), which would leave a StreamHandler
    # in place and cause all log records — including noisy third-party DEBUG messages
    # like urllib3.connectionpool or asyncio — to appear on the terminal.
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
        handler.close()

    file_handler = logging.FileHandler(config.stash[PYTEST_LOG_FILE])
    file_handler.setFormatter(logging.Formatter(log_file_format))
    root_logger.addHandler(file_handler)
    root_logger.setLevel(log_file_level)


def finish_session_log(session: pytest.Session, swept: int) -> None:
    # If all tests passed, remove the log file to save space and avoid confusion with logs from failed runs.
    # We check this at the end of the session to ensure that we have the complete log available for any failed tests.
    # An interrupted session counts no failures, and a sweep that had to dispose of a cluster is
    # recorded nowhere else, so the log stays in both cases.
    if (session.testsfailed == 0 and session.exitstatus == 0 and not swept
            and not session.config.getoption("--save-log-on-success")):
        # Use missing_ok=True because the log file is only created on first write,
        # so it may never have been written if nothing was logged.
        pathlib.Path(session.config.stash[PYTEST_LOG_FILE]).unlink(missing_ok=True)


def save_phase_log(item: pytest.Item, report: pytest.TestReport) -> None:
    # Optionally save test failure logs to files
    if report.failed or item.config.getoption("--save-log-on-success"):
        log_file = (
            pathlib.Path(item.config.getoption("--tmpdir")).absolute() /
            PYTEST_TESTS_LOGS_FOLDER /
            f"{item._nodeid.replace('::', '-').replace('/', '-')}-{report.when}-{HOST_ID}.log"
        )
        with log_file.open("a", encoding="utf-8") as f:
            f.write(report.longreprtext + "\n")
            for section in report.sections:
                f.write(section[0] + "\n")
                f.write(section[1] + "\n")


def save_failed_phase_logs(item: pytest.Item, report: pytest.TestReport, clusters: list[ScyllaCluster],
                           build_mode: str) -> None:
    failed_test_dir_path = make_failed_test_dir(item.config, build_mode, item.name)
    # For a call-phase failure the manager fixture's teardown
    # copies the server logs again (and attaches them to allure);
    # the copy here also covers the phases with no manager to
    # gather them, e.g. a failure during fixture setup.
    for server in chain.from_iterable(cluster.servers.values() for cluster in clusters):
        if server.log_filename.is_file():
            shutil.copyfile(server.log_filename, failed_test_dir_path / server.log_filename.name)
    record_failed_test_artifacts(
        config=item.config,
        properties=item.user_properties,
        failed_test_dir_path=failed_test_dir_path,
        longreprtext=report.longreprtext,
        when=report.when,
    )
