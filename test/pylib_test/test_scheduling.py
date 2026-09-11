#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Tests for the scheduler platform.

The framework path is exercised here — including a stub scheduler of each
shape — regardless of what CI runs.  Otherwise it rots while every gating job
sits on the default.

CI runs this with::

    ./tools/toolchain/dbuild -- pytest test/pylib_test
"""

from __future__ import annotations

import argparse
import configparser
import importlib.util
import json
import pathlib
import re
import sqlite3
import sys
from functools import lru_cache

import pytest

from test import HOST_ID, TEST_DIR, TOP_SRC_DIR
from test.pylib.db.writer import DEFAULT_DB_NAME, SCHEDULER_RUNS_TABLE
from test.pylib.scheduling import execution, registry
from test.pylib.scheduling.config import RunConfig, SchedulerError
from test.pylib.scheduling.scheduler import Scheduler
from test.pylib.scheduling.schedulers.passthrough import Passthrough

SCHEDULING_DIR = TEST_DIR / "pylib" / "scheduling"


@lru_cache(maxsize=None)
def _load_test_py():
    """Import ``test.py`` under a name that does not collide with the ``test`` package."""
    spec = importlib.util.spec_from_file_location("testpy_under_test", TOP_SRC_DIR / "test.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def parse_test_py(argv: list[str]) -> argparse.Namespace:
    """Run ``test.py``'s real command-line parsing, so defaults stay honest."""
    testpy = _load_test_py()
    saved = sys.argv
    sys.argv = ["test.py", *argv]
    try:
        return testpy.parse_cmd_line()
    finally:
        sys.argv = saved


def select_scheduler(options: argparse.Namespace):
    """``test.py``'s own selection step, as ``main()`` calls it."""
    return _load_test_py().select_scheduler(options)


# --- stub schedulers, one of each shape --------------------------------------


class StubConfigure(Scheduler):
    """Turn a knob before pytest starts."""

    name, version = "stub-configure", "7"

    def configure(self, cfg: RunConfig) -> None:
        cfg.set_concurrency(max(1, len(cfg.options.modes)))


class StubPlugin(Scheduler):
    """Decide something, and own dispatch too."""

    name, plugin = "stub-plugin", "some.other.module"

    def configure(self, cfg: RunConfig) -> None:
        cfg.set_dist("loadscope")


class StubPluginOnly(Scheduler):
    """Ship only a plugin: nothing to decide before pytest starts."""

    name, plugin = "stub-plugin-only", "some.plugin.only"


class StubRefuses(Scheduler):
    """Cannot honour this command line, and says so itself."""

    name = "stub-refuses"

    def configure(self, cfg: RunConfig) -> None:
        raise SchedulerError("it schedules whole modules; -k is not supported with it")


@pytest.fixture
def options(tmp_path: pathlib.Path) -> argparse.Namespace:
    return parse_test_py(["--mode=dev", "-j4", f"--tmpdir={tmp_path}"])


# --- the default must not change anything ------------------------------------


def test_passthrough_argv_is_byte_identical(tmp_path: pathlib.Path) -> None:
    """The default produces exactly the argv test.py produced before schedulers.

    This list is a golden value on purpose: changing the argv should be a
    deliberate act, visible in a diff, not a side effect of touching the
    scheduler platform.
    """
    options = parse_test_py(["--mode=dev", "-j4", f"--tmpdir={tmp_path}"])
    cfg = RunConfig.defaults(options)
    assert execution.pytest_args(cfg, Passthrough()) == [
        '--color=yes',
        '--repeat=1',
        '--mode=dev',
        f'--junit-xml={tmp_path}/report/pytest_cpp_{HOST_ID}.xml',
        '-rf',
        '-n4',
        f'--tmpdir={tmp_path}',
        '--maxfail=0',
        f'--alluredir={tmp_path}/report/allure_{HOST_ID}',
        '--dist=worksteal',
        '--gather-metrics',
        '--timeout=3600',
        '--session-timeout=24000',
        '--allure-no-capture',
        '--log-level=INFO',
        str(TOP_SRC_DIR / 'test'),
    ]


def test_passthrough_list_argv_is_byte_identical(tmp_path: pathlib.Path) -> None:
    options = parse_test_py(["--mode=dev", "-j4", "--list", f"--tmpdir={tmp_path}"])
    cfg = RunConfig.defaults(options)
    assert execution.pytest_args(cfg, Passthrough()) == [
        '--color=yes',
        '--repeat=1',
        '--mode=dev',
        '--collect-only',
        '--quiet',
        '--no-header',
        '--gather-metrics',
        '--timeout=3600',
        '--session-timeout=24000',
        '--allure-no-capture',
        '--log-level=INFO',
        str(TOP_SRC_DIR / 'test'),
    ]


def test_passthrough_loads_no_plugin_and_changes_nothing(options) -> None:
    scheduler = Passthrough()
    cfg = RunConfig.defaults(options)
    scheduler.configure(cfg)          # the base class's no-op

    assert "-p" not in execution.pytest_args(cfg, scheduler)
    assert (cfg.concurrency, cfg.dist) == (options.jobs, "worksteal")


def test_default_scheduler_is_passthrough() -> None:
    assert registry.DEFAULT == "passthrough"
    assert isinstance(registry.get_scheduler(registry.DEFAULT), Passthrough)


# --- what test.py does with the scheduler it selected -------------------------


def test_selecting_a_scheduler_lets_it_decide(options, monkeypatch) -> None:
    monkeypatch.setitem(registry.SCHEDULERS, "stub-configure", StubConfigure)
    monkeypatch.setattr(options, "scheduler", "stub-configure")

    scheduler, cfg = select_scheduler(options)

    assert isinstance(scheduler, StubConfigure)
    assert cfg.concurrency == 1                       # one mode was selected


def test_a_scheduler_that_cannot_run_this_command_line_raises(options, monkeypatch) -> None:
    """It owns the message: only it knows what it could not honour."""
    monkeypatch.setitem(registry.SCHEDULERS, "stub-refuses", StubRefuses)
    monkeypatch.setattr(options, "scheduler", "stub-refuses")

    with pytest.raises(SchedulerError, match="-k is not supported"):
        select_scheduler(options)

    # A run that never happened is not a run that was scheduled.
    assert not (pathlib.Path(options.tmpdir) / DEFAULT_DB_NAME).exists()


def test_listing_tests_decides_nothing(options, monkeypatch, capsys) -> None:
    monkeypatch.setitem(registry.SCHEDULERS, "stub-refuses", StubRefuses)
    monkeypatch.setattr(options, "scheduler", "stub-refuses")
    monkeypatch.setattr(options, "list_tests", True)

    scheduler, cfg = select_scheduler(options)      # configure() is not called

    assert isinstance(scheduler, StubRefuses)
    assert capsys.readouterr().out == ""
    assert not (pathlib.Path(options.tmpdir) / DEFAULT_DB_NAME).exists()


def test_exactly_one_scheduler_plugin_is_loaded(options) -> None:
    cfg = RunConfig.defaults(options)
    scheduler = StubPlugin()
    scheduler.configure(cfg)
    args = execution.pytest_args(cfg, scheduler)

    assert [args[i + 1] for i, arg in enumerate(args) if arg == "-p"] == [StubPlugin.plugin]
    assert "--dist=loadscope" in args


def test_a_plugin_only_scheduler_needs_no_configure(options) -> None:
    """Overriding nothing is a complete scheduler: the base class decides nothing."""
    scheduler = StubPluginOnly()
    cfg = RunConfig.defaults(options)
    scheduler.configure(cfg)

    assert (cfg.concurrency, cfg.dist) == (options.jobs, "worksteal")
    args = execution.pytest_args(cfg, scheduler)
    assert [args[i + 1] for i, arg in enumerate(args) if arg == "-p"] == [StubPluginOnly.plugin]
    assert "-n4" in args and "--dist=worksteal" in args


# --- RunConfig on its own -----------------------------------------------------


def test_run_config_is_usable_with_no_scheduler_in_sight(options) -> None:
    cfg = RunConfig.defaults(options)

    assert cfg.concurrency == 4
    assert cfg.dist == "worksteal"


def test_the_setters_are_the_whole_vocabulary(options) -> None:
    cfg = RunConfig.defaults(options)
    cfg.set_concurrency(3)
    cfg.set_dist("loadfile")

    assert (cfg.concurrency, cfg.dist) == (3, "loadfile")


def test_concurrency_must_be_at_least_one(options) -> None:
    with pytest.raises(SchedulerError):
        RunConfig.defaults(options).set_concurrency(0)


# --- what the run says about the scheduler that ran it ------------------------


def test_one_line_names_the_scheduler(options, capsys) -> None:
    RunConfig.defaults(options).log_scheduler(Passthrough())
    assert capsys.readouterr().out.splitlines()[0] == "scheduler: passthrough@1"


def test_the_line_names_the_plugin_when_there_is_one(options, capsys) -> None:
    RunConfig.defaults(options).log_scheduler(StubPlugin())
    assert capsys.readouterr().out.splitlines()[0] == (
        "scheduler: stub-plugin@1 plugin=some.other.module")


def scheduler_runs(tmpdir: pathlib.Path) -> list[sqlite3.Row]:
    """Every scheduler_runs row in the metrics DB under *tmpdir*."""
    connection = sqlite3.connect(pathlib.Path(tmpdir) / DEFAULT_DB_NAME)
    connection.row_factory = sqlite3.Row
    try:
        return list(connection.execute(f"SELECT * FROM {SCHEDULER_RUNS_TABLE}"))
    finally:
        connection.close()


def test_gather_metrics_records_the_run_in_the_db(options) -> None:
    cfg = RunConfig.defaults(options)
    cfg.set_dist("loadfile")
    cfg.log_scheduler(StubPlugin())

    row, = scheduler_runs(options.tmpdir)
    assert (row["name"], row["version"], row["plugin"]) == (
        "stub-plugin", "1", "some.other.module")
    assert row["timestamp"]

    recorded = json.loads(row["config"])
    assert recorded["concurrency"] == 4
    assert recorded["dist"] == "loadfile"
    # The whole command line, so a recorded run can be read back in full.
    assert recorded["options"]["modes"] == ["dev"]
    assert recorded["options"]["jobs"] == 4
    assert recorded["options"]["tmpdir"] == str(options.tmpdir)


def test_the_table_accumulates_and_can_be_selected_by_scheduler(options) -> None:
    """The point of a table rather than a file: rows are selectable.

    A run normally writes its own ``sqlite_<HOST_ID>.db``, so more than one row
    in a single file means the host id was pinned, or the files were collected.
    """
    RunConfig.defaults(options).log_scheduler(Passthrough())
    RunConfig.defaults(options).log_scheduler(StubPlugin())

    assert [row["name"] for row in scheduler_runs(options.tmpdir)] == [
        "passthrough", "stub-plugin"]


def test_without_gather_metrics_only_the_line_is_produced(tmp_path, capsys) -> None:
    options = parse_test_py(["--mode=dev", "--no-gather-metrics", f"--tmpdir={tmp_path}"])
    RunConfig.defaults(options).log_scheduler(Passthrough())

    assert capsys.readouterr().out == "scheduler: passthrough@1\n"
    assert not (tmp_path / DEFAULT_DB_NAME).exists()


def test_the_config_column_survives_an_option_json_cannot_hold(options) -> None:
    options.byte_limit = object()
    recorded = json.loads(RunConfig.defaults(options).as_json())
    assert "object object at" in recorded["options"]["byte_limit"]


# --- the registry -------------------------------------------------------------


def test_an_unknown_scheduler_is_a_command_line_error(capsys) -> None:
    with pytest.raises(SystemExit):
        parse_test_py(["--mode=dev", "--scheduler=does-not-exist"])
    assert "passthrough" in capsys.readouterr().err


def test_scheduler_list_prints_the_registry_and_exits(monkeypatch, capsys) -> None:
    monkeypatch.setitem(registry.SCHEDULERS, "stub-plugin", StubPlugin)
    with pytest.raises(SystemExit) as exit_info:
        parse_test_py([f"--scheduler={registry.LIST_KEYWORD}"])

    assert exit_info.value.code in (None, 0)
    listed = capsys.readouterr().out
    assert "passthrough" in listed and "[default]" in listed
    # Each line says what the scheduler does, from its own docstring.
    assert "Make no decisions and hand the run to xdist" in listed
    assert "stub-plugin" in listed and "Decide something, and own dispatch too." in listed


def test_a_scheduler_only_has_to_declare_a_name() -> None:
    class Minimal(Scheduler):
        name = "minimal"

    assert Minimal.version == "1" and Minimal.plugin is None

    class Nameless(Scheduler):
        """Forgetting the name is caught where the registry is built."""

    with pytest.raises(AttributeError):
        {cls.name: cls for cls in (Nameless,)}


def test_every_registered_scheduler_is_one() -> None:
    # Classes, not instances: a module-level instance would be a singleton
    # shared by every run in the process, which unit tests trip over.
    assert all(isinstance(cls, type) and issubclass(cls, Scheduler)
               for cls in registry.SCHEDULERS.values())
    assert registry.get_scheduler("passthrough") is not registry.get_scheduler("passthrough")


def test_a_scheduler_is_registered_under_the_name_it_reports() -> None:
    assert all(name == cls.name for name, cls in registry.SCHEDULERS.items())


# --- the framework interprets no test metadata --------------------------------


def _declared_markers() -> set[str]:
    # pytest.ini holds %(asctime)s-style log formats: no interpolation.
    parser = configparser.ConfigParser(interpolation=None)
    parser.read(TEST_DIR / "pytest.ini")
    names = set()
    for line in parser["pytest"]["markers"].splitlines():
        line = line.strip()
        if line:
            names.add(re.split(r"[(:]", line, maxsplit=1)[0])
    return names


def test_no_marker_name_appears_in_the_framework() -> None:
    """Only a scheduler knows what a marker means.

    Everything in ``test/pylib/scheduling`` except ``schedulers/`` decides how
    tests are run without ever looking at what a test declares about itself, so
    no marker name may appear in it.
    """
    marker_names = _declared_markers()
    assert marker_names, "expected markers to be declared in test/pytest.ini"
    offenders = []
    for path in SCHEDULING_DIR.rglob("*.py"):
        if path.parent.name == "schedulers":
            continue
        source = path.read_text()
        offenders += [f"{path.relative_to(TOP_SRC_DIR)}: {name}"
                      for name in marker_names if re.search(rf"\b{re.escape(name)}\b", source)]
    assert not offenders, "the framework must not know any marker: " + ", ".join(offenders)


def test_a_refused_run_exits_the_way_a_bad_command_line_does() -> None:
    """Not a code of our own: test.py returns pytest's exit codes verbatim."""
    testpy = _load_test_py()
    assert testpy.EXIT_SCHEDULER_ERROR == pytest.ExitCode.USAGE_ERROR
    assert testpy.EXIT_SCHEDULER_ERROR not in (pytest.ExitCode.OK,
                                               pytest.ExitCode.TESTS_FAILED,
                                               pytest.ExitCode.INTERRUPTED)
