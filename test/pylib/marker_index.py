#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""The selected tests and the markers they carry, from one collect-only pass.

Only pytest knows what a test ends up carrying: markers come from decorators,
`pytestmark` in a module or a conftest, parametrize and collection hooks.

    from test.pylib.marker_index import build_index
    tests = build_index(["--mode", "dev", "test/cluster"], tmpdir)

The same module is the plugin doing the collecting, so it also works as
`python -m pytest --collect-only -p test.pylib.marker_index --marker-index=out.json`
(as a module, so that `-p` can import it before any conftest runs).
The index is uninterpreted on purpose, so a scheduler can weigh tests by any
marker without this module knowing about it.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from test import TOP_SRC_DIR
from test.pylib.running_shards import MARKER as MAX_RUNNING_SHARDS, claimed_shards


INDEX_OPTION = "--marker-index"

# How the collect-only pass loads this module with -p.
PLUGIN = "test.pylib.marker_index"


def index_item(item: pytest.Item) -> dict:
    """One test's entry: what it is, and what it declares."""
    markers = {}
    for marker in item.iter_markers():
        # A name can repeat (parametrize, stacked skip_mode); iter_markers
        # yields the closest first, which is the one that counts.
        markers.setdefault(marker.name, {"args": marker.args, "kwargs": marker.kwargs})

    return {
        "nodeid": item.nodeid,
        "markers": markers,
        # Lifted out of `markers` for the one consumer that exists today; the
        # marker itself stays in `markers` for everyone else.
        MAX_RUNNING_SHARDS: claimed_shards(item),
    }


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(INDEX_OPTION, action="store", default=None, metavar="PATH",
                     help="Write the selected tests and their markers to PATH as JSON "
                          "(see test/pylib/marker_index.py)")


def pytest_collection_finish(session: pytest.Session) -> None:
    if out := session.config.getoption(INDEX_OPTION, default=None):
        index = {"tests": [index_item(item) for item in session.items]}
        # default=repr: a marker may carry anything, and an index that fails on
        # one such argument is useless.
        Path(out).write_text(json.dumps(index, indent=1, sort_keys=True, default=repr) + "\n")


def build_index(pytest_args, tmpdir: Path) -> list[dict]:
    """Collect in a subprocess and return the index.

    A subprocess, not pytest.main(): the caller can be a running pytest
    session, and a nested session in the same interpreter would share plugin
    and fixture state with the outer one.
    """
    out = tmpdir / "marker_index.json"
    argv = [sys.executable, "-m", "pytest", "--collect-only", "-q",
            "-p", PLUGIN, f"{INDEX_OPTION}={out}", f"--tmpdir={tmpdir}", *pytest_args]
    # PYTEST_XDIST*: without dropping it the runner plugin would take the
    # nested collection for a worker of the outer run and skip creating its log
    # directory, as test_no_bare_skips.py found.  PYTEST_ADDOPTS: a -k or
    # --ignore in the caller's environment would narrow this collection, and
    # what is decided from a narrowed one is wrong.
    env = {name: value for name, value in os.environ.items()
           if not name.startswith(("PYTEST_XDIST", "PYTEST_ADDOPTS"))}
    done = subprocess.run(argv, capture_output=True, text=True, cwd=TOP_SRC_DIR, env=env)
    # A collection that fails part-way still reaches pytest_collection_finish,
    # so the index can exist and be quietly short.  Anything decided from a
    # short index is wrong, so a bad exit means no index at all.
    if done.returncode != 0 or not out.exists():
        raise RuntimeError(f"collection failed (exit {done.returncode}).\n"
                           f"$ {' '.join(argv)}\n{done.stdout}\n{done.stderr}")
    return json.loads(out.read_text())["tests"]
