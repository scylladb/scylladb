#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Per-test LLVM coverage profiles: `--coverage-per-test DIR`.

test.py's --coverage sends every process of a suite to one profile per binary,
which answers "what did the suite reach", not "what did this test reach".  With
--coverage-per-test DIR each test gets a profile of its own:

  DIR/<slug>.profdata.zst   the test's sparse indexed profile (zstd)
  DIR/<slug>.json           its node id, outcome of each phase, and how its profile was made

While a test runs (setup, call and teardown), LLVM_PROFILE_FILE in os.environ
points at a directory of its own, with %m so that concurrent writers merge.
Scylla nodes start with a copy of os.environ (ScyllaServer.start(), ccm's
node environment), and so does every `scylla nodetool`/`scylla sstable` the
test runs: the profile covers the scylla binary in all its roles.  Programs
without instrumentation (cqlsh, cassandra-stress, ...) write nothing.

Per-test attribution needs per-test clusters, as the dtests have: a node that
outlives the test keeps writing to the directory of the test that started it.

Nodes still up when a test ends are killed by the harness, and a killed process
never writes its profile, so before teardown every live scylla process of the
test is asked to dump it (POST /system/dump_llvm_profile), which also resets its
counters; with %m, whatever it runs afterwards is merged on top.

Python's random is seeded from the test's id for setup and call, so a test
picks the same data in every run; different data takes different code paths.
The python driver, test.py's host registry and scylla-dtest's cluster-id allocator
(dtest_setup) draw from the global random too -- on every connection, many a query,
every setup -- and the harnesses do not do that alike, so they get a generator of
their own: the seeded one is left to the test.

The module has no dependency on the rest of test/pylib, so the same file can
run in other pytest-based harnesses.
"""

from __future__ import annotations

import hashlib
import json
import os
import random
import re
import shutil
import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import pytest

API_PORT = 10000

_DIR = pytest.StashKey[Path]()
_PHASES = pytest.StashKey[dict]()


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption("--coverage-per-test", action="store", default=None, metavar="DIR",
                     help="Write an LLVM coverage profile of every test (all scylla processes it ran) to "
                          "DIR/<test>.profdata.zst, with its outcome in DIR/<test>.json.  Needs an "
                          "instrumented build (--mode coverage).")


def _root(config: pytest.Config) -> Path | None:
    value = config.getoption("--coverage-per-test", default=None)
    return Path(value).resolve() if value else None


def slug(nodeid: str) -> str:
    """A file name for a test: readable tail of its node id plus a hash of the whole id."""
    readable = "".join(c if c.isalnum() or c in "_.-" else "_" for c in nodeid)[-90:]
    return f"{readable}-{hashlib.sha1(nodeid.encode()).hexdigest()[:10]}"


def _listening_api_ips(pid: int) -> list[str]:
    """IPv4 addresses a process listens on at the REST API port."""
    inodes = set()
    try:
        for fd in os.listdir(f"/proc/{pid}/fd"):
            try:
                link = os.readlink(f"/proc/{pid}/fd/{fd}")
            except OSError:
                continue
            if link.startswith("socket:["):
                inodes.add(link[8:-1])
    except OSError:
        return []
    ips = []
    with open("/proc/net/tcp") as f:
        next(f)
        for line in f:
            parts = line.split()
            local, state, inode = parts[1], parts[3], parts[9]
            if state != "0A" or inode not in inodes:  # 0A = LISTEN
                continue
            hexip, hexport = local.split(":")
            if int(hexport, 16) == API_PORT:
                ips.append(socket.inet_ntoa(bytes.fromhex(hexip)[::-1]))
    return ips


def _scylla_pids_writing_to(profile_dir: Path) -> list[int]:
    pids = []
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        try:
            # By executable: Seastar renames its threads (reactor-N), comm is not "scylla".
            if os.path.basename(os.readlink(f"/proc/{entry}/exe")) != "scylla":
                continue
            with open(f"/proc/{entry}/environ", "rb") as f:
                env = f.read().split(b"\0")
        except OSError:
            continue
        if any(e.startswith(b"LLVM_PROFILE_FILE=") and str(profile_dir).encode() in e for e in env):
            pids.append(int(entry))
    return pids


def _dump_live_profiles(profile_dir: Path) -> list[str]:
    """Ask every live scylla process writing to profile_dir to dump its profile now."""
    done = []
    for pid in _scylla_pids_writing_to(profile_dir):
        for ip in _listening_api_ips(pid):
            try:
                req = urllib.request.Request(f"http://{ip}:{API_PORT}/system/dump_llvm_profile", method="POST")
                urllib.request.urlopen(req, timeout=120).read()
                done.append(f"{pid}@{ip}")
            except Exception as exc:  # noqa: BLE001 -- a node going down on its own is fine
                done.append(f"{pid}@{ip}:failed:{type(exc).__name__}")
            break
    return done


def _convert(profile_dir: Path, out: Path) -> dict:
    """Merge a test's raw profiles into DIR/<slug>.profdata.zst; drop the raw ones."""
    raws = sorted(profile_dir.glob("*.profraw"))
    info = {"raw_files": len(raws), "raw_bytes": sum(p.stat().st_size for p in raws)}
    if raws:
        indexed = profile_dir / "merged.profdata"
        subprocess.run(["llvm-profdata", "merge", "-sparse", "-o", str(indexed), *map(str, raws)], check=True)
        subprocess.run(["zstd", "-q", "-f", "-3", str(indexed), "-o", str(out)], check=True)
        info["profile"] = out.name
    shutil.rmtree(profile_dir, ignore_errors=True)
    return info


def seed_key(nodeid: str) -> str:
    """The part of a node id both harnesses agree on: the file's name and what follows it.

    test.py prefixes the suite path and appends ".<mode>.<run>" (cluster/dtest/x_test.py::T::t.coverage.1);
    scylla-dtest has neither (x_test.py::T::t).  Seeding from what is left gives a test the same
    data on both.
    """
    path, sep, rest = nodeid.partition("::")
    rest = re.sub(r"\.(release|dev|debug|coverage|sanitize)\.\d+$", "", rest)
    return f"{path.rsplit('/', 1)[-1]}{sep}{rest}"


_PRIVATE_RANDOM = random.Random()
_RANDOM_NAMES = ("random", "randint", "randrange", "choice", "shuffle", "sample", "getrandbits", "uniform")


def _isolate_harness_random() -> None:
    """Point the harness's uses of the global random at a private generator."""
    for name, module in list(sys.modules.items()):
        if module is None or not (name == "cassandra" or name.startswith("cassandra.")
                                  or name in ("test.pylib.host_registry", "dtest_setup")):
            continue
        for attr in _RANDOM_NAMES:
            value = getattr(module, attr, None)
            if value is random:
                setattr(module, attr, _PRIVATE_RANDOM)
            elif value is not None and value is getattr(random, attr, None):
                setattr(module, attr, getattr(_PRIVATE_RANDOM, attr))


def _seed_random(item: pytest.Item) -> None:
    _isolate_harness_random()
    random.seed(int(hashlib.sha1(seed_key(item.nodeid).encode()).hexdigest()[:16], 16))


def _outcome(report: pytest.TestReport) -> str:
    """pytest's outcome, except that an xfail (reported as skipped) is "xfailed" and a
    non-strict xpass is "xpassed"."""
    if hasattr(report, "wasxfail"):
        return "xfailed" if report.skipped else "xpassed"
    return report.outcome


def _write_json(path: Path, row: dict) -> None:
    # Under xdist every worker collects -- and deselects -- the same tests, so several processes
    # may write one file at once: each writes its own temporary file and atomically replaces.
    tmp = path.with_suffix(f".json.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(row))
    os.replace(tmp, path)


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_protocol(item: pytest.Item, nextitem):
    root = _root(item.config)
    if root is None:
        yield
        return
    name = slug(item.nodeid)
    profile_dir = root / "raw" / name
    profile_dir.mkdir(parents=True, exist_ok=True)
    item.stash[_DIR] = profile_dir
    item.stash[_PHASES] = {}
    previous = os.environ.get("LLVM_PROFILE_FILE")
    os.environ["LLVM_PROFILE_FILE"] = str(profile_dir / "%m.profraw")
    started = time.time()
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop("LLVM_PROFILE_FILE", None)
        else:
            os.environ["LLVM_PROFILE_FILE"] = previous
        # A test the harness took back without running (test.py's budget scheduler evicts the
        # test a worker it sends home holds, and runs it on another worker) reports no phase:
        # leave its directory and results to the run that will really happen.
        if item.stash[_PHASES]:
            info = _convert(profile_dir, root / f"{name}.profdata.zst")
            _write_json(root / f"{name}.json", {"nodeid": item.nodeid, "phases": item.stash[_PHASES],
                                                "started": started, "finished": time.time(), **info})


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_setup(item: pytest.Item) -> None:
    if _root(item.config):
        _seed_random(item)


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_call(item: pytest.Item) -> None:
    if _root(item.config):
        _seed_random(item)


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_teardown(item: pytest.Item, nextitem):
    if _DIR in item.stash:
        item.stash[_PHASES]["dumped_before_teardown"] = _dump_live_profiles(item.stash[_DIR])
    yield


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item: pytest.Item, call):
    outcome = yield
    if _PHASES in item.stash:
        report = outcome.get_result()
        item.stash[_PHASES][report.when] = {
            "outcome": _outcome(report),
            "duration": round(report.duration, 1),
            "longrepr": (str(report.longrepr)[-600:] if report.failed
                         else f"xfail: {report.wasxfail}"[-600:] if hasattr(report, "wasxfail")
                         else str(report.longrepr[2])[-600:] if report.skipped and isinstance(report.longrepr, tuple)
                         else None),
        }


def pytest_deselected(items: list[pytest.Item]) -> None:
    """Record deselected tests (collection-time skip rules) as skipped, so they have an outcome."""
    if not items or (root := _root(items[0].config)) is None:
        return
    root.mkdir(parents=True, exist_ok=True)
    for item in items:
        _write_json(root / f"{slug(item.nodeid)}.json",
                    {"nodeid": item.nodeid, "phases": {"setup": {"outcome": "skipped", "duration": 0.0,
                                                                 "longrepr": "deselected at collection"}}})
