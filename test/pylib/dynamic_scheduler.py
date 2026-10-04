#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""CPU- and RAM-aware dynamic scheduler for pytest-xdist.

How it works
------------
Every test run records, per test, its wall time, the CPU-seconds its
whole cgroup consumed, its peak anonymous memory, and whether it was
contended (see ``pytest_runtest_protocol`` in
``test/pylib/runner.py``).  The samples land in
``<tmpdir>/dynamic_samples_<HOST_ID>.jsonl`` and are merged into
``<tmpdir>/profile.json`` at the end of the run.

On the next run the controller (the xdist master) uses that profile to
admit tests: a worker that holds a test is only allowed to start it
when the predicted total CPU and RAM of everything already running
plus this test fits into the budget.  The xdist worker protocol makes
that possible without any change to the workers: a worker pops a test
and then blocks until it knows the *next* index (or a shutdown), so
sending one more index is what starts the held test.

The scheduler does not trust the profile blindly.  Admission is
closed-loop: it measures the load the tests actually cause right now
(the worker cgroups plus the services test.py starts), adds the
predicted cost of tests admitted in the last couple of seconds that
the measurement cannot show yet, and admits the next test only if that
plus the test's own cost stays under the target.  A test is predicted
from its average parallelism over its whole run: per-test CPU curves
were measured against it and bought nothing.  A kernel pressure-stall
(PSI) guard shrinks the target when the machine is oversubscribed and
grows it back when calm, and one test is always allowed to start when
nothing is running so a run can never dead-lock.
"""

from __future__ import annotations

import json
import logging
import math
import os
import re
import shlex
import sqlite3
import statistics
import threading
import time
from datetime import datetime
import bisect
from bisect import bisect_left
from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import psutil
import pytest
import yaml

from test import HOST_ID, TOP_SRC_DIR
from test.pylib import sched_dir
from test.pylib.container_accounting import all_container_cgroups, worker_anon
from test.pylib.db.model import SchedulerMetric
from test.pylib.db.writer import DEFAULT_DB_NAME, SCHEDULER_METRICS_TABLE, SQLiteWriter

if TYPE_CHECKING:
    from xdist.workermanage import WorkerController

logger = logging.getLogger(__name__)

GB = 1e9
PROFILE_FILENAME = "profile.json"
SAMPLES_GLOB = "dynamic_samples_*.jsonl"

# Minimum spacing between two consecutive PSI-triggered budget cuts.
PRESSURE_COOLDOWN = 10.0
EMA_ALPHA = 0.3
MEM_DECAY = 0.7
# A run whose cgroup waited for a CPU more than this share of its wall
# time was contended: its wall time and parallelism are not learned
# (only its CPU work).
CONTENDED_STALL_FRAC = 0.10
PROFILE_VERSION = 4       # bumped when the recorded format changes; older files are ignored
TAIL_SECONDS = 20.0       # below this a test is too short to be worth holding capacity for
HELD_FORCE_SECONDS = 60.0 # a worker blocked this long on one test is worth more than the budget
# A reservation is a test's *peak* memory held for its whole life, and
# peaks rarely coincide.  Measured on a run with no profile: 34 GB
# reserved against a 36 GB budget while the tests were really using 13
# GB and 38 GB sat free, with four thousand tests queued and the
# machine at three cores.  So when the reservations are exhausted but
# the measurement says the budget is genuinely free, admission may go
# on -- with what the just-admitted tests have not taken yet counted
# at their predicted size, so that two tests can never be handed the
# same free gigabyte.
MEM_RAMP_SECONDS = 30.0   # how long a test is assumed to still be growing into its predicted peak
# Static memory guesses, for the run that has no profile yet.
# Measured over the 11,433 tests of the release suite (median / p90 /
# max, GB):
#   boost and the other C++ suites    0.19 / 0.36 / 1.84
#   cqlpy, alternator, cql, rest_api  0.20 / 0.24 / 0.48
#   cluster                           0.19 / 0.44 / 3.72
#   scylla_gdb                        2.44 / 3.46 / 16.01
# The seastar -m flag is what a test is *allowed* to reserve, not what
# it takes: a -m2G boost case peaks at a fifth of a gigabyte.  Reading
# it as the estimate booked 2.3 GB apiece, so seventeen tests could
# exhaust a 36 GB budget and the rest of the run starved; meanwhile
# the one suite that is genuinely hungry was booked at 1 GB and
# swapped the machine.  These are set near each family's p90, which
# also keeps a full set of workers inside the budget: 64 x 0.5 GB is
# 32 GB.
STATIC_MEM_CPP = 0.5 * 10**9
STATIC_MEM_PY = 0.3 * 10**9
STATIC_MEM_CLUSTER = 0.6 * 10**9
STATIC_MEM_GDB = 3.5 * 10**9

# First-run guesses, before the profile knows anything.  A debug build
# has no optimisation and carries the sanitizers, so the same test
# costs several times what it costs in release: measured on this tree,
# a C++ case peaks at 1.0 G rather than 0.04 G, an alternator test at
# 1.4 G rather than 0.25 G, a cluster test at 4.2 G.  Guessing release
# numbers for a debug run is what drove the machine into swap in under
# a minute, because admission counts a just-started test at its
# predicted peak -- and the prediction was five times too small.
# Over-guessing only costs some parallelism on the very first run;
# under-guessing costs the machine.  These are p90s, not medians, and
# deliberately so.  A run with no profile is pricing risk, not
# describing a typical test: measured here, cluster tests in debug
# range from 4.3 GB to 8.4 GB, and a single one of them taking 8.4
# against a 4.5 guess is enough to put the box into swap in four
# seconds, which no feedback loop can react to.  Over-pricing costs
# some parallelism on the first run only, because the profile replaces
# these with real numbers.
STATIC_MEM_BY_MODE = {
    # Anonymous peaks measured over a full debug run (10176 tests):
    # cpp p90 1.3G (max 7.0G) over 4372 cases, py p90 1.6G (p99 1.7G)
    # over 5200, cluster p90 5.5G (p99 9.3G, max 11.4G).  Priced above
    # the p90 because a run with no profile is pricing risk; the
    # profile takes over after one run.
    "debug": {"cpp": 2.0 * 10**9, "py": 1.8 * 10**9, "cluster": 8.5 * 10**9, "gdb": 8.0 * 10**9},
    # Firm peaks (anonymous + mapped) learned over a full release run
    # (15,147 tests), p90 each: cpp 0.30G (max 2.4G) over 4553, py
    # 0.30G (max 0.5G) over 5283, cluster outside dtest 0.62G (max
    # 1.7G) over 1539, cluster/dtest 0.90G (p99 3.3G, max 9.4G) over
    # 3726, gdb 6.1G (max 13G) over 46.  The dtests are the heavy tail
    # of the cluster suite, so they get their own guess rather than
    # dragging the rest of it up.
    "release": {"cpp": 0.3 * 10**9, "py": 0.3 * 10**9, "cluster": 0.62 * 10**9, "dtest": 0.9 * 10**9,
                "gdb": 6.1 * 10**9},
}
STATIC_MEM_MODE_ALIAS = {"sanitize": "debug", "coverage": "debug"}
# No memory is held back.  The budget is what is free once the workers
# are up, and a fixed share of RAM kept aside on top of that (12% of
# the machine, and a 70% ceiling) only cost parallelism: on a 33 GB
# machine with 22 GB free it left 17 GB for debug cluster tests that
# take 7-11 GB each.  Admission watches RAM itself: a test starts only
# while the machine has its predicted peak available.  When no waiting
# test fits in what is available, idle workers -- each holding its
# last module's cluster -- are sent home, one per this many seconds,
# so that one retirement is visible before the next.
RETIRE_COOLDOWN = 10.0
# Admission checks every test against the RAM the machine reports, and
# swap is not read at all: the kernel pages cold anonymous memory out
# as ordinary reclaim, with gigabytes free.  Measured on a healthy
# debug run, the tests' swap grew by 0.3-0.8 GB in ten seconds five
# times while 8.5-18 GB was available.  What does mark the machine
# short is the tests losing time to memory: their cgroup's
# memory.pressure "full" -- the share of time every runnable task of
# theirs was stalled on reclaim, swap-in or refaults at once.  At
# MEM_STALL_LIMIT percent over ten seconds nothing more is admitted,
# forced or spawned until it falls back.  The healthy run peaked at
# 4.6% for seconds; the run that filled swap and stalled the machine
# sat at 15-21% for a quarter of an hour.
MEM_STALL_LIMIT = 10.0
# The pool starts at one worker per CPU and grows, one worker at a
# time, while every worker is busy and the machine has CPU and memory
# to spare.  Starting 64 workers at once had half of them holding a
# test and their last module's memory for most of the run: the run
# kept 33-36 tests going on average.  A new worker imports the
# framework and collects the whole suite, about a core and a quarter
# of a gigabyte for several seconds, so the next one waits until it
# has collected, and at least POOL_GROW_SECONDS.  Until then its
# expected overhead is charged to the forecast, so admission cannot
# hand that memory to a test.  A worker's overhead is measured, not
# assumed: what its cgroup holds the moment each test starts.
POOL_GROW_SECONDS = 10.0
POOL_SPAWN_TIMEOUT = 120.0      # a spawned worker that has not collected by then is written off
POOL_WORKER_PRIOR = 0.25 * 10**9
# An idle worker holding this much more than its peers is sent home,
# its held test running on the way out, and the pool grows a fresh one
# if there is room: a worker keeps whatever its tests left behind, and
# one that keeps a lot is memory no test can use.
POOL_BLOAT_MIN = 1.5 * 10**9
POOL_BLOAT_FACTOR = 3.0
# A worker with nothing running for this long is drained: when its
# held test starts it gets the shutdown marker instead of a successor,
# and it exits after that test.  A held test cannot be recalled, so
# this is the only way to take a worker out without losing its test.
POOL_IDLE_SECONDS = 60.0
# The pool never goes below this share of the one it started with.
# Holding the starting pool kept 32 workers through the tail of a
# release run while 2 to 13 tests ran: 19 to 30 idle workers, 0.25-0.5
# GB of heap each, in the stretch where the heaviest dtests were
# swapping.  If the work comes back, the pool grows again.
POOL_FLOOR_FRACTION = 0.25
# Admission gates on measured headroom: what the machine has
# available, minus what the tests admitted recently have not taken
# yet, minus this reserve.  The reserve only keeps admission from
# spending the very last of MemAvailable; a real shortage shows up as
# the tests stalling on memory (MEM_STALL_LIMIT), not as a number to
# hold back in advance.
MEM_RESERVE = 512 * 2**20
# Admission forecasts each test's peak instead of trusting one price
# for it.  A test's memory is known only once it has taken it, and by
# then it cannot be recalled, so what admission needs is where the
# running tests are going, not where they are.  Measured over the
# release dtests, what a test already holds says a lot about that: the
# average peak is 0.54 GB, but a test that has reached 1 GB ends at
# 2.0 GB on average, and one at 2 GB at 3.3 GB.  So each running test
# counts at its expected peak *given what it holds now*, less what it
# holds, and the forecast rises as a heavy test shows itself.  No
# margin is kept on top of the expectation: the reserve and the
# forecast's own correction as tests grow are what protect the
# machine, and a margin priced in the spread of a heavy-tailed kind
# cost most of a run's dtest parallelism.  A test that has stopped
# growing for this long, and has outrun its expected wall time, fades
# out of the forecast with this half-life: it has most likely reached
# its peak, and counting the rest of its distribution for the rest of
# its life would starve the run.
PLATEAU_SECONDS = 30.0
# A file nobody has measured yet sends one scout first.  Its siblings
# wait until the scout has finished, or has run at least SCOUT_SECONDS
# and then stopped growing for SCOUT_QUIET_SECONDS, or has run
# SCOUT_MAX_SECONDS; then they start, forecast on what the scout
# holds.  A fixed wait would release them before a heavy test shows
# itself: the heaviest dtests take a minute or more to grow into their
# peak.  Only the heavy-tailed kinds scout: a cqlpy or boost file is
# cheap and uniform, and waiting there would cost more than it could
# save.
SCOUT_SECONDS = 20.0
SCOUT_QUIET_SECONDS = 10.0
SCOUT_MAX_SECONDS = 120.0
SCOUT_KINDS = frozenset({"cluster", "dtest", "gdb"})
# Peak-memory distributions for the run that has no profile yet, as
# quantiles at PEAK_QUANTILES.  Release: firm peaks over a full
# release run (15,147 tests).  Debug: anonymous peaks of the cluster
# suite over a full debug run (10,176 tests); the other debug kinds
# have no measured distribution and keep their point guess.
PEAK_QUANTILES = (0.0, 0.5, 0.9, 0.99, 1.0)
PEAK_KNOTS_BY_MODE = {
    "release": {
        "dtest": (0.15e9, 0.35e9, 0.90e9, 3.3e9, 9.4e9),
        "cluster": (0.15e9, 0.32e9, 0.62e9, 1.02e9, 1.7e9),
        "py": (0.15e9, 0.28e9, 0.30e9, 0.33e9, 0.53e9),
        "cpp": (0.05e9, 0.24e9, 0.30e9, 0.38e9, 2.4e9),
        "gdb": (1.0e9, 2.9e9, 6.1e9, 13.1e9, 13.1e9),
    },
    "debug": {
        "dtest": (1.0e9, 2.0e9, 5.5e9, 9.3e9, 11.4e9),
        "cluster": (1.0e9, 2.0e9, 5.5e9, 9.3e9, 11.4e9),
    },
}


class PeakDist:
    """A distribution of peak memory, as equally weighted points, for
    conditional forecasts."""

    POINTS = 200

    def __init__(self, values: Iterable[float]):
        self.values = sorted(values)

    @classmethod
    def from_knots(cls, knots: Sequence[float]) -> PeakDist:
        """Points of the piecewise-linear quantile function through
        the knots."""
        points = []
        for i in range(cls.POINTS):
            q = (i + 0.5) / cls.POINTS
            j = next(j for j in range(1, len(PEAK_QUANTILES)) if q <= PEAK_QUANTILES[j])
            lo, hi = PEAK_QUANTILES[j - 1], PEAK_QUANTILES[j]
            points.append(knots[j - 1] + (q - lo) / (hi - lo) * (knots[j] - knots[j - 1]))
        return cls(points)

    def conditional(self, held: float) -> float | None:
        """Expected peak, given the test already holds `held`; None
        past every point."""
        tail = self.values[bisect_left(self.values, held):]
        if not tail:
            return None
        return max(statistics.fmean(tail), held)

# A test admitted less than this long ago is not visible in the
# measured load yet; its predicted cost is added on top of the
# measurement.
RAMP_SECONDS = 2.0
START_CEILING = 2.0       # cores admissible in the very first pass, before anything is measured

# Admission settings.  They are fixed, not options: the scheduler is
# tuned to them.
CPU_TARGET = 0.95          # share of the CPUs the tests' load is kept under
CPU_OVERCOMMIT = 1.5       # CPU bookings never exceed this many times the CPUs
BURST = 0.05               # bookings grow by at most this share of the CPUs a second
PSI_CPU_LIMIT = 25.0       # tests' CPU "some avg10" % above which admission pauses, target shrinks
PSI_MEM_LIMIT = 5.0        # machine memory "some avg10" % above which admission pauses
DEPTH = 1                  # tests a worker may hold queued behind the one it runs
SHORT_SECONDS = 1.0        # tests expected to take less may also hold one more (see _depth)
K_SIGMA = 0.5              # standard deviations of cores added to a test's booking
DEFAULT_COST = (2.0, 2 * GB)   # cores, memory of a test with no profile and no static hint
METRICS_SECONDS = 2.0      # how often --gather-metrics samples the scheduler
POOL_MAX_PER_CPU = 3       # the pool grows to this many workers per CPU at most


def profile_key(nodeid: str) -> str:
    """``cqlpy/test_x.py::test_y[p].dev.1`` ->
    ``dev|cqlpy/test_x.py::test_y[p]``.

    ``modify_pytest_item`` appends ``.<mode>.<run_id>`` to every item;
    strip it so repeats and runs share one entry, and put the mode in
    front because costs differ per build mode.
    """
    parts = nodeid.rsplit(".", 2)
    if len(parts) == 3:
        base, mode, _run_id = parts
    else:
        base, mode = nodeid, "unknown"
    return f"{mode}|{base}"


def file_of_key(key: str) -> str:
    """``dev|cqlpy/test_x.py::test_y`` -> ``dev|cqlpy/test_x.py``."""
    return key.split("::", 1)[0]


def parse_size(text: str) -> float:
    m = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([KMGT]?)i?B?\s*", text, re.IGNORECASE)
    if not m:
        raise ValueError(f"bad size: {text!r}")
    mult = {"": 1, "K": 1e3, "M": 1e6, "G": 1e9, "T": 1e12}[m.group(2).upper()]
    return float(m.group(1)) * mult


def parse_seastar_args(args: str) -> tuple[float | None, float | None]:
    """Return (cores, memory_bytes) from a seastar arg string like
    ``-c2 -m2G``."""
    cores = mem = None
    tokens = shlex.split(args)
    i = 0
    while i < len(tokens):
        tok = tokens[i]
        for flag, kind in (("-c", "cores"), ("--smp", "cores"), ("-m", "mem"), ("--memory", "mem")):
            value = None
            if tok == flag and i + 1 < len(tokens):
                value = tokens[i + 1]
                i += 1
            elif tok.startswith(flag) and len(tok) > len(flag) and (flag.startswith("--") and tok[len(flag)] == "=" or not flag.startswith("--")):
                value = tok[len(flag):].lstrip("=")
            if value is not None:
                try:
                    if kind == "cores":
                        cores = float(value)
                    else:
                        mem = parse_size(value)
                except ValueError:
                    pass
                break
        i += 1
    return cores, mem


@dataclass
class Cost:
    cores: float
    mem: float
    wall: float
    source: str


class CostModel:
    """Loads/saves the profile and predicts the cost of a test from
    its nodeid."""

    def __init__(self, profile_path: Path, ncpus: int, k_sigma: float = 1.0,
                 default_cost: tuple[float, float] = (2.0, 2 * GB), mode: str = "release"):
        self.profile_path = profile_path
        self.ncpus = ncpus
        self.k_sigma = k_sigma
        self.mode = STATIC_MEM_MODE_ALIAS.get(mode, mode)   # for a key that names no known mode
        self._kind_dists: dict[tuple[str, str], PeakDist | None] = {}
        self._sibling_dists: dict[str, PeakDist] = {}       # file -> its measured peaks
        self.default_cores, self.default_mem = default_cost
        self.tests: dict[str, dict[str, Any]] = {}
        self.files: dict[str, dict[str, Any]] = {}
        self._file_index: dict[str, list[str]] | None = None
        self._static_cache: dict[str, dict[str, Any]] = {}
        self.load()

    # -- persistence -------------------------------------------------

    def load(self) -> None:
        try:
            data = json.loads(self.profile_path.read_text())
        except FileNotFoundError:
            return
        except (OSError, ValueError) as e:
            logger.warning("dynamic scheduler: cannot read profile %s: %s", self.profile_path, e)
            return
        if data.get("version") != PROFILE_VERSION:
            logger.info("dynamic scheduler: ignoring profile %s written in format v%s", self.profile_path, data.get("version"))
            return
        self.tests = data.get("tests", {})
        self.files = data.get("files", {})
        self._file_index = None
        self._sibling_dists.clear()

    def save(self) -> None:
        data = {"version": PROFILE_VERSION, "tests": self.tests, "files": self.files}
        tmp = self.profile_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, indent=1, sort_keys=True))
        os.replace(tmp, self.profile_path)

    # -- learning ----------------------------------------------------

    def learn(self, sample: dict[str, Any]) -> None:
        """Merge one execution record.

        Record: wall, usage_sec (cgroup cpu_time = user + system),
        memory_peak (anon peak of the cgroup), cpu_stall_frac
        (contention), first_in_file.

        CPU work (CPU-seconds) is learned from every run.  Natural
        parallelism (cpu_time / wall) and uncontended runtime are
        learned only from runs that were not contended: a contended
        run has a longer wall time and would teach a lower
        parallelism, which would admit more tests, which would contend
        more.  Memory adapts asymmetrically: a higher peak is taken at
        once, a lower one only after repeated evidence.
        """
        key = sample["key"]
        wall = sample.get("wall")
        if wall is None or wall <= 0:
            return
        usage = sample.get("usage_sec")
        cores = usage / wall if usage is not None else None
        mem = sample.get("memory_peak")
        stall = sample.get("cpu_stall_frac")
        contended = bool(sample.get("cpu_contended")) or (stall is not None and stall > CONTENDED_STALL_FRAC)
        entry = self.tests.get(key)
        first = bool(sample.get("first_in_file"))
        fkey = file_of_key(key)

        if first and entry is not None and entry.get("n", 0) >= 1:
            # The first test of a file on a worker pays for the module
            # fixture (cluster start).  Its time and cores go to the
            # file's setup cost, from the excess over the test's own
            # estimate, and stay out of the test's entry.  Its memory
            # does not: what a worker holds at the peak of such a test
            # is what the test costs on a fresh worker, and a small
            # file is first on every worker, so skipping these samples
            # froze its tests' peaks at the one that created them.
            setup = self.files.setdefault(fkey, {"setup_cores": 0.0, "n": 0})
            extra_cores = max(0.0, cores - (entry.get("cores") or 0.0)) if cores is not None else 0.0
            setup["setup_cores"] = _ema(setup["setup_cores"], extra_cores, setup["n"])
            setup["n"] += 1
            self._learn_mem(entry, mem, fkey)
            return

        if entry is None:
            entry = self.tests[key] = {"wall": wall, "cores": cores, "mem": mem,
                                       "var_cores": 0.0, "n": 0, "n_unc": 0, "low_streak": 0}
            self._file_index = None
            self._sibling_dists.pop(fkey, None)
        n = entry["n"]
        # parallelism and runtime: only from uncontended runs (or
        # until one exists)
        if not contended or entry.get("n_unc", 0) == 0:
            n_unc = entry.get("n_unc", 0)
            entry["wall"] = _ema(entry["wall"], wall, n_unc)
            if cores is not None:
                old = entry.get("cores")
                if old is not None:
                    entry["var_cores"] = _ema(entry.get("var_cores", 0.0), (cores - old) ** 2, n_unc)
                entry["cores"] = _ema(old, cores, n_unc)
            if not contended:
                entry["n_unc"] = n_unc + 1
        self._learn_mem(entry, mem, fkey)
        entry["n"] = n + 1

    def predict_at(self, nodeid: str, elapsed: float, future: bool = False) -> float:
        """Cores a test is expected to burn: its natural parallelism,
        over its whole run.

        Per-test CPU curves -- cores at each point of a test's run, so
        a short test could start in a long one's idle phase -- were
        measured against this on the full release suite: the same wall
        time within noise, the same CPU-seconds, a median per-test
        wall ratio of 1.00.  For a look-ahead (`future=True`) a test
        well past its expected wall time counts as finished, otherwise
        the costs of many short tests would pile up.
        """
        cost = self.cost(nodeid)
        if future and elapsed > cost.wall * 1.5 + 1.0:
            return 0.0
        return cost.cores

    # -- prediction --------------------------------------------------

    def cost(self, nodeid: str) -> Cost:
        """Scheduling request for a test.

        cores: expected natural parallelism (cpu_time / uncontended
        wall) plus a small variance margin; this is what the scheduler
        reserves.  Instantaneous peaks are not reserved: bursts of
        independent tests multiplex.  mem: the conservative peak
        (asymmetric estimate), with no margin on top.  The peak is
        already the worst moment of a test's life, held for all of it,
        and peaks rarely coincide; a margin on top priced debug
        cluster tests at 12-14 GB and let one or two of them run at a
        time.  The live RAM check in admission catches a peak that
        really was an underestimate.  wall: expected uncontended
        runtime (ordering).
        """
        key = profile_key(nodeid)
        entry = self.tests.get(key)
        if entry and entry.get("cores") is not None and entry.get("mem") is not None:
            cores = entry["cores"] + self.k_sigma * math.sqrt(max(0.0, entry.get("var_cores", 0.0)))
            return self._finish(cores, entry["mem"], entry["wall"], "profile")
        fam = self._family(key)
        if fam is not None:
            return self._finish(fam[0], fam[1], entry["wall"] if entry else fam[2], "family")
        static = self._static(nodeid, key)
        return self._finish(static["cores"], static["mem"], entry["wall"] if entry else static["wall"], static["source"])

    def setup_cost(self, nodeid: str) -> float:
        """Extra cores paid when a worker switches to this test's file
        (module fixture, cluster start)."""
        fkey = file_of_key(profile_key(nodeid))
        setup = self.files.get(fkey)
        return setup["setup_cores"] if setup else 0.0

    def _finish(self, cores: float, mem: float, wall: float, source: str) -> Cost:
        cores = min(float(self.ncpus), max(0.05, cores))
        return Cost(cores=cores, mem=max(50e6, float(mem)), wall=max(0.05, float(wall)), source=source)

    def _family(self, key: str) -> tuple[float, float, float] | None:
        siblings = [s for s in self._siblings(key) if s.get("cores") is not None and s.get("mem") is not None]
        if not siblings:
            return None
        return (statistics.median(s["cores"] for s in siblings),
                max(s["mem"] for s in siblings),
                statistics.median(s["wall"] for s in siblings))

    def _learn_mem(self, entry: dict[str, Any], mem: float | None, fkey: str) -> None:
        """Memory adapts asymmetrically: a higher peak is taken at
        once, a lower one slowly."""
        if mem is None:
            return
        old_mem = entry.get("mem")
        if not old_mem or mem >= old_mem:
            entry["mem"] = mem                      # up: immediately
            entry["low_streak"] = 0
        elif mem < 0.5 * old_mem:
            entry["low_streak"] = entry.get("low_streak", 0) + 1
            if entry["low_streak"] >= 3:            # down: slowly, after repeated evidence
                entry["mem"] = max(mem, old_mem * 0.8)
                entry["low_streak"] = 0
        else:
            entry["low_streak"] = 0
        self._sibling_dists.pop(fkey, None)

    def _siblings(self, key: str) -> list[dict[str, Any]]:
        if self._file_index is None:
            idx: dict[str, list[str]] = defaultdict(list)
            for k in self.tests:
                idx[file_of_key(k)].append(k)
            self._file_index = idx
        return [self.tests[k] for k in self._file_index.get(file_of_key(key), ())]

    def sibling_peaks(self, key: str) -> list[float]:
        """Measured peaks of the tests in the same file."""
        return [s["mem"] for s in self._siblings(key) if s.get("mem") is not None]

    def peak_kind(self, key: str) -> str:
        """The family whose peaks a test is forecast from."""
        path = key.split("|", 1)[1].split("::", 1)[0]
        if path.endswith(".cc"):
            return "cpp"
        if path.startswith("scylla_gdb/"):
            return "gdb"
        if path.startswith("cluster/dtest/"):
            return "dtest"
        if path.startswith("cluster/"):
            return "cluster"
        return "py"

    def mode_of(self, key: str) -> str:
        """The build mode a profile key belongs to.

        Every test carries its own mode: a run of --mode dev --mode
        debug prices its debug tests with debug's numbers, not with
        those of whichever mode came first.
        """
        mode = STATIC_MEM_MODE_ALIAS.get(key.split("|", 1)[0], key.split("|", 1)[0])
        return mode if mode in ("dev", "release", "debug") else self.mode

    def kind_dist(self, kind: str, mode: str | None = None) -> PeakDist | None:
        """The first-run peak distribution of a family in a build
        mode, if one was measured."""
        mode = mode or self.mode
        if (mode, kind) not in self._kind_dists:
            knots = PEAK_KNOTS_BY_MODE.get(mode, {}).get(kind)
            self._kind_dists[(mode, kind)] = PeakDist.from_knots(knots) if knots else None
        return self._kind_dists[(mode, kind)]

    def sibling_dist(self, key: str) -> PeakDist:
        """The measured peaks of a test's file, sorted once per change
        rather than per forecast."""
        fkey = file_of_key(key)
        dist = self._sibling_dists.get(fkey)
        if dist is None:
            dist = self._sibling_dists[fkey] = PeakDist(self.sibling_peaks(key))
        return dist

    def static_mem(self, kind: str, default: float, mode: str | None = None) -> float:
        """Static memory guess for a family, for a build mode."""
        return STATIC_MEM_BY_MODE.get(mode or self.mode, {}).get(kind, default)

    def _static(self, nodeid: str, key: str) -> dict[str, Any]:
        """Cost guess from the test's location and the suite's static
        config."""
        base = key.split("|", 1)[1]
        path = base.split("::", 1)[0]
        suite = path.split("/", 1)[0]
        mode = self.mode_of(key)
        if path.endswith(".cc"):
            return self._static_cpp(suite, path, base, mode)
        if suite == "scylla_gdb":
            # gdb loads the debug info of the whole binary, and these
            # are the only tests that ever need tens of gigabytes.  A
            # flat 1 GB guess here is what put the machine into swap
            # on the first run.
            return {"cores": 1.0, "mem": self.static_mem("gdb", STATIC_MEM_GDB, mode),
                    "wall": 30.0, "source": "static-gdb"}
        if suite == "cluster":
            cluster_mem = self.static_mem("cluster", STATIC_MEM_CLUSTER, mode)
            if path.startswith("cluster/dtest/"):
                return {"cores": 2.0, "mem": self.static_mem("dtest", cluster_mem, mode),
                        "wall": 15.0, "source": "static-dtest"}
            return {"cores": 2.0, "mem": cluster_mem, "wall": 15.0, "source": "static-cluster"}
        # cqlpy, alternator, cql, rest_api, ...: one shared
        # single-node cluster per module.
        return {"cores": 1.0, "mem": self.static_mem("py", STATIC_MEM_PY, mode),
                "wall": 1.0, "source": "static-python"}

    def _static_cpp(self, suite: str, path: str, base: str, mode: str | None = None) -> dict[str, Any]:
        test_name = Path(path).stem
        case = base.split("::", 1)[1] if "::" in base else ""
        index = 0
        m = re.fullmatch(r"(.*)\.(\d+)", case)
        if m:
            index = int(m.group(2))
        cache_key = f"{mode or self.mode}:{suite}/{test_name}.{index}"
        if cache_key not in self._static_cache:
            cores, mem = 2.0, self.static_mem("cpp", STATIC_MEM_CPP, mode)
            custom_args = self._suite_config(suite).get("custom_args", {}).get(test_name)
            if custom_args:
                args = custom_args[index - 1] if index and index <= len(custom_args) else custom_args[0]
                c, mm = parse_seastar_args(args)
                cores = c if c is not None else cores
                # -m is a ceiling the test is unlikely to reach, so it
                # only ever lowers the guess: a -m256M case cannot
                # take more than that, whatever the p90 is.
                if mm is not None:
                    mem = min(mem, mm + 0.1 * GB)
            self._static_cache[cache_key] = {"cores": cores, "mem": mem, "wall": 2.0, "source": "static-cpp"}
        return self._static_cache[cache_key]

    def _suite_config(self, suite: str) -> dict[str, Any]:
        cache_key = f"cfg:{suite}"
        if cache_key not in self._static_cache:
            cfg: dict[str, Any] = {}
            cfg_path = TOP_SRC_DIR / "test" / suite / "test_config.yaml"
            try:
                cfg = yaml.safe_load(cfg_path.read_text()) or {}
            except OSError:
                pass
            self._static_cache[cache_key] = cfg
        return self._static_cache[cache_key]


def _ema(old: float | None, new: float, n: int) -> float:
    if old is None or n == 0:
        return float(new)
    return (1 - EMA_ALPHA) * float(old) + EMA_ALPHA * float(new)


# --------------------------------------------------------------------
# Live machine readings
# --------------------------------------------------------------------


def read_psi(kind: str, path: Path | None = None, line_kind: str = "some") -> float:
    """Return the ``some`` (or ``full``) ``avg10`` percentage from
    /proc/pressure/<kind>, or from a cgroup's own
    <path>/<kind>.pressure when a path is given (the stalls of *our*
    tests, not of everything else on the machine), or 0 if
    unavailable."""
    try:
        with open(path / f"{kind}.pressure" if path is not None else f"/proc/pressure/{kind}") as f:
            for line in f:
                if line.startswith(line_kind):
                    for tok in line.split():
                        if tok.startswith("avg10="):
                            return float(tok[6:])
    except OSError:
        pass
    return 0.0


class CgroupReader:
    """Reads live CPU (cores) and memory (bytes) of a worker's cgroup
    from the controller."""

    def __init__(self, cgroup_tests: Path | None):
        self.base = cgroup_tests
        self._last: dict[str, tuple[float, float]] = {}   # worker -> (monotonic, usage_sec)
        self._cores: dict[str, float] = {}
        self._window: dict[str, tuple[float, float]] = {}  # path key -> (from, to) of its last rate()
        # worker -> cgroups of the containers it started
        # (test/pylib/container_accounting.py)
        self._containers: dict[str, list[Path]] = {}
        self._mem: dict[str, float | None] = {}             # worker -> memory() this pass

    def refresh(self, worker_ids: Iterable[str]) -> None:
        if self.base is None:
            return
        self._containers = all_container_cgroups()
        self._mem = {}
        now = time.monotonic()
        for wid in worker_ids:
            usage = self._read_usage(wid)
            if usage is None:
                continue
            prev = self._last.get(wid)
            if prev is None:
                self._last[wid] = (now, usage)
                continue
            dt = now - prev[0]
            if dt >= 0.5:
                self._cores[wid] = max(0.0, (usage - prev[1]) / dt)
                self._last[wid] = (now, usage)

    def cores(self, wid: str) -> float | None:
        return self._cores.get(wid)

    def rate(self, path: Path) -> float | None:
        """Cores currently used by an arbitrary cgroup (hierarchical),
        sampled >= 0.5 s apart."""
        key = f"path:{path}"
        usage = self._read_usage_path(path)
        if usage is None:
            return None
        now = time.monotonic()
        prev = self._last.get(key)
        if prev is None:
            self._last[key] = (now, usage)
            return self._cores.get(key)
        dt = now - prev[0]
        if dt >= 0.5:
            self._cores[key] = max(0.0, (usage - prev[1]) / dt)
            self._window[key] = (prev[0], now)
            self._last[key] = (now, usage)
        return self._cores.get(key)

    def window(self, path: Path) -> tuple[float, float] | None:
        """The monotonic interval the last rate() of this cgroup averages."""
        return self._window.get(f"path:{path}")

    @staticmethod
    def _read_usage_path(path: Path) -> float | None:
        try:
            with open(path / "cpu.stat") as f:
                for line in f:
                    if line.startswith("usage_usec"):
                        return float(line.split()[1]) / 1e6
        except (OSError, ValueError, IndexError):
            return None
        return None

    def memory(self, wid: str) -> float | None:
        """A worker's anonymous memory, the containers it started
        included.

        Anonymous, like the peaks the profile learns
        (resource_gather's anon_peak), and read by the same function:
        the forecast subtracts one from the other, and counting the
        worker's mapped file pages on one side only -- the Scylla
        binary, in debug a gigabyte -- read them as growth the test
        had already made.  Read once per pass in refresh(): selection
        asks for it per candidate and per worker, many times a pass.
        """
        if self.base is None:
            return None
        if wid not in self._mem:
            self._mem[wid] = worker_anon(self.base / wid, self._containers.get(wid, ()))
        return self._mem[wid]

    def _read_usage(self, wid: str) -> float | None:
        try:
            with open(self.base / wid / "cpu.stat") as f:
                for line in f:
                    if line.startswith("usage_usec"):
                        return float(line.split()[1]) / 1e6
        except (OSError, ValueError, IndexError):
            return None
        return None


# --------------------------------------------------------------------
# The scheduler
# --------------------------------------------------------------------

class DynamicScheduling:
    """xdist scheduler that admits tests against a CPU and RAM budget.

    Bookkeeping per worker: ``node2pending[node]`` is the list of
    indices sent to it, in order.  By the worker protocol every index
    except the last one is *committed* (running or guaranteed to run
    next); the last one is *held* (the worker is blocked waiting for
    its successor).  Sending a successor or a shutdown commits the
    held item, and that is the moment its cost is charged.
    """

    def __init__(self, config: pytest.Config, log: Any = None, *, model: CostModel | None = None,
                 ncpus: int | None = None, mem_total: float | None = None, cgroup_tests: Path | None = None,
                 now: Any = time.monotonic, available_fn: Any = None, spawn_worker: Any = None,
                 cpu_target: float = CPU_TARGET, cpu_overcommit: float = CPU_OVERCOMMIT,
                 burst: float = BURST, psi_cpu_limit: float = PSI_CPU_LIMIT,
                 psi_mem_limit: float = PSI_MEM_LIMIT, depth: int = DEPTH, k_sigma: float = K_SIGMA,
                 default_cost: tuple[float, float] = DEFAULT_COST, max_workers: int | None = None):
        from xdist.workermanage import parse_tx_spec_config
        self.config = config
        self.numnodes = len(parse_tx_spec_config(config))
        self.log = log.dynamicsched if log is not None else logger.debug
        self.now = now

        self.ncpus = ncpus or len(os.sched_getaffinity(0))
        self._machine_available = available_fn or (lambda: psutil.virtual_memory().available)
        self.mem_stall = 0.0                           # the tests' memory.pressure "full avg10", percent
        opt = config.getoption
        self.depth = max(1, int(depth))
        # The pool grows to POOL_MAX_PER_CPU workers per CPU, or stays at its start when that
        # is larger; 0 keeps it as it starts (unit tests).
        self.max_workers = (max(self.numnodes, POOL_MAX_PER_CPU * self.ncpus)
                            if max_workers is None else int(max_workers))
        self._spawn_worker = spawn_worker or self._spawn_xdist_worker
        self._spawning_ids: set[str] = set()      # workers this scheduler started that have not collected yet
        self._last_spawn = -math.inf
        self._idle_mem: dict[str, float] = {}     # worker id -> what its cgroup held when its last test started
        self._idle_since: dict[WorkerController, float] = {}    # worker -> since when nothing runs on it
        self._last_file_done: dict[WorkerController, str] = {}  # worker -> file of the last test it finished
        self._draining: set[WorkerController] = set()          # workers to shut down after their held test
        self._evicted: dict[WorkerController, int] = {}         # workers sent home told to skip their held test
        self._pool_floor = 0                      # fewest workers the pool keeps: a share of its start
        self._pool_start = 0                      # workers the run started with
        self.cpu_target_frac = float(cpu_target)
        self.cpu_target = self.cpu_target_frac * self.ncpus
        self.cpu_target_floor = 0.5 * self.ncpus
        total = mem_total if mem_total is not None else psutil.virtual_memory().total
        # What the tests may reserve: the memory that is actually free
        # when the run starts.  Not the memory the machine has - if
        # something else already holds a third of the box, handing
        # that third out to tests only means reclaiming page cache and
        # then swapping.  Measured once, at startup, so the budget is
        # a fixed number the reservations add up against rather than a
        # moving target; the live check in admission then catches what
        # changes underneath.  A first cut, before the workers exist;
        # replaced by a measurement once they do.
        free_now = self._available()
        self.mem_target = max(1 * GB, free_now)
        self.mem_reserve = MEM_RESERVE
        self._mem_at_commit: dict[int, float] = {}     # index -> its worker's firm memory when it started
        self._mem_track: dict[int, list[float]] = {}   # index -> [most it has held, when that last grew]
        self._started_at: dict[int, float] = {}        # index -> when it started running (not just committed)
        self._fc_mean = 0.0                            # forecast growth still to come, over the running tests
        self._file_held: dict[str, float] = defaultdict(float)  # file -> most any running test of it holds
        self._scout: dict[str, int] = {}               # file -> the index measuring it for its siblings
        self._scout_done: set[str] = set()
        self._last_forced = -math.inf
        self._mem_charged_workers = False
        self.psi_cpu_limit = float(psi_cpu_limit)
        self.psi_mem_limit = float(psi_mem_limit)
        self.measured_load = 0.0       # test-attributable cores, smoothed
        self._measured_raw: float | None = None    # the tests' cgroup over its last window, unsmoothed
        self._services_raw = 0.0                   # the services' cgroup, likewise
        self._recent_done: list[tuple[float, float, float]] = []   # (started, ended, cores) of finished tests
        # Reservations: acquired when a test is admitted (committed),
        # released on completion or worker loss.  Invariant:
        # sum(res_cpu) <= ncpus * cpu_overcommit at every point after
        # admission.  Memory is not reserved -- admission checks it
        # against the forecast -- and res_mem is kept only to be
        # logged.
        self.res_cpu: dict[int, float] = {}
        self.res_mem: dict[int, float] = {}
        self.cpu_overcommit = float(cpu_overcommit)
        self.cpu_ceiling = self.ncpus * max(1.0, self.cpu_overcommit)
        # Admission ramp.  Reservations may only *grow* at this rate,
        # so the run cannot commit two dozen held tests in one pass
        # before a single one of them is visible in the measurement.
        # It has to be slow: a reservation is a test's average
        # parallelism over its whole life, and a test that averages
        # 0.2 cores over ninety seconds still burns one or two while
        # its node boots, so the first admissions are worth several
        # times what they reserve.  Completions do not lower the
        # ceiling, so a worker that finishes a test refills its slot
        # with no delay: the ramp limits growth of the running set,
        # never churn within it.
        self.burst_rate = max(0.0, float(burst)) * self.ncpus
        self._ceiling = START_CEILING if self.burst_rate > 0 else self.cpu_ceiling
        # Anchored on the first scheduling pass, not on construction:
        # the workers spend their first half-minute collecting, and
        # the ramp must start when the first test could actually be
        # admitted.
        self._ceiling_t: float | None = None
        self._services_path = None
        base = cgroup_tests if cgroup_tests is not None else _cgroup_tests_path()
        if base is not None and base.exists():
            self._services_path = base.parent / "resource_gather"

        if model is None:
            modes = opt("--mode") or []
            model = CostModel(_profile_path(config), self.ncpus, k_sigma=k_sigma, default_cost=default_cost,
                              mode=(modes[0] if modes else "release"))
        self.model = model
        self.live = CgroupReader(cgroup_tests if cgroup_tests is not None else _cgroup_tests_path())

        self.node2collection: dict[WorkerController, list[str]] = {}
        self.node2pending: dict[WorkerController, list[int]] = {}
        self.node_file: dict[WorkerController, str | None] = {}
        self.shutdown_sent: set[WorkerController] = set()
        self.collection: list[str] | None = None
        self.pending_set: set[int] = set()
        self.files: dict[str, list[int]] = {}          # file -> pending indices, longest wall first
        self.file_remaining: dict[str, float] = {}      # file -> sum of pending predicted wall
        self._module_max: dict[str, float] = {}         # file -> longest pending predicted wall
        self.total_remaining = 0.0                      # predicted wall of everything still pending
        self.max_pending_wall = 0.0                     # longest wall ever queued, a cheap upper bound
        # The wall each pending test was queued with.  In-run learning
        # changes a test's estimate while it waits, so the running
        # totals must be undone with the number they were built from,
        # or they drift and the critical-path test is never found.
        self._queued_wall: dict[int, float] = {}
        self.held_since: dict[int, float] = {}          # index -> when a worker was given it to hold
        self.hold_for: int | None = None                # a critical-path test whose capacity is kept free
        self.committed_at: dict[int, float] = {}        # index -> monotonic time of commit
        self._costs: dict[int, Cost] = {}
        self._costs_by_file: dict[str, set[int]] = defaultdict(set)   # file -> indices with a cached cost
        # --repeat runs one test several times, and the copies share a
        # profile key.  A test the profile does not know is priced by
        # a static guess, and running every copy on that guess at once
        # is what the guess cannot afford: the first copy's
        # measurement is the whole point.  So only one copy of such a
        # test runs until it completes, and the rest are admitted on
        # what it measured (see _waits_for_first_run).
        self._keys: dict[int, str] = {}
        self._modules: dict[int, str] = {}
        self._first_run: dict[str, int] = {}            # key -> the index measuring it for the others
        self._first_run_done: set[str] = set()
        self._last_pressure_cut = -math.inf
        self._last_retire = -math.inf
        self._psi = (0.0, 0.0)
        self.stats = defaultdict(int)
        self._tick_timer: threading.Timer | None = None
        self._stopped = False
        # With --gather-metrics, what admission saw and booked goes to
        # the run's metrics database next to the machine's own samples,
        # so a slow run can be explained after the fact.
        self._metrics = (SQLiteWriter(Path(opt("--tmpdir")).absolute() / DEFAULT_DB_NAME)
                         if opt("--gather-metrics") else None)
        self._metrics_at = -math.inf

        self.log(f"dynamic scheduler: ncpus={self.ncpus} cpu_target={self.cpu_target:.1f} "
                  f"mem_target={self.mem_target / GB:.1f}G (of {total / GB:.0f}G total, "
                  f"{free_now / GB:.0f}G free at start) depth={self.depth} "
                  f"cpu_ceiling={self.cpu_ceiling:.1f} psi_cpu_limit={self.psi_cpu_limit:.0f} "
                  f"profile_entries={len(self.model.tests)}"
)

    # -- protocol: properties ----------------------------------------

    @property
    def nodes(self) -> list[WorkerController]:
        return list(self.node2pending.keys())

    @property
    def collection_is_completed(self) -> bool:
        return len(self.node2collection) >= self.numnodes

    @property
    def tests_finished(self) -> bool:
        if not self.collection_is_completed or self.collection is None:
            return False
        if self.pending_set or self._evicted:
            # An evicted test is queued again only once its worker
            # says it skipped it.
            return False
        for node, queued in self.node2pending.items():
            if queued and not node.shutting_down:
                return False
        return True

    @property
    def has_pending(self) -> bool:
        return bool(self.pending_set) or any(self.node2pending.values())

    # -- protocol: node lifecycle ------------------------------------

    def add_node(self, node: WorkerController) -> None:
        assert node not in self.node2pending
        self.node2pending[node] = []
        self.node_file[node] = None

    def add_node_collection(self, node: WorkerController, collection: Sequence[str]) -> None:
        assert node in self.node2pending
        # Only a worker this scheduler spawned: xdist also brings up a
        # clone of a crashed worker, and counting that one would write
        # off a spawn still under way.
        self._spawning_ids.discard(node.gateway.id)
        if self.collection_is_completed and self.collection is not None:
            if list(collection) != self.collection:
                from xdist.report import report_collection_diff
                other = next(iter(self.node2collection))
                self.log(report_collection_diff(self.collection, list(collection), other.gateway.id, node.gateway.id))
                return
        self.node2collection[node] = list(collection)

    def remove_node(self, node: WorkerController) -> str | None:
        queued = self.node2pending.pop(node, [])
        self._idle_mem.pop(node.gateway.id, None)
        self._idle_since.pop(node, None)
        self._last_file_done.pop(node, None)
        self._draining.discard(node)
        # A worker that goes before it has collected is no longer a
        # spawn in flight.
        self._spawning_ids.discard(node.gateway.id)
        self.node_file.pop(node, None)
        was_shutdown = node in self.shutdown_sent
        self.shutdown_sent.discard(node)
        # Evicted, and gone before it said it skipped the test: that
        # test never started.
        evicted = self._evicted.pop(node, None) is not None
        crashitem = None
        if queued:
            committed = queued if was_shutdown and not evicted else queued[:-1]
            if committed:
                # The first committed item was running when the worker
                # died.
                crashitem = self.collection[queued[0]]
                requeue = queued[1:]
            else:
                # Only a held item that never started: nothing
                # crashed.
                requeue = queued
            for idx in queued:
                self.committed_at.pop(idx, None)
                self._started_at.pop(idx, None)
                self._mem_at_commit.pop(idx, None)
                self._mem_track.pop(idx, None)
                if self._scout.get(self._profile_file_of(idx)) == idx:
                    del self._scout[self._profile_file_of(idx)]
                self.res_cpu.pop(idx, None)
                self.res_mem.pop(idx, None)
                # It never reported: the next copy to be picked
                # measures in its place.
                if self._first_run.get(self._key_of(idx)) == idx:
                    del self._first_run[self._key_of(idx)]
            for idx in requeue:
                self._add_pending(idx, front=True)
        self.check_schedule()
        return crashitem

    # -- protocol: test lifecycle ------------------------------------

    def mark_test_complete(self, node: WorkerController, item_index: int, duration: float | None = None) -> None:
        if self._evicted.get(node) == item_index:
            self._requeue_evicted(node, item_index)
            return
        self._last_file_done[node] = self._file_of(item_index)
        queued = self.node2pending.get(node)
        if queued is not None and item_index in queued:
            queued.remove(item_index)
        committed = self.committed_at.pop(item_index, None)
        started = self._started_at.pop(item_index, None)
        if committed is not None and started is not None:
            now = self.now()
            self._recent_done.append((started, now, self._predict_running(item_index, now - committed)))
        self._mem_at_commit.pop(item_index, None)
        self._mem_track.pop(item_index, None)
        if self._scout.get(self._profile_file_of(item_index)) == item_index:
            self._scout_done.add(self._profile_file_of(item_index))
        # The test committed behind it on this worker starts now.
        nxt = next((i for i in (queued or ()) if i in self.committed_at), None)
        if nxt is not None and nxt not in self._started_at:
            now = self.now()
            self._started_at[nxt] = now
            self._mem_track[nxt] = [0.0, now]
        self.res_cpu.pop(item_index, None)
        self.res_mem.pop(item_index, None)
        # Released whether or not a sample came with it: a copy that
        # reported nothing must not keep the others waiting for the
        # rest of the run.
        if self._first_run.get(self._key_of(item_index)) == item_index:
            self._first_run_done.add(self._key_of(item_index))
        self.stats["completed"] += 1
        self.check_schedule()

    def _requeue_evicted(self, node: WorkerController, idx: int) -> None:
        """The evicted worker skipped its held test: queue it again,
        at the front."""
        del self._evicted[node]
        queued = self.node2pending.get(node)
        if queued is not None and idx in queued:
            queued.remove(idx)
        if self._scout.get(self._profile_file_of(idx)) == idx:
            del self._scout[self._profile_file_of(idx)]
        if self._first_run.get(self._key_of(idx)) == idx:
            del self._first_run[self._key_of(idx)]
        self._add_pending(idx, front=True)
        self.stats["evicted_requeued"] += 1
        self.log(f"requeued {self.collection[idx]}: {node.gateway.id} exited without running it")
        self.check_schedule()

    def mark_test_pending(self, item: str) -> None:
        assert self.collection is not None
        self._add_pending(self.collection.index(item), front=True)
        self.check_schedule()

    def remove_pending_tests_from_node(self, node: WorkerController, indices: Sequence[int]) -> None:
        # We never send `steal`, so this is not expected.
        pass

    def schedule(self) -> None:
        assert self.collection_is_completed
        if self.collection is not None:
            self.check_schedule()
            return
        if not self._check_nodes_have_same_collection():
            self.log("**Different tests collected, aborting run**")
            return
        # A test's reservation is the peak memory of its worker's
        # cgroup, so a *running* worker is paid for.  A worker that is
        # only holding a test is not, and there is one of those per
        # worker all run long.  Charge them once, now that we know how
        # many there are, or the budget is over by that much on a
        # machine with many workers.
        if not self._mem_charged_workers:
            self._mem_charged_workers = True
            # Every worker is up and has finished collecting, so
            # whatever they cost is already spent and already visible.
            # Measure what is left instead of guessing what they took:
            # the guess was a per-worker constant, and at two workers
            # per CPU a constant that is wrong by 50 MB is wrong by
            # gigabytes.
            free_now = self._available()
            old_target = self.mem_target
            self.mem_target = max(1 * GB, free_now)
            self.log(f"{len(self.node2pending)} workers up, {free_now / GB:.1f}G still free; memory budget "
                      f"{old_target / GB:.1f}G -> {self.mem_target / GB:.1f}G")
        self.collection = next(iter(self.node2collection.values()))
        self._pool_start = len(self.node2pending)
        self._pool_floor = max(2, int(POOL_FLOOR_FRACTION * self._pool_start))
        for idx in range(len(self.collection)):
            self._add_pending(idx)
        self.log(f"scheduling {len(self.collection)} tests over {len(self.node2pending)} workers; "
                  f"cost sources: {dict(self._source_histogram())}")
        self._start_tick()
        self.check_schedule()

    # -- core --------------------------------------------------------

    def check_schedule(self) -> None:
        if self.collection is None or self._stopped:
            return
        nodes = [n for n in self.node2pending if not n.shutting_down]
        self._grow_ceiling()
        self.live.refresh(n.gateway.id for n in nodes)
        self._refresh_measurement()
        self._refresh_forecasts()
        self._ram_guard()
        self._recycle_bloated_worker()
        self._maybe_shrink_pool()
        pressure = self._pressure_guard()
        was = self.hold_for
        self.hold_for = self._critical_test()
        if self.hold_for is not None and self.hold_for != was:
            c = self._costs_for(self.hold_for)
            self.stats["critical_holds"] += 1
            self.log(f"critical path: holding {c.cores:.2f} cores and {c.mem / GB:.1f}G for "
                      f"{self.collection[self.hold_for]} (wall {c.wall:.0f}s, "
                      f"{self.total_remaining / max(float(self.ncpus), float(len(self.committed_at))):.0f}s "
                      f"of work left per running slot)")

        # Workers that are idle with a held test come first: they can
        # start right away.
        def prio(n: WorkerController) -> tuple[int, float]:
            held = self._held(n)
            return (len(self._committed(n)), self._costs_for(held).cores if held is not None else math.inf)
        nodes.sort(key=prio)

        admitted_any = False
        # Two passes: first give every idle worker one held test (so
        # backfilled tests land on idle workers, not behind a running
        # one), then commit held tests and assign successors.
        for phase in ("prime", "fill"):
            for node in nodes:
                if node in self.shutdown_sent:
                    # Retired earlier in this pass: anything sent now
                    # arrives after the shutdown, and the worker exits
                    # with it queued -- xdist calls that a crash.
                    continue
                if phase == "prime" and self.node2pending[node]:
                    continue
                limit = 1 if phase == "prime" else self._depth(node) + 1
                while len(self.node2pending[node]) < limit:
                    held = self._held(node)
                    if held is not None and self._waits_for_first_run(held):
                        # Parked only because nothing else was left to
                        # pick; it starts once the first copy has
                        # reported, priced on what that copy measured.
                        self.stats["held_first_run"] += 1
                        break
                    if held is not None and not self._fits(held, node, pressure):
                        # A test sent to a worker cannot be recalled,
                        # so a worker whose held test is never
                        # admissible does nothing at all.  One test
                        # over budget costs less than one worker idle
                        # for a whole run.
                        waited = self.now() - self.held_since.get(held, self.now())
                        # Never force past memory.  CPU is
                        # compressible: a test over the CPU budget
                        # makes everything a little slower.  Memory is
                        # not: a test over the memory budget swaps,
                        # and the machine then spends more CPU on
                        # reclaim than the test was ever going to use.
                        # A forced start may override the CPU budget,
                        # not the forecast: it may dip into half the
                        # reserve, only while the machine shows no
                        # memory pressure at all, and only one at a
                        # time, so each forced test shows what it
                        # takes before the next one goes.
                        mem_ok = (not self._stalled()
                                  and self._forecast_need(held, node) + self._hold_reservation(held)[1]
                                  <= self._available() - 0.5 * self.mem_reserve
                                  and self._psi[1] == 0.0
                                  and self.now() - self._last_forced >= MEM_RAMP_SECONDS)
                        if waited >= HELD_FORCE_SECONDS and mem_ok:
                            self.log(f"unblocking {node.gateway.id}: {self.collection[held]} has been "
                                      f"held {waited:.0f}s without fitting; starting it anyway")
                            self.stats["forced_held"] += 1
                            self._last_forced = self.now()
                        else:
                            self.stats["held_waiting"] += 1
                            break
                    if held is not None and node in self._draining:
                        # Draining: shutdown is the successor, so the
                        # held test runs and the worker exits.
                        self._commit(held, node)
                        admitted_any = True
                        node.shutdown()
                        self.shutdown_sent.add(node)
                        self._draining.discard(node)
                        break
                    cand = self._pick(node)
                    if cand is None:
                        if held is not None:
                            # Nothing left to send: shutdown commits
                            # the held item.
                            self._commit(held, node)
                            node.shutdown()
                            self.shutdown_sent.add(node)
                        elif not self.pending_set and not self.node2pending[node]:
                            node.shutdown()
                            self.shutdown_sent.add(node)
                        break
                    if held is not None:
                        self._commit(held, node)
                        admitted_any = True
                    self._send(node, cand)
                    if held is None:
                        # Just primed an idle worker; the new item is
                        # held, loop to try to commit it.
                        continue
        if not admitted_any and not self.committed_at:
            # Nothing runs anywhere: never dead-lock on an over-sized
            # test.
            held_nodes = [n for n in nodes if self._held(n) is not None and n not in self.shutdown_sent
                          and not self._waits_for_first_run(self._held(n))]
            if held_nodes:
                node = min(held_nodes, key=lambda n: self._costs_for(self._held(n)).cores)
                held = self._held(node)
                self.log(f"progress rule: forcing {self.collection[held]} on {node.gateway.id}")
                self.stats["forced"] += 1
                cand = self._pick(node)
                self._commit(held, node)
                if cand is None:
                    node.shutdown()
                    self.shutdown_sent.add(node)
                else:
                    self._send(node, cand)
        self._maybe_grow_pool(pressure)
        self._record_state(nodes)

    def _record_state(self, nodes: list[WorkerController]) -> None:
        """Sample admission's inputs into the metrics database, at most
        every METRICS_SECONDS."""
        now = self.now()
        if self._metrics is None or now - self._metrics_at < METRICS_SECONDS:
            return
        self._metrics_at = now
        row = SchedulerMetric(
            host_id=HOST_ID,
            ncpus=self.ncpus,
            measured_cores=self.measured_load,
            estimated_cores=self._estimate_now(now),
            booked_cores=sum(self.res_cpu.values()),
            cpu_target=self.cpu_target,
            cpu_ceiling=self._ceiling,
            psi_cpu=self._psi[0],
            psi_mem=self._psi[1],
            mem_stall=self.mem_stall,
            mem_available=int(self._available()),
            mem_booked=int(sum(self.res_mem.values())),
            mem_forecast_growth=int(self._fc_mean),
            workers=len(self._live_workers()),
            draining=len(self._draining),
            spawning=len(self._spawning_ids),
            running=len(self.committed_at),
            held=sum(1 for n in nodes if self._held(n) is not None),
            pending=len(self.pending_set),
            stats=json.dumps(self.stats, sort_keys=True),
            timestamp=datetime.now(),
        )
        try:
            self._metrics.write_row(row, SCHEDULER_METRICS_TABLE)
        except sqlite3.Error as e:
            logger.warning("dynamic scheduler: cannot record its state, stopping: %s", e)
            self._metrics = None

    def _grow_ceiling(self) -> None:
        """Let the admission ceiling rise with time, and never below
        what already runs."""
        if self.burst_rate <= 0:
            return
        now = self.now()
        if self._ceiling_t is None:
            self._ceiling_t = now
            return
        self._ceiling = min(self.cpu_ceiling,
                            max(self._ceiling + self.burst_rate * max(0.0, now - self._ceiling_t),
                                sum(self.res_cpu.values())))
        self._ceiling_t = now

    def _fits(self, idx: int, node: WorkerController, pressure: bool) -> bool:
        """Admission.

        RAM: what the running tests are still forecast to take, plus
        this test's forecast peak, must fit what the machine has
        available less the reserve (see _forecast).  CPU is
        compressible: reservations (natural parallelism) may exceed
        the core count up to a hard ceiling, but only when the machine
        shows slack (low PSI and measured utilization below target,
        counting what was just admitted and is not visible yet).
        Above the ceiling, never.
        """
        if self._stalled():
            # The tests are losing time to memory already: another
            # test only adds to it.  The progress rule still starts
            # one when nothing runs at all.
            self.stats["rejected_mem_stall"] += 1
            return False
        cost = self._costs_for(idx)
        now = self.now()
        # --- RAM: the running tests' forecast growth plus this test
        # must fit what is free -- (less the reserve, and less the
        # room kept for the critical-path test).
        hold_cpu, hold_mem = self._hold_reservation(idx)
        if self._forecast_need(idx, node) + hold_mem > self._available() - self.mem_reserve:
            self.stats["rejected_mem"] += 1
            return False

        # --- CPU: reservations with a controlled over-commit band
        # ---------------
        setup_cores = self._setup_for(idx, node)
        req = cost.cores + setup_cores
        reserved = sum(self.res_cpu.values()) + hold_cpu
        if reserved + req > self.cpu_ceiling:
            self.stats["rejected_cpu_ceiling"] += 1
            return False
        if self.burst_rate > 0 and reserved + req > self._ceiling:
            self.stats["rejected_burst"] += 1
            return False
        # Measured headroom, in both bands.  A reservation is the
        # test's *average* parallelism, and a test's first seconds are
        # its heaviest: process start, cluster start, compaction.
        # Reservations inside the core count can therefore still drive
        # the machine past it, which is what the run's first
        # half-minute used to look like.  So admission also needs the
        # load we can actually see to leave room, counting what was
        # admitted too recently to be visible yet.  The
        # measured-headroom gate.  Normally the machine itself is the
        # limit, but a target set above the core count is a deliberate
        # request to over-subscribe -- to keep more work runnable than
        # there are cores, so none ever idles waiting for the next
        # test to be admitted -- and then the target is the limit.
        headroom_limit = max(float(self.ncpus), self.cpu_target)
        if self._estimate_now(now) + req > headroom_limit:
            self.stats["rejected_no_headroom"] += 1
            return False
        if reserved + req > self.ncpus:
            # over-commit band: only while the tests' own cgroup shows
            # no CPU pressure, and with evidence of real slack;
            # recently admitted tests count at full weight so one
            # stale reading cannot admit a wave.
            if pressure:
                self.stats["rejected_pressure"] += 1
                return False
            if self._estimate_now(now) + req > self.cpu_target:
                self.stats["rejected_cpu_band"] += 1
                return False
        return True

    def _hold_reservation(self, idx: int | None) -> tuple[float, float]:
        """Capacity kept free for the critical-path test, as seen by
        any other test."""
        if self.hold_for is None or self.hold_for == idx:
            return 0.0, 0.0
        c = self._costs_for(self.hold_for)
        return c.cores, c.mem

    def _free_capacity(self, node: WorkerController | None = None) -> tuple[float, float]:
        """(cores, bytes) a *selection* may still count on.

        Running reservations are not the whole picture here.  Every
        other worker is already holding a test that has been chosen
        and will start as soon as the machine lets it, and a test sent
        to a worker cannot be recalled.  If selection ignored those,
        each worker would choose as though the machine were free, they
        would all pick something heavy at once, and the ones that lose
        the race would sit blocked.  So a pick sees what runs, plus
        what its colleagues are about to run, plus whatever is being
        kept free for the critical path.
        """
        held = self._held(node) if node is not None else None
        hc, hm = self._hold_reservation(held)
        for other, queued in self.node2pending.items():
            if other is node or not queued:
                continue
            idx = queued[-1]
            # chosen, not started yet -- except the critical test,
            # already in the hold
            if idx not in self.committed_at and idx != self.hold_for:
                hc += self._costs_for(idx).cores
                hm += self._forecast_new(idx, other)
        return (self.ncpus - sum(self.res_cpu.values()) - hc, self._mem_headroom() - hm)

    def _mem_headroom(self) -> float:
        """Memory a new test may still take: available now, less the
        running tests' forecast growth, less the reserve."""
        return self._available() - self.mem_reserve - self._fc_mean

    def _forecast(self, idx: int, held: float, file_held: float = 0.0) -> float:
        """Expected peak of a test, given what it holds now.

        A profiled test is forecast at its learned peak.  A test whose
        file has been measured is forecast from its siblings' peaks,
        and one from an unmeasured file from its family's
        distribution.  Either way the forecast is conditional: once a
        test holds more than most of its kind ever take, it is
        expected to be one of the heavy ones.  A test that has not
        started is conditioned on what its file's running tests hold:
        a running test of a file prices the rest of it.
        """
        cost = self._costs_for(idx)
        if cost.source == "profile":
            return max(cost.mem, held)
        key = self._key_of(idx)
        dist = self.model.kind_dist(self.model.peak_kind(key), self.model.mode_of(key))
        basis = max(held, file_held)
        if cost.source == "family":
            fc = self.model.sibling_dist(key).conditional(basis)
            if fc is not None:
                return max(fc, held)
        elif dist is None:
            return max(cost.mem, held)
        fc = dist.conditional(basis) if dist is not None else None
        # Past everything its kind has ever taken: nothing to go by
        # but what it holds.
        return max(basis, held) if fc is None else max(fc, held)

    def _growth_weight(self, idx: int, now: float) -> float:
        """1 while a test may still be growing into its peak, halving
        every PLATEAU_SECONDS once it has stopped.

        Stopped means no growth for PLATEAU_SECONDS and past its first
        MEM_RAMP_SECONDS (or its wall, if shorter).  A test that
        settled below its forecast releases the rest: peaks rarely
        coincide, and holding every settled test at its peak for all
        its life is what left 34 GB reserved against 13 GB in use.
        """
        started = self._started_at.get(idx)
        if started is None:
            return 1.0          # committed behind a running test: it has not begun to grow
        _, grew_at = self._mem_track.get(idx, (0.0, started))
        quiet_from = max(grew_at + PLATEAU_SECONDS, started + min(MEM_RAMP_SECONDS, self._costs_for(idx).wall))
        if now <= quiet_from:
            return 1.0
        return 0.5 ** ((now - quiet_from) / PLATEAU_SECONDS)

    def _refresh_forecasts(self) -> None:
        """Forecast growth of every running test, from what each one
        holds now."""
        now = self.now()
        self._fc_mean = 0.0
        self._file_held = defaultdict(float)
        running: list[tuple[int, float]] = []
        for node, queued in self.node2pending.items():
            live = self.live.memory(node.gateway.id) or 0.0
            first = True
            for idx in queued:
                if idx not in self.committed_at:
                    continue
                # Only the first committed test on a worker is
                # running.  What it holds is its worker's whole
                # cgroup, the worker's own interpreter and whatever
                # its module keeps alive included: that is what a
                # learned peak measures too.
                held = live if first else 0.0
                first = False
                track = self._mem_track.setdefault(idx, [0.0, self._started_at.get(idx, now)])
                if held > track[0] * 1.02 + 0.02 * GB:
                    track[0], track[1] = held, now
                file = self._file_of(idx)
                self._file_held[file] = max(self._file_held[file], held)
                running.append((idx, held))
        for idx, held in running:
            self._fc_mean += max(0.0, self._forecast(idx, held) - held) * self._growth_weight(idx, now)
        if self._spawning and now - self._last_spawn > POOL_SPAWN_TIMEOUT:
            self.log(f"pool: {self._spawning} spawned worker(s) never collected; writing them off")
            self._spawning_ids.clear()
        self._fc_mean += self._spawning * self.worker_cost()

    @property
    def _spawning(self) -> int:
        return len(self._spawning_ids)

    def worker_cost(self) -> float:
        """What a worker holds between tests: the median over the
        workers measured, the prior before any is."""
        return max(POOL_WORKER_PRIOR, statistics.median(self._idle_mem.values())) if self._idle_mem else POOL_WORKER_PRIOR

    def _live_workers(self) -> list[WorkerController]:
        return [n for n in self.node2pending if n not in self.shutdown_sent and not n.shutting_down]

    def _maybe_grow_pool(self, pressure: bool) -> None:
        """Add one worker when every worker is busy and the machine
        has CPU and memory to spare."""
        if self.max_workers <= 0 or self._spawning or not self.pending_set:
            return
        now = self.now()
        if now - self._last_spawn < POOL_GROW_SECONDS:
            return
        live = self._live_workers()
        if len(live) >= self.max_workers:
            return
        # A worker with nothing running is free, or blocked on a test
        # that does not fit; either way another worker would not start
        # anything more -- unless the pool is below its floor (a
        # bloated worker was recycled): then it is refilled.
        refill = len(live) < self._pool_floor
        if not refill and any(not self._committed(n) for n in live):
            return
        if pressure or self._estimate_now(now) + 1.0 > self.cpu_target:
            return
        # Low CPU load is not idle capacity when the tests are stalled
        # on memory: a test waiting for its pages to come back from
        # swap burns no CPU.  Measured, the pool grew from 12 to 33
        # workers in four minutes on a load of 1-5 cores while swap
        # filled.
        if self._stalled():
            return
        head = next((lst[0] for lst in self.files.values() if lst and not self._waits_for_first_run(lst[0])), None)
        if head is None:
            return
        fc = self._forecast(head, 0.0, self._file_held.get(self._file_of(head), 0.0))
        cost = self.worker_cost()
        headroom = self._mem_headroom()
        if headroom < cost + fc:
            return
        wid = self._spawn_worker()
        if not wid:
            return
        self._spawning_ids.add(wid)
        self._last_spawn = now
        self.stats["pool_grown"] += 1
        self._fc_mean += cost
        self.log(f"pool: {len(live)} -> {len(live) + 1} workers ({'refilling' if refill else 'all busy'}, load {self._estimate_now(now):.1f}/"
                  f"{self.cpu_target:.1f} cores, {headroom / GB:.1f}G headroom, a worker holds {cost / GB:.2f}G)")

    def _maybe_shrink_pool(self) -> None:
        """Drain one worker that has had nothing running for
        POOL_IDLE_SECONDS, while the pool is above its start."""
        now = self.now()
        live = self._live_workers()
        for node in live:
            if self._committed(node):
                self._idle_since.pop(node, None)
            else:
                self._idle_since.setdefault(node, now)
        if self.max_workers <= 0 or now - self._last_retire < RETIRE_COOLDOWN:
            return
        if len(live) - len(self._draining) <= self._pool_floor:
            return
        idle = [n for n in live if n not in self._draining and now - self._idle_since.get(n, now) >= POOL_IDLE_SECONDS
                and not self._continues_module(n)]
        if not idle:
            return
        node = min(idle, key=lambda n: self._idle_since[n])
        self._draining.add(node)
        self._last_retire = now
        self.stats["pool_drained"] += 1
        self.log(f"pool: draining {node.gateway.id} (nothing running for {now - self._idle_since[node]:.0f}s); "
                  f"{len(live) - len(self._draining)} workers will remain")
        if self._held(node) is None:
            node.shutdown()
            self.shutdown_sent.add(node)
            self._draining.discard(node)

    def _spawn_xdist_worker(self) -> str | None:
        """Start one more xdist worker, the way xdist replaces a
        crashed one; return its id."""
        dsession = self.config.pluginmanager.getplugin("dsession")
        template = next(iter(self.node2pending), None)
        if dsession is None or getattr(dsession, "shuttingdown", True) or template is None:
            return None
        import execnet
        spec = execnet.XSpec(template.gateway.spec._spec)
        spec.id = None
        dsession.nodemanager.group.allocate_id(spec)
        clone = dsession.nodemanager.setup_node(spec, dsession.queue.put)
        dsession._active_nodes.add(clone)
        return spec.id

    def _recycle_bloated_worker(self) -> None:
        """Send home a worker holding far more between tests than its
        peers; the pool may grow a fresh one.

        It finishes what it is running and its held test first, as at
        the end of a run, so the held test has to fit now like any
        other.
        """
        now = self.now()
        # Only a pool that can grow back may send a worker home for
        # this: with a fixed pool (-j) every recycled worker is one
        # lost for the rest of the run.
        if self.max_workers <= 0 or now - self._last_retire < RETIRE_COOLDOWN or len(self._idle_mem) < 3:
            return
        limit = max(POOL_BLOAT_MIN, POOL_BLOAT_FACTOR * self.worker_cost())
        for node in self._live_workers():
            overhead = self._idle_mem.get(node.gateway.id, 0.0)
            if overhead <= limit:
                continue
            held = self._held(node)
            if held is not None and held not in self.committed_at:
                if self._waits_for_first_run(held):
                    continue        # its test waits for a scout or a first copy: not now
                if self._forecast_need(held, node) > self._available() - self.mem_reserve:
                    continue
                self._commit(held, node)
            node.shutdown()
            self.shutdown_sent.add(node)
            self._idle_mem.pop(node.gateway.id, None)
            self._last_retire = now
            self.stats["recycled"] += 1
            self.log(f"recycling {node.gateway.id}: it held {overhead / GB:.2f}G when its last test started, "
                      f"against {self.worker_cost() / GB:.2f}G for a typical worker")
            return

    def _forecast_new(self, idx: int, node: WorkerController | None = None) -> float:
        """What a test that has not started will add, conditioned on
        its file's running tests.

        A peak is learned as the whole worker cgroup at its highest:
        the worker's interpreter and whatever its module keeps alive
        included.  That part is already resident and already out of
        MemAvailable, so on a known worker only the peak beyond what
        the worker holds now is new.  A new worker's own overhead is
        charged separately.
        """
        peak = self._forecast(idx, 0.0, self._file_held.get(self._file_of(idx), 0.0))
        if node is None:
            return peak
        return max(0.0, peak - (self.live.memory(node.gateway.id) or 0.0))

    def _forecast_need(self, idx: int, node: WorkerController | None = None) -> float:
        """Memory the running tests are still going to take, with this
        one added."""
        return self._fc_mean + self._forecast_new(idx, node)

    def _refresh_measurement(self) -> None:
        """Test-attributable load: all worker cgroups (hierarchical)
        plus the services cgroup."""
        cores = None
        if self.live.base is not None:
            tests_rate = self.live.rate(self.live.base)
            if tests_rate is not None:
                cores = tests_rate
            self._measured_raw = tests_rate
            if self._services_path is not None:
                svc = self.live.rate(self._services_path)
                if svc is not None:
                    cores = (cores or 0.0) + svc
                    self._services_raw = svc
            self._set_mem_stall(read_psi("memory", self.live.base, "full"))
        if cores is None:
            # no cgroup data (unit tests, foreign environment): fall
            # back to per-node readings
            cores = sum((self.live.cores(n.gateway.id) or 0.0) for n in self.node2pending)
        self.measured_load = 0.5 * self.measured_load + 0.5 * cores

    def _predict_running(self, idx: int, elapsed: float, tau: float = 0.0) -> float:
        """Cores a running test is expected to use `tau` seconds from
        now."""
        return self.model.predict_at(self.collection[idx], elapsed + tau, future=tau > 0)

    def _inflight(self, now: float) -> list[int]:
        return [i for i, t in self.committed_at.items() if self._ramp_weight(now, i) > 0.0]

    def _inflight_pred(self, now: float, idx: int) -> float:
        return self._predict_running(idx, now - self.committed_at[idx])

    def _ramp_weight(self, now: float, idx: int) -> float:
        """1 right after admission, fading to 0 as the measurement
        catches up.

        The window is the shorter of RAMP_SECONDS and the test's
        expected wall time: a 0.3 s test is over (and measured) long
        before 2 s have passed, and with a dozen such completions per
        second a fixed window would charge more in-flight cost than
        the whole budget.
        """
        started = self._started_at.get(idx)
        if started is None:
            return 0.0          # committed behind a running test: it burns nothing yet
        window = max(0.2, min(RAMP_SECONDS, self._costs_for(idx).wall))
        return max(0.0, 1.0 - (now - started) / window)

    def _estimate_now(self, now: float) -> float:
        """The load the running tests put on the machine now.

        The measurement averages the tests' cgroup over its last
        window, so it lags in both directions: a test started during or
        after the window is only partly in it, or not at all, and a test
        that ended inside the window is in it although it no longer
        runs.  Correct both against that window.  Charging every new
        test in full for a fixed time instead counted a sub-second test
        twice, once in the measurement and once on top: at thirty
        starts a second that was seven to thirteen threads of load
        that did not exist, and the machine sat at 70% while admission
        refused tests.
        """
        window = self.live.window(self.live.base) if self.live.base is not None else None
        if window is None or self._measured_raw is None or window[1] <= window[0]:
            inflight = sum(self._inflight_pred(now, i) * self._ramp_weight(now, i) for i in self._inflight(now))
            return self.measured_load + inflight
        w0, w1 = window
        span = w1 - w0
        seen = lambda a, b: min(1.0, max(0.0, (min(b, w1) - max(a, w0)) / span))
        estimate = self._measured_raw + self._services_raw
        for idx, started in self._started_at.items():
            if idx in self.committed_at:
                estimate += self._inflight_pred(now, idx) * (1.0 - seen(started, w1))
        self._recent_done = [d for d in self._recent_done if d[1] > w0]
        for started, ended, cores in self._recent_done:
            estimate -= cores * seen(started, ended)
        return max(0.0, estimate)

    def _available(self) -> float:
        """What the machine has available for tests (MemAvailable);
        swap is not counted either way."""
        return self._machine_available()

    def _stalled(self) -> bool:
        """The tests stall on memory (see MEM_STALL_LIMIT)."""
        return self.mem_stall >= MEM_STALL_LIMIT

    def _set_mem_stall(self, stall: float) -> None:
        was = self._stalled()
        self.mem_stall = stall
        if self._stalled() != was:
            self.stats["mem_stalls"] += int(not was)
            self.log(f"memory: the tests stall on memory {stall:.1f}% of the time"
                      + ("; nothing more is admitted until it falls below "
                         f"{MEM_STALL_LIMIT:.0f}%" if not was else "; admitting again"))

    def _ram_guard(self) -> None:
        """Send home an idle worker while the run is stalled on
        memory.

        Idle workers keep their heap and whatever their last module
        left behind, memory no test can use.  When no held test fits
        the forecast, one such worker goes per cooldown.  An idle
        worker usually holds its next test, and a shutdown alone would
        start it, so that worker is told to skip it first (see
        request_eviction): it exits without running the test, and the
        test goes back to the queue for a worker that has room.
        """
        now = self.now()
        if now - self._last_retire < RETIRE_COOLDOWN:
            return
        room = self._available() - self.mem_reserve
        waiting = [(n, i) for n in self._live_workers() if (i := self._held(n)) is not None
                   and i not in self.committed_at]
        if not waiting:
            return
        needs = [self._forecast_need(i, n) + self._hold_reservation(i)[1] for n, i in waiting]
        if min(needs) <= room:
            return
        why = (f"{self._available() / GB:.1f}G available, {max(0.0, room) / GB:.1f}G after the reserve; "
               f"the smallest of {len(waiting)} held tests needs {min(needs) / GB:.1f}G")
        if self._retire_idle_worker(why):
            self._last_retire = now

    def _retire_idle_worker(self, why: str) -> bool:
        """Send home the idle worker holding the most memory, if the
        pool may shrink.

        A worker holding no test goes first, as nothing has to be
        queued again.  Never below the pool's floor, and with a fixed
        pool (-j), which cannot grow back, never below the size it
        started with.  The worker exits cleanly, so xdist neither
        replaces it nor counts it as a crash.
        """
        live = self._live_workers()
        floor = self._pool_floor if self.max_workers > 0 else max(self._pool_floor, self._pool_start)
        if len(live) <= max(1, floor):
            return False
        idle = [n for n in live if not self._committed(n)]
        if not idle:
            return False
        node = max(idle, key=lambda n: (self._held(n) is None, self.live.memory(n.gateway.id) or 0.0))
        held = self._held(node)
        wid = node.gateway.id
        if held is not None:
            if not request_eviction(wid, self.collection[held]):
                return False
            self._evicted[node] = held
            self.held_since.pop(held, None)
        node.shutdown()
        self.shutdown_sent.add(node)
        self._draining.discard(node)
        self._idle_since.pop(node, None)
        mem = self.live.memory(wid) or 0.0
        if held is None:
            self.stats["retired"] += 1
            self.log(f"retiring {wid}, holding {mem / GB:.1f}G ({why}); {len(live) - 1} workers left")
        else:
            self.stats["evicted"] += 1
            self.log(f"evicting {wid}, holding {mem / GB:.1f}G, on purpose: it holds {self.collection[held]}, "
                      f"which cannot start ({why}); the worker exits without running it and the test is "
                      f"queued again; {len(live) - 1} workers left")
        return True

    def _pressure_guard(self) -> bool:
        # CPU pressure of the tests' own cgroup tree when available:
        # on a pinned or shared machine the system-wide file also
        # counts stalls on other cores and of other processes.  Memory
        # pressure is machine-wide by nature.
        psi_cpu = read_psi("cpu", self.live.base) if self.live.base is not None and (self.live.base / "cpu.pressure").exists() else read_psi("cpu")
        psi_mem = read_psi("memory")
        self._psi = (psi_cpu, psi_mem)
        now = self.now()
        # Not the run queue: it was tried as a second signal and
        # carried nothing PSI does not.  PSI sat at 2% whether the
        # queue read 8 or 28, and the queue is over the core count a
        # quarter of the time on a busy box simply because it counts
        # the tasks on the CPUs as well.
        if psi_cpu <= self.psi_cpu_limit and psi_mem <= self.psi_mem_limit:
            # Calm again: grow the target back slowly towards the
            # configured one (additive-increase /
            # multiplicative-decrease, like TCP).
            full = self.cpu_target_frac * self.ncpus
            if self.cpu_target < full and now - self._last_pressure_cut >= PRESSURE_COOLDOWN:
                self.cpu_target = min(full, self.cpu_target + 0.02 * self.ncpus)
            return False
        if now - self._last_pressure_cut >= PRESSURE_COOLDOWN:
            old = self.cpu_target
            self.cpu_target = max(self.cpu_target_floor, self.cpu_target * 0.9)
            self._last_pressure_cut = now
            self.stats["pressure_cuts"] += 1
            self.log(f"pressure: psi cpu={psi_cpu:.1f}% mem={psi_mem:.1f}%; "
                      f"cpu target {old:.1f} -> {self.cpu_target:.1f}")
        return True

    # -- selection ---------------------------------------------------

    def _pick(self, node: WorkerController) -> int | None:
        """Next test for this worker.

        A module offers only its next test, in collection order.  The
        worker's own module comes first; otherwise modules nobody else
        is in, the one with the longest pending test first.  Backfill:
        if that module's next test does not fit the capacity free right
        now, take another module's that does, so an unfittable big test
        does not idle the worker.

        A passed-over test is normally not reserved for: no test has a
        deadline and everything runs before the session ends.  The
        exception is the critical path.  Once the longest test still
        pending is longer than the time the rest of the work needs,
        starting it any later makes it the end of the run on its own,
        and the machine drains behind it.  From that point its
        capacity is held: smaller tests are admitted only with what is
        left over, so the room it needs appears.  Early in a run
        nothing is critical and this costs nothing.
        """
        if not self.pending_set:
            return None
        # Longest test first, globally.  Ordering by a file's *total*
        # remaining work put a 139-second test in a small file at the
        # back of the queue, so it started when the machine had
        # drained and then ran alone.  So the modules are ordered by
        # their longest pending test.
        by_remaining = sorted(self.files, key=lambda f: -self._module_max[f])
        # The test we are holding capacity for gets first refusal:
        # everyone else has been admitted against a budget that
        # already excludes it, so when it fits, it goes now.
        if self.hold_for is not None and self.hold_for in self.pending_set:
            c = self._costs_for(self.hold_for)
            if (sum(self.res_cpu.values()) + c.cores <= self.ncpus
                    and self._forecast_need(self.hold_for, node) <= self._available() - self.mem_reserve):
                return self._take(self._file_of(self.hold_for), self.hold_for)
        free_cpu, free_mem = self._free_capacity(node)
        # Selection has to be as strict about memory as admission is.
        # A test sent to a worker cannot be recalled: the worker holds
        # it until it can start.  Picking one that admission will
        # refuse parks that worker for as long as the refusal lasts,
        # and takes the test off the pending queue where the
        # critical-path check looks.
        # Stay in the module the worker is in while it has a test that
        # can start: moving to another one tears this module's
        # fixtures down and sets the next one's up, which costs a
        # cqlpy or alternator test in debug mode about five seconds,
        # ten times the test itself.  Otherwise take a module no other
        # worker is in, so a module is split between workers only
        # once nothing else is left.
        current = self.node_file.get(node)
        if current in self.files and not self._waits_for_first_run(self.files[current][0]):
            files = [current]
        else:
            taken = {self.node_file.get(n) for n in self.node2pending if n is not node and not n.shutting_down}
            files = sorted(by_remaining, key=lambda f: f in taken)
        head = None
        chosen = None
        smallest = None            # where to park when nothing fits right now
        waiting = None             # a repeat waiting for its first copy: the very last resort
        # Each module offers only its next test, in collection order.
        for f in files:
            idx = self.files[f][0]
            if self._waits_for_first_run(idx):
                waiting = waiting or (f, idx)
                continue
            if head is None:
                head = idx
            if smallest is None:
                smallest = (f, idx)        # parking stays in the order too
            c = self._costs_for(idx)
            if c.cores <= free_cpu and self._forecast_new(idx, node) <= free_mem:
                chosen = (f, idx)
                break
        if chosen is None:
            # Nothing fits the capacity free right now.  Park the
            # worker on the next test of the first module in order: a
            # held test is off the queue and blocks its worker until
            # it fits.  A worker blocked for the whole run on a test that needs
            # half the machine's memory is a worker lost, and the test
            # itself disappears from the pending set, where the
            # critical-path check would have made room for it.
            if smallest is None and head is not None:
                smallest = (self._file_of(head), head)
            if smallest is None:
                if waiting is None:
                    return None
                # Everything left is a repeat of a test whose first
                # copy is still running.  Handing one out anyway keeps
                # the worker from being shut down; admission will not
                # start it before the first copy has reported.
                self.stats["parked_first_run"] += 1
                return self._take(*waiting)
            chosen = smallest
            self.stats["parked_smallest"] += 1
        if chosen[1] != head:
            self.stats["backfilled"] += 1
        return self._take(*chosen)

    def _parallel(self) -> float:
        return max(float(self.ncpus), float(len(self.committed_at)))

    def _critical_test(self) -> int | None:
        """The longest pending test, once it is longer than what the
        rest of the work needs.

        Only a module's next test can start, so the candidates are
        those.  Scanning them is only worth it when the run has drained far enough for any test to be on
        the critical path at all, which is a single comparison against
        the longest wall we have ever queued.

        Once chosen, a test stays chosen until it starts.  Being
        handed to a worker is not starting: the worker holds it until
        admission lets it go, and if the hold moved on at that point,
        smaller tests would take the room back.  Measured, that hold
        jumped across eight tests in fourteen seconds, and the
        113-second test it had picked first started five and a half
        minutes later and ended the run on its own.
        """
        if self.hold_for is not None and self.hold_for not in self.committed_at:
            return self.hold_for
        if not self.files:
            return None
        # How long the rest of the work needs, at the concurrency this
        # run actually gets.
        budget = self.total_remaining / self._parallel()
        if self.max_pending_wall < max(TAIL_SECONDS, budget):
            return None
        best, best_wall = None, 0.0
        for lst in self.files.values():
            if not lst or self._waits_for_first_run(lst[0]):
                continue
            c = self._costs_for(lst[0])
            if (c.wall > best_wall and c.cores <= self.cpu_ceiling
                    and c.mem <= self.mem_target):
                best, best_wall = lst[0], c.wall
        if best is None or best_wall < max(TAIL_SECONDS, budget):
            return None
        return best

    def _take(self, file: str, idx: int | None = None) -> int:
        lst = self.files[file]
        if idx is None:
            idx = lst.pop(0)
        else:
            lst.remove(idx)
        self.pending_set.discard(idx)
        self._claim_first_run(idx)
        wall = self._queued_wall.pop(idx, self._costs_for(idx).wall)
        self.file_remaining[file] -= wall
        self.total_remaining = max(0.0, self.total_remaining - wall)
        if not lst:
            del self.files[file]
            del self.file_remaining[file]
            del self._module_max[file]
        elif wall >= self._module_max[file]:
            self._module_max[file] = max(self._queued_wall.get(i, 0.0) for i in lst)
        return idx

    def _add_pending(self, idx: int, front: bool = False) -> None:
        file = self._file_of(idx)
        lst = self.files.setdefault(file, [])
        wall = self._costs_for(idx).wall
        if front:
            lst.insert(0, idx)
        else:
            # A module's tests run in collection order, as they would
            # under plain xdist: tests in a file may depend on it.  A
            # TRUNCATE that test_using_timeout lets time out keeps
            # running in Scylla and, started before test_batch, wiped
            # its rows.
            bisect.insort(lst, idx)
        self._module_max[file] = max(self._module_max.get(file, 0.0), wall)
        self.file_remaining[file] = self.file_remaining.get(file, 0.0) + wall
        self.total_remaining += wall
        self._queued_wall[idx] = wall
        self.max_pending_wall = max(self.max_pending_wall, wall)
        self.pending_set.add(idx)

    # -- helpers -----------------------------------------------------

    def _continues_module(self, node: WorkerController, idx: int | None = None) -> bool:
        """Whether the test a worker holds (or `idx`) is the next one of
        the module its last test was from, with nothing else running on
        it: the worker is in the middle of that module.

        Such a worker is not idle, however long admission keeps its
        test waiting.  Draining it started the held test regardless of
        admission and handed the rest of the module to another worker,
        which set it up again: a scylla_gdb module, 70 seconds to set
        up, went through five workers in one release run.
        """
        held = self._held(node) if idx is None else idx
        return (held is not None and not self._committed(node)
                and self._last_file_done.get(node) == self._file_of(held))
    def _depth(self, node: WorkerController) -> int:
        """Tests a worker may hold queued behind the one it runs.

        A worker starts the test it holds only once it has been sent
        the next one, so between two tests it waits for a round trip
        through the controller.  For sub-second tests that wait is a
        large part of the worker's time: 165 ms between tests at the
        median, against 1-2 ms under plain xdist, which sends tests
        in batches.  So a worker whose queued tests are all expected to
        be short holds one more, and runs them back to back.
        """
        queued = self.node2pending.get(node) or []
        if queued and all(self._costs_for(i).wall < SHORT_SECONDS for i in queued):
            return self.depth + 1
        return self.depth

    def _held(self, node: WorkerController) -> int | None:
        queued = self.node2pending.get(node) or []
        if queued and node not in self.shutdown_sent:
            return queued[-1]
        return None

    def _committed(self, node: WorkerController) -> list[int]:
        queued = self.node2pending.get(node) or []
        return queued if node in self.shutdown_sent and node not in self._evicted else queued[:-1]

    def _commit(self, idx: int, node: WorkerController) -> None:
        """Admit: acquire the reservations atomically with the
        decision (single scheduler thread)."""
        cost = self._costs_for(idx)
        # Normally claimed when it was picked; this is for a copy
        # parked behind a first run whose worker died, which now
        # measures for the rest.
        self._claim_first_run(idx)
        fc_mean = self._forecast_new(idx, node)
        # Committed behind a running test (a retired or recycled
        # worker, the end of a run, --dynamic-depth > 1) it starts
        # when that test ends, and its clocks with it.
        behind = any(i in self.committed_at for i in self.node2pending.get(node, ()) if i != idx)
        self.committed_at[idx] = self.now()
        if not behind:
            self._started_at[idx] = self.committed_at[idx]
        self._mem_at_commit[idx] = self.live.memory(node.gateway.id) or 0.0
        if (self._mem_at_commit[idx] > 0 and not self._committed(node)
                and self._last_file_done.get(node) != self._file_of(idx)):
            # Nothing else runs on it, and its last module has been
            # torn down (the test it starts now is from another file):
            # what it holds now is its own overhead.  After a test of
            # the same file it would be that module's cluster, which
            # this test reuses.
            self._idle_mem[node.gateway.id] = self._mem_at_commit[idx]
        self._mem_track[idx] = [0.0, self.committed_at[idx]]
        self._fc_mean += fc_mean
        self.res_cpu[idx] = cost.cores
        self.res_mem[idx] = cost.mem
        if self.hold_for == idx:
            self.hold_for = None
        self.held_since.pop(idx, None)
        self.stats["admitted"] += 1
        self.log(f"start {node.gateway.id} {self.collection[idx]} cores={cost.cores:.2f} "
                  f"mem={cost.mem / GB:.2f}G forecast={fc_mean / GB:.2f}G wall={cost.wall:.1f}s src={cost.source} "
                  f"reserved={sum(self.res_cpu.values()):.1f}/{self.cpu_ceiling:.1f} "
                  f"mem_reserved={sum(self.res_mem.values()) / GB:.1f}/{self.mem_target / GB:.1f}G "
                  f"measured={self.measured_load:.1f}")

    def _send(self, node: WorkerController, idx: int) -> None:
        self.held_since[idx] = self.now()
        self.node2pending[node].append(idx)
        if self.node_file.get(node) not in (None, self._file_of(idx)):
            self.stats["module_switches"] += 1
        self.node_file[node] = self._file_of(idx)
        node.send_runtest_some([idx])

    def _file_of(self, idx: int) -> str:
        """The module a test runs in: its file, and its run under
        --repeat, since each run of a file is a module of its own,
        with its own fixtures to set up."""
        module = self._modules.get(idx)
        if module is None:
            parts = self.collection[idx].rsplit(".", 2)
            module = self._modules[idx] = f"{self._profile_file_of(idx)}#{parts[2] if len(parts) == 3 else ''}"
        return module

    def _profile_file_of(self, idx: int) -> str:
        """The file a test belongs to in the profile, whatever its run."""
        return file_of_key(self._key_of(idx))

    def _key_of(self, idx: int) -> str:
        key = self._keys.get(idx)
        if key is None:
            key = self._keys[idx] = profile_key(self.collection[idx])
        return key

    def _scouts(self, idx: int) -> bool:
        """Whether a test belongs to an unmeasured file of a
        heavy-tailed kind."""
        return (self._costs_for(idx).source.startswith("static")
                and self.model.peak_kind(self._key_of(idx)) in SCOUT_KINDS)

    def _claim_first_run(self, idx: int) -> None:
        key = self._key_of(idx)
        if (key not in self._first_run and key not in self._first_run_done
                and self._costs_for(idx).source != "profile"):
            self._first_run[key] = idx
        file = self._profile_file_of(idx)
        if file not in self._scout and file not in self._scout_done and self._scouts(idx):
            self._scout[file] = idx
            self.stats["scouts"] += 1

    def _waits_for_first_run(self, idx: int) -> bool:
        """A test waiting for a measurement: a repeat of an unknown
        test while another copy measures it, or a sibling in an
        unmeasured file while its scout is young."""
        key = self._key_of(idx)
        first = self._first_run.get(key)
        if (first is not None and first != idx and key not in self._first_run_done
                and self._costs_for(idx).source != "profile"):
            return True
        file = self._profile_file_of(idx)
        scout = self._scout.get(file)
        if scout is None or scout == idx or file in self._scout_done or not self._scouts(idx):
            return False
        started = self.committed_at.get(scout)
        if started is None:
            return True
        now = self.now()
        if now - started >= SCOUT_MAX_SECONDS:
            return False
        _, grew_at = self._mem_track.get(scout, (0.0, started))
        return now - started < SCOUT_SECONDS or now - grew_at < SCOUT_QUIET_SECONDS

    def _costs_for(self, idx: int) -> Cost:
        cost = self._costs.get(idx)
        if cost is None:
            cost = self._costs[idx] = self.model.cost(self.collection[idx])
            self._costs_by_file[self._profile_file_of(idx)].add(idx)
        return cost

    def _setup_for(self, idx: int, node: WorkerController) -> float:
        if self.node_file.get(node) == self._file_of(idx):
            return 0.0
        return self.model.setup_cost(self.collection[idx])

    def learn(self, sample: dict[str, Any]) -> None:
        """In-run learning from a finished test's measured cost."""
        self.model.learn(sample)
        # Everything not started yet, held tests included: a repeat
        # parked on a worker behind its first copy must start on what
        # that copy has just measured.  Only this file's costs:
        # walking every cached cost on every report was quadratic in
        # the suite, on the loop that also schedules.
        cached = self._costs_by_file.get(file_of_key(sample["key"]), set())
        for idx in list(cached):
            if idx not in self.committed_at:
                self._costs.pop(idx, None)
                cached.discard(idx)

    def _source_histogram(self) -> dict[str, int]:
        hist: dict[str, int] = defaultdict(int)
        for idx in range(len(self.collection or [])):
            hist[self._costs_for(idx).source] += 1
        return hist

    def _check_nodes_have_same_collection(self) -> bool:
        from xdist.report import report_collection_diff
        items = list(self.node2collection.items())
        first_node, col = items[0]
        same = True
        for node, collection in items[1:]:
            msg = report_collection_diff(col, collection, first_node.gateway.id, node.gateway.id)
            if msg:
                same = False
                self.log(msg)
                rep = pytest.CollectReport(nodeid=node.gateway.id, outcome="failed", longrepr=msg, result=[])
                self.config.hook.pytest_collectreport(report=rep)
        return same

    # -- periodic tick (runs on the DSession main loop thread) -------

    def _start_tick(self) -> None:
        dsession = self.config.pluginmanager.getplugin("dsession")
        if dsession is None or not hasattr(dsession, "queue"):
            return
        sched = self

        def worker_dynamic_tick(**kwargs: Any) -> None:
            sched.check_schedule()

        dsession.worker_dynamic_tick = worker_dynamic_tick

        def fire() -> None:
            if self._stopped:
                return
            try:
                dsession.queue.put(("dynamic_tick", {}))
            except Exception:  # pragma: no cover - never let the timer thread die loudly
                pass
            self._tick_timer = threading.Timer(1.0, fire)
            self._tick_timer.daemon = True
            self._tick_timer.start()

        fire()

    def _stop_tick(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        if self._tick_timer is not None:
            self._tick_timer.cancel()
        self.log(f"done: {dict(self.stats)}")


# --------------------------------------------------------------------
# Profile merge at the end of a run
# --------------------------------------------------------------------

def _profile_path(config: pytest.Config) -> Path:
    return Path(config.getoption("--tmpdir")).absolute() / PROFILE_FILENAME


def _cgroup_tests_path() -> Path | None:
    try:
        from test.pylib.resource_gather import CGROUP_TESTS
        return CGROUP_TESTS if CGROUP_TESTS.exists() else None
    except Exception:
        return None


# An idle worker already holds the next test it will run, and xdist
# cannot take a test back: the shutdown that sends the worker home is
# also what starts that test.  So the controller first leaves a flag,
# one file per worker naming the held test, and the worker skips a
# test it finds flagged (see take_eviction).  It exits without running
# it, and the test goes back to the queue.  Killing the worker instead
# would make xdist replace it and count a crash.
def request_eviction(worker_id: str, nodeid: str) -> bool:
    """Tell a worker to skip the test it holds; False when no worker
    could see it."""
    directory = sched_dir.evictions()
    if directory is None:
        return False
    try:
        directory.mkdir(parents=True, exist_ok=True)
        (directory / worker_id).write_text(nodeid)
    except OSError:
        return False
    return True


def take_eviction(worker_id: str | None, nodeid: str) -> bool:
    """On a worker: whether the controller told it to skip this test,
    consuming the flag."""
    directory = sched_dir.evictions()
    if directory is None or worker_id is None:
        return False
    flag = directory / worker_id
    try:
        if flag.read_text() != nodeid:
            return False
        flag.unlink()
    except OSError:
        return False
    return True


def samples_path(tmpdir: Path) -> Path:
    return tmpdir / f"dynamic_samples_{HOST_ID}.jsonl"


def append_sample(tmpdir: Path, sample: dict[str, Any]) -> None:
    line = json.dumps(sample, separators=(",", ":")) + "\n"
    with open(samples_path(tmpdir), "a") as f:
        f.write(line)   # < PIPE_BUF with O_APPEND: atomic across workers


def load_samples(path: Path) -> list[dict[str, Any]]:
    samples = []
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        samples.append(json.loads(line))
                    except ValueError:
                        pass
    except OSError:
        pass
    return samples


def merge_run_into_profile(tmpdir: Path, profile_path: Path, ncpus: int) -> int:
    """Merge this run's samples into the profile, then remove them.

    The samples file is named after the host id, which is fixed when
    SCYLLA_TEST_HOST_ID is set; a file left behind would be merged
    again by every later run, counting the same samples over and over.
    """
    path = samples_path(tmpdir)
    samples = load_samples(path)
    if not samples:
        return 0
    model = CostModel(profile_path, ncpus)
    for sample in samples:
        model.learn(sample)
    model.save()
    path.unlink(missing_ok=True)
    return len(samples)


# --------------------------------------------------------------------
# pytest plugin hooks (controller side)
# --------------------------------------------------------------------

_scheduler: DynamicScheduling | None = None


@pytest.hookimpl(optionalhook=True)
def pytest_xdist_make_scheduler(config: pytest.Config, log: Any) -> Any:
    global _scheduler
    if not config.getoption("--dynamic-scheduler"):
        return None
    _scheduler = DynamicScheduling(config, log)
    return _scheduler


@pytest.hookimpl
def pytest_testnodedown(node: Any, error: Any) -> None:
    """Close the gateway of a worker that went down before the end of
    the run.

    xdist tears the worker processes down only when the session ends,
    which is fine while workers only ever finish then.  The pool
    drains, recycles and retires workers mid-run, and each one
    finished its session but kept its process: idle in execnet's serve
    loop, with its heap and a multiprocessing resource tracker.
    Measured on a debug run, 82 of them held 11.3 GB while the
    scheduler counted five live workers, and admission starved on the
    memory they took.
    """
    if _scheduler is None:
        return
    dsession = _scheduler.config.pluginmanager.getplugin("dsession")
    if dsession is None or getattr(dsession, "shuttingdown", False):
        return          # the end of the session tears every gateway down anyway
    try:
        node.ensure_teardown()
        _scheduler.stats["gateways_closed"] += 1
    except Exception as e:  # never break the session over a worker that is gone anyway
        logger.debug("dynamic scheduler: closing %s failed: %s", node, e)


@pytest.hookimpl
def pytest_runtest_logreport(report: pytest.TestReport) -> None:
    cost = getattr(report, "scylla_cost", None)
    if cost and _scheduler is not None:
        try:
            _scheduler.learn(cost)
        except Exception as e:  # never break reporting
            logger.debug("dynamic scheduler: in-run learn failed: %s", e)


@pytest.hookimpl(trylast=True)
def pytest_sessionfinish(session: pytest.Session) -> None:
    if session.config.getoption("--collect-only"):
        return
    if os.environ.get("PYTEST_XDIST_WORKER"):
        return
    if _scheduler is not None:
        # Not from tests_finished(): that predicate turns true while
        # the last worker is still running its test, and stopping
        # there would end the diagnostics (and the loop) before the
        # run does.
        _scheduler._stop_tick()
    try:
        tmpdir = Path(session.config.getoption("--tmpdir")).absolute()
        ncpus = len(os.sched_getaffinity(0))
        n = merge_run_into_profile(tmpdir, _profile_path(session.config), ncpus)
        if n:
            logger.info("dynamic scheduler: merged %d samples into %s", n, _profile_path(session.config))
    except Exception as e:
        logger.warning("dynamic scheduler: profile merge failed: %s", e)
