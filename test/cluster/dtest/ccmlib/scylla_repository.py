#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Stand-in for ccm's ccmlib/scylla_repository.py.

ccm downloads a relocatable Scylla package for a version spec and unpacks it into
a cache directory, so a test can run one node on one version and another node on
another one.  In-tree that job is already done by
`test.pylib.version_fetch_utils.fetch_and_install_scylla_version()`, which is how
the non-dtest upgrade tests (test/cluster/test_fencing.py and friends, through the
`scylla_2025_1` fixture) get hold of an older Scylla.  This module puts ccm's
interface on top of it, so the ported upgrade dtests can ask for "release:2025.1"
and get a genuinely older binary.

Two kinds of version spec are understood:

  * ``release:<major>.<minor>[.<patch>]`` -- a released relocatable package from
    ScyllaDB's download server, cached under XDG_CACHE_HOME (see
    `fetch_and_install_scylla_version()`).  Without a patch number the latest
    patch release of that series is used.
  * the version of the build under test, as `current_version()` returns it --
    resolved to the tree this test run was started from.

ccm's remaining specs (``unstable/<branch>:<timestamp>`` and the like, which point
at nightly builds) are not supported: raise rather than silently hand back the
wrong Scylla.

An install is described by `ScyllaInstall`:

  * ``install_dir`` is ccm's "install dir": it holds ``conf/scylla.yaml`` (which
    `ccmlib.common.get_default_scylla_yaml()` reads to decide, for example,
    whether a version has tablets on by default) and ``SCYLLA-VERSION-FILE``.
  * ``exe`` is the executable a node runs.  It is None for the build under test,
    whose path belongs to the test runner (``--exe-path``/the build mode), not to
    this module; `ScyllaCluster` fills it in from the cluster manager.
"""

from __future__ import annotations

import logging
import platform
import re
from dataclasses import dataclass
from pathlib import Path
from threading import Lock

from test.pylib.version_fetch_utils import fetch_and_install_scylla_version

logger = logging.getLogger("ccm")

# test/cluster/dtest/ccmlib/scylla_repository.py -> the scylladb checkout.
SCYLLA_REPO_ROOT = Path(__file__).resolve().parents[4]
SCYLLA_VERSION_FILE = SCYLLA_REPO_ROOT / "build" / "SCYLLA-VERSION-FILE"

RELEASE_SPEC = re.compile(r"^release:(?P<major>\d+)\.(?P<minor>\d+)(?:\.(?P<patch>\d+))?$")

# Build modes whose Scylla is built with assertions and without optimizations; the
# download server publishes a matching "debug" relocatable for them.
DEBUG_BUILD_MODES = frozenset({"debug", "sanitize"})


@dataclass(frozen=True)
class ScyllaInstall:
    """One Scylla version a test can run a node on."""

    spec: str  # the version spec as the test asked for it, e.g. "release:2025.1"
    version: str  # the resolved version, e.g. "2025.1.15" or "2026.4.0-dev"
    install_dir: Path  # ccm-style install dir: conf/scylla.yaml, SCYLLA-VERSION-FILE
    exe: Path | None  # the executable, or None for the build under test

    @property
    def is_current(self) -> bool:
        return self.exe is None


_lock = Lock()
_installs: dict[tuple[str, str], ScyllaInstall] = {}  # (spec, pack) -> install
_by_dir: dict[str, ScyllaInstall] = {}  # str(install_dir) -> install
_build_mode = "release"


def set_build_mode(mode: str) -> None:
    """Remember which build mode the run uses, so we pick the matching relocatable.

    A debug run has to upgrade from a debug relocatable: a release package runs so
    much faster that the timeouts a debug run is tuned for stop meaning anything,
    and it carries none of the assertions a debug build is there for.
    """
    global _build_mode  # noqa: PLW0603
    _build_mode = mode


def _pack() -> str:
    return "debug" if _build_mode in DEBUG_BUILD_MODES else ""


def current_version() -> str:
    """The version of the Scylla built in this tree, e.g. "2026.4.0-dev"."""
    try:
        return SCYLLA_VERSION_FILE.read_text().strip()
    except OSError as exc:
        raise RuntimeError(
            f"cannot read the version of the build under test from {SCYLLA_VERSION_FILE}: {exc}"
        ) from exc


def is_current(version: str) -> bool:
    """Is this spec the build under test rather than a downloadable release?"""
    return not RELEASE_SPEC.match(version)


def install(version: str) -> ScyllaInstall:
    """Resolve a version spec to a Scylla that can actually be run.

    Downloads and installs a released relocatable package if needed; repeated
    calls for the same spec reuse the cached install.
    """
    if is_current(version):
        current = current_version()
        if version != current:
            raise ValueError(
                f"unsupported Scylla version spec {version!r}: this tree can only run released"
                f" packages (release:<major>.<minor>[.<patch>]) or the build under test ({current})"
            )
        found = ScyllaInstall(spec=version, version=current, install_dir=SCYLLA_REPO_ROOT, exe=None)
        with _lock:
            _by_dir[str(found.install_dir)] = found
        return found

    pack = _pack()
    key = (version, pack)
    with _lock:
        if cached := _installs.get(key):
            return cached

    match = RELEASE_SPEC.match(version)
    major = int(match.group("major"))
    minor = int(match.group("minor"))
    patch = int(match.group("patch")) if match.group("patch") else None

    logger.info("Fetching Scylla %s (pack=%s) relocatable package", version, pack or "default")
    exe = Path(
        fetch_and_install_scylla_version(major, minor, patch, arch=platform.machine(), pack=pack)
    )

    # fetch_and_install_scylla_version() lays an archive out as
    #   <cache>/<archive>/unpacked/scylla/        -- the relocatable as shipped
    #   <cache>/<archive>/installed/bin/scylla    -- what its install.sh produced
    # The first is ccm's install dir (conf/, SCYLLA-VERSION-FILE), the second is
    # the runnable executable.
    install_dir = exe.parents[2] / "unpacked" / "scylla"
    version_file = install_dir / "SCYLLA-VERSION-FILE"
    if not version_file.is_file():
        raise RuntimeError(f"{version}: no SCYLLA-VERSION-FILE under {install_dir}")
    release = version_file.read_text().strip()

    found = ScyllaInstall(spec=version, version=release, install_dir=install_dir, exe=exe)
    with _lock:
        _installs[key] = found
        _by_dir[str(install_dir)] = found
    logger.info("Scylla %s is %s at %s", version, release, exe)
    return found


def install_for_dir(install_dir: str | Path) -> ScyllaInstall:
    """The install that `setup()` handed out for this directory.

    ccm passes versions around as install directories (`Cluster.set_install_dir()`),
    so this is the way back from one to the version it belongs to.
    """
    resolved = str(Path(install_dir).resolve())
    with _lock:
        if found := _by_dir.get(resolved):
            return found
    if resolved == str(SCYLLA_REPO_ROOT):
        return install(current_version())
    raise KeyError(f"{install_dir} is not a Scylla install this run set up")


def setup(version: str, verbose: bool = True, skip_downloads: bool = False) -> tuple[str, str]:
    """ccm's entry point: return (install dir, resolved version) for a version spec."""
    found = install(version)
    return str(found.install_dir), found.version
