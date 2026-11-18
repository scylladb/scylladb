#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# This module must only import the Python standard library: it runs before
# the test dependencies are available.

import hashlib
import os
import pathlib
import re
import subprocess
import sys


def ensure_running_under_uv(script: str) -> None:
    """Re-exec the given Python script under `uv run --locked`, so the whole
    process (not just some packages spliced onto sys.path) runs inside a
    venv synced from test/pyproject.toml / test/uv.lock, instead of
    depending on whatever happens to be installed on the host or frozen
    into the toolchain image. `script` is the path of the running script
    (e.g., __file__ or sys.argv[0]), and it is re-run with the same
    command-line arguments.

    `--locked` makes `uv run` fail loudly instead of silently re-resolving
    if test/pyproject.toml and test/uv.lock have drifted apart. `uv run`
    also re-syncs the venv in place before running if the lockfile changed
    since the last run, so bumping a pin there takes effect immediately,
    with no toolchain image rebuild required.

    The venv lives under build/test-dependencies, keyed by the ABI and the
    path of the interpreter `uv` will actually run it with (per
    test/pyproject.toml's requires-python, not necessarily the interpreter
    currently running this function), so `rm -rf build` wipes it along with
    everything else, and different checkouts (potentially on different Python
    versions) never share (or clobber) each other's packages.

    Set DISABLE_VENV=1 to skip all of this and run the script directly under
    whatever Python/environment invoked it -- e.g. to use a specific
    interpreter, a manually-managed venv, or a debugger, or when `uv` isn't
    available. Dependencies are then whatever's already importable; nothing
    installs or syncs them. The re-exec sets DISABLE_VENV=1 itself, so the
    same variable doubles as the guard that makes this run exactly once per
    process tree rather than looping.
    """
    if os.environ.get("DISABLE_VENV"):
        return

    top_src_dir = pathlib.Path(__file__).resolve().parent.parent.parent
    os.environ["UV_PROJECT"] = str(top_src_dir / "test")
    try:
        resolved_python = _find_python(top_src_dir)
        soabi = subprocess.check_output(
            [resolved_python, "-c", "import sysconfig; print(sysconfig.get_config_var('SOABI'))"],
            text=True).strip()
    except FileNotFoundError:
        sys.exit("Running the tests requires 'uv'. Install it (e.g., with ./install-dependencies.sh), "
                 "or set DISABLE_VENV=1 to use the Python packages installed on the system.")
    # A venv is tied to the interpreter it was created with, so the venv's
    # name includes the interpreter's path and not just its ABI. Otherwise, two
    # interpreters of the same version, e.g., the host's and the frozen
    # toolchain's, would share a venv, and uv would silently rebuild it every
    # time we switch between them.
    path_hash = hashlib.sha256(os.path.realpath(resolved_python).encode()).hexdigest()[:8]
    venv_dir = top_src_dir / "build" / "test-dependencies" / f"{soabi}-{path_hash}"

    # The first sync of a new venv can take minutes, so let uv show its
    # progress then, instead of looking hung.
    quiet = ["--quiet"] if venv_dir.exists() else []
    # Pass on the interpreter's own options, e.g., "python3 -X dev test.py".
    interpreter_options = sys.orig_argv[1:len(sys.orig_argv) - len(sys.argv)]

    os.environ["DISABLE_VENV"] = "1"
    os.environ["UV_PROJECT_ENVIRONMENT"] = str(venv_dir)
    # "python3" in `uv run` is the synced venv's own interpreter. We name it
    # explicitly because the script may not have a .py extension (e.g.,
    # test/cqlpy/run), and then `uv run` wouldn't know it's a Python script.
    os.execvp("uv", ["uv", "run", "--locked", *quiet, "python3", *interpreter_options,
                     os.path.abspath(script), *sys.argv[1:]])


def _find_python(top_src_dir: pathlib.Path) -> str:
    """Return the interpreter uv will use for test/pyproject.toml.

    Ask uv which interpreter it will use to satisfy test/pyproject.toml's
    requires-python, rather than assuming it matches the one running this
    function -- they can differ, e.g. the system's default "python3" is
    older than what the project requires and uv fetches/uses a newer one.
    "uv python find" only looks at interpreters that are already installed,
    so if none of them is new enough, install one first, as "uv run" would.
    """
    try:
        return subprocess.check_output(["uv", "python", "find"], text=True,
                                       stderr=subprocess.DEVNULL).strip()
    except subprocess.CalledProcessError:
        pass
    # tomllib isn't available before Python 3.11, which may be the host's.
    pyproject = (top_src_dir / "test" / "pyproject.toml").read_text()
    requires_python = re.search(r'^requires-python\s*=\s*"([^"]+)"', pyproject, re.MULTILINE).group(1)
    # --no-bin: unlike a plain "uv python install", don't also put the new
    # interpreter in ~/.local/bin, where it could shadow the system's python3.X.
    subprocess.check_call(["uv", "python", "install", "--no-bin", requires_python])
    return subprocess.check_output(["uv", "python", "find"], text=True).strip()
