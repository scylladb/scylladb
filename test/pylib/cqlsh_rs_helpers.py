# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""Shared helpers for the cqlsh-rs cargo integration test wrappers.

cqlsh-rs groups its integration tests into categories, each needing a
differently configured Scylla.  The categories are described by
tools/cqlsh-rs/tests/test_categories.toml, which upstream maintains
specifically for external orchestrators like test.py, so this module reads
that manifest instead of hardcoding the mapping.

Everything is built once with the ``test-all`` feature into a dedicated
target directory.  ``test-all`` is the union of the per-category features, so
a single binary serves every suite and cargo never has to rebuild because the
feature set changed.  That also makes test discovery deterministic: the binary
already exists by the time any test module is imported, so every xdist worker
lists exactly the same tests.
"""

import fcntl
import os
import shutil
import subprocess
import tomllib

from test import TOP_SRC_DIR


CQLSH_RS_DIR = TOP_SRC_DIR / "tools" / "cqlsh-rs"

MANIFEST_PATH = CQLSH_RS_DIR / "tests" / "test_categories.toml"

# All suites share one target directory and one feature set, so the test binary
# is compiled exactly once.  Keeping it under the submodule's target/ keeps it
# out of the way of a developer's own `cargo build`, and keeps the build lock
# per checkout -- concurrent Jenkins workspaces on one node must not share it.
CARGO_TARGET_DIR = CQLSH_RS_DIR / "target" / "pytest"

BUILD_LOCK_PATH = CARGO_TARGET_DIR / "build.lock"

CARGO_FEATURES = "test-all"

# Long enough for a cold build of the test binary and its dependencies.
BUILD_TIMEOUT = 1800


class CargoError(Exception):
    """cargo failed in a way that means the tests could not be trusted."""


def cargo_available() -> bool:
    return shutil.which("cargo") is not None


def load_categories() -> dict[str, dict]:
    """Return the category table from cqlsh-rs' test_categories.toml."""
    with open(MANIFEST_PATH, "rb") as f:
        return tomllib.load(f)


def category_modules(category: str) -> list[str]:
    """Test modules belonging to `category`, per the upstream manifest.

    An empty list means the category exists but has no tests yet, which is a
    fact about cqlsh-rs rather than a failure on our side.
    """
    categories = load_categories()
    if category not in categories:
        raise CargoError(
            f"category {category!r} is not in {MANIFEST_PATH}; "
            f"known categories: {sorted(categories)}"
        )
    return list(categories[category].get("modules", []))


def _cargo_env() -> dict[str, str]:
    return {**os.environ, "CARGO_TARGET_DIR": str(CARGO_TARGET_DIR)}


def build_test_binary() -> None:
    """Compile the integration test binary, once per checkout.

    Serialised with a file lock so parallel xdist workers cooperate instead of
    fighting over cargo's own lock.  Raises CargoError if the build fails --
    a cqlsh-rs that does not compile must fail the suite, not skip it.
    """
    CARGO_TARGET_DIR.mkdir(parents=True, exist_ok=True)
    with open(BUILD_LOCK_PATH, "w") as lock_fd:
        fcntl.flock(lock_fd, fcntl.LOCK_EX)
        try:
            result = subprocess.run(
                ["cargo", "test", "--test", "integration",
                 "--features", CARGO_FEATURES, "--no-run"],
                cwd=CQLSH_RS_DIR,
                env=_cargo_env(),
                capture_output=True,
                text=True,
                timeout=BUILD_TIMEOUT,
            )
        except subprocess.TimeoutExpired as exc:
            raise CargoError(
                f"building the cqlsh-rs integration tests timed out after {BUILD_TIMEOUT}s"
            ) from exc
        finally:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)

    if result.returncode != 0:
        raise CargoError(
            "failed to build the cqlsh-rs integration tests:\n"
            f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )


def discover_tests(category: str) -> list[str]:
    """Names of `category`'s tests, taken from the built binary.

    Returns [] only when the category has no modules upstream.  Any other
    empty result means something went wrong and is raised, so a broken build
    can never masquerade as a suite with nothing to run.
    """
    modules = category_modules(category)
    if not modules:
        return []

    build_test_binary()

    result = subprocess.run(
        ["cargo", "test", "--test", "integration", "--features", CARGO_FEATURES,
         "--", "--ignored", "--list"],
        cwd=CQLSH_RS_DIR,
        env=_cargo_env(),
        capture_output=True,
        text=True,
        timeout=BUILD_TIMEOUT,
    )
    if result.returncode != 0:
        raise CargoError(
            f"listing the cqlsh-rs integration tests failed:\n"
            f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )

    prefixes = tuple(f"{module}::" for module in modules)
    tests = [
        line.removesuffix(": test")
        for line in result.stdout.splitlines()
        if line.endswith(": test") and line.startswith(prefixes)
    ]
    if not tests:
        raise CargoError(
            f"category {category!r} lists modules {modules} in {MANIFEST_PATH}, "
            f"but none of their tests were found in the built binary. "
            f"cargo test --list said:\n{result.stdout}"
        )
    return tests


def run_test(test_name: str, env: dict, timeout: int = 300) -> None:
    """Run a single cargo integration test against an already-running Scylla."""
    result = subprocess.run(
        ["cargo", "test", "--test", "integration", "--features", CARGO_FEATURES,
         test_name, "--", "--exact", "--ignored"],
        cwd=CQLSH_RS_DIR,
        env={**env, "CARGO_TARGET_DIR": str(CARGO_TARGET_DIR)},
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    assert result.returncode == 0, (
        f"cargo test {test_name!r} failed:\n"
        f"STDOUT:\n{result.stdout}\n"
        f"STDERR:\n{result.stderr}"
    )
