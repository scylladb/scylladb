#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Check that run_porcupine_checker() retries failures of the container runtime, and only those.

On CI, podman sometimes loses the checker container and exits with 127, with or
without a message on stderr, although the checker never got to give a verdict
(SCYLLADB-6545).  A fake runtime stands in for podman here: it fails a given
number of times with a given exit code and then answers like the checker would.
"""

import os
from pathlib import Path

import pytest

from test.cluster.tools.porcupine import porcupine
from test.cluster.tools.porcupine import run_porcupine_checker

HISTORY = '{"id":0,"client_id":0,"kind":"call","op":"write","key":1,"value":5,"time_ns":1}\n'

FAKE_RUNTIME = """#!/bin/sh
# "run --rm -i --name <name> ..." and "rm --force --time 0 <name>" both have the name as $5.
if [ "$1" = rm ]; then
    echo "$5" >> "$FAKE_RUNTIME_CALLS.removed"
    exit "${FAKE_RUNTIME_RM_EXIT_CODE:-0}"
fi
cat > /dev/null
calls=$(($(cat "$FAKE_RUNTIME_CALLS" 2>/dev/null || echo 0) + 1))
echo "$calls" > "$FAKE_RUNTIME_CALLS"
if [ "$calls" -le "$FAKE_RUNTIME_FAILURES" ]; then
    echo "$5" >> "$FAKE_RUNTIME_CALLS.failed"
    echo "Error: failed to connect to container's attach socket: no such file or directory" >&2
    exit "$FAKE_RUNTIME_EXIT_CODE"
fi
echo '{"valid": true, "keys_checked": 1, "total_ops": 1}'
"""


@pytest.fixture
def fake_runtime(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Point the checker at the fake runtime.

    Returns a function setting its failures, one counting its runs, and one checking that the
    container of every failed run, and only those, was removed.
    """

    runtime = tmp_path / "fake-podman"
    runtime.write_text(FAKE_RUNTIME)
    runtime.chmod(0o755)
    calls = tmp_path / "calls"
    monkeypatch.setattr(porcupine, "CONTAINER_RUNTIME", str(runtime))
    monkeypatch.setattr(porcupine, "USE_CONTAINER_CHECKER", True)
    monkeypatch.setenv("FAKE_RUNTIME_CALLS", str(calls))

    async def no_sleep(_seconds: float) -> None:
        pass
    monkeypatch.setattr(porcupine.asyncio, "sleep", no_sleep)

    def fail(times: int, exit_code: int) -> None:
        monkeypatch.setenv("FAKE_RUNTIME_FAILURES", str(times))
        monkeypatch.setenv("FAKE_RUNTIME_EXIT_CODE", str(exit_code))

    def removed_exactly_failed() -> bool:
        failed = Path(f"{calls}.failed")
        removed = Path(f"{calls}.removed")
        names = lambda path: sorted(path.read_text().split()) if path.exists() else []
        return names(removed) == names(failed)

    return fail, lambda: int(calls.read_text()), removed_exactly_failed


@pytest.mark.parametrize("exit_code", porcupine.RUNTIME_FAILURE_CODES)
async def test_runtime_failure_is_retried(fake_runtime, exit_code: int) -> None:
    """Runtime failures on all attempts but the last still end with the checker's verdict."""

    fail, calls, removed_exactly_failed = fake_runtime
    fail(porcupine.RUNTIME_FAILURE_ATTEMPTS - 1, exit_code)
    result = await run_porcupine_checker(HISTORY)
    assert result["valid"] is True
    assert calls() == porcupine.RUNTIME_FAILURE_ATTEMPTS
    assert removed_exactly_failed()


async def test_persistent_runtime_failure_is_reported(fake_runtime) -> None:
    """A runtime that fails every attempt is reported with its exit code and stderr, after the last attempt."""

    fail, calls, removed_exactly_failed = fake_runtime
    fail(porcupine.RUNTIME_FAILURE_ATTEMPTS, 127)
    with pytest.raises(RuntimeError, match=r"(?s)exited with code 127.*attach socket"):
        await run_porcupine_checker(HISTORY)
    assert calls() == porcupine.RUNTIME_FAILURE_ATTEMPTS
    assert removed_exactly_failed()


async def test_checker_error_is_not_retried(fake_runtime) -> None:
    """Exit 2 is the checker rejecting its input, not the runtime failing: no retry."""

    fail, calls, _ = fake_runtime
    fail(1, 2)
    with pytest.raises(RuntimeError, match="exited with code 2"):
        await run_porcupine_checker(HISTORY)
    assert calls() == 1
    assert not Path(f"{os.environ['FAKE_RUNTIME_CALLS']}.removed").exists()


async def test_no_retry_while_failed_container_may_run(fake_runtime, monkeypatch: pytest.MonkeyPatch) -> None:
    """If the container of a failed run cannot be removed, it may still run on the same output directory: no retry."""

    fail, calls, removed_exactly_failed = fake_runtime
    fail(1, 127)
    monkeypatch.setenv("FAKE_RUNTIME_RM_EXIT_CODE", "1")
    with pytest.raises(RuntimeError, match=r"(?s)exited with code 127.*could not be removed.*attach socket"):
        await run_porcupine_checker(HISTORY)
    assert calls() == 1
    assert removed_exactly_failed()
