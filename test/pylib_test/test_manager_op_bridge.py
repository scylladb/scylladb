#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""Tests for how ScyllaClusterManager operations reach the manager's loop.

The manager runs on its own event loop and owns state that belongs to it: the
Scylla subprocess transports and the per-server asyncio locks.  Its operations
therefore have to run on that loop, whichever loop -- or thread -- the caller
happens to be on.  The manager_op decorator is what guarantees that.

The exercise here uses a real ScyllaClusterManager with a stub cluster, so the
decorator and the bridge under test are the production ones.
"""

import asyncio
import concurrent.futures
import logging
import threading
from collections.abc import Iterator

import pytest

from test.pylib.internal_types import ServerNum
from test.pylib.util import LogPrefixAdapter
from test.pylib.scylla_cluster_manager import ScyllaClusterManager


SERVER_ID = ServerNum(1)
TIMEOUT = 30


class StubCluster:
    """Stands in for ScyllaCluster, with one genuinely loop-bound object.

    ScyllaServer.stop() awaits its subprocess transport, and that transport
    suspends on a future belonging to the loop that spawned Scylla (see
    _UnixSubprocessTransport._wait).  `_release` plays that part.  Awaiting it
    from another loop is what raises "got Future attached to a different loop",
    so an operation has to actually suspend on it for these tests to mean
    anything -- hence the callers below release it only after the operation has
    entered.
    """

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop
        self._release = loop.create_future()
        self.entered = threading.Event()      # readable from any thread
        self.stopped: list[ServerNum] = []

    async def server_stop(self, server_id: ServerNum, gracefully: bool) -> None:
        self.entered.set()
        await self._release
        self.stopped.append(server_id)

    def release(self) -> None:
        """Let a suspended server_stop() finish; callable from any thread."""
        self._loop.call_soon_threadsafe(
            lambda: None if self._release.done() else self._release.set_result(None))


@pytest.fixture
def manager() -> Iterator[tuple[ScyllaClusterManager, StubCluster, asyncio.AbstractEventLoop]]:
    """A ScyllaClusterManager running on its own thread and loop.

    Mirrors the _scylla_cluster_manager fixture in test/cluster/conftest.py: the loop
    stays alive but idle until teardown, so callers on other loops and threads
    have something to hand work to.
    """
    ready: concurrent.futures.Future = concurrent.futures.Future()
    stop_event = threading.Event()

    async def run_manager() -> None:
        mgr = ScyllaClusterManager(
            test_uname="test_manager_op_bridge",
            create_cluster=None,
            base_dir="",
            port=9042,
            use_ssl=False,
            auth_provider=None,
        )
        mgr.logger = LogPrefixAdapter(logging.getLogger("test_manager_op_bridge"), {"prefix": "test"})
        mgr.cluster = StubCluster(asyncio.get_running_loop())
        ready.set_result((mgr, asyncio.get_running_loop()))
        await asyncio.get_running_loop().run_in_executor(None, stop_event.wait)

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(asyncio.run, run_manager())
        future.add_done_callback(
            lambda f: None if ready.done() else ready.set_exception(
                f.exception() or RuntimeError("manager exited before signaling readiness")))
        mgr, loop = ready.result(timeout=TIMEOUT)
        try:
            yield mgr, mgr.cluster, loop
        finally:
            mgr.cluster.release()   # don't strand an operation on shutdown
            stop_event.set()
            future.result(timeout=TIMEOUT)


async def test_operation_runs_on_the_managers_loop(manager) -> None:
    """An async caller on its own loop still gets the manager's loop."""
    mgr, cluster, manager_loop = manager
    assert asyncio.get_running_loop() is not manager_loop

    stopping = asyncio.ensure_future(mgr.server_stop(SERVER_ID, convict=False))
    assert await asyncio.to_thread(cluster.entered.wait, TIMEOUT), "operation never started"
    cluster.release()
    await asyncio.wait_for(stopping, TIMEOUT)

    assert cluster.stopped == [SERVER_ID]


def test_operation_from_a_worker_thread(manager) -> None:
    """A synchronous caller on a worker thread reaches loop-bound state.

    This is the dtest pattern -- `executor.submit(node.stop, ...)` -- and the
    reason manager_op cannot simply await the entry point: universalasync
    creates a fresh event loop per thread, and the manager's state does not
    belong to it.
    """
    mgr, cluster, _ = manager
    errors: list[BaseException] = []

    def worker() -> None:
        try:
            # No running loop here, so universalasync drives the call itself.
            mgr.server_stop(SERVER_ID, convict=False)
        except BaseException as exc:  # pylint: disable=broad-except
            errors.append(exc)

    thread = threading.Thread(target=worker)
    thread.start()
    assert cluster.entered.wait(TIMEOUT), "operation never started"
    cluster.release()
    thread.join(timeout=TIMEOUT)

    assert not thread.is_alive(), "call from a worker thread hung"
    assert not errors, f"call from a worker thread failed: {errors[0]!r}"
    assert cluster.stopped == [SERVER_ID]


async def test_caller_timeout_leaves_the_operation_running(manager) -> None:
    """A timed-out caller orphans the operation instead of aborting it.

    The HTTP server never cancelled a handler when its client went away, and
    after_test() relies on that: it drains whatever a test left running.
    """
    mgr, cluster, manager_loop = manager

    with pytest.raises(asyncio.TimeoutError):
        async with asyncio.timeout(0.2):
            await mgr._server_stop(SERVER_ID)

    assert cluster.entered.is_set(), "operation never started"
    assert len(mgr.tasks_history) == 1, "operation was not left for the drain"
    op, descr = next(iter(mgr.tasks_history.items()))
    assert not op.done()
    assert descr.startswith("_server_stop")

    # Once it completes it removes itself, which is what leaves after_test()'s
    # drain looking only at genuinely in-flight work.  op_finished is
    # registered before gather's own done callback, so by the time the wait
    # below returns the entry is already gone.
    cluster.release()

    async def wait_operations_done() -> None:
        await asyncio.gather(*mgr.tasks_history)

    await asyncio.wrap_future(
        asyncio.run_coroutine_threadsafe(wait_operations_done(), manager_loop))
    assert not mgr.tasks_history
    assert cluster.stopped == [SERVER_ID]


async def test_exception_keeps_its_type(manager) -> None:
    """Failures arrive as themselves, not as an HTTP status turned into a string."""
    mgr, cluster, _ = manager

    async def fail(server_id: ServerNum, gracefully: bool) -> None:
        raise ValueError("stop failed")

    cluster.server_stop = fail

    with pytest.raises(ValueError, match="stop failed"):
        await mgr.server_stop(SERVER_ID, convict=False)
