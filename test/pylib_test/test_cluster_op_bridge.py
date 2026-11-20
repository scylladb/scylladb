#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""Tests for how ScyllaCluster operations reach the cluster's loop.

The cluster runs on its own event loop and owns state that belongs to it: the
Scylla subprocess transports and the per-server asyncio locks.  Its operations
therefore have to run on that loop, whichever loop -- or thread -- the caller
happens to be on.  The cluster_op decorator is what guarantees that.

The exercise here uses a real ScyllaCluster with a stub server stop, so the
decorator and the bridge under test are the production ones.
"""

import asyncio
import pathlib
import threading
import time
from collections.abc import AsyncIterator

import pytest

from test.pylib.internal_types import ServerNum
from test.pylib.scylla_cluster import ScyllaCluster
from test.pylib.util import Blocking
from test.pylib_test._cluster import make_cluster


SERVER_ID = ServerNum(1)
TIMEOUT = 30


class StubStopServer:
    """Stands in for ScyllaCluster._stop_server(), with one genuinely loop-bound object.

    ScyllaServer.stop() awaits its subprocess transport, and that transport
    suspends on a future belonging to the loop that spawned Scylla (see
    _UnixSubprocessTransport._wait).  `_release` plays that part.  Awaiting it
    from another loop is what raises "got Future attached to a different loop",
    so an operation has to actually suspend on it for these tests to mean
    anything -- hence the callers below release it only after the operation has
    entered.
    """

    def __init__(self) -> None:
        self._loop: asyncio.AbstractEventLoop | None = None
        self._release: asyncio.Future | None = None
        self.entered = threading.Event()      # readable from any thread
        self.stopped: list[ServerNum] = []

    async def __call__(self, server_id: ServerNum, gracefully: bool) -> None:
        # The future belongs to the loop the operation runs on -- the
        # cluster's -- like a real server's subprocess transport.
        self._loop = asyncio.get_running_loop()
        self._release = self._loop.create_future()
        self.entered.set()
        await self._release
        self.stopped.append(server_id)

    def release(self) -> None:
        """Let a suspended stop finish; callable from any thread."""
        if self._loop is not None:
            self._loop.call_soon_threadsafe(
                lambda: None if self._release.done() else self._release.set_result(None))


@pytest.fixture
async def cluster(tmp_path: pathlib.Path) -> AsyncIterator[tuple[ScyllaCluster, StubStopServer, asyncio.AbstractEventLoop]]:
    """A ScyllaCluster running on its own thread and loop, with a stub server stop."""
    stub = StubStopServer()
    cluster = make_cluster(tmp_path)
    cluster._stop_server = stub
    try:
        async with cluster.run_in_thread():
            try:
                yield cluster, stub, cluster._loop
            finally:
                stub.release()   # don't strand an operation on shutdown
    finally:
        await cluster.recycle()


async def test_operation_runs_on_the_clusters_loop(cluster) -> None:
    """An async caller on its own loop still gets the cluster's loop."""
    cluster, stub, cluster_loop = cluster
    assert asyncio.get_running_loop() is not cluster_loop

    stopping = asyncio.ensure_future(cluster.server_stop(SERVER_ID, convict=False))
    assert await asyncio.to_thread(stub.entered.wait, TIMEOUT), "operation never started"
    assert stub._loop is cluster_loop, "operation ran off the cluster's loop"
    stub.release()
    await asyncio.wait_for(stopping, TIMEOUT)

    assert stub.stopped == [SERVER_ID]


def test_operation_from_a_worker_thread(cluster) -> None:
    """A synchronous caller on a worker thread reaches loop-bound state.

    This is the dtest pattern -- `executor.submit(node.stop, ...)` -- and the
    reason cluster_op cannot simply await the entry point: Blocking runs
    the call on a fresh event loop, and the cluster's state does not belong
    to it.
    """
    cluster, stub, cluster_loop = cluster
    errors: list[BaseException] = []

    def worker() -> None:
        try:
            Blocking(cluster).server_stop(SERVER_ID, convict=False)
        except BaseException as exc:  # pylint: disable=broad-except
            errors.append(exc)

    thread = threading.Thread(target=worker)
    thread.start()
    assert stub.entered.wait(TIMEOUT), "operation never started"
    assert stub._loop is cluster_loop, "operation ran off the cluster's loop"
    stub.release()
    thread.join(timeout=TIMEOUT)

    assert not thread.is_alive(), "call from a worker thread hung"
    assert not errors, f"call from a worker thread failed: {errors[0]!r}"
    assert stub.stopped == [SERVER_ID]


async def test_caller_timeout_leaves_the_operation_running(cluster) -> None:
    """A timed-out caller orphans the operation instead of aborting it.

    The HTTP server never cancelled a handler when its client went away, and
    after_test() relies on that: it drains whatever a test left running.
    """
    cluster, stub, cluster_loop = cluster

    with pytest.raises(asyncio.TimeoutError):
        async with asyncio.timeout(0.2):
            await cluster._server_stop(SERVER_ID)

    assert stub.entered.is_set(), "operation never started"
    assert stub._loop is cluster_loop, "operation ran off the cluster's loop"
    assert len(cluster.tasks_history) == 1, "operation was not left for the drain"
    op, descr = next(iter(cluster.tasks_history.items()))
    assert not op.done()
    assert descr.startswith("_server_stop")

    # Once it completes it removes itself, which is what leaves after_test()'s
    # drain looking only at genuinely in-flight work.  op_finished is
    # registered before gather's own done callback, so by the time the wait
    # below returns the entry is already gone.
    stub.release()

    async def wait_operations_done() -> None:
        await asyncio.gather(*cluster.tasks_history)

    await asyncio.wrap_future(
        asyncio.run_coroutine_threadsafe(wait_operations_done(), cluster_loop))
    assert not cluster.tasks_history
    assert stub.stopped == [SERVER_ID]


async def test_exception_keeps_its_type(cluster) -> None:
    """Failures arrive as themselves, not as an HTTP status turned into a string."""
    cluster, _, _ = cluster

    async def fail(server_id: ServerNum, gracefully: bool) -> None:
        raise ValueError("stop failed")

    cluster._stop_server = fail

    with pytest.raises(ValueError, match="stop failed"):
        await cluster.server_stop(SERVER_ID, convict=False)


async def test_without_a_thread_an_operation_runs_on_the_callers_loop(tmp_path: pathlib.Path) -> None:
    """A cluster that never starts its thread, like a module-scoped one, belongs to its caller's loop."""
    cluster = make_cluster(tmp_path)
    loops = []

    async def stop_server(server_id: ServerNum, gracefully: bool) -> None:
        loops.append(asyncio.get_running_loop())

    cluster._stop_server = stop_server
    try:
        await cluster.server_stop(SERVER_ID, convict=False)
    finally:
        await cluster.recycle()

    assert loops == [asyncio.get_running_loop()]


async def test_operation_after_the_thread_is_gone_fails(tmp_path: pathlib.Path) -> None:
    """A caller that outlives run_in_thread() is told that the cluster is stopped."""
    cluster = make_cluster(tmp_path)
    try:
        async with cluster.run_in_thread():
            pass
        with pytest.raises(AssertionError, match="ScyllaCluster is stopped"):
            await cluster.running_servers()
    finally:
        await cluster.recycle()


async def test_without_a_thread_cancelling_the_caller_cancels_the_operation(tmp_path: pathlib.Path) -> None:
    """Without a thread, nothing would drain an orphaned operation before recycle(), so none is left."""
    cluster = make_cluster(tmp_path)
    entered = asyncio.Event()
    cancelled = []

    async def stop_server(server_id: ServerNum, gracefully: bool) -> None:
        entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.append(server_id)
            raise

    cluster._stop_server = stop_server
    try:
        stopping = asyncio.ensure_future(cluster.server_stop(SERVER_ID, convict=False))
        await asyncio.wait_for(entered.wait(), TIMEOUT)
        stopping.cancel()
        with pytest.raises(asyncio.CancelledError):
            await stopping

        assert cancelled == [SERVER_ID], "the operation outlived its cancelled caller"
        assert not cluster.tasks_history
    finally:
        await cluster.recycle()


async def test_leftover_operation_ends_before_the_final_stop(tmp_path: pathlib.Path) -> None:
    """An operation still running when run_in_thread() exits ends before stop() looks at the servers.

    after_test() normally drains such operations, but not when the fixture
    never got that far, or the drain ran out of time.  An add left running
    across the final stop() could put a server in `running` after stop()
    took its list, and the server would be marked stopped while it runs.
    """
    cluster = make_cluster(tmp_path)
    events = []
    entered = threading.Event()

    async def stop_server(server_id: ServerNum, gracefully: bool) -> None:
        entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            events.append("operation cancelled")
            raise

    real_stop = cluster.stop

    async def stop() -> None:
        events.append("stop")
        await real_stop()

    cluster._stop_server = stop_server
    cluster.stop = stop
    try:
        async with cluster.run_in_thread():
            with pytest.raises(TimeoutError):
                async with asyncio.timeout(0.5):
                    await cluster.server_stop(SERVER_ID, convict=False)
            assert entered.is_set(), "operation never started"
    finally:
        await cluster.recycle()

    assert events[:2] == ["operation cancelled", "stop"]


def test_cancelled_entry_lets_the_cluster_thread_finish(tmp_path: pathlib.Path) -> None:
    """A run_in_thread() cancelled while its thread starts still tells the thread to finish.

    Otherwise leaving the context waits for a thread that is itself waiting to
    be told to finish, and the pytest worker hangs.
    """
    cluster = make_cluster(tmp_path)
    setting_up = threading.Event()
    logger = cluster.logger

    class SlowStart:
        """The cluster's logger, holding the cluster thread just before it signals readiness."""

        def __getattr__(self, name: str) -> object:
            return getattr(logger, name)

        def info(self, msg: str, *args: object) -> None:
            if msg == "Setting up %s":
                setting_up.set()
                time.sleep(0.5)
            logger.info(msg, *args)

    cluster.logger = SlowStart()
    errors: list[BaseException] = []

    async def scenario() -> None:
        async def enter() -> None:
            async with cluster.run_in_thread():
                pass

        entering = asyncio.ensure_future(enter())
        assert await asyncio.to_thread(setting_up.wait, TIMEOUT), "cluster thread never started"
        entering.cancel()
        with pytest.raises(asyncio.CancelledError):
            await entering
        await cluster.recycle()

    def run() -> None:
        try:
            asyncio.run(scenario())
        except BaseException as exc:  # pylint: disable=broad-except
            errors.append(exc)

    # On a thread of its own, so that a hang fails the test instead of stalling the run.
    runner = threading.Thread(target=run, daemon=True)
    runner.start()
    runner.join(TIMEOUT)

    assert not runner.is_alive(), "leaving a cancelled run_in_thread() hung"
    assert not errors, f"the scenario failed: {errors[0]!r}"
