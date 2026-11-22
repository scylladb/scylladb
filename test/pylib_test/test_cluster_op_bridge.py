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

The exercise here uses a real ScyllaCluster with a stub server, so the
decorator and the bridge under test are the production ones.
"""

import asyncio
import pathlib
import threading
import time
from collections.abc import AsyncIterator
from types import SimpleNamespace

import pytest

from test.cluster.dtest.ccmlib.common import Blocking
from test.pylib.internal_types import ServerNum
from test.pylib.scylla_cluster import ScyllaCluster
from test.pylib_test._cluster import make_cluster


SERVER_ID = ServerNum(1)
TIMEOUT = 30


class StubServer:
    """Stands in for a running ScyllaServer, with one genuinely loop-bound object.

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
        self.released = False
        self.stopped = False
        # What recycle() touches.
        self.log_file = None
        self.maintenance_socket_dir = SimpleNamespace(cleanup=lambda: None)

    async def stop(self) -> None:
        # Only the first stop before release() suspends; a later one, like the
        # cluster's final stop(), returns at once.
        if not self.entered.is_set() and not self.released:
            # The future belongs to the loop the operation runs on -- the
            # cluster's -- like a real server's subprocess transport.
            self._loop = asyncio.get_running_loop()
            self._release = self._loop.create_future()
            self.entered.set()
            await self._release
        self.stopped = True

    def release(self) -> None:
        """Let a suspended stop finish, and later ones not suspend; callable from any thread."""
        self.released = True
        if self._loop is not None:
            self._loop.call_soon_threadsafe(
                lambda: None if self._release.done() else self._release.set_result(None))


@pytest.fixture
async def cluster(tmp_path: pathlib.Path) -> AsyncIterator[tuple[ScyllaCluster, StubServer, asyncio.AbstractEventLoop]]:
    """A ScyllaCluster running on its own thread and loop, with one running stub server."""
    stub = StubServer()
    cluster = make_cluster(tmp_path)
    cluster.running[SERVER_ID] = stub
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

    assert stub.stopped
    assert cluster.stopped == {SERVER_ID: stub}


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
    assert stub.stopped
    assert cluster.stopped == {SERVER_ID: stub}


async def test_caller_timeout_leaves_the_operation_running(cluster) -> None:
    """A timed-out caller orphans the operation instead of aborting it.

    The HTTP server never cancelled a handler when its client went away, and
    after_test() relies on that: it drains whatever a test left running.
    """
    cluster, stub, cluster_loop = cluster

    with pytest.raises(asyncio.TimeoutError):
        async with asyncio.timeout(0.2):
            await cluster._server_stop(SERVER_ID, gracefully=False)

    assert stub.entered.is_set(), "operation never started"
    assert stub._loop is cluster_loop, "operation ran off the cluster's loop"
    assert len(cluster.tasks_history) == 1, "operation was not left for the drain"
    op, descr = next(iter(cluster.tasks_history.items()))
    assert not op.done()
    assert descr == "_server_stop(1,)"

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
    assert stub.stopped
    assert cluster.stopped == {SERVER_ID: stub}


async def test_leftover_operations_share_one_drain_budget(tmp_path: pathlib.Path,
                                                          monkeypatch: pytest.MonkeyPatch) -> None:
    """after_test() gives the operations a test left running one budget between them, not one each.

    A batch add leaves an operation per server; with a budget each, the batch
    could outlast the budget before the drain reports it.
    """
    budget = 2.0
    # The drain's budget is graceful_stop_timeout() + 60 s.
    monkeypatch.setattr("test.pylib.scylla_cluster.graceful_stop_timeout", lambda mode: budget - 60)
    cluster = make_cluster(tmp_path)
    first, second = StubServer(), StubServer()
    cluster.running[ServerNum(1)] = first
    cluster.running[ServerNum(2)] = second
    try:
        async with cluster.run_in_thread():
            for server_id in (ServerNum(1), ServerNum(2)):
                with pytest.raises(TimeoutError):
                    async with asyncio.timeout(0.2):
                        await cluster._server_stop(server_id, gracefully=False)
            # The second ends within a budget of the first one's end, but past
            # the budget of the drain as a whole.
            loop = asyncio.get_running_loop()
            releases = [loop.call_later(budget / 2, first.release),
                        loop.call_later(budget * 1.25, second.release)]
            try:
                result = await cluster.after_test(success=True, gather_io=False)
            finally:
                for release in releases:
                    release.cancel()
                first.release()
                second.release()
    finally:
        await cluster.recycle()

    assert result["tasks_leaked"]
    assert result["message"].startswith("timed out waiting")


async def test_exception_keeps_its_type(cluster) -> None:
    """Failures arrive as themselves, not as an HTTP status turned into a string."""
    cluster, _, _ = cluster

    with pytest.raises(AssertionError, match="Server 2 unknown"):
        await cluster.server_stop(ServerNum(2), convict=False)


async def test_add_server_runs_on_the_clusters_loop(cluster, monkeypatch: pytest.MonkeyPatch) -> None:
    """add_server() is an operation too: it leases its address on the cluster's loop."""
    cluster, _, cluster_loop = cluster
    loops = []

    async def lease_host() -> str:
        loops.append(asyncio.get_running_loop())
        raise RuntimeError("no address")

    # The host registry is shared by the whole session: undo the stub at teardown.
    monkeypatch.setattr(cluster.host_registry, "lease_host", lease_host)

    with pytest.raises(RuntimeError, match="no address"):
        await cluster.add_server()
    assert loops == [cluster_loop]


async def test_operation_without_the_thread_fails(tmp_path: pathlib.Path) -> None:
    """An operation fails loudly unless the cluster runs on its thread, before run_in_thread() as after it."""
    cluster = make_cluster(tmp_path)
    try:
        with pytest.raises(AssertionError, match="not running on its thread"):
            await cluster.running_servers()
        async with cluster.run_in_thread():
            pass
        with pytest.raises(AssertionError, match="not running on its thread"):
            await cluster.running_servers()
    finally:
        await cluster.recycle()


async def test_leftover_operation_ends_before_the_final_stop(tmp_path: pathlib.Path,
                                                             caplog: pytest.LogCaptureFixture) -> None:
    """An operation still running when run_in_thread() exits ends before stop() looks at the servers.

    after_test() normally drains such operations, but not when the fixture
    never got that far, or the drain ran out of time.  An add left running
    across the final stop() could put a server in `running` after stop()
    took its list, and the server would be marked stopped while it runs.
    """
    cluster = make_cluster(tmp_path)
    stub = StubServer()
    cluster.running[SERVER_ID] = stub
    running_at_stop = []
    real_stop = cluster.stop

    async def stop() -> None:
        running_at_stop.append(list(cluster.tasks_history.values()))
        await real_stop()

    cluster.stop = stop
    try:
        async with cluster.run_in_thread():
            with pytest.raises(TimeoutError):
                async with asyncio.timeout(0.5):
                    await cluster.server_stop(SERVER_ID, convict=False)
            assert stub.entered.is_set(), "operation never started"
    finally:
        await cluster.recycle()

    assert running_at_stop[0] == [], "an operation was still running at the final stop()"
    assert "Cancelling the operations left running: _server_stop(1,)" in caplog.text


class SettlingServer(StubServer):
    """A stub server whose stop, when cancelled, takes a while to settle."""

    def __init__(self) -> None:
        super().__init__()
        self.settling = threading.Event()     # readable from any thread

    async def stop(self) -> None:
        try:
            await super().stop()
        except asyncio.CancelledError:
            self.settling.set()
            await asyncio.sleep(0.5)
            raise


async def test_operation_arriving_during_shutdown_ends_before_the_final_stop(tmp_path: pathlib.Path) -> None:
    """An operation that arrives while the leftovers settle is ended before stop() too."""
    cluster = make_cluster(tmp_path)
    leftover, late = SettlingServer(), StubServer()
    cluster.running[ServerNum(1)] = leftover
    cluster.running[ServerNum(2)] = late
    running_at_stop = []
    real_stop = cluster.stop

    async def stop() -> None:
        running_at_stop.append(list(cluster.tasks_history.values()))
        await real_stop()

    async def arrive_late() -> None:
        assert await asyncio.to_thread(leftover.settling.wait, TIMEOUT), "the leftover was never cancelled"
        await cluster.server_stop(ServerNum(2), convict=False)

    cluster.stop = stop
    arriving = None
    try:
        async with cluster.run_in_thread():
            with pytest.raises(TimeoutError):
                async with asyncio.timeout(0.5):
                    await cluster.server_stop(ServerNum(1), convict=False)
            arriving = asyncio.ensure_future(arrive_late())
    finally:
        await cluster.recycle()
        if arriving is not None:
            await asyncio.gather(arriving, return_exceptions=True)

    assert late.entered.is_set(), "the late operation never started"
    assert running_at_stop[0] == [], "an operation was still running at the final stop()"


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
