# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""
async_cql:

This module provides a helper to run async CQL queries with Cassandra's Python driver
from asyncio loop.

Example usage:
    from cassandra.cluster import Session, Cluster
    from test.pylib.async_cql import event_loop, run_async

    Session.run_async = run_async
    ccluster = Cluster(...)
    cql = cluster.connect(...)
    await cql.run_async(f"SELECT * FROM {table}")
"""

import asyncio
import logging
import pytest
from cassandra.cluster import ResponseFuture     # type: ignore # pylint: disable=no-name-in-module


logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def event_loop(request):
    """Change default pytest-asyncio event_loop fixture scope to session to
       allow async fixtures with scope larger than function. (e.g. manager fixture)
       See https://github.com/pytest-dev/pytest-asyncio/issues/68"""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


def _wrap_future(driver_response_future: ResponseFuture) -> asyncio.Future:
    """Wrap a cassandra Future into an asyncio.Future object.

    Args:
        driver_response_future: future to wrap

    Returns:
        And asyncio.Future object which can be awaited.
    """
    loop = asyncio.get_event_loop()
    aio_future = loop.create_future()

    def on_result(result):
        if not aio_future.done():
            loop.call_soon_threadsafe(aio_future.set_result, result)
        else:
            logger.debug("_wrap_future: on_result() on already done future: %s", result)

    def on_error(exception, *_):
        if not aio_future.done():
            loop.call_soon_threadsafe(aio_future.set_exception, exception)
        else:
            logger.debug("_wrap_future: on_error(): %s", exception)

    driver_response_future.add_callback(on_result)
    driver_response_future.add_errback(on_error)
    return aio_future


def run_async(self, *args, **kwargs) -> asyncio.Future:
    """Execute a CQL query asynchronously by wrapping the driver's future"""
    # The default timeouts should have been more than enough, but in some
    # extreme cases with a very slow debug build running on a slow or very busy
    # machine, they may not be. Observed tests reach 160 seconds. So it's
    # incremented to 200 seconds.
    # See issue #11289.
    kwargs.setdefault("timeout", 200.0)
    return _wrap_future(self.execute_async(*args, **kwargs))
