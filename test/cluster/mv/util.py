#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
from typing import Any

from cassandra.cluster import Session  # type: ignore
from cassandra.policies import FallthroughRetryPolicy  # type: ignore
from cassandra.pool import Host  # type: ignore
from cassandra.protocol import OverloadedErrorMessage  # type: ignore
from cassandra.query import PreparedStatement  # type: ignore


MAX_RETRIES = 300
RETRY_DELAY = 0.1


async def run_with_overload_retries(cql: Session, statement: PreparedStatement, *args: Any, host: Host, **kwargs: Any) -> Any:
    """Run a statement on one host, retrying only overload responses."""
    statement.retry_policy = FallthroughRetryPolicy()
    retries = 0
    while True:
        try:
            return await cql.run_async(statement, *args, host=host, **kwargs)
        except OverloadedErrorMessage:
            if retries >= MAX_RETRIES:
                raise
            retries += 1
            # MV backlog normally clears after a one-second gossip round. Bound
            # retries to a 30-second delay budget so a persistent overload
            # fails instead of hanging a test.
            await asyncio.sleep(RETRY_DELAY)
