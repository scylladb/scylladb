#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from cassandra.policies import FallthroughRetryPolicy  # type: ignore
from cassandra.protocol import OverloadedErrorMessage  # type: ignore


class RetryOverloadedOnSameHost(FallthroughRetryPolicy):
    """Retry overload responses on the explicitly selected coordinator."""

    # MV backlog normally clears after a one-second gossip round. Bound retries
    # to five seconds so a persistent overload fails instead of hanging a test.
    max_retries = 50
    retry_delay = 0.1

    def on_request_error(self, query, consistency, error, retry_num):
        if isinstance(error, OverloadedErrorMessage) and retry_num < self.max_retries:
            return self.RETRY, consistency, self.retry_delay
        return self.RETHROW, None
