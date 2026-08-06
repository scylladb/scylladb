#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Utilities for working with the scylla-driver (cassandra-driver)."""

import logging

from cassandra.cluster import Cluster  # type: ignore # pylint: disable=no-name-in-module

logger = logging.getLogger(__name__)

# How long to wait for the driver's Task Scheduler thread to finish
_SCHEDULER_JOIN_TIMEOUT = 2.0


def safe_driver_shutdown(cluster: Cluster) -> None:
    """Shut down a cassandra-driver Cluster, working around the Task Scheduler race.

    Works around a race where the "Task Scheduler" thread raises RuntimeError
    after Cluster.shutdown() returns, or during the call itself.

    Safe to call on a Cluster whose connect() failed: shutting down a partially
    initialized Cluster may raise, but that must never mask the original error,
    so any other exception is logged instead of propagated.
    """
    # Capture scheduler thread before shutdown to join it later
    scheduler = getattr(cluster, 'scheduler', None)

    try:
        cluster.shutdown()
    except RuntimeError as exc:
        if 'cannot schedule new futures after shutdown' not in str(exc):
            logger.warning("Error shutting down driver Cluster: %s", exc)
        else:
            logger.debug("Suppressed expected RuntimeError during driver shutdown: %s", exc)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Error shutting down driver Cluster: %s", exc)

    if scheduler:
        scheduler.join(timeout=_SCHEDULER_JOIN_TIMEOUT)
        if scheduler.is_alive():
            logger.warning("Driver Task Scheduler thread did not terminate within %.1fs", _SCHEDULER_JOIN_TIMEOUT)
