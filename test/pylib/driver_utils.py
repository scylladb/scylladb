#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Utilities for working with the scylla-driver (cassandra-driver)."""

import logging
import re
from collections.abc import Iterator
from contextlib import contextmanager

import cassandra.cluster as cassandra_cluster  # type: ignore
from cassandra.cluster import Cluster  # type: ignore # pylint: disable=no-name-in-module

logger = logging.getLogger(__name__)

# How long to wait for the driver's Task Scheduler thread to finish
_SCHEDULER_JOIN_TIMEOUT = 2.0


def control_connection_query_fallback_options(*, skip_pool_creation: bool = False) -> dict[str, object]:
    """Return the driver's option for running queries over its control connection.

    Fallback uses a normal query pool when one is available and sends queries
    over the control connection when no pool is usable. SkipPoolCreation skips
    pools and sends every query over the control connection. Scylla driver 3.29.7
    predates this option but retains explicit contact points as query hosts, so
    neither mode is needed there. Scylla driver 3.29.8 and 3.29.9 neither retain
    those hosts nor expose this option, so they cannot query excluded contact points.
    """
    fallback = getattr(cassandra_cluster, "ControlConnectionQueryFallback", None)
    if fallback is None:
        return {}

    mode = fallback.SkipPoolCreation if skip_pool_creation else fallback.Fallback
    return {"allow_control_connection_query_fallback": mode}


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


@contextmanager
def safe_shutting_down(cluster: Cluster) -> Iterator[Cluster]:
    """Scope a Cluster so it is always torn down with safe_driver_shutdown().

    Cluster starts its "Task Scheduler" thread in the constructor and has no
    __del__, so a Cluster that is dropped without shutdown() leaks that thread.
    Cluster's own context manager calls a plain shutdown(), which lacks the
    Task Scheduler race workaround; use this instead.
    """
    try:
        yield cluster
    finally:
        safe_driver_shutdown(cluster)


# This function normalizes the SSL cipher suite name (a string),
# which we need to do because tests use python library and scylla server uses C library,
# and both the python's and the C's library naming conventions are different,
# so we need some translation in order to compare them.
def normalize_cipher(cipher_name: str) -> str:
    if cipher_name.startswith("TLS_"):
        cipher_name = cipher_name[len("TLS_"):] # Remove leading "TLS_" if present.
    cipher_name = cipher_name.replace("_WITH_", "-")
    cipher_name = cipher_name.replace("_", "-")
    # Remove hyphen between letters and digits: e.g. convert "AES-256" to "AES256"
    cipher_name = re.sub(r'([A-Z]+)-(\d+)', r'\1\2', cipher_name)
    return cipher_name
