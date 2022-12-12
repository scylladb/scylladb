"""Helper utilities to work around Scylla/Cassandra driver issues,
   specifically not reconnecting automatically after a host or a set of
   hosts were restarted."""
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
import logging
from test.pylib.manager_client import ManagerClient
from cassandra.cluster import Session                   # type: ignore # pylint: disable=no-name-in-module


async def reconnect_driver(manager: ManagerClient) -> Session:
    """Workaround for scylladb/python-driver#170:
       the existing driver session may not reconnect, create a new one.
    """
    logging.info("Reconnecting driver")
    manager.driver_close()
    await manager.driver_connect()
    cql = manager.cql
    assert cql
    return cql

