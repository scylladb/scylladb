#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from cassandra.protocol import InvalidRequest
from test.pylib.manager_client import ManagerClient
import pytest
import logging
import asyncio

logger = logging.getLogger(__name__)


