#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import pytest
import time
import logging
logger = logging.getLogger(__name__)

@pytest.mark.asyncio
async def test_boot(manager):
    rbno = True
    cfg = {'enable_repair_based_node_ops': rbno, 'num_tokens': 256}
    nr_nodes = 3
    for i in range(nr_nodes):
        logger.info(f"booting node {i+1}")
        start = time.time()
        await manager.server_add(config=cfg)
        end = time.time()
        logger.info(f"node {i+1} took {end - start}ms to boot")
