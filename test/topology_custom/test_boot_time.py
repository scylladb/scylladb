import asyncio
import pytest
import logging
import time

logger = logging.getLogger(__name__)

@pytest.mark.asyncio
async def test_boot(manager):
    rbno = True
    cfg = {'enable_repair_based_node_ops': rbno}
    await manager.server_add(config=cfg)

    for i in range(3):
        logger.info(f"booting node {i+1}")
        start = time.time()
        await manager.server_add(config=cfg)
        end = time.time()
        logger.info(f"node {i+1} took {end - start}ms to boot")
