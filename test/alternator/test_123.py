import pytest
import logging

log = logging.getLogger(__name__)

@pytest.fixture
def fruit_bowl():
    yield "apple", "banana", "orange"
    log.debug("Cleaning up fruit bowl")
    assert False

def test_333(fruit_bowl):
    log.debug("Testing with fruit bowl: %s", fruit_bowl)
    pass

def test_123():
    logging.error("This test is expected to fail")
    assert False