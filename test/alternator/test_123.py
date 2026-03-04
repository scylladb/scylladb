import pytest
import logging

log = logging.getLogger(__name__)

@pytest.mark.xfail
def test_123():
    logging.error("This test is expected to fail")
    assert True