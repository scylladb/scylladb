import logging

import requests
from ccmlib.scylla_node import ScyllaNode

logger = logging.getLogger(__name__)


def enable_slow_query_tracing(node: ScyllaNode, fast: bool, threshold: int = 500000):
    api_cmd = f"http://{node.address()}:10000/storage_service/slow_query?fast={str(fast).lower()}&enable=true&threshold={threshold}"
    logger.debug("Enable slow query tracing: %s" % api_cmd)
    r = requests.post(api_cmd)
    assert r.status_code == 200, "Status code is %d. Expected 200. API enabling failed" % r.status_code


def validate_slow_query_tracing_is_enabled(node: ScyllaNode, fast: bool, threshold: int = 500000):
    api_cmd = f"http://{node.address()}:10000/storage_service/slow_query"
    logger.debug("Validate that fast slow query tracing is enabled: %s" % api_cmd)
    response = requests.get(api_cmd)
    response_json = response.json()

    if fast:
        assert "fast" in response_json, "Fast slow query tracing is not supported"

    if "fast" in response_json:
        assert response_json["fast"] == fast, f"Fast slow query tracing is {not fast}"

    assert response_json["enable"], f"Slow query tracing is not enabled"
    assert response_json["threshold"] == threshold, f"Slow query tracing is not enabled"
