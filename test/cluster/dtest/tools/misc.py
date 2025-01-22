#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING
from concurrent.futures import ThreadPoolExecutor

if TYPE_CHECKING:
    from collections.abc import Callable

    from test.cluster.dtest.ccmlib.scylla_node import ScyllaNode


logger = logging.getLogger(__name__)


def retry_till_success[T, **P](fun: Callable[P, T], *args: P.args, **kwargs: P.kwargs) -> T:
    timeout = kwargs.pop("timeout", 60)
    bypassed_exception = kwargs.pop("bypassed_exception", Exception)

    deadline = time.perf_counter() + timeout
    while True:
        try:
            return fun(*args, **kwargs)
        except bypassed_exception:
            if time.perf_counter() > deadline:
                raise

        # Brief pause before next attempt.
        time.sleep(0.1)


def set_trace_probability(nodes: list[ScyllaNode], probability_value: float) -> None:
    def _set_trace_probability_for_node(_node: ScyllaNode) -> None:
        logger.debug(f'{"Enable" if probability_value else "Disable"} trace for {_node.name} with {probability_value=}')
        _node.cluster.manager.api.set_trace_probability(node_ip=_node.address(), probability=probability_value)

    with ThreadPoolExecutor(max_workers=len(nodes)) as executor:
        threads = [executor.submit(_set_trace_probability_for_node, node) for node in nodes]
        [thread.result() for thread in threads]
