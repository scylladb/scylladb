#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import logging
import os
import sys
import time
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor

from test.dtest.ccmlib.scylla_node import ScyllaNode


logger = logging.getLogger(__name__)

colors = {
    "yellow": "\033[93m",
    "reset": "\033[0m",
}


def retry_till_success(fun, *args, **kwargs):
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
        time.sleep(0.25)


def get_current_test_name():
    """
    See https://docs.pytest.org/en/latest/example/simple.html#pytest-current-test-environment-variable
    :return: returns just the name of the current running test name
    """
    pytest_current_test = os.environ.get("PYTEST_CURRENT_TEST")
    test_splits = pytest_current_test.split("::")
    current_test_name = test_splits[len(test_splits) - 1]
    current_test_name = current_test_name.replace(" (call)", "")
    current_test_name = current_test_name.replace(" (setup)", "")
    current_test_name = current_test_name.replace(" (teardown)", "")
    return current_test_name


class ImmutableMapping(Mapping):
    """
    Convenience class for when you want an immutable-ish map.

    Useful at class level to prevent mutability problems (such as a method altering the class level mutable)
    """

    def __init__(self, init_dict):
        self._data = init_dict.copy()

    def __getitem__(self, key):
        return self._data[key]

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)

    def __repr__(self):
        return f"{self.__class__.__name__}({self._data})"


def set_trace_probability(nodes: list[ScyllaNode], probability_value: float) -> None:
    def _set_trace_probability_for_node(_node: ScyllaNode) -> None:
        logger.debug(f'{"Enable" if probability_value else "Disable"} trace for {_node.name} with {probability_value=}')
        _node.cluster.manager.api.set_trace_probability(node_ip=_node.address(), probability=probability_value)

    with ThreadPoolExecutor(max_workers=len(nodes)) as executor:
        threads = [executor.submit(_set_trace_probability_for_node, node) for node in nodes]
        [thread.result() for thread in threads]


def colored_text(text: str, color: str) -> str:
    if sys.stdout.isatty():  # Check if the output is a terminal
        return f"{colors[color]}{text}{colors['reset']}"
    return text
