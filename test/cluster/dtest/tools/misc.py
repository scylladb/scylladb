#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from __future__ import annotations

import hashlib
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


def list_to_hashed_dict(query_response_list):
    """
    takes a list and hashes the contents and puts them into a dict so the contents can be compared
    without order. unfortunately, we need to do a little massaging of our input; the result from
    the driver can return a OrderedMapSerializedKey (e.g. [0, 9, OrderedMapSerializedKey([(10, 11)])])
    but our "expected" list is simply a list of elements (or list of list). this means if we
    hash the values as is we'll get different results. to avoid this, when we see a dict,
    convert the raw values (key, value) into a list and insert that list into a new list
    :param query_response_list the list to convert
    :return: dict containing the contents fo the list with the hashed contents
    """
    hashed_dict = dict()
    for item_lst in query_response_list:
        normalized_list = []
        for item in item_lst:
            if hasattr(item, "items"):
                tmp_list = []
                for a, b in item.items():
                    tmp_list.append(a)
                    tmp_list.append(b)
                normalized_list.append(tmp_list)
            else:
                normalized_list.append(item)
        list_str = str(normalized_list)
        utf8 = list_str.encode("utf-8", "ignore")
        list_digest = hashlib.sha256(utf8).hexdigest()
        hashed_dict[list_digest] = normalized_list
    return hashed_dict


def set_trace_probability(nodes: list[ScyllaNode], probability_value: float) -> None:
    def _set_trace_probability_for_node(_node: ScyllaNode) -> None:
        logger.debug(f'{"Enable" if probability_value else "Disable"} trace for {_node.name} with {probability_value=}')
        _node.cluster.manager.api.set_trace_probability(node_ip=_node.address(), probability=probability_value)

    with ThreadPoolExecutor(max_workers=len(nodes)) as executor:
        threads = [executor.submit(_set_trace_probability_for_node, node) for node in nodes]
        [thread.result() for thread in threads]
