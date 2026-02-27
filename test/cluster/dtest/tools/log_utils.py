#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from __future__ import annotations

import itertools
import logging
import re
import time
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from test.cluster.dtest.ccmlib.scylla_node import ScyllaNode


control_chars = "".join(map(chr, itertools.chain(range(0x20), range(0x7F, 0xA0))))
control_char_re = re.compile("[%s]" % re.escape(control_chars))


def remove_control_chars(s):
    return control_char_re.sub("", s)


class DisableLogger:
    def __init__(self, logger_name):
        self.logger_name = logger_name
        self.level = 0

    def __enter__(self):
        self.level = logging.getLogger(self.logger_name).level
        logging.getLogger(self.logger_name).setLevel(logging.WARNING)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        logging.getLogger(self.logger_name).setLevel(self.level)


def wait_for_any_log(nodes: list[ScyllaNode],
                     patterns: str | list[str],
                     timeout: int,
                     dispersed: bool = False) -> ScyllaNode | list[ScyllaNode]:
    """Look for a pattern in the system.log of any in a given list of nodes.

    :param nodes: The list of nodes whose logs to scan
    :param patterns: The target pattern (a string, or a list of strings)
    :param timeout: How long to wait for the pattern. Note that
                    strictly speaking, timeout is not really a timeout,
                    but a maximum number of attempts. This implies that
                    the all the grepping takes no time at all, so it is
                    somewhat inaccurate, but probably close enough.
    :return: The first node in whose log the pattern was found, if not dispersed.
             Otherwise, if dispersed=True, return a list of all nodes with any of the patterns.
    """
    if isinstance(patterns, str):
        patterns = [patterns]

    if dispersed:
        patterns = patterns.copy()
        ret = set()
        for _ in range(timeout):
            for node in nodes:
                for p in patterns.copy():
                    if node.scylla_log_file.grep(expr=p, max_count=1):
                        patterns.remove(p)
                        ret.add(node)
            if not patterns:
                return list(ret)
            time.sleep(1)
    else:
        for _ in range(timeout):
            for node in nodes:
                if all(node.scylla_log_file.grep(expr=p, max_count=1) for p in patterns):
                    return node
            time.sleep(1)

    raise TimeoutError(f"{datetime.now():%d %b %Y %H:%M:%S} Unable to find :{patterns} in any node log within {timeout}s")
