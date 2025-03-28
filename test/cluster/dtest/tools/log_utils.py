#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import itertools
import logging
import re


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
