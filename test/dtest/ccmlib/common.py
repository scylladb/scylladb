#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import logging


logger = logging.getLogger("ccm")


class CCMError(Exception):
    ...


class ArgumentError(CCMError):
    ...
