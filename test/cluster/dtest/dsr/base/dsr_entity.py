#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from .d_entity import DEntity


class DSREntity(DEntity):
    """
    Deterministic State-Aware Randomization Entity
    Protocol that allows build class-based state-aware randomization
    """

    # TBD: State-aware logic that is currently implemented in ScyllaClusterTest to be generalized and brought in here
    _get_random = True

    def execute(self, *args, **kwargs):
        """
        Execute action, should be overridden
        """
        raise NotImplementedError("Not implemented")

    def randomize(self):
        """ """
        raise NotImplementedError("Not implemented")

    def get_probability_coeff(self) -> int:
        """
        Return number of variants, so that parent could make chances of getting into that variants even between children
        """
        raise NotImplementedError("Not implemented")
