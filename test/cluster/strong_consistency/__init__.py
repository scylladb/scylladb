#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Tests for strongly-consistent (SC) tables, and what they are built from.

The ``test_*`` modules next to this one are the tests; the rest is the shared
machinery they run on, imported from the module that defines it:

  * ``config`` — cluster and keyspace setup;
  * ``workload`` — the register workload (writers, readers, history, Porcupine);
  * ``outcomes`` — how a failed operation is reported to the checker.
"""
