#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Scheduler platform for ``test.py``.

A *scheduler* decides how the selected tests are spread over the machine.
Exactly one is selected per run with ``./test.py --scheduler=<name>``; the
default (``passthrough``) makes no decisions at all and hands the run to
pytest-xdist, which is what ``test.py`` has always done.

This package knows nothing about what any test declares about itself.  A
scheduler that needs test metadata reads it in its own pytest plugin, where the
tests are.

See ``docs/dev/test-scheduler.md`` for how to write one.
"""
