#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""Helpers for tests that look at the sstable files in a data directory."""

import glob
import os


def sstable_toc_path(component_path: str) -> str:
    """The path of the TOC.txt of the sstable that the given component belongs to.

    Component file names look like `<version>-<generation>-<format>-<component>`,
    e.g. `me-3gou_0fvw_4r94g2h8nw60b8ly4c-big-Data.db`.
    """
    dirname, basename = os.path.split(component_path)
    return os.path.join(dirname, basename.rsplit('-', 1)[0] + '-TOC.txt')


def glob_live_sstable_files(pattern: str) -> list[str]:
    """The files matching `pattern` that belong to live sstables, i.e. sstables
    with a TOC.txt.

    An atomic deletion (of the inputs of a compaction, of the sstables of a
    cleaned up tablet, of a truncated table, etc.) renames the TOC.txt of each
    sstable to TOC.txt.tmp before it returns. The other files of the sstable
    are removed later, in the background, once the last reference to the
    sstable is dropped. Until then, a glob of the data directory still finds
    them.
    """
    return [f for f in glob.glob(pattern) if os.path.exists(sstable_toc_path(f))]
