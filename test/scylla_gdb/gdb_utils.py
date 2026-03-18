# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
"""
GDB helper functions for `scylla_gdb` tests.
They should be loaded to GDB by "-x {dir}/gdb_utils.py}",
when loaded, they can be run in gdb e.g. `python get_sstables()`

Depends on helper functions injected to GDB by `scylla-gdb.py` script.
(sharded, for_each_table, seastar_lw_shared_ptr, find_sstables, find_vptrs, resolve,
get_seastar_memory_start_and_size).
"""

import gdb
import uuid


def get_schema():
    """Execute GDB commands to get schema information."""
    db = sharded(gdb.parse_and_eval('::debug::the_database')).local()
    table = next(for_each_table(db))
    ptr = seastar_lw_shared_ptr(table['_schema']).get()
    print('schema=', ptr)


def get_sstables():
    """Execute GDB commands to get sstables information."""
    sst = next(find_sstables())
    print(f"sst=(sstables::sstable *)", sst)


def get_task():
    """
    Some commands need a task to work on. The following fixture finds one.
    Because we stopped Scylla while it was idle, we don't expect to find
    any ready task with get_local_tasks(), but we can find one with a
    find_vptrs() loop. I noticed that a nice one (with multiple tasks chained
    to it for "scylla fiber") is one from http_server::do_accept_one.
    """
    for obj_addr, vtable_addr in find_vptrs():
        name = resolve(vtable_addr, startswith='vtable for seastar::continuation')
        if name and 'do_accept_one' in name:
            print(f"task={obj_addr.cast(gdb.lookup_type('uintptr_t'))}")
            break


def get_coroutine():
    """Similar to get_task(), but looks for a coroutine frame."""
    target = 'service::topology_coordinator::run() [clone .resume]'
    for obj_addr, vtable_addr in find_vptrs():
        name = resolve(vtable_addr)
        if name and name.strip() == target:
            print(f"coroutine_config={obj_addr.cast(gdb.lookup_type('uintptr_t'))}")
