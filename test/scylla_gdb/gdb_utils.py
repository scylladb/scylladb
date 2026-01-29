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


def coroutine_debug_config(tmpdir):
    """
    Check if scylla_find agrees with find_vptrs, for debugging.

    Execute GDB commands for coroutine debugging with detailed output.
    This test fails sometimes, but rarely and unreliably.
    We want to get a coredump from it the next time it fails.
    Sending a SIGSEGV should induce that.
    https://github.com/scylladb/scylladb/issues/22501
    """
    target = 'service::topology_coordinator::run() [clone .resume]'
    target_addr = int(gdb.parse_and_eval(f"&'{target}'"))
    find_command = f"scylla find -a 0x{target_addr:x}"
    gdb.write(f"Didn't find {target} (0x{target_addr:x}). Running '{find_command}'\n")
    mem_range = get_seastar_memory_start_and_size()
    gdb.execute(find_command)
    gdb.write(f"Memory range: 0x{mem_range[0]:x} 0x{mem_range[1]:x}\n")
    gdb.write("Found coroutines:\n")
    for obj_addr, vtable_addr in find_vptrs():
        name = resolve(vtable_addr)
        if name and '.resume' in name.strip():
            gdb.write(f"{name}\n")
    core_filename = f"{tmpdir}/../scylla_gdb_coro_task-{uuid.uuid4()}.core"
    gdb.execute(f"gcore {core_filename}")
    raise gdb.error(f"No coroutine frames found with expected name. Dumped Scylla core to {core_filename}")
