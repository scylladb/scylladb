# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
"""
GDB helper functions for `scylla_gdb` tests.
They should be loaded to GDB by "-x {dir}/gdb_utils.py}",
when loaded, they can be run in gdb e.g. `$get_sstables()`

Depends on helper functions injected to GDB by `scylla-gdb.py` script.
(sharded, for_each_table, seastar_lw_shared_ptr, find_sstables, find_vptrs, resolve,
get_seastar_memory_start_and_size).
"""

import gdb
import uuid


class get_schema(gdb.Function):
    """Finds and returns a schema pointer."""

    def __init__(self):
        super(get_schema, self).__init__('get_schema')

    def invoke(self):
        db = sharded(gdb.parse_and_eval('::debug::the_database')).local()
        table = next(for_each_table(db))
        return seastar_lw_shared_ptr(table['_schema']).get()


class get_sstable(gdb.Function):
    """Finds and returns an sstable pointer."""

    def __init__(self):
        super(get_sstable, self).__init__('get_sstable')

    def invoke(self):
        return next(find_sstables())


class get_task(gdb.Function):
    """
    Finds and returns a Scylla fiber task.
    Because we stopped Scylla while it was idle, we don't expect to find
    any ready task with get_local_tasks(), but we can find one with a
    find_vptrs() loop. I noticed that a nice one (with multiple tasks chained
    to it for "scylla fiber") is one from http_server's accept loop.

    Current seastar: accept_loop/do_accept_one are coroutines. find_vptrs()
    picks up the coroutine frame via its resume function pointer (first
    word). The task (promise_type) lives at offset +16 in the frame,
    after the resume and destroy function pointers.

    Older seastar: do_accept_one uses .then() chains, producing
    seastar::continuation objects (which are tasks directly).
    """
    def __init__(self):
        super(get_task, self).__init__('get_task')

    def invoke(self):
        continuation_task = None
        for obj_addr, vtable_addr in find_vptrs():
            name = resolve(vtable_addr)
            if not name:
                continue
            # New seastar: accept_loop is a coroutine
            if 'accept_loop' in name:
                return (obj_addr + 16).cast(gdb.lookup_type('uintptr_t'))
            # Old seastar: continuation with do_accept_one
            if continuation_task is None \
                    and name.startswith('vtable for seastar::continuation') \
                    and 'do_accept_one' in name:
                continuation_task = obj_addr.cast(gdb.lookup_type('uintptr_t'))
        return continuation_task


class get_coroutine(gdb.Function):
    """
    Finds and returns a coroutine frame.
    Prints COROUTINE_NOT_FOUND if the coroutine is not present.
    """
    def __init__(self):
        super(get_coroutine, self).__init__('get_coroutine')

    def invoke(self):
        target = 'service::topology_coordinator::run() [clone .resume]'
        for obj_addr, vtable_addr in find_vptrs():
            name = resolve(vtable_addr)
            if name and name.strip() == target:
                return obj_addr.cast(gdb.lookup_type('uintptr_t'))
        print("COROUTINE_NOT_FOUND")


# Register the functions in GDB
get_schema()
get_sstable()
get_task()
get_coroutine()
