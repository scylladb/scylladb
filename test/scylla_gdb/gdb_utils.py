# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
"""
GDB helper functions for `scylla_gdb` tests.
They should be loaded to GDB by "-x {dir}/gdb_utils.py}",
when loaded, they can be run in gdb e.g. `$get_sstables()`

Depends on helper functions injected to GDB by `scylla-gdb.py` script.
(sharded, for_each_table, seastar_lw_shared_ptr, find_sstables, schema_ptr, find_vptrs, resolve,
get_seastar_memory_start_and_size, double_decker, intrusive_btree).
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


class get_table_sstable(gdb.Function):
    """Finds and returns an sstable pointer of the given keyspace and table."""

    def __init__(self):
        super(get_table_sstable, self).__init__('get_table_sstable')

    def invoke(self, ks, cf):
        for sst in find_sstables():
            schema = schema_ptr(sst['_schema'])
            if (schema.ks_name, schema.cf_name) == (ks.string(), cf.string()):
                return sst
        raise gdb.GdbError(f"No sstable of {ks.string()}.{cf.string()} on this shard")


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


class get_row(gdb.Function):
    """
    Finds and returns the cells (a `row`) of a clustering row of the given
    keyspace and table, from the row cache of this shard.
    """
    def __init__(self):
        super(get_row, self).__init__('get_row')

    def invoke(self, ks, cf):
        for table in for_each_table():
            schema = schema_ptr(table['_schema'])
            if (schema.ks_name, schema.cf_name) != (ks.string(), cf.string()):
                continue
            partition_version_ptr = gdb.lookup_type('partition_version').pointer()
            rows_entry_type = gdb.lookup_type('rows_entry')
            rows_entry_ptr = rows_entry_type.pointer()
            link_offset = next(f.bitpos for f in rows_entry_type.fields() if f.name == '_link') // 8
            for entry in double_decker(table['_cache']['_partitions']):
                version_ref = entry['_pe']['_version']
                try:
                    # A tagged pointer; bit 0 is a flag.
                    version = gdb.Value(int(version_ref['_tagged']) & ~1).cast(partition_version_ptr)
                except gdb.error:
                    version = version_ref['_version']
                if not version:
                    continue
                for hook in intrusive_btree(version['_partition']['_rows']):
                    # intrusive_btree casts the hook to the entry, but the hook isn't
                    # at offset 0 of rows_entry.
                    rows_entry = gdb.Value(int(hook.address) - link_offset).cast(rows_entry_ptr).dereference()
                    if not rows_entry['_flags']['_dummy']:
                        return rows_entry['_row']['_cells']
        raise gdb.GdbError(f"No cached row of {ks.string()}.{cf.string()} on this shard")


# Register the functions in GDB
get_schema()
get_sstable()
get_table_sstable()
get_task()
get_coroutine()
get_row()
