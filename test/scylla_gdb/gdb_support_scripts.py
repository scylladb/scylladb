#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GDB helper functions for `scylla_gdb` tests.

Loaded (after `scylla-gdb.py`) to print stable, parseable lines:
  schema= / sst= / task= / coroutine_config=
Depends on helper symbols injected by `scylla-gdb.py` (sharded, for_each_table,
seastar_lw_shared_ptr, find_sstables, find_vptrs, resolve,
get_seastar_memory_start_and_size). No returns; tests parse stdout.
"""

import gdb

def schema_config():
    """Execute GDB commands to get schema information."""
    db = sharded(gdb.parse_and_eval('::debug::the_database')).local()
    table = next(for_each_table(db))
    ptr = seastar_lw_shared_ptr(table['_schema']).get()
    print('schema=', ptr)


def sstables_config():
    """Execute GDB commands to get sstables informatzion."""
    db = sharded(gdb.parse_and_eval("::debug::the_database")).local()
    sst = next(find_sstables())
    print(f"sst=(sstables::sstable *)", sst)


def task_config():
    """Find a continuation whose vtable contains 'do_accept_one' and print its task pointer."""
    for obj_addr, vtable_addr in find_vptrs():
        name = resolve(vtable_addr, startswith='vtable for seastar::continuation')
        if name and 'do_accept_one' in name:
            print(f"task={obj_addr.cast(gdb.lookup_type('uintptr_t'))}")
            break


def coroutine_config():
    """Execute GDB commands to find coroutine configuration."""
    target = 'service::topology_coordinator::run() [clone .resume]'
    for obj_addr, vtable_addr in find_vptrs():
        name = resolve(vtable_addr)
        if name and name.strip() == target:
            print(f"coroutine_config={obj_addr.cast(gdb.lookup_type('uintptr_t'))}")


def coroutine_debug_config():
    """Execute GDB commands for coroutine debugging with detailed output."""
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
    gdb.execute("signal SIGSEGV")
    raise gdb.error("No coroutine frames found with expected name")