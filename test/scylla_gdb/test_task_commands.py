# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
"""
Tests for commands, that need a some task to work on.
Each only checks that the command does not fail - but not what it does or returns.
"""


import pytest
import re


task_config = """python
for obj_addr, vtable_addr in find_vptrs():
    name = resolve(vtable_addr, startswith='vtable for seastar::continuation')
    if name and 'do_accept_one' in name:
        print(f"task={obj_addr.cast(gdb.lookup_type('uintptr_t'))}")
        break
"""


@pytest.fixture(scope="module")
def task(gdb_execute):
    """
    Finds a Scylla fiber task using a `find_vptrs()` loop.

    Since Scylla is fresh‑booted, `get_local_tasks()` returns nothing.
    Nevertheless, a `find_vptrs()` scan can still discover the first task
    skeleton created by `http_server::do_accept_one` (often the earliest
    “Scylla fiber” to appear).
    """
    args = ["-ex", task_config]
    result = gdb_execute(args=args)
    match = re.search(r"task=(\d+)", result.stdout)
    assert match is not None, f"No task was present in {result.stdout}"
    task = match.group(1) if match else None
    return task


async def test_fiber(gdb_execute, task):
    gdb_execute(f"fiber {task}")


coroutine_config = """python
target = 'service::topology_coordinator::run() [clone .resume]'
for obj_addr, vtable_addr in find_vptrs():
    name = resolve(vtable_addr)
    if name and name.strip() == target:
        found = True
        print(f"coroutine_config={obj_addr.cast(gdb.lookup_type('uintptr_t'))}")
if not found:
    gdb.execute("quit 42")
"""

coroutine_debug_config = """python
target = 'service::topology_coordinator::run() [clone .resume]'
target_addr = int(gdb.parse_and_eval(f"&'{target}'"))
find_command = f"scylla find -a 0x{target_addr:x}"
gdb.write(f"Didn't find {target} (0x{target_addr:x}). Running '{find_command}'\\n")
mem_range = get_seastar_memory_start_and_size()
gdb.execute(find_command)
gdb.write(f"Memory range: 0x{mem_range[0]:x} 0x{mem_range[1]:x}\\n")
gdb.write("Found coroutines:\\n")
for obj_addr, vtable_addr in find_vptrs():
    name = resolve(vtable_addr)
    if name and '.resume' in name.strip():
        gdb.write(f"{name}\\n")

# This test fails sometimes, but rarely and unreliably.
# We want to get a coredump from it the next time it fails.
# Sending a SIGSEGV should induce that.
# https://github.com/scylladb/scylladb/issues/22501
gdb.execute("signal SIGSEGV")
raise gdb.error("No coroutine frames found with expected name")
"""


@pytest.fixture(scope="module")
def coroutine_task(gdb_execute):
    """
    Finds a coroutine task, similar to the `task` fixture.

    This fixture executes the `coroutine_config` script in GDB to locate a
    specific coroutine task. If the task is not found, the script intentionally
    causes GDB to exit with return code 42.

    When this specific return code is detected, a debugging script
    (`coroutine_debug_config`) is executed to collect relevant logs.
    This debugging script then forces a coredump to capture additional
    diagnostic information before the test is marked as failed.
    """
    args = ["-ex", coroutine_config]
    result = gdb_execute(args=args, assert_response=False)
    if result.returncode == 42:
        args = ["-ex", coroutine_debug_config]
        result = gdb_execute(args=args, assert_response=False)
        pytest.fail(
            f"Failed to find coroutine task. Debugging logs have been collected\n"
            f"return code: {result.returncode}\n"
            f"stdout: {result.stdout}\n"
            f"stderr: {result.stderr}"
        )

    match = re.search(r"coroutine_config=\s*(.*)", result.stdout)
    assert match, "Failed to find schema pointer in response"
    return match.group(1) if match else None


def test_coroutine_frame(gdb_execute, coroutine_task):
    """
    Offsets the pointer by two words to shift from the outer coroutine frame
    to the inner `seastar::task`, as required by `$coro_frame`, which expects
    a `seastar::task*`.
    """
    args = ["-ex", f"p *$coro_frame({coroutine_task} + 16)"]
    gdb_execute(args=args)
