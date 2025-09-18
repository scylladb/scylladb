# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
"""
Tests for commands, that need a some task to work on.
Each only checks that the command does not fail - but not what it does or returns.
"""


import pytest
import re


task_config = (
    "python\n"
    "for obj_addr, vtable_addr in find_vptrs():\n"
    "    name = resolve(vtable_addr, startswith='vtable for seastar::continuation')\n"
    "    if name and 'do_accept_one' in name:\n"
    "        print(f\"task={obj_addr.cast(gdb.lookup_type('uintptr_t'))}\")\n"
    "        break\n"
    "end"
)

@pytest.fixture(scope="module")
def task(gdb_execute):
    """
    Finds a Scylla fiber task using a `find_vptrs()` loop.

    Since Scylla is fresh‑booted, `get_local_tasks()` returns nothing.
    Nevertheless, a `find_vptrs()` scan can still discover the first task
    skeleton created by `http_server::do_accept_one` (often the earliest
    “Scylla fiber” to appear).
    """
    result = gdb_execute(full_command=task_config, skip_lines=6)
    match = re.search(r"task=(\d+)", result)
    assert match is not None, f"No task was present in {result.stdout}"
    task = match.group(1) if match else None
    return task


def test_fiber(gdb_execute, task):
    gdb_execute(f"fiber {task}")


coroutine_config = (
    "python\n"
    "target = 'service::topology_coordinator::run() [clone .resume]'\n"
    "for obj_addr, vtable_addr in find_vptrs():\n"
    "    name = resolve(vtable_addr)\n"
    "    if name and name.strip() == target:\n"
    "        print(f\"coroutine_config={obj_addr.cast(gdb.lookup_type('uintptr_t'))}\")\n"
    "end"
)
coroutine_debug_config = (
    "python\n"
    "target = 'service::topology_coordinator::run() [clone .resume]'\n"
    "target_addr = int(gdb.parse_and_eval(f\"&'{target}'\"))\n"
    "find_command = f\"scylla find -a 0x{target_addr:x}\"\n"
    "gdb.write(f\"Didn't find {target} (0x{target_addr:x}). Running '{find_command}'\\n\")\n"
    "mem_range = get_seastar_memory_start_and_size()\n"
    "gdb.execute(find_command)\n"
    "gdb.write(f\"Memory range: 0x{mem_range[0]:x} 0x{mem_range[1]:x}\\n\")\n"
    "gdb.write(\"Found coroutines:\\n\")\n"
    "for obj_addr, vtable_addr in find_vptrs():\n"
    "    name = resolve(vtable_addr)\n"
    "    if name and '.resume' in name.strip():\n"
    "        gdb.write(f\"{name}\\n\")\n"
    "gdb.execute(\"signal SIGSEGV\")\n"
    "raise gdb.error(\"No coroutine frames found with expected name\")\n"
    "end"
)


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
    result = gdb_execute(full_command=coroutine_config, skip_lines=10)
    match = re.search(r"coroutine_config=\s*(.*)", result)
    if not match:
        result = gdb_execute(full_command=coroutine_debug_config)
        pytest.fail(
            f"Failed to find coroutine task. Debugging logs have been collected\n"
            f"Debugging code result: {result}\n"
        )

    return match.group(1).strip()


def test_coroutine_frame(gdb_execute, coroutine_task):
    """
    Offsets the pointer by two words to shift from the outer coroutine frame
    to the inner `seastar::task`, as required by `$coro_frame`, which expects
    a `seastar::task*`.
    """
    gdb_execute(full_command=f"p *$coro_frame({coroutine_task} + 16)")
