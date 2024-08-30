import pytest
import re

# Convenience function to execute a scylla command in gdb, returning its
# output as a string - or a gdb.error exception.
def scylla(gdb, cmd):
    return gdb.execute('scylla ' + cmd, from_tty=False, to_string=True)

# Check that trying an unknown subcommand of the "scylla" subcommand
# produces the right error message.
def test_nonexistent_scylla_command(gdb):
        with pytest.raises(gdb.error, match='Undefined scylla command'):
            scylla(gdb, 'nonexistent_command')

# Minimal test for some of the commands. Each only checks that the command
# does not fail - but not what it does or returns. These tests are still
# useful - importantly, they can detect that one of the commands relies on
# some internal implementation detail which no longer works, and needs to
# be fixed.

def test_features(gdb):
    scylla(gdb, 'features')

def test_compaction_tasks(gdb):
    scylla(gdb, 'compaction-tasks')

def test_databases(gdb):
    scylla(gdb, 'databases')

def test_commitlog(gdb):
    scylla(gdb, 'commitlog')

def test_tables(gdb):
    scylla(gdb, 'tables')

def test_table(gdb):
    scylla(gdb, 'table system.local')

def test_keyspaces(gdb):
    scylla(gdb, 'keyspaces')

def test_active_sstables(gdb):
    scylla(gdb, 'active-sstables')

def test_sstables(gdb):
    scylla(gdb, 'sstables')

def test_memtables(gdb):
    scylla(gdb, 'memtables')

def test_repairs(gdb):
    scylla(gdb, 'repairs')

def test_gms(gdb):
    scylla(gdb, 'gms')

def test_heapprof(gdb):
    scylla(gdb, 'heapprof')

def test_io_queues(gdb):
    scylla(gdb, 'io-queues')

def test_cache(gdb):
    scylla(gdb, 'cache')

def test_mem_range(gdb):
    scylla(gdb, 'mem-range')

def test_mem_ranges(gdb):
    scylla(gdb, 'mem-ranges')

def test_memory(gdb):
    scylla(gdb, 'memory')

def test_segment_descs(gdb):
    scylla(gdb, 'segment-descs')

def test_small_object_1(gdb):
    scylla(gdb, 'small-object -o 32 --random-page')

def test_small_object_2(gdb):
    scylla(gdb, 'small-object -o 64 --summarize')

def test_large_objects_1(gdb):
    scylla(gdb, 'large-objects -o 131072 --random-page')

def test_large_objects_2(gdb):
    scylla(gdb, 'large-objects -o 32768 --summarize')

def test_lsa(gdb):
    scylla(gdb, 'lsa')

def test_netw(gdb):
    scylla(gdb, 'netw')

def test_smp_queues(gdb):
    scylla(gdb, 'smp-queues')

def test_task_queues(gdb):
    scylla(gdb, 'task-queues')

def test_task_histogram(gdb):
    scylla(gdb, 'task_histogram')

def test_task_histogram_coro(gdb):
    h = scylla(gdb, 'task_histogram -a')
    if re.search(r'\) \[clone \.\w+\]', h) is None:
        raise gdb.error('no coroutine entries in task histogram')

def test_tasks(gdb):
    scylla(gdb, 'tasks')

def test_threads(gdb):
    scylla(gdb, 'threads')

def test_timers(gdb):
    scylla(gdb, 'timers')

# Some commands need a schema to work on. The following fixture finds
# one (the schema of the first table - note that even without any user
# tables, we will always have system tables).
@pytest.fixture(scope="module")
def schema(gdb, scylla_gdb):
    db = scylla_gdb.sharded(gdb.parse_and_eval('::debug::the_database')).local()
    table = next(scylla_gdb.for_each_table(db))
    gdb.set_convenience_variable('schema', 
        table['_schema']['_p'].reinterpret_cast(gdb.lookup_type('schema').pointer()))
    yield '$schema'

@pytest.fixture(scope="module")
def sstable(gdb, scylla_gdb):
    db = scylla_gdb.sharded(gdb.parse_and_eval('::debug::the_database')).local()
    sst = next(scylla_gdb.find_sstables())
    gdb.set_convenience_variable('sst', sst)
    yield '$sst'

def test_schema(gdb, schema):
    scylla(gdb, f'schema {schema}')

def test_find(gdb, schema):
    scylla(gdb, f'find -r {schema}')

def test_ptr(gdb, schema):
    scylla(gdb, f'ptr {schema}')

def test_generate_object_graph(gdb, schema, request):
    tmpdir = request.config.getoption('scylla_tmp_dir')
    scylla(gdb, f'generate-object-graph -o {tmpdir}/og.dot -d 2 -t 10 {schema}')

# Some commands need a task to work on. The following fixture finds one.
# Because we stopped Scylla while it was idle, we don't expect to find
# any ready task with get_local_tasks(), but we can find one with a
# find_vptrs() loop. I noticed that a nice one (with multiple tasks chained
# to it for "scylla fiber") is one from http_server::do_accept_one.
@pytest.fixture(scope="module")
def task(gdb, scylla_gdb):
    for obj_addr, vtable_addr in scylla_gdb.find_vptrs():
        name = scylla_gdb.resolve(vtable_addr, startswith='vtable for seastar::continuation')
        if name and 'do_accept_one' in name:
            return obj_addr.cast(gdb.lookup_type('uintptr_t'))
    raise gdb.error("no tasks found with expected name")

def test_fiber(gdb, task):
    scylla(gdb, f'fiber {task}')

# Similar to task(), but looks for a coroutine frame.
@pytest.fixture(scope="module")
def coro_task(gdb, scylla_gdb):
    for obj_addr, vtable_addr in scylla_gdb.find_vptrs():
        name = scylla_gdb.resolve(vtable_addr)
        if name and name.strip() == 'service::topology_coordinator::run() [clone .resume]':
            return obj_addr.cast(gdb.lookup_type('uintptr_t'))
    raise gdb.error("No coroutine frames found with expected name")

def test_coro_frame(gdb, coro_task):
    # Note the offset by two words.
    # This moves the pointer from the outer coroutine frame to the inner seastar::task.
    # $coro_frame expects a seastar::task*.
    gdb.execute(f'p *$coro_frame({coro_task} + 16)')

def test_sstable_summary(gdb, sstable):
    scylla(gdb, f'sstable-summary {sstable}')

def test_sstable_summary(gdb, sstable):
    scylla(gdb, f'sstable-index-cache {sstable}')

def test_read_stats(gdb, sstable):
    scylla(gdb, f'read-stats')

def test_get_config_value(gdb):
    scylla(gdb, f'get-config-value compaction_static_shares')

@pytest.mark.without_scylla
def test_run_without_scylla(scylla_gdb):
    # just try to load the scylla-gdb module without attaching to scylla.
    #
    # please note, if this test fails, there are good chances that scylla-gdb.py
    # is unable to load without debug symbols. Calls to "gdb.lookup_type()" and
    # similar functions that rely on debug symbols should be made within GDB
    # commands themselves when they get exuecuted. To address potential
    # failures, consider moving code that references debug symbols into a code
    # path executed only when debug symbols is loaded. If the value of the
    # symbol is a constant, consider caching it. using functools.cache
    # decorator.
    _ = scylla_gdb

# FIXME: need a simple test for lsa-segment
