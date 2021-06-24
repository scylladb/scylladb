import gdb


def do(cmd, known_broken=False, loudly=False):
    if loudly:
        gdb.write('\n\n*** CMD: {}\n\n'.format(cmd))

    try:
        ret = gdb.execute(cmd)
    except Exception:
        if not known_broken:
            raise
        else:
            gdb.write('(known to be broken, ignoring...\n')
    return ret


def dos(cmd, known_broken=False):
    return do('scylla ' + cmd, known_broken, loudly=True)


do('handle SIG34 SIG35 SIGUSR1 nostop noprint pass')
do('set print static-members no')
do('set python print-stack full')

# let Scylla run for some time
do('break sstables::compact_sstables')
do('ignore 1 10')
do('run')

db = sharded(gdb.parse_and_eval('::debug::db')).local()
gdb.set_convenience_variable('db', db)

# now try some commands
dos('features')

dos('compaction-tasks')
dos('databases')
dos('column_families')
dos('keyspaces')
dos('active-sstables')
dos('memtables')
# there are no repairs, but at least check that the command doesn't crash:
dos('repairs')

# use the schema of the first table
table = next(for_each_table(db))
schema = table['_schema']['_p'].reinterpret_cast(gdb.lookup_type('schema').pointer())
gdb.set_convenience_variable('schema', schema)
dos('schema $schema')

dos('gms')
dos('heapprof')
dos('io-queues')

dos('cache')

dos('mem-range')
dos('mem-ranges')
dos('memory')
dos('segment-descs')
dos('small-object -o 32 --random-page')
dos('small-object -o 64 --summarize')

task = next(get_local_tasks())
gdb.set_convenience_variable('task', task)
dos('fiber $task')

dos('find -r $schema')
dos('generate-object-graph -o {}/og.dot -d 2 -t 10 $schema'.format(os.environ['_WORKDIR']))

dos('lsa')

# FIXME need a simple test for this:
# dos('lsa-segment ???')
dos('ptr $schema')

dos('netw')

# probably empty:
dos('smp-queues')

dos('task-queues')
dos('task_histogram')
dos('tasks')
dos('threads')
dos('timers')

# we run gdb with "-return-child-result", since otherwise it always
# returns 0 because of "-batch" (I'm not sure I follow the logic, but
# it is what it is).  so if we got here (which means all is well), we
# need to return 0 explicitly -- otherwise gdb will return 255 to let
# the world know that the inferior was killed.
do('quit 0')
