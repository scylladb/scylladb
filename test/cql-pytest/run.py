#!/usr/bin/env python3

import os
import glob
import sys
import time
import shutil
import signal
import atexit
import tempfile

# run_with_temporary_dir() is a utility function for running a process, such
# as Scylla, Cassandra or Redis, inside its own new temporary directory,
# and ensure that on exit for any reason - success, failure, signal or
# exception - the subprocess is killed and its temporary directory is deleted.
#
# The parameter to run_with_temporary_dir() is a function which receives the
# new process id and the new temporary directory's path, and builds the
# command line to run and map of extra environment variables. This function
# can put files in the directory (which already exists when it is called).
# See below the example run_scylla_cmd.

def pid_to_dir(pid):
    return os.path.join(os.getenv('TMPDIR', '/tmp'), 'scylla-test-'+str(pid))

def run_with_temporary_dir(run_cmd_generator):
    global run_with_temporary_dir_pids
    # Below, there is a small time window, after we fork and the child
    # started running but before we save this child's process id in
    # run_with_temporary_dir_pids. In that small time window, a signal may
    # kill the parent process but not cleanup the child. So we use sigmask
    # to postpone signal delivery during that time window:
    mask = signal.pthread_sigmask(signal.SIG_BLOCK, {})
    signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGINT, signal.SIGQUIT, signal.SIGTERM})
    sys.stdout.flush()
    sys.stderr.flush()
    pid = os.fork()
    if pid == 0:
        # Child
        run_with_temporary_dir_pids = set() # no children to clean up on child
        pid = os.getpid()
        dir = pid_to_dir(pid)
        os.mkdir(dir)
        (cmd, env) = run_cmd_generator(pid, dir)
        # redirect stdout and stderr to log file, as in a shell's >log 2>&1:
        log = os.path.join(dir, 'log')
        fd = os.open(log, os.O_WRONLY | os.O_CREAT, mode=0o666)
        sys.stdout.flush()
        os.close(1)
        os.dup2(fd, 1)
        sys.stderr.flush()
        os.close(2)
        os.dup2(fd, 2)
        # Detach child from parent's "session", so that a SIGINT will be
        # delivered just to the parent, not to the child. Instead, the parent
        # will eventually deliver a SIGKILL as part of cleanup_all().
        os.setsid()
        os.execve(cmd[0], cmd, dict(os.environ, **env))
        # execvp will not return. If it cannot run the program, it will raise
        # an exception.
    # Parent
    run_with_temporary_dir_pids.add(pid)
    signal.pthread_sigmask(signal.SIG_SETMASK, mask)
    return pid

# run_with_temporary_dir_pids is a set of process ids previously created
# by run_with_temporary_dir(). On exit, the processes listed here are
# cleaned up. Note that there is a known mapping (pid_to_dir()) from each
# pid to the temporary directory - which will also be removed.
run_with_temporary_dir_pids = set()

# abort_run_with_temporary_dir() kills a process started earlier by
# run_with_temporary_directory, and and removes its temporary directory.
# Currently, the log file is opened and returned so the caller can show
# it to the standard output even after the directory is removed. In the
# future we may want to change this - and save the log somewhere instead
# of copying it to stdout.
def abort_run_with_temporary_dir(pid):
    tmpdir = pid_to_dir(pid)
    try:
        os.kill(pid, 9)
        os.waitpid(pid, 0) # don't leave an annoying zombie
    except ProcessLookupError:
        pass
    # We want to read tmpdir/log to stdout, but if stdout is piped, this can
    # take a long time and be interrupted. We don't want the rmtree() below
    # to not happen in that case. So we need to open the log file first,
    # delete the directory (the open file will not be really deleted unti we
    # close it) - and only then start showing the log file.
    f = open(os.path.join(tmpdir, 'log'), 'rb')
    # Be paranoid about rmtree accidentally removing the entire disk...
    # TODO: check tmpdir is actually in TMPDIR and refuse to remove it
    # if not.
    if tmpdir != '/':
        shutil.rmtree(tmpdir)
    return f

summary=''

def cleanup_all():
    global summary
    global run_with_temporary_dir_pids
    for pid in run_with_temporary_dir_pids:
        f = abort_run_with_temporary_dir(pid)
        print('\nSubprocess output:\n')
        sys.stdout.flush()
        shutil.copyfileobj(f, sys.stdout.buffer)
    scylla_set = set()
    print(summary)

# We run the cleanup_all() function on exit for any reason - successful finish
# of the script, an uncaught exception, or a signal. It ensures that the
# subprocesses (e.g., Scylla) is killed and its temporary storage directory
# is deleted. It also shows the subprocesses's output log.
atexit.register(cleanup_all)

##############################

# When we run a server - e.g., Scylla, Cassandra, or Redis - we want to
# have it listen on a unique IP address so it doesn't collide with other
# servers run by other concurrent tests. Luckily, Linux allows us to use any
# IP address in the range 127/8 (i.e., 127.*.*.*). If we pick an IP address
# based on the server's main process id, we know two severers will not
# get the same IP address. We avoid 127.0.0.* because CCM (a different test
# framework) assumes it will be available for it to run Scylla instances.
# 127.255.255.255 is also illegal. So for simplicity we use 127.{1-254}.*.*.
# This gives us a total of 253*255*255 possible IP addresses - which is
# significantly more than /proc/sys/kernel/pid_max on any system I know.
def pid_to_ip(pid):
    bytes = pid.to_bytes(3, byteorder='big')
    return '127.' + str(bytes[0]+1) + '.' + str(bytes[1]) + '.' + str(bytes[2])

##############################

# Specific code for running *Scylla*:

import cassandra.cluster

# Find a Scylla executable. By default, we take the latest build/*/scylla
# next to the location of this script, but this can be overridden by setting
# a SCYLLA environment variable:
source_path = os.path.realpath(os.path.join(__file__, '../../..'))
def find_scylla():
    scyllas = glob.glob(os.path.join(source_path, 'build/*/scylla'))
    if not scyllas:
        print("Can't find a Scylla executable in {}.\nPlease build Scylla or set SCYLLA to the path of a Scylla executable.".format(source_path))
        exit(1)
    return max(scyllas, key=os.path.getmtime)

scylla = os.path.abspath(os.getenv('SCYLLA') or find_scylla())

def run_scylla_cmd(pid, dir):
    ip = pid_to_ip(pid)
    print('Booting Scylla on ' + ip + ' in ' + dir + '...')
    global scylla
    global source_path
    # To make things easier for users of "killall", "top", and similar,
    # we want the Scylla executable which we run during the test to have
    # a different name from manual runs of Scylla. Unfortunately, using
    # execve() to change just argv[0] isn't good enough - because killall
    # inspects the actual executable filename in /proc/<pid>/stat. So we
    # need to name the executable differently. Luckily, using a symbolic
    # link is good enough.
    scylla_link = os.path.join(dir, 'test_scylla')
    os.symlink(scylla, scylla_link)
    return ([scylla_link,
        '--options-file',  source_path + '/conf/scylla.yaml',
        '--developer-mode', '1',
        '--ring-delay-ms', '0',
        '--collectd', '0',
        '--smp', '2',
        '-m', '1G',
        '--overprovisioned',
        '--unsafe-bypass-fsync', '1',
        '--api-address', ip,
        '--rpc-address', ip,
        '--listen-address', ip,
        '--prometheus-address', ip,
        '--seed-provider-parameters', 'seeds=' + ip,
        '--workdir', dir,
        '--auto-snapshot', '0',
        '--skip-wait-for-gossip-to-settle', '0',
        '--logger-log-level', 'compaction=warn',
        '--logger-log-level', 'migration_manager=warn',
        '--num-tokens', '16',
        # Allow testing experimental features
        '--experimental', '1', '--enable-user-defined-functions', '1',
        ], {})

## Test that CQL is serving.
def check_cql(ip):
    cassandra.cluster.Cluster([ip]).connect()

# Wait for scylla to finish booting successfully. Raises an exception if
# we know it did not.
def wait_for_cql(pid, ip):
    start_time = time.time()
    cql_ready = False
    while time.time() < start_time + 200:
        time.sleep(0.1)
        # To check if Scylla died already (i.e., failed to boot), we need
        # to first get rid of the zombie (if it exists) with waitpid, and
        # then check if the process still exists, with kill.
        os.waitpid(pid, os.P_NOWAIT)
        os.kill(pid, 0)
        try:
            check_cql(ip)
            # if check_cql did not raise an exception, we're done:
            cql_ready = True
            break
        except cassandra.cluster.NoHostAvailable:
            pass
    duration = str(round(time.time() - start_time, 1)) + ' seconds'
    if not cql_ready:
        print(f'Boot did not complete in {duration}.')
        check_cql(ip) # this will fail, and show why
    os.waitpid(pid, os.P_NOWAIT)
    os.kill(pid, 0)
    print(f'Boot successful ({duration}).')
    sys.stdout.flush()

def run_cql_pytest(ip, additional_parameters):
    cql_pytest_dir = os.path.join(source_path, 'test/cql-pytest')
    sys.stdout.flush()
    sys.stderr.flush()
    pid = os.fork()
    if pid == 0:
        # child:
        global run_with_temporary_dir_pids
        run_with_temporary_dir_pids = set() # no children to clean up on child
        os.chdir(cql_pytest_dir)
        os.execvp('pytest', ['pytest',
            '--host', ip, '-o', 'junit_family=xunit2'] + additional_parameters)
        exit(1)
    # parent:
    if os.waitpid(pid, 0)[1]:
        return False
    else:
        return True
