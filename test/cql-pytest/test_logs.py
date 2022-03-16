# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Most cql-pytest tests should limit themselves to the CQL API provided by
# the Cassandra Python driver, and should work when running against any CQL-
# compatible database, including Scylla and Cassandra, running locally or
# remotely. In particular, tests should generally *not* attempt to look at
# Scylla's log file - as this log file is completely different between
# Scylla and Cassandra, and not available at all when testing a remote server.
#
# Nevertheless, in some cases we might want to verify that some specific
# log messages do appear in Scylla's log file. These tests should be
# concentrated in this source file. All these tests are skipped when the
# test is not running against Scylla, or Scylla's log file cannot be
# found - e.g., because Scylla is running on a remote machine, or configured
# not to write to a log file).
#
# The code below automatically figures out where the log file is, when
# Scylla is running locally. First the local Scylla process is detected
# (it is the process listening to the our CQL connection, if we can find
# one), then its standard output is guessed to be the log file - and then
# we verify that it really is.
#############################################################################

import pytest
import os
import io
import time
import re

from cassandra import InvalidRequest

from util import new_test_table, local_process_id
from test_batch import generate_big_batch

# A fixture to find the Scylla log file, returning the log file's path.
# If the log file cannot be found, or it's not Scylla, the fixture calls
# pytest.skip() to skip any test which uses it. The fixture has module
# scope, so looking for the log file only happens once. Individual tests
# should use the function-scope fixture "logfile" below, which takes care
# of opening the log file for reading in the right place.
# We look for the log file by looking for a local process listening to the
# given CQL connection, assuming its standard output is the log file, and
# then verifying that this file is a proper Scylla log file.
@pytest.fixture(scope="module")
def logfile_path(cql):
    pid = local_process_id(cql)
    if not pid:
        pytest.skip("Can't find local process")
    # Now that we know the process id, use /proc to find if its standard
    # output is redirected to a file. If it is, that's the log file. If it
    # isn't a file, we don't where the user is piping the log.
    try:
        log = os.readlink(f'/proc/{pid}/fd/1')
    except:
        pytest.skip("Can't find local log file")
    # If the process's standard output is some pipe or device, it's
    # not the log file we were hoping for...
    if not log.startswith('/') or not os.path.isfile(log):
        pytest.skip("Can't find local log file")
    # Scylla can be configured to put the log in syslog, not in the standard
    # output. So let's verify that the file which we found actually looks
    # like a Scylla log and isn't just empty or something... The Scylla log
    # file always starts with the line: "Scylla version ... starting ..."
    with open(log, 'r') as f:
        head = f.read(7)
        if head != 'Scylla ':
            pytest.skip("Not a Scylla log file")
        yield log

# The "logfile" fixture returns the log file open for reading at the end.
# Testing if something appears in the log usually involves taking this
# fixture, and then checking with wait_for_log() for the desired message to
# have appeared. Because each logfile fixture opens the log file separately,
# it is ok if several tests are run in parallel - but they will see each
# other's log messages so should try to ensure that unique strings (e.g.,
# random table names) appear in the log message.
@pytest.fixture(scope="function")
def logfile(logfile_path):
    with open(logfile_path, 'r') as f:
        f.seek(0, io.SEEK_END)
        yield f

# wait_for_log() checks if the log, starting at its current position
# (probably set by the logfile fixture), contains the given message -
# and if it doesn't call pytest.fail().
# Because it may take time for the log message to be flushed, and sometimes
# we may want to look for messages about various delayed events, this
# function doesn't give up when it reaches the end of file, and rather
# retries until a given timeout. The timeout may be long, because successful
# tests will not wait for it. Note, however, that long timeouts will make
# xfailing tests slow.
def wait_for_log(logfile, pattern, timeout=5):
    contents = logfile.read()
    prog = re.compile(pattern)
    if prog.search(contents):
        return
    end = time.time() + timeout
    while time.time() < end:
        s = logfile.read()
        if s:
            # Though unlikely, it is possible that a single message got
            # split into two reads, so we need to check (and recheck)
            # the entire content since the beginning of this wait :-(
            contents = contents + s
            if prog.search(contents):
                return
        time.sleep(0.1)
    pytest.fail(f'Timed out ({timeout} seconds) looking for {pattern} in log file. Got:\n' + contents)

# A simple example of testing the log file - we check that a table creation,
# and table deletion, both cause messages to appear on the log.
def test_log_table_operations(cql, test_keyspace, logfile):
    with new_test_table(cql, test_keyspace, 'p int PRIMARY KEY') as table:
        wait_for_log(logfile, f'Creating {table}')
    wait_for_log(logfile, f'Dropping {table}')


@pytest.fixture(scope="module")
def table_batch(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "k int primary key, t text") as table:
        yield table


def test_warning_message_for_batch_exceeding_warning_threshold(cql, table_batch, logfile):
    """Batch size above threshold should add warning message to scylla logs"""
    warning_pattern = r"WARN .+BatchStatement - Batch modifying [\d]+ partitions in [_\w.]+ is of size [\d]+ bytes, "\
                      r"exceeding specified WARN threshold of"
    cql.execute(generate_big_batch(table_batch, 256))
    wait_for_log(logfile, warning_pattern, timeout=1)


def test_error_message_for_batch_exceeding_fail_threshold(cql, table_batch, logfile):
    """Batch size above threshold should add error message to scylla logs"""
    try:
        cql.execute(generate_big_batch(table_batch, 1025))
    except InvalidRequest:
        err_pattern = r"ERROR .+ BatchStatement - Batch modifying [\d]+ partitions in [_\w.]+ is of size [\d]+ bytes, "\
                          r"exceeding specified FAIL threshold of"
        wait_for_log(logfile, err_pattern, timeout=1)
    else:
        pytest.fail("Oversized batch didn't fail.")
