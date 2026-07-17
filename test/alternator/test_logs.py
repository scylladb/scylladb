# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

###############################################################################
# Most Alternator tests should limit themselves to the DynamoDB API provided by
# the AWS SDK, and should work when running against any DynamoDB-compatible
# database, including DynamoDB and Scylla - the latter running locally or
# remotely. In particular, tests should generally *not* attempt to look at
# Scylla's log file - as this log file is not available at all when testing
# a remote server.
#
# Nevertheless, in some cases we might want to verify that some specific
# log messages do appear in Scylla's log file. These tests should be
# concentrated in this source file. All these tests are skipped when the
# test is not running against Scylla, or Scylla's log file cannot be
# found - e.g., because Scylla is running on a remote machine, or configured
# not to write to a log file.
#
# The code below automatically figures out where the log file is - when
# Scylla is running locally. First the local Scylla process is detected
# (it is the process listening to the our HTTP requests, if we can find
# one), then its standard output is guessed to be the log file - and then
# we verify that it really is.
#############################################################################

import pytest
import os
import io
import time
import re
import urllib.parse
from contextlib import contextmanager
from botocore.exceptions import ClientError

from .util import new_test_table, scylla_config_temporary
from .test_cql_rbac import new_dynamodb, new_role

# Utility function for trying to find a local process which is listening to
# the given local IP address and port. If such a process exists, return its
# process id (as a string). Otherwise, return None. Note that the local
# process needs to belong to the same user running this test, or it cannot
# be found.
def local_process_id(ip, port):
    # Implement something like the shell "lsof -Pni @{ip}:{port}", just
    # using /proc without any external shell command.
    # First, we look in /proc/net/tcp for a LISTEN socket (state 0x0A) at the
    # desired local address. The address is specially-formatted hex of the ip
    # and port, with 0100007F:2352 for 127.0.0.1:9042. We check for two
    # listening addresses: one is the specific IP address given, and the
    # other is listening on address 0 (INADDR_ANY).
    ip2hex = lambda ip: ''.join([f'{int(x):02X}' for x in reversed(ip.split('.'))])
    port2hex = lambda port: f'{int(port):04X}'
    try:
        addr1 = ip2hex(ip) + ':' + port2hex(port)
    except:
        return None
    addr2 = ip2hex('0.0.0.0') + ':' + port2hex(port)
    LISTEN = '0A'
    with open('/proc/net/tcp', 'r') as f:
        for line in f:
            cols = line.split()
            if cols[3] == LISTEN and (cols[1] == addr1 or cols[1] == addr2):
                inode = cols[9]
                break
        else:
            # Didn't find a process listening on the given address
            return None
    # Now look in /proc/*/fd/* for processes that have this socket "inode"
    # as one of its open files. We can only find a process that belongs to
    # the same user.
    target = f'socket:[{inode}]'
    for proc in os.listdir('/proc'):
        if not proc.isnumeric():
            continue
        dir = f'/proc/{proc}/fd/'
        try:
            for fd in os.listdir(dir):
                if os.readlink(dir + fd) == target:
                    # Found the process!
                    return proc
        except:
            # Ignore errors. We can't check processes we don't own.
            pass
    return None

# A fixture to find the Scylla log file, returning the log file's path.
# If the log file cannot be found, or it's not Scylla, the fixture calls
# pytest.skip() to skip any test which uses it. The fixture has module
# scope, so looking for the log file only happens once. Individual tests
# should use the function-scope fixture "logfile" below, which takes care
# of opening the log file for reading in the right place.
# We look for the log file by looking for a local process listening to the
# given DynamoDB API connection, assuming its standard output is the log file,
# and then verifying that this file is a proper Scylla log file.
@pytest.fixture(scope="module")
def logfile_path(dynamodb):
    # Split the endpoint URL into host and port part. If the port is not
    # explicitly specified, we need to assume it is the default http port
    # (80) or default https (443) port, depending on the scheme.
    endpoint_url = dynamodb.meta.client._endpoint.host
    p = urllib.parse.urlparse(endpoint_url)
    # If hostname is a string not an ip address, it's unlikely to be a local
    # process anyway... test/alternator/run uses an IP address.
    ip = p.hostname
    port = p.port
    if port is None:
        port = 443 if p.scheme == 'https' else 80
    pid = local_process_id(ip, port)
    if not pid:
        pytest.skip("Can't find local process")
    # Now that we know the process id, use /proc to find if its standard
    # output is redirected to a file. If it is, that's the log file. If it
    # isn't a file, we don't known where the user is writing the log...
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
# and if it doesn't, calls pytest.fail().
# Because it may take time for the log message to be flushed, and sometimes
# we may want to look for messages about various delayed events, this
# function doesn't give up when it reaches the end of file, and rather
# retries until a given timeout. The timeout may be long, because successful
# tests will not wait for it. Note, however, that long timeouts will make
# xfailing tests slow.
def wait_for_log(logfile, pattern, re_flags=re.MULTILINE, timeout=5):
    contents = logfile.read()
    prog = re.compile(pattern, re_flags)
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
def test_log_table_operations(dynamodb, logfile):
    schema = {
        'KeySchema': [{ 'AttributeName': 'p', 'KeyType': 'HASH' }],
        'AttributeDefinitions': [{ 'AttributeName': 'p', 'AttributeType': 'S' }]
    }
    with new_test_table(dynamodb, **schema) as table:
        wait_for_log(logfile, f'Creating keyspace alternator_{table.name}')
    wait_for_log(logfile, f'Dropping keyspace alternator_{table.name}')

# Test that when alternator_warn_authorization is set to true, WARN-level
# log messages are generated on authentication or authorization errors.
# This is in addition to the metric counting these errors, which are tested
# in test_metrics.py. These are tests for issue #25308.
# We check that alternator_warn_authorization enables log messages regardless
# of what alternator_enforce_authorization is set to. If enforce_authorization
# is also true, a message is logged and the request failed - and if it is
# false the same message is logged and the request succeeds.
#
# It's important to have a regression test that these log messages appear,
# because it is documented that they appear in "warn" mode and users may
# rely on them instead of metrics to learn about auth setup problems.
#
# Note that we do not test that when alternator_warn_authorization is
# set false, warnings are NOT logged - this is less important, and also
# trickier to test what does NOT appear on the log (we definitely don't want
# to wait for a timeout).
#
# We have several tests here, for several kinds of authentication and
# authorization errors.

@contextmanager
def scylla_config_auth_temporary(dynamodb, enforce_auth, warn_auth):
    with scylla_config_temporary(dynamodb, 'alternator_enforce_authorization', 'true' if enforce_auth else 'false'):
        with scylla_config_temporary(dynamodb, 'alternator_warn_authorization', 'true' if warn_auth else 'false'):
            yield

# authentication failure 1: bogus username and secret key
@pytest.mark.parametrize("enforce_auth", [True, False])
def test_log_authentication_failure_1(dynamodb, logfile, test_table_s, enforce_auth):
    with scylla_config_auth_temporary(dynamodb, enforce_auth, True):
        with new_dynamodb(dynamodb, 'bogus_username', 'bogus_secret_key') as d:
            tab = d.Table(test_table_s.name)
            # We don't expect get_item() to find any item, it should either
            # pass or fail depending on enforce_auth - but in any case it
            # should log the error.
            try:
                tab.get_item(Key={'p': 'dog'})
                operation_succeeded = True
            except ClientError as e:
                assert 'UnrecognizedClientException' in str(e)
                operation_succeeded = False
            if enforce_auth:
                assert not operation_succeeded
            else:
                assert operation_succeeded
            wait_for_log(logfile, '^WARN .*user bogus_username.*client address')

# authentication failure 2: real username, wrong secret key
# Unfortunately, tests that create a new role need to use CQL too.
@pytest.mark.parametrize("enforce_auth", [True, False])
def test_log_authentication_failure_2(dynamodb, cql, logfile, test_table_s, enforce_auth):
    with scylla_config_auth_temporary(dynamodb, enforce_auth, True):
        with new_role(cql) as (role, key):
            with new_dynamodb(dynamodb, role, 'bogus_secret_key') as d:
                tab = d.Table(test_table_s.name)
                try:
                    tab.get_item(Key={'p': 'dog'})
                    operation_succeeded = True
                except ClientError as e:
                    assert 'UnrecognizedClientException' in str(e)
                    operation_succeeded = False
                if enforce_auth:
                    assert not operation_succeeded
                else:
                    assert operation_succeeded
                wait_for_log(logfile, f'^WARN .*wrong signature for user {role}.*client address')

# Authorization failure - a valid user but without permissions to do a
# given operation.
@pytest.mark.parametrize("enforce_auth", [True, False])
def test_log_authorization_failure(dynamodb, cql, logfile, test_table_s, enforce_auth):
    with scylla_config_auth_temporary(dynamodb, enforce_auth, True):
        with new_role(cql) as (role, key):
            with new_dynamodb(dynamodb, role, key) as d:
                tab = d.Table(test_table_s.name)
                # The new role is not a superuser, so should not have
                # permissions to read from this table created earlier by
                # the superuser.
                try:
                    tab.get_item(Key={'p': 'dog'})
                    operation_succeeded = True
                except ClientError as e:
                    assert 'AccessDeniedException' in str(e)
                    operation_succeeded = False
                if enforce_auth:
                    assert not operation_succeeded
                else:
                    assert operation_succeeded
                wait_for_log(logfile, f'^WARN .*SELECT access on table.*{test_table_s.name} is denied to role {role}.*client address')
