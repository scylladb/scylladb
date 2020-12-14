# Copyright 2020 ScyllaDB
#
# This file is part of Scylla.
#
# Scylla is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Scylla is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Scylla.  If not, see <http://www.gnu.org/licenses/>.

##############################################################################
# Tests for Scylla's "tracing" feature (see docs/tracing.md) for Alternator
# queries. Reproduces issue #6891, where although tracing was implemented in
# Alternator, it only worked correctly for some operations but not others.
# In the tests here we attempt to ensure that the tracing feature continues
# to work for the relevant operations.
#
# Note that all tests in this file test Scylla-specific features, and are
# "skipped" when not running against Scylla, or when unable to enable tracing
# through out-of-band HTTP requests.
##############################################################################

import pytest
import requests
import re
import time
from botocore.exceptions import ClientError
from util import random_string, full_scan, full_query

# The "with_tracing" fixture ensures that tracing is enabled throughout
# the run of a test function, and disabled when it ends. If tracing cannot be
# enabled, the test is pytest.skip()ed. This will of course happens if we run
# the test with "--aws" (tracing is a Scylla-only feature).
# Note that to support (in the future) the ability for Alternator tests to
# run in parallel, the tests here need to be prepared that completely
# unrelated requests get traced during a test with with_tracing.
@pytest.fixture(scope="function")
def with_tracing(dynamodb):
    print("with_tracing enabling tracing")
    if dynamodb.meta.client._endpoint.host.endswith('.amazonaws.com'):
        pytest.skip('Scylla-only feature not supported by AWS')
    url = dynamodb.meta.client._endpoint.host
    # The REST API is on port 10000, and always http, not https.
    url = re.sub(r':[0-9]+(/|$)', ':10000', url)
    url = re.sub(r'^https:', 'http:', url)
    probability_resp = requests.get(url+'/storage_service/trace_probability')
    if probability_resp.status_code != 200:
        pytest.skip('Failed to fetch tracing probability')
    probability = probability_resp.text
    response = requests.post(url+'/storage_service/trace_probability?probability=1')
    if response.status_code != 200:
        pytest.skip('Failed to enable tracing')
    # verify tha tracing is really enabled
    response = requests.get(url+'/storage_service/trace_probability')
    if response.status_code != 200 or response.content.decode('utf-8') != '1':
        pytest.skip('Failed to verify tracing')
    yield
    print("with_tracing restoring tracing")
    response = requests.post(url+'/storage_service/trace_probability?probability='+probability)
    if response.status_code != 200:
        pytest.fail('Failed to disable tracing after with_tracing test')
    response = requests.get(url+'/storage_service/trace_probability')
    if response.status_code != 200 or response.content.decode('utf-8') != '0':
        pytest.skip('Failed to verify tracing disabled')

# Unfortunately, we currently have no way of directly finding the tracing
# session of a specific Alternator request. We could have returned the
# tracing session ID as a field of the response - but currently we don't.
# What we do instead is to scan the entire tracing session table looking for
# a given string in one of the commands' parameters. If this string is a long
# random string unique to the specific command, we'll find the right session.
# This approach is inefficient on a real production system (we don't even have
# a way to clear the sessions table after we use it so it grows longer and
# longer), but is good enough for tests on a throwable boot of Scylla as in
# test/alternator/run.
last_scan = None
def find_tracing_session(dynamodb, str):
    # The tracing session table does not get updated immediately - we may need
    # to sleep a bit until the requested string appears. We save the previous
    # session table in last_scan, so if we're looking for sessions of number of
    # different requests started together, it might be enough to read from disk
    # the session table just once, and not re-read (and of course, not sleep)
    # when looking for the other requests.
    global last_scan
    trace_sessions_table = dynamodb.Table('.scylla.alternator.system_traces.sessions')
    delay = 0.2
    if last_scan == None:
        # The trace tables have RF=2, even on a one-node test setup, and
        # thus fail reads with ConsistentRead=True (Quorum)...
        last_scan = full_scan(trace_sessions_table, ConsistentRead=False)
    while delay < 10:
        for entry in last_scan:
            if str in entry['parameters']:
                return entry['session_id']
        time.sleep(delay)
        delay = delay * 2
        last_scan = full_scan(trace_sessions_table, ConsistentRead=False)
    pytest.fail("Couldn't find tracing session")

# For the given tracing session_id of a request, read the list of "activities"
# that happened during this request. We discard the other information we have
# about each activity such as time, shard, source, and so on, and only keep
# the textual "activity" strings, and return them sorted like they were in
# the events table (i.e., by time).
def get_tracing_events(dynamodb, session_id):
    ret = []
    trace_events_table = dynamodb.Table('.scylla.alternator.system_traces.events')
    results = full_query(trace_events_table, ConsistentRead=False,
        KeyConditionExpression='session_id = :s',
        ExpressionAttributeValues={':s': session_id})
    for result in results:
        # as explained above, we only save the 'activity' string. There are
        # other interesting fields (like 'source_elapsed', the elapsed time)
        # which we don't save.
        ret.append(result['activity'])
    return ret

# We have no way to know whether the tracing events returned by
# get_tracing_events() is the entire trace. Even though we have already seen
# the tracing session, it doesn't guarantee that all the events were fully
# written (both are written independently and asynchronously). So the
# expect_tracing_events() has to retry the get_tracing_events() call until
# a timeout. In the successful case, we'll finish very quickly (usually,
# even immediately).
def expect_tracing_events(dynamodb, str, expected_events):
    delay = 0.1
    while delay < 10:
        events = get_tracing_events(dynamodb, find_tracing_session(dynamodb, str))
        for event in expected_events:
            if not event in events:
                break
        else:
            # Success!
            # Besides the specific expected_events, we want to check that
            # the event list isn't ridiculously short.
            if len(events) > 3:
                return
        time.sleep(delay)
        delay = delay * 2
    # Failed until the timeout. Repeat the last tests with an assertion,
    # to get a useful error report from pytest.
    assert len(events) > 3
    for event in expected_events:
        assert event in events

# Because tracing is asynchronous and sometimes appears as much as one
# second after the request, it is inefficient to have separate tests
# for separate request types - each of these tests will have a latency
# of as much as one second. Instead, we write just one test for all the
# different request types. This test enables tracing, runs a bunch of
# different requests - and only then looks for all of them in the trace
# table.
def test_tracing_all(with_tracing, test_table_s, dynamodb):
    # Run the different requests, each one containing a long random string
    # that we can later use to find with find_tracing_session():

    # PutItem:
    p_putitem = random_string(20)
    test_table_s.put_item(Item={'p': p_putitem})
    # GetItem:
    p_getitem = random_string(20)
    test_table_s.get_item(Key={'p': p_getitem})
    # DeleteItem:
    p_deleteitem = random_string(20)
    test_table_s.delete_item(Key={'p': p_deleteitem})
    # UpdateItem:
    p_updateitem = random_string(20)
    test_table_s.update_item(Key={'p': p_updateitem}, AttributeUpdates={})
    # BatchGetItem:
    p_batchgetitem = random_string(20)
    test_table_s.meta.client.batch_get_item(RequestItems = {test_table_s.name: {'Keys': [{'p': p_batchgetitem}]}})
    # BatchWriteItem:
    p_batchwriteitem = random_string(20)
    test_table_s.meta.client.batch_write_item(RequestItems = {test_table_s.name: [{'PutRequest':  {'Item': {'p': p_batchwriteitem}}}]})
    # Query:
    p_query = random_string(20)
    full_query(test_table_s, KeyConditionExpression='p = :p', ExpressionAttributeValues={':p': p_query})
    # Scan:
    p_scan = random_string(20)
    full_scan(test_table_s, FilterExpression='p = :p', ExpressionAttributeValues={':p': p_scan})

    # Check the traces. NOTE: the following checks are fairly arbitrary, and
    # may break in the future if we change the tracing messages...
    expect_tracing_events(dynamodb, p_putitem, ['PutItem', 'CAS successful'])
    expect_tracing_events(dynamodb, p_getitem, ['GetItem', 'Querying is done'])
    expect_tracing_events(dynamodb, p_deleteitem, ['DeleteItem', 'CAS successful'])
    expect_tracing_events(dynamodb, p_updateitem, ['UpdateItem', 'CAS successful'])
    expect_tracing_events(dynamodb, p_batchgetitem, ['BatchGetItem', 'Querying is done'])
    expect_tracing_events(dynamodb, p_batchwriteitem, ['BatchWriteItem', 'CAS successful'])
    expect_tracing_events(dynamodb, p_query, ['Query', 'Querying is done'])
    expect_tracing_events(dynamodb, p_scan, ['Scan', 'Performing a database query'])

# TODO:
# We could use traces to show that the right things actually happen during a
# request. In issue #6747 we suspected that maybe GetItem doesn't read just
# the requested item, and a tracing test can prove or disprove that hunch.
