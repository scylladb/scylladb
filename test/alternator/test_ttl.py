# Copyright 2021 ScyllaDB
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

# Tests for the Time To Live (TTL) feature for item expiration.

import pytest
import time
import re
from botocore.exceptions import ClientError
from util import new_test_table, random_string, full_query, unique_table_name
from contextlib import contextmanager

# passes_or_raises() is similar to pytest.raises(), except that while raises()
# expects a certain exception must happen, the new passes_or_raises()
# expects the code to either pass (not raise), or if it throws, it must
# throw the specific specified exception.
@contextmanager
def passes_or_raises(expected_exception, match=None):
    # Sadly __tracebackhide__=True only drops some of the unhelpful backtrace
    # lines. See https://github.com/pytest-dev/pytest/issues/2057
    __tracebackhide__ = True
    try:
        yield
        # The user's "with" code is running during the yield. If it didn't
        # throw we return from the function - the raises_or_not() passed as
        # the "or not" case.
        return
    except expected_exception as err:
        if match == None or re.search(match, str(err)):
            # The raises_or_not() passed on as the "raises" case
            return
        pytest.fail(f"exception message '{err}' did not match '{match}'")
    except Exception as err:
        pytest.fail(f"Got unexpected exception type {type(err).__name__} instead of {expected_exception.__name__}: {err}")

# Test the DescribeTimeToLive operation on a table where the time-to-live
# feature was *not* enabled.
def test_describe_ttl_without_ttl(test_table):
    response = test_table.meta.client.describe_time_to_live(TableName=test_table.name)
    assert 'TimeToLiveDescription' in response
    assert 'TimeToLiveStatus' in response['TimeToLiveDescription']
    assert response['TimeToLiveDescription']['TimeToLiveStatus'] == 'DISABLED'
    assert not 'AttributeName' in response['TimeToLiveDescription']

# Test that UpdateTimeToLive can be used to pick an expiration-time attribute
# and this information becomes available via DescribeTimeToLive
def test_ttl_enable(dynamodb):
    with new_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }, ],
        AttributeDefinitions=[ { 'AttributeName': 'p', 'AttributeType': 'S' } ]) as table:
        client = table.meta.client
        ttl_spec = {'AttributeName': 'expiration', 'Enabled': True}
        response = client.update_time_to_live(TableName=table.name,
            TimeToLiveSpecification=ttl_spec)
        assert 'TimeToLiveSpecification' in response
        assert response['TimeToLiveSpecification'] == ttl_spec
        # Verify that DescribeTimeToLive can recall this setting:
        response = client.describe_time_to_live(TableName=table.name)
        assert 'TimeToLiveDescription' in response
        assert response['TimeToLiveDescription'] == {
            'TimeToLiveStatus': 'ENABLED', 'AttributeName': 'expiration'}
        # Verify that UpdateTimeToLive cannot enable TTL is it is already
        # enabled. A user is not allowed to change the expiration attribute
        # without disabling TTL first, and it's an error even to try to
        # enable TTL with exactly the same attribute as already enabled.
        # (the error message uses slightly different wording in those two
        # cases)
        with pytest.raises(ClientError, match='ValidationException.*(active|enabled)'):
            client.update_time_to_live(TableName=table.name,
                TimeToLiveSpecification=ttl_spec)
        with pytest.raises(ClientError, match='ValidationException.*(active|enabled)'):
            new_ttl_spec = {'AttributeName': 'new', 'Enabled': True}
            client.update_time_to_live(TableName=table.name,
                TimeToLiveSpecification=new_ttl_spec)

# Test various *wrong* ways of disabling TTL. Although we test here various
# error cases of how to disable TTL incorrectly, we don't actually check in
# this test case the successful disabling case, because DynamoDB refuses to
# disable TTL if it was enabled in the last hour (according to DynamoDB's
# documentation). We have below a much longer test for the successful TTL
# disable case.
def test_ttl_disable_errors(dynamodb):
    with new_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }, ],
        AttributeDefinitions=[ { 'AttributeName': 'p', 'AttributeType': 'S' } ]) as table:
        client = table.meta.client
        # We can't disable TTL if it's not already enabled.
        with pytest.raises(ClientError, match='ValidationException.*disabled'):
            client.update_time_to_live(TableName=table.name,
                TimeToLiveSpecification={'AttributeName': 'expiration', 'Enabled': False})
        # So enable TTL, before disabling it:
        client.update_time_to_live(TableName=table.name,
            TimeToLiveSpecification={'AttributeName': 'expiration', 'Enabled': True})
        response = client.describe_time_to_live(TableName=table.name)
        assert response['TimeToLiveDescription'] == {
            'TimeToLiveStatus': 'ENABLED', 'AttributeName': 'expiration'}
        # To disable TTL, the user must specify the current expiration
        # attribute in the command - you can't not specify it, or specify
        # the wrong one!
        with pytest.raises(ClientError, match='ValidationException.*different'):
            client.update_time_to_live(TableName=table.name,
                TimeToLiveSpecification={'AttributeName': 'dog', 'Enabled': False})
        # Finally disable TTL the right way :-) On DynamoDB this fails
        # because you are not allowed to modify the TTL setting twice in
        # one hour, but in our implementation it can also pass quickly.
        with passes_or_raises(ClientError, match='ValidationException.*multiple times'):
            client.update_time_to_live(TableName=table.name,
                TimeToLiveSpecification={'AttributeName': 'expiration', 'Enabled': False})

# Test *sucessful* disabling of TTL. This is an extremely slow test, as
# DynamoDB refuses to disable TTL if it was enabled in the last half hour
# (the documentation suggests a full hour, but in practice it seems 30
# minutes).
@pytest.mark.veryslow
def test_ttl_disable(dynamodb):
    with new_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }, ],
        AttributeDefinitions=[ { 'AttributeName': 'p', 'AttributeType': 'S' } ]) as table:
        client = table.meta.client
        client.update_time_to_live(TableName=table.name,
            TimeToLiveSpecification={'AttributeName': 'expiration', 'Enabled': True})
        assert client.describe_time_to_live(TableName=table.name)['TimeToLiveDescription'] == {
            'TimeToLiveStatus': 'ENABLED', 'AttributeName': 'expiration'}
        start_time = time.time()
        while time.time() < start_time + 4000:
            try:
                client.update_time_to_live(TableName=table.name,
                    TimeToLiveSpecification={'AttributeName': 'expiration', 'Enabled': False})
                break
            except ClientError as error:
                # As long as we get "Time to live has been modified multiple
                # times within a fixed interval", we need to try again -
                # the documentation says for a full hour!
                assert 'times' in error.response['Error']['Message']
                print(time.time() - start_time)
                time.sleep(60)
                continue
        assert client.describe_time_to_live(TableName=table.name)['TimeToLiveDescription'] == {
            'TimeToLiveStatus': 'DISABLED'}

# Test various errors in the UpdateTimeToLive request.
# Unfortunately, the boto3 library catches most incorrect inputs on the
# client side, so we can't check the following errors here :-(
# 1. Missing UpdateTimeToLive parameters AttributeName or Enabled
# 2. Wrong types for them (e.g., string for Enabled)
# 3. Empty string for AttributeName
# There are only a few things that boto3 doesn't guard, so we can check them:
def test_update_ttl_errors(dynamodb):
    client = dynamodb.meta.client
    # Can't set TTL on a non-existent table
    nonexistent_table = unique_table_name()
    with pytest.raises(ClientError, match='ResourceNotFoundException'):
        client.update_time_to_live(TableName=nonexistent_table,
            TimeToLiveSpecification={'AttributeName': 'expiration', 'Enabled': True})
    with new_test_table(dynamodb,
            KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }, ],
            AttributeDefinitions=[ { 'AttributeName': 'p', 'AttributeType': 'S' } ]) as table:
        # AttributeName must be between 1 and 255 characters long. We
        # can't check 0 (boto3 guards against this in the client), but
        # can check >255.
        with pytest.raises(ClientError, match='ValidationException.*length'):
            client.update_time_to_live(TableName=table.name,
                TimeToLiveSpecification={'AttributeName': 'x'*256, 'Enabled': True})

# Basic test that expiration indeed expires items that should be expired,
# and doesn't expire items which shouldn't be expired.
# This is an extremely slow test - DynamoDB documentation says that
# expiration may even be delayed for 48 hours. But in practice, at the time
# of this writing, for tiny tables we see delays of around 10 minutes.
# The following test is set to always run for "duration" seconds, currently
# 15 minutes. During this time, we expect to see the items which should
# have expired to be expired - and the items that should not have expired
# should still exist.
@pytest.mark.veryslow
@pytest.mark.xfail(reason="TTL not implemented yet #5060")
def test_ttl_expiration(dynamodb):
    duration = 900
    with new_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }, ],
        AttributeDefinitions=[ { 'AttributeName': 'p', 'AttributeType': 'S' } ]) as table:
        # Insert one expiring item *before* enabling the TTL, to verify that
        # items that already exist when TTL is configured also get handled.
        p0 = random_string()
        table.put_item(Item={'p': p0, 'expiration': int(time.time())-60})
        # Enable TTL, using the attribute "expiration":
        client = table.meta.client
        ttl_spec = {'AttributeName': 'expiration', 'Enabled': True}
        response = client.update_time_to_live(TableName=table.name,
            TimeToLiveSpecification=ttl_spec)
        assert response['TimeToLiveSpecification'] == ttl_spec
        # This item should never expire, it is missing the "expiration"
        # attribute:
        p1 = random_string()
        table.put_item(Item={'p': p1})
        # This item should expire ASAP, as its "expiration" has already
        # passed, one minute ago:
        p2 = random_string()
        table.put_item(Item={'p': p2, 'expiration': int(time.time())-60})
        # This item has an expiration more than 5 years in the past (it is my
        # birth date...), so according to the DynamoDB documentation it should
        # be ignored and p3 should never expire:
        p3 = random_string()
        table.put_item(Item={'p': p3, 'expiration': 162777600})
        # This item has as its expiration time one minute into the future,
        # so should expire by the time the test ends:
        p4 = random_string()
        table.put_item(Item={'p': p4, 'expiration': int(time.time())+60})
        # This item starts with expiration time two minutes into the future,
        # but below every minute move it again, so it should never expire:
        p5 = random_string()
        table.put_item(Item={'p': p5, 'expiration': int(time.time())+120})
        # This item has an expiration time 10 hours into the future, so it
        # will not expire by the time the test ends:
        p6 = random_string()
        table.put_item(Item={'p': p6, 'expiration': int(time.time())+36000})
        # We could have just done time.sleep(duration) here, but in case a
        # user is watching this long test, let's output the status every
        # minute, and it also allows us to test what happens when an item
        # p5's expiration time is continuously pushed back into the future:
        start_time = time.time()
        while time.time() < start_time + duration:
            print(f"--- {int(time.time()-start_time)} seconds")
            if 'Item' in table.get_item(Key={'p': p0}):
                print("p0 alive")
            if 'Item' in table.get_item(Key={'p': p1}):
                print("p1 alive")
            if 'Item' in table.get_item(Key={'p': p2}):
                print("p2 alive")
            if 'Item' in table.get_item(Key={'p': p3}):
                print("p3 alive")
            if 'Item' in table.get_item(Key={'p': p4}):
                print("p4 alive")
            if 'Item' in table.get_item(Key={'p': p5}):
                print("p5 alive")
            if 'Item' in table.get_item(Key={'p': p6}):
                print("p6 alive")
            # Always keep p5's expiration two minutes into the future :-)
            table.update_item(Key={'p': p5},
                AttributeUpdates={'expiration': {'Value': int(time.time())+120, 'Action': 'PUT'}})
            time.sleep(60)

        # After the delay, p1,p3,p5,p6 should be alive, p0,p2,p4 should not
        assert not 'Item' in table.get_item(Key={'p': p0})
        assert 'Item' in table.get_item(Key={'p': p1})
        assert not 'Item' in table.get_item(Key={'p': p2})
        assert 'Item' in table.get_item(Key={'p': p3})
        assert not 'Item' in table.get_item(Key={'p': p4})
        assert 'Item' in table.get_item(Key={'p': p5})
        assert 'Item' in table.get_item(Key={'p': p6})

# While it probably makes little sense to do this, the designated
# expiration-time attribute *may* be the hash or range key attributes.
# So let's test that this indeed works and items indeed expire
# Just like test_ttl_expiration() above, these tests are extremely slow
# because DynamoDB delays expiration by around 10 minutes.
@pytest.mark.veryslow
@pytest.mark.xfail(reason="TTL not implemented yet #5060")
def test_ttl_expiration_hash(dynamodb):
    duration = 1200
    with new_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }, ],
        AttributeDefinitions=[ { 'AttributeName': 'p', 'AttributeType': 'N' } ]) as table:
        ttl_spec = {'AttributeName': 'p', 'Enabled': True}
        table.meta.client.update_time_to_live(TableName=table.name,
            TimeToLiveSpecification=ttl_spec)
        # p1 is in the past, and should be expired. p2 is in the distant
        # future and should not be expired.
        p1 = int(time.time()) - 60
        p2 = int(time.time()) + 3600
        table.put_item(Item={'p': p1})
        table.put_item(Item={'p': p2})
        start_time = time.time()
        while time.time() < start_time + duration:
            print(f"--- {int(time.time()-start_time)} seconds")
            if 'Item' in table.get_item(Key={'p': p1}):
                print("p1 alive")
            if 'Item' in table.get_item(Key={'p': p2}):
                print("p2 alive")
            time.sleep(60)
        # After the delay, p2 should be alive, p1 should not
        assert not 'Item' in table.get_item(Key={'p': p1})
        assert 'Item' in table.get_item(Key={'p': p2})

@pytest.mark.veryslow
@pytest.mark.xfail(reason="TTL not implemented yet #5060")
def test_ttl_expiration_range(dynamodb):
    duration = 1200
    with new_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }, { 'AttributeName': 'c', 'KeyType': 'RANGE' } ],
        AttributeDefinitions=[ { 'AttributeName': 'p', 'AttributeType': 'S' }, { 'AttributeName': 'c', 'AttributeType': 'N' } ]) as table:
        ttl_spec = {'AttributeName': 'c', 'Enabled': True}
        table.meta.client.update_time_to_live(TableName=table.name,
            TimeToLiveSpecification=ttl_spec)
        # c1 is in the past, and should be expired. c2 is in the distant
        # future and should not be expired.
        p = random_string()
        c1 = int(time.time()) - 60
        c2 = int(time.time()) + 3600
        table.put_item(Item={'p': p, 'c': c1})
        table.put_item(Item={'p': p, 'c': c2})
        start_time = time.time()
        while time.time() < start_time + duration:
            print(f"--- {int(time.time()-start_time)} seconds")
            if 'Item' in table.get_item(Key={'p': p, 'c': c1}):
                print("c1 alive")
            if 'Item' in table.get_item(Key={'p': p, 'c': c2}):
                print("c2 alive")
            time.sleep(60)
        # After the delay, c2 should be alive, c1 should not
        assert not 'Item' in table.get_item(Key={'p': p, 'c': c1})
        assert 'Item' in table.get_item(Key={'p': p, 'c': c2})

# Test how in a table with a GSI or LSI, expiring an item also removes it
# from the GSI and LSI.
# We already tested above the various reasons for an item expiring or not
# expiring, so we don't need to continue testing these various cases here,
# and can test just one expiring item. This also allows us to finish the
# test as soon as the item expires, instead of after a fixed "duration" as
# in the above tests.
@pytest.mark.veryslow
@pytest.mark.xfail(reason="TTL not implemented yet #5060")
def test_ttl_expiration_gsi_lsi(dynamodb):
    # In our experiments we noticed that when a table has secondary indexes,
    # items are expired with significant delay. Whereas a 10 minute delay
    # for regular tables was typical, in the table we have here we saw
    # a typical delay of 30 minutes before items expired.
    max_duration = 3600
    with new_test_table(dynamodb,
        KeySchema=[
            { 'AttributeName': 'p', 'KeyType': 'HASH' },
            { 'AttributeName': 'c', 'KeyType': 'RANGE' },
        ],
        LocalSecondaryIndexes=[
            {   'IndexName': 'lsi',
                'KeySchema': [
                    { 'AttributeName': 'p', 'KeyType': 'HASH' },
                    { 'AttributeName': 'l', 'KeyType': 'RANGE' },
                ],
                'Projection': { 'ProjectionType': 'ALL' },
            },
        ],
        GlobalSecondaryIndexes=[
            {   'IndexName': 'gsi',
                'KeySchema': [
                    { 'AttributeName': 'g', 'KeyType': 'HASH' },
                ],
                'Projection': { 'ProjectionType': 'ALL' }
            },
        ],
        AttributeDefinitions=[
            { 'AttributeName': 'p', 'AttributeType': 'S' },
            { 'AttributeName': 'c', 'AttributeType': 'S' },
            { 'AttributeName': 'g', 'AttributeType': 'S' },
            { 'AttributeName': 'l', 'AttributeType': 'S' },
        ]) as table:
        ttl_spec = {'AttributeName': 'expiration', 'Enabled': True}
        response = table.meta.client.update_time_to_live(TableName=table.name,
            TimeToLiveSpecification=ttl_spec)
        assert 'TimeToLiveSpecification' in response
        assert response['TimeToLiveSpecification'] == ttl_spec
        p = random_string()
        c = random_string()
        g = random_string()
        l = random_string()
        # expiration one minute in the past, so item should expire ASAP.
        expiration = int(time.time()) - 60
        table.put_item(Item={'p': p, 'c': c, 'g': g, 'l': l, 'expiration': expiration})
        start_time = time.time()
        gsi_was_alive = False
        while time.time() < start_time + max_duration:
            print(f"--- {int(time.time()-start_time)} seconds")
            base_alive = 'Item' in table.get_item(Key={'p': p, 'c': c})
            gsi_alive = bool(full_query(table, IndexName='gsi',
                ConsistentRead=False,
                KeyConditionExpression="g=:g",
                ExpressionAttributeValues={':g': g}))
            lsi_alive = bool(full_query(table, IndexName='lsi',
                KeyConditionExpression="p=:p and l=:l",
                ExpressionAttributeValues={':p': p, ':l': l}))
            if base_alive:
                print("base alive")
            if gsi_alive:
                print("gsi alive")
                # gsi takes time to go up, so make sure it did
                gsi_was_alive = True
            if lsi_alive:
                print("lsi alive")
            # If the base item, gsi item and lsi item have all expired, the
            # test is done - and successful:
            if not base_alive and not gsi_alive and gsi_was_alive and not lsi_alive:
                return
            time.sleep(60)
        pytest.fail('base, gsi, or lsi not expired')

# Above in test_ttl_expiration_hash() and test_ttl_expiration_range() we
# checked the case where TTL's expiration-time attribute is not a regular
# attribute but one of the two key attributes. Alternator encodes these
# attributes differently (as a real column in the schema - not a serialized
# JSON inside a map column), so needed to check that such an attribute is
# usable as well. LSI (and, currently, also GSI) *also* implement their keys
# differently (like the base key), so let's check that having the TTL column
# a GSI or LSI key still works. Even though it is unlikely that any user
# would want to have such a use case.
# The following test only tries LSI, which we're sure will always have this
# special case (for GSI, we are considering changing the implementation
# and make their keys regular attributes - to support adding GSIs after a
# table already exists - so after such a change GSIs will no longer be a
# special case.
@pytest.mark.veryslow
@pytest.mark.xfail(reason="TTL not implemented yet #5060")
def test_ttl_expiration_lsi_key(dynamodb):
    # In my experiments, a 30-minute (1800 seconds) is the typical delay
    # for expiration in this test for DynamoDB
    max_duration = 3600
    with new_test_table(dynamodb,
        KeySchema=[
            { 'AttributeName': 'p', 'KeyType': 'HASH' },
            { 'AttributeName': 'c', 'KeyType': 'RANGE' },
        ],
        LocalSecondaryIndexes=[
            {   'IndexName': 'lsi',
                'KeySchema': [
                    { 'AttributeName': 'p', 'KeyType': 'HASH' },
                    { 'AttributeName': 'l', 'KeyType': 'RANGE' },
                ],
                'Projection': { 'ProjectionType': 'ALL' },
            },
        ],
        AttributeDefinitions=[
            { 'AttributeName': 'p', 'AttributeType': 'S' },
            { 'AttributeName': 'c', 'AttributeType': 'S' },
            { 'AttributeName': 'l', 'AttributeType': 'N' },
        ]) as table:
        # The expiration-time attribute is the LSI key "l":
        ttl_spec = {'AttributeName': 'l', 'Enabled': True}
        response = table.meta.client.update_time_to_live(TableName=table.name,
            TimeToLiveSpecification=ttl_spec)
        assert 'TimeToLiveSpecification' in response
        assert response['TimeToLiveSpecification'] == ttl_spec
        p = random_string()
        c = random_string()
        l = random_string()
        # expiration one minute in the past, so item should expire ASAP.
        expiration = int(time.time()) - 60
        table.put_item(Item={'p': p, 'c': c, 'l': expiration})
        start_time = time.time()
        gsi_was_alive = False
        while time.time() < start_time + max_duration:
            print(f"--- {int(time.time()-start_time)} seconds")
            if 'Item' in table.get_item(Key={'p': p, 'c': c}):
                print("base alive")
            else:
                # test is done - and successful:
                return
            time.sleep(60)
        pytest.fail('base not expired')

# Check that in the DynamoDB Streams API, an event appears about an item
# becoming expired. This event should contain be a REMOVE event, contain
# the appropriate information about the expired item (its key and/or its
# content), and a special userIdentity flag saying that this is not a regular
# REMOVE but an expiration.
@pytest.mark.veryslow
@pytest.mark.xfail(reason="TTL not implemented yet #5060")
def test_ttl_expiration_streams(dynamodb, dynamodbstreams):
    # In my experiments, a 30-minute (1800 seconds) is the typical
    # expiration delay in this test. If the test doesn't finish within
    # max_duration, we report a failure.
    max_duration = 3600
    with new_test_table(dynamodb,
        KeySchema=[
            { 'AttributeName': 'p', 'KeyType': 'HASH' },
            { 'AttributeName': 'c', 'KeyType': 'RANGE' },
        ],
        AttributeDefinitions=[
            { 'AttributeName': 'p', 'AttributeType': 'S' },
            { 'AttributeName': 'c', 'AttributeType': 'S' },
        ],
        StreamSpecification={
            'StreamEnabled': True,
            'StreamViewType':  'NEW_AND_OLD_IMAGES'}
        ) as table:
        ttl_spec = {'AttributeName': 'expiration', 'Enabled': True}
        table.meta.client.update_time_to_live(TableName=table.name,
            TimeToLiveSpecification=ttl_spec)

        # Before writing to the table, wait for the stream to become active
        # so we can be sure that the expiration - even if it miraculously
        # happens in a second (it usually takes 30 minutes) - is guaranteed
        # to reach the stream.
        stream_enabled = False
        start_time = time.time()
        while time.time() < start_time + 120:
            desc = table.meta.client.describe_table(TableName=table.name)['Table']
            if 'LatestStreamArn' in desc:
                arn = desc['LatestStreamArn']
                desc = dynamodbstreams.describe_stream(StreamArn=arn)
                if desc['StreamDescription']['StreamStatus'] == 'ENABLED':
                    stream_enabled = True
                    break
            time.sleep(10)
        assert stream_enabled

        # Write a single expiring item. Set its expiration one minute in the
        # past, so the item should expire ASAP.
        p = random_string()
        c = random_string()
        expiration = int(time.time()) - 60
        table.put_item(Item={'p': p, 'c': c, 'animal': 'dog', 'expiration': expiration})

        # Wait (up to max_duration) for the item to expire, and for the
        # expiration event to reach the stream:
        start_time = time.time()
        event_found = False
        while time.time() < start_time + max_duration:
            print(f"--- {int(time.time()-start_time)} seconds")
            item_expired = not 'Item' in table.get_item(Key={'p': p, 'c': c})
            for record in read_entire_stream(dynamodbstreams, table):
                # An expired item has a specific userIdentity as follows:
                if record.get('userIdentity') == { 'Type': 'Service', 'PrincipalId': 'dynamodb.amazonaws.com' }:
                    # The expired item should be a REMOVE, and because we
                    # asked for old images and both the key and the full
                    # content.
                    assert record['eventName'] == 'REMOVE'
                    assert record['dynamodb']['Keys'] == {'p': {'S': p}, 'c': {'S': c}}
                    assert record['dynamodb']['OldImage'] == {'p': {'S': p}, 'c': {'S': c}, 'animal': {'S': 'dog'}, 'expiration': {'N': str(expiration)} }
                    event_found = True
            print(f"item expired {item_expired} event {event_found}")
            if item_expired and event_found:
                return
            time.sleep(60)
        pytest.fail('item did not expire or event did not reach stream')

# Utility function for reading the entire contents of a table's DynamoDB
# Streams. This function is only useful when we expect only a handful of
# items, and performance is not important, because nothing is cached between
# calls. So it's only used in "veryslow"-marked tests above.
def read_entire_stream(dynamodbstreams, table):
    # Look for the latest stream. If there is none, return nothing
    desc = table.meta.client.describe_table(TableName=table.name)['Table']
    if not 'LatestStreamArn' in desc:
        return []
    arn = desc['LatestStreamArn']
    # List all shards of the stream in an array "shards":
    response = dynamodbstreams.describe_stream(StreamArn=arn)['StreamDescription']
    shards = [x['ShardId'] for x in response['Shards']]
    while 'LastEvaluatedShardId' in response:
        response = dynamodbstreams.describe_stream(StreamArn=arn,
            ExclusiveStartShardId=response['LastEvaluatedShardId'])['StreamDescription']
        shards.extend([x['ShardId'] for x in response['Shards']])
    records = []
    for shard_id in shards:
        # Get an interator for everything (TRIM_HORIZON) in the shard
        iter = dynamodbstreams.get_shard_iterator(StreamArn=arn,
            ShardId=shard_id, ShardIteratorType='TRIM_HORIZON')['ShardIterator']
        while iter != None:
            response = dynamodbstreams.get_records(ShardIterator=iter)
            # DynamoDB will continue returning records until reaching the
            # current end, and only then we will get an empty response.
            if len(response['Records']) == 0:
                break
            records.extend(response['Records'])
            iter = response.get('NextShardIterator')
    return records
