# -*- coding: utf-8 -*-
# Copyright 2019-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

# Tests for Tagging:
# 1. TagResource - tagging a table with a (key, value) pair
# 2. UntagResource
# 3. ListTagsOfResource

import threading

import pytest
from botocore.exceptions import ClientError
from packaging.version import Version

from test.alternator.util import multiset, create_test_table, unique_table_name, random_string


def delete_tags(table, arn):
    got = table.meta.client.list_tags_of_resource(ResourceArn=arn)
    print(got['Tags'])
    if len(got['Tags']):
        table.meta.client.untag_resource(ResourceArn=arn, TagKeys=[tag['Key'] for tag in got['Tags']])

# Test checking that tagging and untagging is correctly handled
def test_tag_resource_basic(test_table):
    got = test_table.meta.client.describe_table(TableName=test_table.name)['Table']
    arn =  got['TableArn']
    tags = [
        {
            'Key': 'string',
            'Value': 'string'
        },
        {
            'Key': 'string2',
            'Value': 'string4'
        },
        {
            'Key': '7',
            'Value': ' '
        },
        {
            'Key': ' ',
            'Value': '9'
        },
    ]

    delete_tags(test_table, arn)
    got = test_table.meta.client.list_tags_of_resource(ResourceArn=arn)
    assert len(got['Tags']) == 0
    test_table.meta.client.tag_resource(ResourceArn=arn, Tags=tags)
    got = test_table.meta.client.list_tags_of_resource(ResourceArn=arn)
    assert 'Tags' in got
    assert multiset(got['Tags']) == multiset(tags)

    # Removing non-existent tags is legal
    test_table.meta.client.untag_resource(ResourceArn=arn, TagKeys=['string2', 'non-nexistent', 'zzz2'])
    tags.remove({'Key': 'string2', 'Value': 'string4'})
    got = test_table.meta.client.list_tags_of_resource(ResourceArn=arn)
    assert 'Tags' in got
    assert multiset(got['Tags']) == multiset(tags)

    delete_tags(test_table, arn)
    got = test_table.meta.client.list_tags_of_resource(ResourceArn=arn)
    assert len(got['Tags']) == 0

def test_tag_resource_overwrite(test_table):
    got = test_table.meta.client.describe_table(TableName=test_table.name)['Table']
    arn =  got['TableArn']
    tags = [
        {
            'Key': 'string',
            'Value': 'string'
        },
    ]
    delete_tags(test_table, arn)
    test_table.meta.client.tag_resource(ResourceArn=arn, Tags=tags)
    got = test_table.meta.client.list_tags_of_resource(ResourceArn=arn)
    assert 'Tags' in got
    assert multiset(got['Tags']) == multiset(tags)
    tags = [
        {
            'Key': 'string',
            'Value': 'different_string_value'
        },
    ]
    test_table.meta.client.tag_resource(ResourceArn=arn, Tags=tags)
    got = test_table.meta.client.list_tags_of_resource(ResourceArn=arn)
    assert 'Tags' in got
    assert multiset(got['Tags']) == multiset(tags)

PREDEFINED_TAGS = [{'Key': 'str1', 'Value': 'str2'}, {'Key': 'kkk', 'Value': 'vv'}, {'Key': 'keykey', 'Value': 'valvalvalval'}]
@pytest.fixture(scope="module")
def test_table_tags(dynamodb):
    # The feature of creating a table already with tags was only added to
    # DynamoDB in April 2019, and to the botocore library in version 1.12.136
    # https://aws.amazon.com/about-aws/whats-new/2019/04/now-you-can-tag-amazon-dynamodb-tables-when-you-create-them/
    # so older versions of the library cannot run this test.
    import botocore
    if (Version(botocore.__version__) < Version('1.12.136')):
        pytest.skip("Botocore version 1.12.136 or above required to run this test")

    table = create_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' }, { 'AttributeName': 'c', 'KeyType': 'RANGE' } ],
        AttributeDefinitions=[ { 'AttributeName': 'p', 'AttributeType': 'S' }, { 'AttributeName': 'c', 'AttributeType': 'N' } ],
        Tags=PREDEFINED_TAGS)
    yield table
    table.delete()

# Test checking that tagging works during table creation
def test_list_tags_from_creation(test_table_tags):
    got = test_table_tags.meta.client.describe_table(TableName=test_table_tags.name)['Table']
    arn =  got['TableArn']
    got = test_table_tags.meta.client.list_tags_of_resource(ResourceArn=arn)
    assert multiset(got['Tags']) == multiset(PREDEFINED_TAGS)

# Test checking that incorrect parameters return proper error codes
def test_tag_resource_incorrect(test_table):
    got = test_table.meta.client.describe_table(TableName=test_table.name)['Table']
    arn =  got['TableArn']
    # Note: Tags must have two entries in the map: Key and Value, and their values
    # must be at least 1 character long, but these are validated on boto3 level
    with pytest.raises(ClientError, match='AccessDeniedException'):
        test_table.meta.client.tag_resource(ResourceArn='I_do_not_exist', Tags=[{'Key': '7', 'Value': '8'}])
    with pytest.raises(ClientError, match='ValidationException'):
        test_table.meta.client.tag_resource(ResourceArn=arn, Tags=[])
    test_table.meta.client.tag_resource(ResourceArn=arn, Tags=[{'Key': str(i), 'Value': str(i)} for i in range(30)])
    test_table.meta.client.tag_resource(ResourceArn=arn, Tags=[{'Key': str(i), 'Value': str(i)} for i in range(20, 40)])
    with pytest.raises(ClientError, match='ValidationException'):
        test_table.meta.client.tag_resource(ResourceArn=arn, Tags=[{'Key': str(i), 'Value': str(i)} for i in range(40, 60)])
    for incorrect_arn in ['arn:not/a/good/format', 'x'*125, 'arn:'+'scylla/'*15, ':/'*30, ' ', 'незаконные буквы']:
        with pytest.raises(ClientError, match='.*Exception'):
            test_table.meta.client.tag_resource(ResourceArn=incorrect_arn, Tags=[{'Key':'x', 'Value':'y'}])
    for incorrect_tag in [('ok', '#!%%^$$&'), ('->>;-)])', 'ok'), ('!!!\\|','<><')]:
        with pytest.raises(ClientError, match='ValidationException'):
            test_table.meta.client.tag_resource(ResourceArn=arn, Tags=[{'Key':incorrect_tag[0],'Value':incorrect_tag[1]}])

# Test that only specific values are allowed for write isolation (system:write_isolation tag)
def test_tag_resource_write_isolation_values(scylla_only, test_table):
    got = test_table.meta.client.describe_table(TableName=test_table.name)['Table']
    arn =  got['TableArn']
    for i in ['f', 'forbid', 'forbid_rmw', 'a', 'always', 'always_use_lwt', 'o', 'only_rmw_uses_lwt', 'u', 'unsafe', 'unsafe_rmw']:
        test_table.meta.client.tag_resource(ResourceArn=arn, Tags=[{'Key':'system:write_isolation', 'Value':i}])
    with pytest.raises(ClientError, match='ValidationException'):
        test_table.meta.client.tag_resource(ResourceArn=arn, Tags=[{'Key':'system:write_isolation', 'Value':'bah'}])

# Test that if trying to create a table with forbidden tags (in this test,
# a list of tags longer than the maximum allowed of 50 tags), the table
# is not created at all.
def test_too_long_tags_from_creation(dynamodb):
    # The feature of creating a table already with tags was only added to
    # DynamoDB in April 2019, and to the botocore library in version 1.12.136
    # so older versions of the library cannot run this test.
    import botocore
    if (Version(botocore.__version__) < Version('1.12.136')):
        pytest.skip("Botocore version 1.12.136 or above required to run this test")
    name = unique_table_name()
    # Setting 100 tags is not allowed, the following table creation should fail:
    with pytest.raises(ClientError, match='ValidationException'):
        dynamodb.create_table(TableName=name,
            BillingMode='PAY_PER_REQUEST',
            KeySchema=[{ 'AttributeName': 'p', 'KeyType': 'HASH' }],
            AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }],
            Tags=[{'Key': str(i), 'Value': str(i)} for i in range(100)])
    # After the table creation failed, the table should not exist.
    with pytest.raises(ClientError, match='ResourceNotFoundException'):
        dynamodb.meta.client.describe_table(TableName=name)

# This test is similar to the above, but uses another case of forbidden tags -
# here an illegal value for the system::write_isolation tag. This is a
# scylla_only test because only Alternator checks the validity of the
# system::write_isolation tag.
# Reproduces issue #6809, where the table creation appeared to fail, but it
# was actually created (without the tag).
def test_forbidden_tags_from_creation(scylla_only, dynamodb):
    # The feature of creating a table already with tags was only added to
    # DynamoDB in April 2019, and to the botocore library in version 1.12.136
    # so older versions of the library cannot run this test.
    import botocore
    if (Version(botocore.__version__) < Version('1.12.136')):
        pytest.skip("Botocore version 1.12.136 or above required to run this test")
    name = unique_table_name()
    # It is not allowed to set the system:write_isolation to "dog", so the
    # following table creation should fail:
    with pytest.raises(ClientError, match='ValidationException'):
        dynamodb.create_table(TableName=name,
            BillingMode='PAY_PER_REQUEST',
            KeySchema=[{ 'AttributeName': 'p', 'KeyType': 'HASH' }],
            AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }],
            Tags=[{'Key': 'system:write_isolation', 'Value': 'dog'}])
    # After the table creation failed, the table should not exist.
    with pytest.raises(ClientError, match='ResourceNotFoundException'):
        dynamodb.meta.client.describe_table(TableName=name)

# Test checking that unicode tags are allowed
@pytest.mark.xfail(reason="unicode tags not yet supported")
def test_tag_resource_unicode(test_table):
    got = test_table.meta.client.describe_table(TableName=test_table.name)['Table']
    arn =  got['TableArn']
    tags = [
        {
            'Key': 'законные буквы',
            'Value': 'string'
        },
        {
            'Key': 'ѮѮ Ѯ',
            'Value': 'string4'
        },
        {
            'Key': 'ѮѮ',
            'Value': 'ѮѮѮѮѮѮѮѮѮѮѮѮѮѮ'
        },
        {
            'Key': 'keyѮѮѮ',
            'Value': 'ѮѮѮvalue'
        },
    ]

    delete_tags(test_table, arn)
    got = test_table.meta.client.list_tags_of_resource(ResourceArn=arn)
    assert len(got['Tags']) == 0
    test_table.meta.client.tag_resource(ResourceArn=arn, Tags=tags)
    got = test_table.meta.client.list_tags_of_resource(ResourceArn=arn)
    assert 'Tags' in got
    assert multiset(got['Tags']) == multiset(tags)

# Test that the Tags option of TagResource is required
def test_tag_resource_missing_tags(test_table):
    client = test_table.meta.client
    arn = client.describe_table(TableName=test_table.name)['Table']['TableArn']
    with pytest.raises(ClientError, match='ValidationException'):
        client.tag_resource(ResourceArn=arn)

# A simple table with both gsi and lsi (which happen to be the same), which
# we'll use for testing tagging of GSIs and LSIs
# Use a function-scoped fixture to get a fresh untagged table in each test.
@pytest.fixture(scope="function")
def table_lsi_gsi(dynamodb):
    table = create_test_table(dynamodb,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' },
                    { 'AttributeName': 'c', 'KeyType': 'RANGE' } ],
        AttributeDefinitions=[
                    { 'AttributeName': 'p', 'AttributeType': 'S' },
                    { 'AttributeName': 'c', 'AttributeType': 'S' },
                    { 'AttributeName': 'x1', 'AttributeType': 'S' },
        ],
        GlobalSecondaryIndexes=[
            {   'IndexName': 'gsi',
                'KeySchema': [
                    { 'AttributeName': 'p', 'KeyType': 'HASH' },
                    { 'AttributeName': 'x1', 'KeyType': 'RANGE' }
                ],
                'Projection': { 'ProjectionType': 'KEYS_ONLY' }
            }
        ],
        LocalSecondaryIndexes=[
            {   'IndexName': 'lsi',
                'KeySchema': [
                    { 'AttributeName': 'p', 'KeyType': 'HASH' },
                    { 'AttributeName': 'x1', 'KeyType': 'RANGE' }
                ],
                'Projection': { 'ProjectionType': 'KEYS_ONLY' }
            }
        ])
    yield table
    table.delete()

# Although GSIs and LSIs have their own ARN (listed by DescribeTable), it
# turns out that they cannot be used to retrieve or set tags on the GSI or
# LSI. If this is attempted, DynamoDB complains that the given ARN is not
# a *table* ARN:
# "An error occurred (ValidationException) when calling the ListTagsOfResource
# operation: Invalid TableArn: Invalid ResourceArn provided as input
# arn:aws:dynamodb:us-east-1:797456418907:table/alternator_Test_1655117822792/index/gsi"
#
# See issue #10786 discussing maybe we want in Alternator not to follow
# DynamoDB here, and to *allow* tagging GSIs and LSIs separately. But until
# then, this test verifies that we don't allow it - just like DynamoDB.
def test_tag_lsi_gsi(table_lsi_gsi):
    table_desc = table_lsi_gsi.meta.client.describe_table(TableName=table_lsi_gsi.name)['Table']
    table_arn =  table_desc['TableArn']
    gsi_arn =  table_desc['GlobalSecondaryIndexes'][0]['IndexArn']
    lsi_arn =  table_desc['LocalSecondaryIndexes'][0]['IndexArn']
    assert [] == table_lsi_gsi.meta.client.list_tags_of_resource(ResourceArn=table_arn)['Tags']
    with pytest.raises(ClientError, match='ValidationException.*ResourceArn'):
        assert [] == table_lsi_gsi.meta.client.list_tags_of_resource(ResourceArn=gsi_arn)['Tags']
    with pytest.raises(ClientError, match='ValidationException.*ResourceArn'):
        assert [] == table_lsi_gsi.meta.client.list_tags_of_resource(ResourceArn=lsi_arn)['Tags']
    tags = [ { 'Key': 'hi', 'Value': 'hello' } ]
    table_lsi_gsi.meta.client.tag_resource(ResourceArn=table_arn, Tags=tags)
    with pytest.raises(ClientError, match='ValidationException.*ResourceArn'):
        table_lsi_gsi.meta.client.tag_resource(ResourceArn=gsi_arn, Tags=tags)
    with pytest.raises(ClientError, match='ValidationException.*ResourceArn'):
        table_lsi_gsi.meta.client.tag_resource(ResourceArn=lsi_arn, Tags=tags)

# Test that if we concurrently add tags A and B to a table, both survive.
# If the process of adding tag A involved reading the current tags, adding
# A and then over-writing the tags back, if we did this for A and B
# concurrently the risk is that both would read the state before both changes.
# To solve this, Scylla needs to serialize tag modification. This test
# is designed to fail if this serialization is missing.  Reproduces #6389
@pytest.mark.veryslow
def test_concurrent_tag(dynamodb, test_table):
    client = test_table.meta.client
    arn = client.describe_table(TableName=test_table.name)['Table']['TableArn']
    # Unfortunately by default Python threads print their exceptions
    # (e.g., assertion failures) but don't propagate them to the join(),
    # so the overall test doesn't fail. The following Thread wrapper
    # causes join() to rethrow the exception, so the test will fail.
    class ThreadWrapper(threading.Thread):
        def run(self):
            try:
                self.ret = self._target(*self._args, **self._kwargs)
            except BaseException as e:
                self.exception = e
        def join(self, timeout=None):
            super().join(timeout)
            if hasattr(self, 'exception'):
                raise self.exception
            return self.ret

    def tag_untag_once(tag):
        client.tag_resource(ResourceArn=arn, Tags=[{'Key': tag, 'Value': 'Hello'}])
        # Check that the tag that we just added is still on the table (and
        # wasn't overwritten by a concurrent addition of a different tag):
        got = test_table.meta.client.list_tags_of_resource(ResourceArn=arn)['Tags']
        assert [x['Value'] for x in got if x['Key']==tag] == ['Hello']
        client.untag_resource(ResourceArn=arn, TagKeys=[tag])
        got = test_table.meta.client.list_tags_of_resource(ResourceArn=arn)['Tags']
        assert [x['Value'] for x in got if x['Key']==tag] == []
    def tag_loop(tag, count):
        for i in range(count):
            tag_untag_once(tag)
    # The more iterations we do, the higher the chance of reproducing
    # this issue. On my laptop, count = 100 reproduces the bug every time.
    # Lower numbers have some chance of not catching the bug. If this
    # issue starts to xpass, we may need to increase the count.
    count = 200
    t1 = ThreadWrapper(target=lambda: tag_loop('A', count))
    t2 = ThreadWrapper(target=lambda: tag_loop('B', count))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

# An empty string is allowed as a tag's value
# Reproduces #16904.
def test_empty_tag_value(dynamodb, test_table):
    client = dynamodb.meta.client
    arn = client.describe_table(TableName=test_table.name)['Table']['TableArn']
    tag = random_string()
    client.tag_resource(ResourceArn=arn, Tags=[{'Key': tag, 'Value': ''}])
    # Verify that the tag with the empty value was correctly saved:
    tags = client.list_tags_of_resource(ResourceArn=arn)['Tags']
    assert {'Key': tag, 'Value': ''} in tags
    # Clean up the tag we just added
    client.untag_resource(ResourceArn=arn, TagKeys=[tag])

# However, an empty string is NOT allowed as a tag's key
def test_empty_tag_key(dynamodb, test_table):
    client = dynamodb.meta.client
    arn = client.describe_table(TableName=test_table.name)['Table']['TableArn']
    with pytest.raises(ClientError, match='ValidationException'):
        client.tag_resource(ResourceArn=arn, Tags=[{'Key': '', 'Value': 'dog'}])

# Although an empty Value is allowed for a tag, a *missing* Value is not
# allowed:
def test_missing_tag_value(dynamodb, test_table):
    client = dynamodb.meta.client
    arn = client.describe_table(TableName=test_table.name)['Table']['TableArn']
    with pytest.raises(ClientError, match='ValidationException'):
        client.tag_resource(ResourceArn=arn, Tags=[{'Key': 'dog'}])

# According to the DynamoDB documentation, the maximum tag key length allowed
# is 128 characters. Actually, it's 128 *unicode* characters which are
# allowed, not 128 bytes.
# Reproduces #16908
@pytest.mark.parametrize("is_ascii", [
        True,
        pytest.param(False, marks=pytest.mark.xfail(reason="#16908"))])
def test_tag_key_length_128_allowed(dynamodb, test_table, is_ascii):
    client = dynamodb.meta.client
    arn = client.describe_table(TableName=test_table.name)['Table']['TableArn']
    tag = ('x' if is_ascii else 'א') * 128
    client.tag_resource(ResourceArn=arn, Tags=[{'Key': tag, 'Value': 'dog'}])
    tags = client.list_tags_of_resource(ResourceArn=arn)['Tags']
    assert {'Key': tag, 'Value': 'dog'} in tags
    client.untag_resource(ResourceArn=arn, TagKeys=[tag])

def test_tag_key_length_129_forbidden(dynamodb, test_table):
    client = dynamodb.meta.client
    arn = client.describe_table(TableName=test_table.name)['Table']['TableArn']
    tag = 'x'*129
    with pytest.raises(ClientError, match='ValidationException'):
        client.tag_resource(ResourceArn=arn, Tags=[{'Key': tag, 'Value': 'dog'}])

# According to the DynamoDB documentation, the maximum tag value length
# allowed is 256 characters. Actually, it's 256 *unicode* characters which
# are allowed, not 256 bytes.
# Reproduces #16908
@pytest.mark.parametrize("is_ascii", [
        True,
        pytest.param(False, marks=pytest.mark.xfail(reason="#16908"))])
def test_tag_value_length_256_allowed(dynamodb, test_table, is_ascii):
    client = dynamodb.meta.client
    arn = client.describe_table(TableName=test_table.name)['Table']['TableArn']
    tag = random_string()
    value = ('x' if is_ascii else 'א') * 256
    client.tag_resource(ResourceArn=arn, Tags=[{'Key': tag, 'Value': value}])
    tags = client.list_tags_of_resource(ResourceArn=arn)['Tags']
    assert {'Key': tag, 'Value': value} in tags
    client.untag_resource(ResourceArn=arn, TagKeys=[tag])

def test_tag_value_length_257_forbidden(dynamodb, test_table):
    client = dynamodb.meta.client
    arn = client.describe_table(TableName=test_table.name)['Table']['TableArn']
    value = 'x'*257
    with pytest.raises(ClientError, match='ValidationException'):
        client.tag_resource(ResourceArn=arn, Tags=[{'Key': 'dog', 'Value': value}])

# According to the DynamoDB documentation, only letters, whitespace, numbers,
# and the characters [+-=._:/] are allowed in both tag keys or values.
# Let's check that other non-letter characters are not allowed in either
# key or value.
def test_tag_forbidden_chars(dynamodb, test_table):
    client = dynamodb.meta.client
    arn = client.describe_table(TableName=test_table.name)['Table']['TableArn']
    for x in ['hi!', 'd%g', '"hello"']:
        with pytest.raises(ClientError, match='ValidationException'):
            client.tag_resource(ResourceArn=arn, Tags=[{'Key': x, 'Value': 'dog'}])
        with pytest.raises(ClientError, match='ValidationException'):
            client.tag_resource(ResourceArn=arn, Tags=[{'Key': 'dog', 'Value': x}])

# Check that's it's allowed to reassign a new value to an existing tag.
def test_tag_reassign(dynamodb, test_table):
    client = dynamodb.meta.client
    arn = client.describe_table(TableName=test_table.name)['Table']['TableArn']
    tag = random_string()
    value1 = random_string()
    value2 = random_string()
    client.tag_resource(ResourceArn=arn, Tags=[{'Key': tag, 'Value': value1}])
    tags = client.list_tags_of_resource(ResourceArn=arn)['Tags']
    assert {'Key': tag, 'Value': value1} in tags
    client.tag_resource(ResourceArn=arn, Tags=[{'Key': tag, 'Value': value2}])
    tags = client.list_tags_of_resource(ResourceArn=arn)['Tags']
    assert {'Key': tag, 'Value': value2} in tags
    client.untag_resource(ResourceArn=arn, TagKeys=[tag])
