# -*- coding: utf-8 -*-
# Copyright 2019 ScyllaDB
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

# Tests for Tagging:
# 1. TagResource - tagging a table with a (key, value) pair
# 2. UntagResource
# 3. ListTagsOfResource

import pytest
from botocore.exceptions import ClientError
import re
import time
from util import multiset, create_test_table

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
@pytest.fixture(scope="session")
def test_table_tags(dynamodb):
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
