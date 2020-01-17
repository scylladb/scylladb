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
from util import multiset

def delete_tags(table, arn):
    got = table.meta.client.list_tags_of_resource(ResourceArn=arn)
    print(got['Tags'])
    if len(got['Tags']):
        table.meta.client.untag_resource(ResourceArn=arn, TagKeys=[tag['Key'] for tag in got['Tags']])

# Test checking that tagging and untagging is correctly handled
@pytest.mark.xfail(reason="DescribeTable does not return ARN")
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

# Test checking that incorrect parameters return proper error codes
@pytest.mark.xfail(reason="DescribeTable does not return ARN")
def test_tag_resource_incorrect(test_table):
    got = test_table.meta.client.describe_table(TableName=test_table.name)['Table']
    arn =  got['TableArn']
    # Note: Tags must have two entries in the map: Key and Value, and their values
    # must be at least 1 character long, but these are validated on boto3 level
    with pytest.raises(ClientError, match='AccessDeniedException'):
        test_table.meta.client.tag_resource(ResourceArn='I_do_not_exist', Tags=[{'Key': '7', 'Value': '8'}])
    with pytest.raises(ClientError, match='ValidationException'):
        test_table.meta.client.tag_resource(ResourceArn=arn, Tags=[])
