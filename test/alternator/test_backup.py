# Copyright 2022 ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

# Tests for the different types of backup and restore features in
# DynamoDB.
# Because we didn't implementing this feature yet (see issue #5063), this
# test suite is very partial.

import pytest
from botocore.exceptions import ClientError

# Test the DescribeContinuousBackups operation on a table where the
# continuous backup feature was *not* enabled.
#
# Oddly, as of May 2022, this can fail on AWS DynamoDB: Right after a
# table is created, it has continuous backups disabled - but a few seconds
# later, it suddenly reports that they are enabled! I don't know how to
# explain this, but I believe this is not a behavior we should reproduce.
def test_describe_continuous_backups_without_continuous_backups(test_table):
    response = test_table.meta.client.describe_continuous_backups(TableName=test_table.name)
    print(response)
    assert 'ContinuousBackupsDescription' in response
    assert 'ContinuousBackupsStatus' in response['ContinuousBackupsDescription']
    assert response['ContinuousBackupsDescription']['ContinuousBackupsStatus'] == 'DISABLED'
    assert 'PointInTimeRecoveryDescription' in response['ContinuousBackupsDescription']
    assert response['ContinuousBackupsDescription']['PointInTimeRecoveryDescription'] == {'PointInTimeRecoveryStatus': 'DISABLED'}

# Test the DescribeContinuousBackups operation on a table that doesn't
# exist. It should report a TableNotFoundException - not that continuous
# backups are disabled.
def test_describe_continuous_backups_nonexistent(test_table):
    with pytest.raises(ClientError, match='TableNotFoundException'):
        test_table.meta.client.describe_continuous_backups(TableName=test_table.name+'nonexistent')

# Test the DescribeContinuousBackups operation without a table name.
# It should fail with ValidationException.
def test_describe_continuous_backups_missing(test_table):
    with pytest.raises(ClientError, match='ValidationException'):
        test_table.meta.client.describe_continuous_backups()
