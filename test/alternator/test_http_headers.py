# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# Tests for HTTP response header configuration in Alternator.
# Tests the ability to suppress the Server, Date, and Content-Type headers,
# and to customize the Server header value, via configuration options that
# can be updated at runtime.
#
# The default-behavior assertions in each test (headers present with correct
# values) run against both Scylla and Amazon DynamoDB. The configuration-
# manipulation parts are Scylla-only and are skipped when run with --aws.

import time
import pytest
from botocore.exceptions import ClientError
from test.alternator.util import scylla_config_temporary, is_aws

# Test that the Server header value is controlled by alternator_http_response_server_header:
# by default the header is absent (empty config value), a non-empty value
# enables it, and a whitespace-only string suppresses it again.
def test_server_header(dynamodb, test_table_s):
    # DynamoDB sends a Server header; Scylla's default is to omit it.
    response = test_table_s.get_item(Key={'p': 'test_key'})
    headers = response['ResponseMetadata']['HTTPHeaders']
    assert ('server' in headers) == is_aws(dynamodb)
    with pytest.raises(ClientError) as exc_info:
        dynamodb.meta.client.describe_table(TableName='nonexistent_table_xxxxx')
    assert ('server' in exc_info.value.response['ResponseMetadata']['HTTPHeaders']) == is_aws(dynamodb)
    if is_aws(dynamodb):
        return

    # Setting a non-empty value enables the header with that value on both
    # success and error paths. Nested inside, a whitespace-only string
    # suppresses the header again (empty strings can't be stored in the
    # config table as they are key attributes).
    with scylla_config_temporary(dynamodb, 'alternator_http_response_server_header', 'MyCustomServer/1.0'):
        response = test_table_s.get_item(Key={'p': 'test_key'})
        headers = response['ResponseMetadata']['HTTPHeaders']
        assert headers['server'] == 'MyCustomServer/1.0'
        with pytest.raises(ClientError) as exc_info:
            dynamodb.meta.client.describe_table(TableName='nonexistent_table_xxxxx')
        assert exc_info.value.response['ResponseMetadata']['HTTPHeaders']['server'] == 'MyCustomServer/1.0'
        with scylla_config_temporary(dynamodb, 'alternator_http_response_server_header', ' '):
            response = test_table_s.get_item(Key={'p': 'test_key'})
            headers = response['ResponseMetadata']['HTTPHeaders']
            assert 'server' not in headers
            with pytest.raises(ClientError) as exc_info:
                dynamodb.meta.client.describe_table(TableName='nonexistent_table_xxxxx')
            assert 'server' not in exc_info.value.response['ResponseMetadata']['HTTPHeaders']
        # sanitize_header_value strips control characters from the value.
        # Here an embedded SOH (\x01) between "Server" and "Name" is removed,
        # leaving the header set to "ServerName".
        with scylla_config_temporary(dynamodb, 'alternator_http_response_server_header', 'Server\x01Name'):
            response = test_table_s.get_item(Key={'p': 'test_key'})
            headers = response['ResponseMetadata']['HTTPHeaders']
            assert headers['server'] == 'ServerName'
        # A value consisting entirely of control characters becomes empty
        # after sanitization, which suppresses the header.
        with scylla_config_temporary(dynamodb, 'alternator_http_response_server_header', '\x01\x02\x03'):
            response = test_table_s.get_item(Key={'p': 'test_key'})
            headers = response['ResponseMetadata']['HTTPHeaders']
            assert 'server' not in headers

# Test that the Date header is present by default and can be suppressed via
# the alternator_http_response_disable_date_header live-update option. After
# re-enabling explicitly (setting to 'false'), the header is present immediately.
def test_config_date_header(dynamodb, test_table_s):
    # By default the Date header should be present on both success and error paths.
    response = test_table_s.get_item(Key={'p': 'test_key'})
    headers = response['ResponseMetadata']['HTTPHeaders']
    assert 'date' in headers
    with pytest.raises(ClientError) as exc_info:
        dynamodb.meta.client.describe_table(TableName='nonexistent_table_xxxxx')
    assert 'date' in exc_info.value.response['ResponseMetadata']['HTTPHeaders']

    if is_aws(dynamodb):
        return

    # With the option set to true the Date header should be absent on both paths.
    # Nested inside the suppression block, re-enable explicitly and verify the
    # header is present again immediately -- no waiting required.
    with scylla_config_temporary(dynamodb, 'alternator_http_response_disable_date_header', 'true'):
        response = test_table_s.get_item(Key={'p': 'test_key'})
        headers = response['ResponseMetadata']['HTTPHeaders']
        assert 'date' not in headers
        with pytest.raises(ClientError) as exc_info:
            dynamodb.meta.client.describe_table(TableName='nonexistent_table_xxxxx')
        assert 'date' not in exc_info.value.response['ResponseMetadata']['HTTPHeaders']
        with scylla_config_temporary(dynamodb, 'alternator_http_response_disable_date_header', 'false'):
            response = test_table_s.get_item(Key={'p': 'test_key'})
            headers = response['ResponseMetadata']['HTTPHeaders']
            assert 'date' in headers
            with pytest.raises(ClientError) as exc_info:
                dynamodb.meta.client.describe_table(TableName='nonexistent_table_xxxxx')
            assert 'date' in exc_info.value.response['ResponseMetadata']['HTTPHeaders']

# Verifies that the Date header value actually changes over time under normal
# operation, and that the timer continues to run after a suppress-then-re-enable
# cycle. Uses polling rather than fixed sleeps.
@pytest.mark.veryslow
def test_date_header_updates(dynamodb, test_table_s):
    def poll_for_date_change(deadline_seconds):
        first_date = None
        deadline = time.monotonic() + deadline_seconds
        while time.monotonic() < deadline:
            r = test_table_s.get_item(Key={'p': 'test_key'})
            d = r['ResponseMetadata']['HTTPHeaders'].get('date')
            assert d is not None, "Date header missing"
            if first_date is None:
                first_date = d
            elif d != first_date:
                return True
            time.sleep(0.1)
        return False

    # By default the Date header should be present and change over time
    # (Seastar's date header timer fires every ~1 s).
    assert poll_for_date_change(5), "Date header did not change -- timer may not be updating"

    if is_aws(dynamodb):
        return

    # Suppress, then re-enable explicitly. Inside the re-enable block the timer
    # must be live again: confirm the date value changes within a deadline.
    with scylla_config_temporary(dynamodb, 'alternator_http_response_disable_date_header', 'true'):
        response = test_table_s.get_item(Key={'p': 'test_key'})
        assert 'date' not in response['ResponseMetadata']['HTTPHeaders']
        with scylla_config_temporary(dynamodb, 'alternator_http_response_disable_date_header', 'false'):
            assert poll_for_date_change(5), "Date header did not change after re-enable -- timer may not have been re-armed"

# Test that the Content-Type header is present by default and can be suppressed
# via the alternator_http_response_disable_content_type_header live-update option.
# Also verifies the error code path (generate_error_reply) which sets
# Content-Type independently from the success path (write_body).
def test_config_content_type_header(dynamodb, test_table_s):
    # By default the Content-Type header should be present on both paths.
    response = test_table_s.get_item(Key={'p': 'test_key'})
    headers = response['ResponseMetadata']['HTTPHeaders']
    assert 'content-type' in headers
    with pytest.raises(ClientError) as exc_info:
        dynamodb.meta.client.describe_table(TableName='nonexistent_table_xxxxx')
    assert 'content-type' in exc_info.value.response['ResponseMetadata']['HTTPHeaders']

    if is_aws(dynamodb):
        return

    # Setting the option to true suppresses the header on both success and error
    # paths. Nested inside the suppression block, re-enable explicitly and verify
    # the header is present again on both paths.
    with scylla_config_temporary(dynamodb, 'alternator_http_response_disable_content_type_header', 'true'):
        response = test_table_s.get_item(Key={'p': 'test_key'})
        headers = response['ResponseMetadata']['HTTPHeaders']
        assert 'content-type' not in headers
        with pytest.raises(ClientError) as exc_info:
            dynamodb.meta.client.describe_table(TableName='nonexistent_table_xxxxx')
        assert 'content-type' not in exc_info.value.response['ResponseMetadata']['HTTPHeaders']
        with scylla_config_temporary(dynamodb, 'alternator_http_response_disable_content_type_header', 'false'):
            response = test_table_s.get_item(Key={'p': 'test_key'})
            headers = response['ResponseMetadata']['HTTPHeaders']
            assert 'content-type' in headers
            with pytest.raises(ClientError) as exc_info:
                dynamodb.meta.client.describe_table(TableName='nonexistent_table_xxxxx')
            assert 'content-type' in exc_info.value.response['ResponseMetadata']['HTTPHeaders']
