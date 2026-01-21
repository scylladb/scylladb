# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

# Tests for HTTP response header configuration in Alternator.
# Tests the ability to customize or skip Server, Date, and Content-Type headers
# via configuration options that can be updated at runtime.

from test.alternator.util import scylla_config_temporary

# Tests that the Server header can be dynamically skipped via runtime configuration.
def test_skip_server_header_live_update(dynamodb, test_table_s):
    # Initially headers should be present
    response = test_table_s.get_item(Key={'p': 'test_key'})
    headers = response['ResponseMetadata']['HTTPHeaders']
    assert 'server' in headers

    # Update config to skip Server header
    with scylla_config_temporary(dynamodb, 'alternator_response_skip_header__server', 'true'):
        # Server header should now be absent
        response = test_table_s.get_item(Key={'p': 'test_key'})
        headers = response['ResponseMetadata']['HTTPHeaders']
        assert 'server' not in headers

# Tests that the Server header can be customized with a user-defined value via runtime configuration.
# It also verifies that the skip setting takes precedence (custom value is ignored).
def test_custom_server_header_live_update(dynamodb, test_table_s):
    # Default Server value
    response = test_table_s.get_item(Key={'p': 'test_key'})
    headers = response['ResponseMetadata']['HTTPHeaders']
    assert headers['server'] == 'Server'
    
    # Update to custom value
    with scylla_config_temporary(dynamodb, 'alternator_response_custom_header__server', 'MyCustomServer/1.0'):
        # Should see custom value
        response = test_table_s.get_item(Key={'p': 'test_key'})
        headers = response['ResponseMetadata']['HTTPHeaders']
        assert headers['server'] == 'MyCustomServer/1.0'

        # Now skip it - custom value should be ignored
        with scylla_config_temporary(dynamodb, 'alternator_response_skip_header__server', 'true'):
            response = test_table_s.get_item(Key={'p': 'test_key'})
            headers = response['ResponseMetadata']['HTTPHeaders']
            assert 'server' not in headers

# Tests that the Date header can be dynamically skipped via runtime configuration.
def test_skip_date_header_live_update(dynamodb, test_table_s):
    # Initially Date header should be present
    response = test_table_s.get_item(Key={'p': 'test_key'})
    headers = response['ResponseMetadata']['HTTPHeaders']
    assert 'date' in headers

    # Update config to skip Date header
    with scylla_config_temporary(dynamodb, 'alternator_response_skip_header__date', 'true'):
        # Date header should now be absent
        response = test_table_s.get_item(Key={'p': 'test_key'})
        headers = response['ResponseMetadata']['HTTPHeaders']
        assert 'date' not in headers

# The test ensures the default DynamoDB Content-Type header (application/x-amz-json-1.0)
# is present by default and can be removed when configured.
def test_skip_content_type_header(dynamodb, test_table_s):
    # Content-Type should be present by default (for DynamoDB API responses)
    response = test_table_s.get_item(Key={'p': 'test_key'})
    headers = response['ResponseMetadata']['HTTPHeaders']
    assert 'content-type' in headers and headers['content-type'] == 'application/x-amz-json-1.0'
    
    # Update config to skip Content-Type header
    with scylla_config_temporary(dynamodb, 'alternator_response_skip_header__content_type', 'true'):
        # Content-Type header should now be absent
        response = test_table_s.get_item(Key={'p': 'test_key'})
        headers = response['ResponseMetadata']['HTTPHeaders']
        assert 'content-type' not in headers
