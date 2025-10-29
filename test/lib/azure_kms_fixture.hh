
/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <string>
#include <memory>

#include <seastar/core/future.hh>

/*
    Simple Azure KMS mock/real provider.

    User1 is assumed to have permissions to wrap/unwrap using the given key.
    User2 is assumed to _not_ have permissions to wrap/unwrap using the given key.

    This test is parameterized with env vars:

    * AZURE_KEY_NAME - set to <vault_name>/<keyname> - if set, assume real Azure env
    * AZURE_TENANT_ID - the tenant where the principals live
    * AZURE_USER_1_CLIENT_ID - the client ID of user1
    * AZURE_USER_1_CLIENT_SECRET - the secret of user1
    * AZURE_USER_1_CLIENT_CERTIFICATE - the PEM-encoded certificate and private key of user1
    * AZURE_USER_2_CLIENT_ID - the client ID of user2
    * AZURE_USER_2_CLIENT_SECRET - the secret of user2
    * AZURE_USER_2_CLIENT_CERTIFICATE - the PEM-encoded certificate and private key of user2
*/

struct azure_test_env {
    std::string key_name;
    std::string tenant_id;
    std::string user_1_client_id;
    std::string user_1_client_secret;
    std::string user_1_client_certificate;
    std::string user_2_client_id;
    std::string user_2_client_secret;
    std::string user_2_client_certificate;
    std::string authority_host;
    std::string imds_endpoint;
};

enum class azure_mode {
    any, local, real
};

class azure_kms_fixture {
    class impl;
    std::unique_ptr<impl> _impl;
    azure_kms_fixture* _prev = nullptr;
public:
    azure_kms_fixture(azure_mode = azure_mode::any);
    ~azure_kms_fixture();

    const azure_test_env& test_env() const;

    seastar::future<> setup();
    seastar::future<> teardown();

    static azure_kms_fixture* active();
};

/**
 * Inheritance-only (intended at least) fixture
 * for getting a suite-shared fixture above and
 * also helping clean up test local objects.
 * 
 * If no suite-level azure_kms_fixture is active, it
 * will create one in setup and kill it in teardown
 */
class local_azure_kms_wrapper 
    : public azure_test_env
{
    std::unique_ptr<azure_kms_fixture> _local;
    azure_mode _mode;
public:
    local_azure_kms_wrapper(azure_mode = azure_mode::any);
    ~local_azure_kms_wrapper();

    seastar::future<> setup();
    seastar::future<> teardown();
};
