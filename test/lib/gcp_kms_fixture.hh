
/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <string>
#include <memory>

#include <seastar/core/future.hh>

class tmpdir;

/*
    Simple GCP KMS mock/real provider. Uses either real or local, fake, endpoint.

    Note: fake kms server does not have any credentials or permissions

    This fixture is parameterized with env vars, if set we will just expose 
    a real (we assume) KMS endpoint:

    * GCP_KEY_NAME - set to name of key you have access to. If set, the fixture will assume we run real kms
    * GCP_LOCATION - set to whatever location your key is in.
    * GCP_PROJECT_ID
    * GCP_USER_1_CREDENTIALS
    * GCP_USER_2_CREDENTIALS

    In CI, we provide the vars from jenkins, with working values

*/

class gcp_kms_fixture {
    class impl;
    std::unique_ptr<impl> _impl;
public:
    gcp_kms_fixture();
    ~gcp_kms_fixture();

    const std::string& gcp_key_name() const;
    const std::string& gcp_location() const;
    const std::string& gcp_project_id() const;
    const std::string& gcp_user_1_credentials() const;
    const std::string& gcp_user_2_credentials() const;
    const std::string& gcp_iam_endpoint_override() const;

    // this will be empty if using real KMS.
    const std::string& endpoint() const;

    seastar::future<> setup();
    seastar::future<> teardown();

    static gcp_kms_fixture* active();
};

/**
 * Inheritance-only (intended at least) fixture
 * for getting a suite-shared fixture above and
 * also helping clean up test local objects.
 * 
 * If no suite-level aws_kms_fixture is active, it
 * will create one in setup and kill it in teardown
 */
class local_gcp_kms_wrapper {
    std::unique_ptr<gcp_kms_fixture> _local;
public:
    local_gcp_kms_wrapper();
    ~local_gcp_kms_wrapper();

    std::string endpoint;
    std::string gcp_key_name;
    std::string gcp_location;
    std::string gcp_project_id;
    std::string gcp_user_1_credentials;
    std::string gcp_user_2_credentials;
    std::string gcp_iam_endpoint_override;

    seastar::future<> setup();
    seastar::future<> teardown();
};
