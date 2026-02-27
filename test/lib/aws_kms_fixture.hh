
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

class tmpdir;

/*
    Simple AWS KMS mock/real provider. Uses either real or local, fake, endpoint.

    Note: fake kms server does not have any credentials or permissions

    This fixture is parameterized with env vars, if set we will just expose 
    a real (we assume) KMS endpoint:

    * KMS_KEY_ALIAS - set to key alias you have access to. If set, the fixture will assume we run real kms
    * KMS_AWS_REGION - default us-east-1 - set to whatever region your key is in.

    NOTE: When run via test.py, the minio server used there will, unless already set,
    put AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY into the inherited process env, with
    values purely fictional, and only usable by itself. This _will_ screw up credentials
    resolution in the KMS connector, and will lead to errors not intended.

    In CI, we provide the vars from jenkins, with working values, and the minio
    respects this.

    As a workaround, try setting the vars yourself to something that actually works (i.e. 
    values from your .awscredentials). Or complain until we find a way to make the minio
    server optional for tests.
*/

class aws_kms_fixture {
    class impl;
    std::unique_ptr<impl> _impl;
public:
    aws_kms_fixture();
    ~aws_kms_fixture();

    const std::string& kms_key_alias() const;
    const std::string& kms_aws_region() const;
    const std::string& kms_aws_profile() const;

    // this will be empty if using real KMS.
    const std::string& endpoint() const;

    seastar::future<> setup();
    seastar::future<> teardown();

    static aws_kms_fixture* active();
};

/**
 * Inheritance-only (intended at least) fixture
 * for getting a suite-shared fixture above and
 * also helping clean up test local objects.
 * 
 * If no suite-level aws_kms_fixture is active, it
 * will create one in setup and kill it in teardown
 */
class local_aws_kms_wrapper {
    std::unique_ptr<aws_kms_fixture> _local;
public:
    local_aws_kms_wrapper();
    ~local_aws_kms_wrapper();

    std::string endpoint;
    std::string kms_key_alias;
    std::string kms_aws_region;
    std::string kms_aws_profile;

    seastar::future<> setup();
    seastar::future<> teardown();
};
