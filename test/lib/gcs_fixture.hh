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

#include "utils/gcp/object_storage.hh"

/*
    Simple Google object storage endpoint provider. Uses either real or local, fake, endpoint.

    Note: the below text blobs are service account credentials, including private keys. 
    _Never_ give any real priviledges to these accounts, as we are obviously exposing them here.

    User1 is assumed to have permissions to read/write the bucket
    User2 is assumed to only have permissions to read the bucket, but permission to 
    impersonate User1.

    Note: fake gcp storage does not have any credentials or permissions, so
    for testing with such, leave them unset to skip those tests.

    * GCP_STORAGE_ENDPOINT - set to endpoint host. default is https://storage.googleapis.com
    * GCP_STORAGE_PROJECT - project in which to create bucket (if not specified)
    * GCP_STORAGE_USER_1_CREDENTIALS - set to credentials file for user1
    * GCP_STORAGE_USER_2_CREDENTIALS - set to credentials file for user2
    * GCP_STORAGE_BUCKET - set to test bucket
*/

class gcs_fixture {
    class impl;
    std::unique_ptr<impl> _impl;
public:
    gcs_fixture();
    ~gcs_fixture();

    utils::gcp::storage::client& client() const;

    const std::string& endpoint() const;
    const std::string& project() const;
    const std::string& bucket() const;

    void add_object_to_delete(const std::string&);

    seastar::future<> setup();
    seastar::future<> teardown();

    static gcs_fixture* active();
};

/**
 * Inheritance-only (intended at least) fixture
 * for getting a suite-shared fixture above and
 * also helping clean up test local objects.
 * 
 * If no suite-level gcs_fixture is active, it
 * will create one in setup and kill it in teardown
 */
class local_gcs_wrapper {
    std::unique_ptr<gcs_fixture> _local;
public:
    local_gcs_wrapper();
    ~local_gcs_wrapper();

    utils::gcp::storage::client& client() const;
    std::string endpoint;
    std::string project;
    std::string bucket;

    mutable std::vector<std::string> objects_to_delete;

    seastar::future<> setup();
    seastar::future<> teardown();
};
