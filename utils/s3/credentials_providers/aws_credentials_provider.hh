/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once
#include "utils/s3/creds.hh"
#include <seastar/core/future.hh>

namespace aws {
/*
 * Abstract class for retrieving AWS credentials. Create a derived class from this to allow various methods of retrieving credentials.
 */
class aws_credentials_provider {
public:
    virtual ~aws_credentials_provider() = default;

    /*
     * The core of the credential provider interface. Override this method to control how credentials are retrieved.
     */
    [[nodiscard]] seastar::future<s3::aws_credentials> get_aws_credentials();
    void invalidate_credentials();
    [[nodiscard]] virtual const char* get_name() const = 0;

protected:
    virtual seastar::future<> reload() = 0;
    s3::aws_credentials creds;
};
} // namespace aws
