/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
    [[nodiscard]] virtual seastar::future<s3::aws_credentials> get_aws_credentials() = 0;
    [[nodiscard]] virtual const char* get_name() const = 0;

protected:
    [[nodiscard]] virtual bool is_time_to_refresh() const = 0;
    virtual seastar::future<> reload() = 0;
};
} // namespace aws
