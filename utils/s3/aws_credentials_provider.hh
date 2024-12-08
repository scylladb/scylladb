/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once
#include "utils/s3/creds.hh"

namespace aws {
/*
 * Abstract class for retrieving AWS credentials. Create a derived class from this to allow various methods of storing and retrieving credentials. Examples
 * would be cognito-identity, some encrypted store etc...
 */
class aws_credentials_provider {
public:
    virtual ~aws_credentials_provider() = default;

    /*
     * The core of the credential provider interface. Override this method to control how credentials are retrieved.
     */
    virtual s3::endpoint_config::aws_config get_aws_credentials() = 0;

protected:
    /**
     * The default implementation keeps up with the cache times and lets you know if it's time to refresh your internal caching to aid your implementation of
     * GetAWSCredentials.
     */
    virtual bool is_time_to_refresh(long reloadFrequency) = 0;
    virtual void reload() = 0;
};
} // namespace aws
