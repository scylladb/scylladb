/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once
#include "aws_credentials_provider.hh"

namespace aws {

/**
 * Reads credentials profile from the default Profile Config File. Refreshes at set interval for credential rotation. Looks for environment variables
 * AWS_SHARED_CREDENTIALS_FILE and AWS_PROFILE. If they aren't found, then it defaults to the default profile in ~/.aws/credentials. Optionally a user can
 * specify the profile and it will override the environment variable and defaults. To alter the file this pulls from, then the user should alter the
 * AWS_SHARED_CREDENTIALS_FILE variable.
 */

class config_file_aws_credentials_provider final : public aws_credentials_provider {
public:
    s3::endpoint_config::aws_config get_aws_credentials() override;

protected:
    bool is_time_to_refresh(long reloadFrequency) override;
    void reload() override;
};
} // namespace aws
