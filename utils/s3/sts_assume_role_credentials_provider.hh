/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once
#include "aws_credentials_provider.hh"
namespace aws {

/*
 * Credentials provider for STS Assume Role
 */
class sts_assume_role_credentials_provider final : public aws_credentials_provider {
public:
    s3::endpoint_config::aws_config get_aws_credentials() override;

protected:
    bool is_time_to_refresh(long reloadFrequency) override;
    void reload() override;
};

} // namespace aws
