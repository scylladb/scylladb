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
 * Reads AWS credentials from the Environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY and AWS_SESSION_TOKEN if they exist. If they are not found,
 * empty credentials are returned.
 */
class environment_aws_credentials_provider final : public aws_credentials_provider {
public:
    environment_aws_credentials_provider();
    [[nodiscard]] seastar::future<s3::aws_credentials> get_aws_credentials() override;
    [[nodiscard]] const char* get_name() const override { return "environment_aws_credentials_provider"; }

protected:
    [[nodiscard]] bool is_time_to_refresh() const override { return false; }
    seastar::future<> reload() override { return seastar::make_ready_future(); }

private:
    s3::aws_credentials creds;
};
} // namespace aws
