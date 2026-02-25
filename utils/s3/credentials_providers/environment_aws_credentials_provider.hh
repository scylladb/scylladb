/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
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
    [[nodiscard]] const char* get_name() const override { return "environment_aws_credentials_provider"; }

protected:
    seastar::future<> reload() override;
};
} // namespace aws
