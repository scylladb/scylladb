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
 * Reads credentials form yaml file as described in https://github.com/scylladb/scylladb/blob/master/docs/dev/object_storage.md
 */

class config_file_aws_credentials_provider final : public aws_credentials_provider {
public:
    explicit config_file_aws_credentials_provider(const std::string& _creds_file);
    [[nodiscard]] seastar::future<s3::aws_credentials> get_aws_credentials() override;
    [[nodiscard]] const char* get_name() const override { return "config_file_aws_credentials_provider"; }

protected:
    [[nodiscard]] bool is_time_to_refresh() const override { return false; }
    seastar::future<> reload() override;

private:
    std::string creds_file;
    s3::aws_credentials creds;
};
} // namespace aws
