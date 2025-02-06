/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
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
    [[nodiscard]] const char* get_name() const override { return "config_file_aws_credentials_provider"; }

protected:
    seastar::future<> reload() override;

private:
    std::string creds_file;
};
} // namespace aws
