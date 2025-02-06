/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once
#include "aws_credentials_provider.hh"

namespace aws {

/*
 * Credentials provider for STS Assume Role
 */
class sts_assume_role_credentials_provider final : public aws_credentials_provider {
public:
    sts_assume_role_credentials_provider(const std::string& _host, unsigned _port, bool _is_secured); // For tests
    sts_assume_role_credentials_provider(const std::string& _region, const std::string& _role_arn);
    [[nodiscard]] const char* get_name() const override { return "sts_assume_role_credentials_provider"; }

protected:
    [[nodiscard]] bool is_time_to_refresh() const;
    seastar::future<> reload() override;

private:
    seastar::future<> update_credentials();
    s3::aws_credentials parse_creds(seastar::sstring& body);

    std::string sts_host;
    std::string role_arn;
    unsigned port{443};
    static constexpr unsigned session_duration{43200};
    bool is_secured{true};
};

} // namespace aws
