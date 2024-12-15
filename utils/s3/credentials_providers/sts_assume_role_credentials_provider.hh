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
    sts_assume_role_credentials_provider(const std::string& _host, unsigned _port, bool _is_secured); // For tests
    sts_assume_role_credentials_provider(const std::string& _region, const std::string& _role_arn);
    [[nodiscard]] seastar::future<s3::aws_credentials> get_aws_credentials() override;
    [[nodiscard]] const char* get_name() const override { return "sts_assume_role_credentials_provider"; }

protected:
    [[nodiscard]] bool is_time_to_refresh() const;
    seastar::future<> reload();

private:
    seastar::future<> update_credentials();
    s3::aws_credentials parse_creds(seastar::sstring& body);

    std::string sts_host;
    std::string role_arn;
    s3::aws_credentials creds;
    unsigned port{443};
    static constexpr unsigned session_duration{43200};
    bool is_secured{true};
};

} // namespace aws
