/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once
#include "aws_credentials_provider.hh"
#include "utils/s3/retry_strategy.hh"

namespace aws {

/*
 * Credentials provider implementation that loads credentials from the Amazon EC2 Instance Metadata Service.
 */
class instance_profile_credentials_provider final : public aws_credentials_provider {
public:
    instance_profile_credentials_provider() = default;
    instance_profile_credentials_provider(const std::string& _host, unsigned _port); // For tests
    [[nodiscard]] seastar::future<s3::aws_credentials> get_aws_credentials() override;
    [[nodiscard]] const char* get_name() const override { return "instance_profile_credentials_provider"; }

protected:
    [[nodiscard]] bool is_time_to_refresh() const override;
    seastar::future<> reload() override;

private:
    seastar::future<> update_credentials();
    void parse_creds(const seastar::sstring& creds);

    default_retry_strategy retry_strategy;
    std::string ec2_metadata_ip{"169.254.169.254"};
    s3::aws_credentials creds;
    unsigned port{80};
    static constexpr unsigned session_duration{21600};
};

} // namespace aws
