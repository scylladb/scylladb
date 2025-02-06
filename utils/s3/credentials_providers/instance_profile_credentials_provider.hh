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
 * Credentials provider implementation that loads credentials from the Amazon EC2 Instance Metadata Service.
 */
class instance_profile_credentials_provider final : public aws_credentials_provider {
public:
    instance_profile_credentials_provider() = default;
    instance_profile_credentials_provider(const std::string& _host, unsigned _port); // For tests
    [[nodiscard]] const char* get_name() const override { return "instance_profile_credentials_provider"; }

protected:
    [[nodiscard]] bool is_time_to_refresh() const;
    seastar::future<> reload() override;

private:
    seastar::future<> update_credentials();
    s3::aws_credentials parse_creds(const seastar::sstring& creds);

    std::string ec2_metadata_ip{"169.254.169.254"};
    unsigned port{80};
    static constexpr unsigned session_duration{21600};
};

} // namespace aws
