/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once
#include "aws_credentials_provider.hh"

namespace aws {

class aws_credentials_provider_chain final {
public:
    [[nodiscard]] seastar::future<s3::aws_credentials> get_aws_credentials();
    aws_credentials_provider_chain& add_credentials_provider(std::unique_ptr<aws_credentials_provider>&& provider);

private:
    std::vector<std::unique_ptr<aws_credentials_provider>> providers;
};

} // namespace aws
