/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "seastarx.hh"
#include "utils/http.hh"

namespace utils::http {
class aws_dns_connection_factory : public dns_connection_factory {
public:
    aws_dns_connection_factory(std::string host, int port, bool use_https, logging::logger& logger);

    future<connected_socket> make(abort_source*) override;
};
} // namespace utils::http
