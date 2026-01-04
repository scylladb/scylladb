/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */


#include "aws_dns_connection_factory.hh"

namespace utils::http {

aws_dns_connection_factory::aws_dns_connection_factory(std::string host, int port, bool use_https, logging::logger& logger)
    : dns_connection_factory(std::move(host), port, use_https, logger) {
}

future<connected_socket> aws_dns_connection_factory::make(abort_source*) {
    co_await _addr_provider.reset();
    co_return co_await connect();
}
} // namespace utils::http
