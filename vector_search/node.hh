/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "client.hh"
#include <seastar/core/gate.hh>
#include <seastar/core/abort_source.hh>

namespace vector_search {

class node {
public:
    explicit node(client::endpoint_type ep);

    bool is_up() const;

    seastar::future<client::response> ann(
            seastar::sstring keyspace, seastar::sstring name, std::vector<float> embedding, std::size_t limit, seastar::abort_source& as);

    seastar::future<> close();

    client::endpoint_type endpoint() const {
        return _client.endpoint();
    }

private:
    seastar::future<> restart_pinging();
    seastar::future<> start_pinging();
    seastar::future<> stop_pinging();
    seastar::future<> ping();

    client _client;
    bool _is_up{true};
    seastar::future<> _pinging = seastar::make_ready_future<>();
    seastar::abort_source _as;
};

} // namespace vector_search
