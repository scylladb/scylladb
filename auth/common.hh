/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <string_view>

#include <seastar/core/future.hh>
#include <seastar/core/abort_source.hh>
#include <seastar/util/noncopyable_function.hh>
#include <seastar/core/sstring.hh>

#include "types/types.hh"
#include "service/raft/raft_group0_client.hh"
#include "timeout_config.hh"

using namespace std::chrono_literals;

namespace service {
class query_state;
}

namespace cql3 {
class query_processor;
}

namespace auth {

namespace meta {

namespace legacy {
extern constinit const std::string_view AUTH_KS;
} // namespace legacy

constexpr std::string_view DEFAULT_SUPERUSER_NAME("cassandra");
extern constinit const std::string_view AUTH_PACKAGE_NAME;

} // namespace meta

constexpr std::string_view PERMISSIONS_CF = "role_permissions";
constexpr std::string_view ROLE_MEMBERS_CF = "role_members";
constexpr std::string_view ROLE_ATTRIBUTES_CF = "role_attributes";

// Auth data is stored in the system keyspace.
std::string_view get_auth_ks_name(cql3::query_processor& qp);

template <class Task>
future<> once_among_shards(Task&& f) {
    if (this_shard_id() == 0u) {
        return f();
    }

    return make_ready_future<>();
}

// Func must support being invoked more than once.
future<> do_after_system_ready(seastar::abort_source& as, seastar::noncopyable_function<future<>()> func);

///
/// Time-outs for internal, non-local CQL queries.
///
::service::query_state& internal_distributed_query_state() noexcept;

::service::raft_timeout get_raft_timeout() noexcept;

// Execute update query via group0 mechanism, mutations will be applied on all nodes.
future<> announce_mutations(
        cql3::query_processor& qp,
        ::service::raft_group0_client& group0_client,
        const sstring query_string,
        std::vector<data_value_or_unset> values,
        seastar::abort_source& as,
        std::optional<::service::raft_timeout> timeout);

// Appends mutations to a collector, they will be applied later on all nodes via group0 mechanism.
future<> collect_mutations(
        cql3::query_processor& qp,
        ::service::group0_batch& collector,
        const sstring query_string,
        std::vector<data_value_or_unset> values);
}
