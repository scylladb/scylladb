/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <string_view>

#include <seastar/core/future.hh>
#include <seastar/core/abort_source.hh>
#include <seastar/util/noncopyable_function.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/resource.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/smp.hh>

#include "schema/schema_registry.hh"
#include "types/types.hh"
#include "service/raft/raft_group0_client.hh"

using namespace std::chrono_literals;

namespace replica {
class database;
}

namespace service {
class migration_manager;
class query_state;
}

namespace cql3 {
class query_processor;
}

namespace auth {

namespace meta {

namespace legacy {
extern constinit const std::string_view AUTH_KS;
extern constinit const std::string_view USERS_CF;
} // namespace legacy

constexpr std::string_view DEFAULT_SUPERUSER_NAME("cassandra");
extern constinit const std::string_view AUTH_PACKAGE_NAME;

} // namespace meta

// This is a helper to check whether auth-v2 is on.
bool legacy_mode(cql3::query_processor& qp);

// We have legacy implementation using different keyspace
// and need to parametrize depending on runtime feature.
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

future<> create_metadata_table_if_missing(
        std::string_view table_name,
        cql3::query_processor&,
        std::string_view cql,
        ::service::migration_manager&) noexcept;

///
/// Time-outs for internal, non-local CQL queries.
///
::service::query_state& internal_distributed_query_state() noexcept;

// Execute update query via group0 mechanism, mutations will be applied on all nodes.
// Use this function when need to perform read before write on a single guard or if
// you have more than one mutation and potentially exceed single command size limit.
using start_operation_func_t = std::function<future<::service::group0_guard>(abort_source*)>;
using mutations_generator = coroutine::experimental::generator<mutation>;
future<> announce_mutations_with_batching(
        ::service::raft_group0_client& group0_client,
        // since we can operate also in topology coordinator context where we need stronger
        // guarantees than start_operation from group0_client gives we allow to inject custom
        // function here
        start_operation_func_t start_operation_func,
        std::function<mutations_generator(api::timestamp_type& t)> gen,
        seastar::abort_source* as,
        std::optional<::service::raft_timeout> timeout);

// Execute update query via group0 mechanism, mutations will be applied on all nodes.
future<> announce_mutations(
        cql3::query_processor& qp,
        ::service::raft_group0_client& group0_client,
        const sstring query_string,
        std::vector<data_value_or_unset> values,
        seastar::abort_source* as,
        std::optional<::service::raft_timeout> timeout);

}
