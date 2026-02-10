/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "auth/common.hh"

#include <optional>
#include <seastar/core/coroutine.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sharded.hh>

#include "mutation/canonical_mutation.hh"
#include "mutation/timestamp.hh"
#include "utils/exponential_backoff_retry.hh"
#include "cql3/query_processor.hh"
#include "service/raft/group0_state_machine.hh"
#include "timeout_config.hh"
#include "db/system_keyspace.hh"

namespace auth {

namespace meta {

namespace legacy {
    constinit const std::string_view AUTH_KS("system_auth");
} // namespace legacy
constinit const std::string_view AUTH_PACKAGE_NAME("org.apache.cassandra.auth.");
} // namespace meta

static logging::logger auth_log("auth");

std::string_view get_auth_ks_name(cql3::query_processor&) {
    return db::system_keyspace::NAME;
}

// Func must support being invoked more than once.
future<> do_after_system_ready(seastar::abort_source& as, seastar::noncopyable_function<future<>()> func) {
    struct empty_state { };
    return exponential_backoff_retry::do_until_value(1s, 1min, as, [func = std::move(func)] {
        return func().then_wrapped([] (auto&& f) -> std::optional<empty_state> {
            if (f.failed()) {
                auth_log.debug("Auth task failed with error, rescheduling: {}", f.get_exception());
                return { };
            }
            return { empty_state() };
        });
    }).discard_result();
}

::service::query_state& internal_distributed_query_state() noexcept {
#ifdef DEBUG
    // Give the much slower debug tests more headroom for completing auth queries.
    static const auto t = 30s;
#else
    static const auto t = 5s;
#endif
    static const timeout_config tc{t, t, t, t, t, t, t};
    static thread_local ::service::client_state cs(::service::client_state::internal_tag{}, tc);
    static thread_local ::service::query_state qs(cs, empty_service_permit());
    return qs;
}

::service::raft_timeout get_raft_timeout() noexcept {
    auto dur = internal_distributed_query_state().get_client_state().get_timeout_config().other_timeout;
    return ::service::raft_timeout{.value = lowres_clock::now() + dur};
}

static future<> announce_mutations_with_guard(
        ::service::raft_group0_client& group0_client,
        utils::chunked_vector<canonical_mutation> muts,
        ::service::group0_guard group0_guard,
        seastar::abort_source& as,
        std::optional<::service::raft_timeout> timeout) {
    auto group0_cmd = group0_client.prepare_command(
        ::service::write_mutations{
            .mutations{std::move(muts)},
        },
        group0_guard,
        "auth: modify internal data"
    );
    return group0_client.add_entry(std::move(group0_cmd), std::move(group0_guard), as, timeout);
}

future<> announce_mutations(
        cql3::query_processor& qp,
        ::service::raft_group0_client& group0_client,
        const sstring query_string,
        std::vector<data_value_or_unset> values,
        seastar::abort_source& as,
        std::optional<::service::raft_timeout> timeout) {
    auto group0_guard = co_await group0_client.start_operation(as, timeout);
    auto timestamp = group0_guard.write_timestamp();
    auto muts = co_await qp.get_mutations_internal(
            query_string,
            internal_distributed_query_state(),
            timestamp,
            std::move(values));
    utils::chunked_vector<canonical_mutation> cmuts = {muts.begin(), muts.end()};
    co_await announce_mutations_with_guard(group0_client, std::move(cmuts), std::move(group0_guard), as, timeout);
}

future<> collect_mutations(
        cql3::query_processor& qp,
        ::service::group0_batch& collector,
        const sstring query_string,
        std::vector<data_value_or_unset> values) {
    auto muts = co_await qp.get_mutations_internal(
            query_string,
            internal_distributed_query_state(),
            collector.write_timestamp(),
            std::move(values));
    collector.add_mutations(std::move(muts), format("auth internal statement: {}", query_string));
}

}
