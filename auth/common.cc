/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "auth/common.hh"

#include <optional>
#include <seastar/core/coroutine.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sharded.hh>

#include "mutation/canonical_mutation.hh"
#include "schema/schema_fwd.hh"
#include "timestamp.hh"
#include "utils/exponential_backoff_retry.hh"
#include "cql3/query_processor.hh"
#include "cql3/statements/create_table_statement.hh"
#include "schema/schema_builder.hh"
#include "service/migration_manager.hh"
#include "service/raft/group0_state_machine.hh"
#include "timeout_config.hh"
#include "db/config.hh"
#include "db/system_auth_keyspace.hh"
#include "utils/error_injection.hh"

namespace auth {

namespace meta {

namespace legacy {
    constinit const std::string_view AUTH_KS("system_auth");
    constinit const std::string_view USERS_CF("users");
} // namespace legacy
constinit const std::string_view AUTH_PACKAGE_NAME("org.apache.cassandra.auth.");
} // namespace meta

static logging::logger auth_log("auth");

bool legacy_mode(cql3::query_processor& qp) {
    return !qp.db().get_config().check_experimental(
        db::experimental_features_t::feature::CONSISTENT_TOPOLOGY_CHANGES) ||
        qp.auth_version <= db::system_auth_keyspace::version_t::v1;
}

std::string_view get_auth_ks_name(cql3::query_processor& qp) {
    if (legacy_mode(qp)) {
        return meta::legacy::AUTH_KS;
    }
    return db::system_auth_keyspace::NAME;
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

static future<> create_metadata_table_if_missing_impl(
        std::string_view table_name,
        cql3::query_processor& qp,
        std::string_view cql,
        ::service::migration_manager& mm) {
    assert(this_shard_id() == 0); // once_among_shards makes sure a function is executed on shard 0 only

    auto db = qp.db();
    auto parsed_statement = cql3::query_processor::parse_statement(cql);
    auto& parsed_cf_statement = static_cast<cql3::statements::raw::cf_statement&>(*parsed_statement);

    parsed_cf_statement.prepare_keyspace(meta::legacy::AUTH_KS);

    auto statement = static_pointer_cast<cql3::statements::create_table_statement>(
            parsed_cf_statement.prepare(db, qp.get_cql_stats())->statement);

    const auto schema = statement->get_cf_meta_data(qp.db());
    const auto uuid = generate_legacy_id(schema->ks_name(), schema->cf_name());

    schema_builder b(schema);
    b.set_uuid(uuid);
    schema_ptr table = b.build();

    if (!db.has_schema(table->ks_name(), table->cf_name())) {
        auto group0_guard = co_await mm.start_group0_operation();
        auto ts = group0_guard.write_timestamp();
        try {
            co_return co_await mm.announce(co_await ::service::prepare_new_column_family_announcement(qp.proxy(), table, ts),
                    std::move(group0_guard), format("auth: create {} metadata table", table->cf_name()));
        } catch (exceptions::already_exists_exception&) {}
    }
}

future<> create_metadata_table_if_missing(
        std::string_view table_name,
        cql3::query_processor& qp,
        std::string_view cql,
        ::service::migration_manager& mm) noexcept {
    return futurize_invoke(create_metadata_table_if_missing_impl, table_name, qp, cql, mm);
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

static future<> announce_mutations_with_guard(
        ::service::raft_group0_client& group0_client,
        std::vector<canonical_mutation> muts,
        ::service::group0_guard group0_guard,
        seastar::abort_source* as,
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

future<> announce_mutations_with_batching(
        ::service::raft_group0_client& group0_client,
        start_operation_func_t start_operation_func,
        std::function<mutations_generator(api::timestamp_type& t)> gen,
        seastar::abort_source* as,
        std::optional<::service::raft_timeout> timeout) {
    // account for command's overhead, it's better to use smaller threshold than constantly bounce off the limit
    size_t memory_threshold = group0_client.max_command_size() * 0.75;
    utils::get_local_injector().inject("auth_announce_mutations_command_max_size",
        [&memory_threshold] {
        memory_threshold = 1000;
    });

    size_t memory_usage = 0;
    std::vector<canonical_mutation> muts;

    // guard has to be taken before we execute code in gen as
    // it can do read-before-write and we want announce_mutations
    // operation to be linearizable with other such calls,
    // for instance if we do select and then delete in gen
    // we want both to operate on the same data or fail
    // if someone else modified it in the middle
    std::optional<::service::group0_guard> group0_guard;
    group0_guard = co_await start_operation_func(as);
    auto timestamp = group0_guard->write_timestamp();

    auto g = gen(timestamp);
    while (auto mut = co_await g()) {
        muts.push_back(canonical_mutation{*mut});
        memory_usage += muts.back().representation().size();
        if (memory_usage >= memory_threshold) {
            if (!group0_guard) {
                group0_guard = co_await start_operation_func(as);
                timestamp = group0_guard->write_timestamp();
            }
            co_await announce_mutations_with_guard(group0_client, std::move(muts), std::move(*group0_guard), as, timeout);
            group0_guard = std::nullopt;
            memory_usage = 0;
            muts = {};
        }
    }
    if (!muts.empty()) {
        if (!group0_guard) {
            group0_guard = co_await start_operation_func(as);
            timestamp = group0_guard->write_timestamp();
        }
        co_await announce_mutations_with_guard(group0_client, std::move(muts), std::move(*group0_guard), as, timeout);
    }
}

future<> announce_mutations(
        cql3::query_processor& qp,
        ::service::raft_group0_client& group0_client,
        const sstring query_string,
        std::vector<data_value_or_unset> values,
        seastar::abort_source* as,
        std::optional<::service::raft_timeout> timeout) {
    auto group0_guard = co_await group0_client.start_operation(as, timeout);
    auto timestamp = group0_guard.write_timestamp();
    auto muts = co_await qp.get_mutations_internal(
            query_string,
            internal_distributed_query_state(),
            timestamp,
            std::move(values));
    std::vector<canonical_mutation> cmuts = {muts.begin(), muts.end()};
    co_await announce_mutations_with_guard(group0_client, std::move(cmuts), std::move(group0_guard), as, timeout);
}

}
