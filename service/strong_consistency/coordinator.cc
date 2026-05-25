/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "coordinator.hh"
#include "db/consistency_level_type.hh"
#include "exceptions/exceptions.hh"
#include "raft/raft.hh"
#include "schema/schema.hh"
#include "replica/database.hh"
#include "locator/tablet_replication_strategy.hh"
#include "service/strong_consistency/state_machine.hh"
#include "service/strong_consistency/groups_manager.hh"
#include "utils/error_injection.hh"
#include "idl/strong_consistency/state_machine.dist.hh"
#include "idl/strong_consistency/state_machine.dist.impl.hh"
#include "gms/gossiper.hh"
#include "utils/histogram_metrics_helper.hh"

namespace service::strong_consistency {

static logging::logger logger("sc_coordinator");

// FIXME: Once the drivers support new error codes corresponding
// to timeouts of queries to strongly consistent tables, use
// a new, dedicated exception type instead of this.
struct write_timeout : public exceptions::mutation_write_timeout_exception {
    write_timeout(std::string_view ks, std::string_view cf)
        : exceptions::mutation_write_timeout_exception(
            seastar::format("Query timed out for {}.{}", ks, cf),
            db::consistency_level::ONE, 0, 1, db::write_type::SIMPLE
        )
    {}
};

// FIXME: Once the drivers support new error codes corresponding
// to timeouts of queries to strongly consistent tables, use
// a new, dedicated exception type instead of this.
struct read_timeout : public exceptions::read_timeout_exception {
    read_timeout(std::string_view ks, std::string_view cf)
        : exceptions::read_timeout_exception(
            seastar::format("Query timed out for {}.{}", ks, cf),
            db::consistency_level::ONE, 0, 1, false
        )
    {}
};

void stats::register_stats() {
    namespace sm = seastar::metrics;
    sm::label reason_label("reason");
    sm::label read_type_label("read_type");

    _metrics.add_group("strong_consistency_coordinator", {
        sm::make_summary("write_latency_summary", sm::description("Strong consistency write latency summary"),
            [this] { return to_metrics_summary(write.summary()); }).set_skip_when_empty(),

        sm::make_histogram("write_latency", sm::description("Strong consistency write latency histogram"),
            {}, [this] { return to_metrics_histogram(write.histogram()); })
            .aggregate({seastar::metrics::shard_label}).set_skip_when_empty(),

        sm::make_counter("write_errors", write_errors_timeout,
            sm::description("number of strong consistency write requests that failed"),
            {reason_label("timeout")})
            .set_skip_when_empty(),

        sm::make_counter("write_errors", write_errors_status_unknown,
            sm::description("number of strong consistency write requests that failed"),
            {reason_label("status_unknown")})
            .set_skip_when_empty(),

        sm::make_counter("write_errors", write_errors_other,
            sm::description("number of strong consistency write requests that failed"),
            {reason_label("other")})
            .set_skip_when_empty(),

        sm::make_counter("write_node_bounces", write_node_bounces,
            sm::description("number of strong consistency write requests bounced to another node"))
            .set_skip_when_empty(),

        sm::make_counter("write_shard_bounces", write_shard_bounces,
            sm::description("number of strong consistency write requests bounced to another shard"))
            .set_skip_when_empty(),

        sm::make_summary("read_latency_summary", sm::description("Strong consistency read latency summary"),
            [this] { return to_metrics_summary(linearizable_read.summary()); })(read_type_label("linearizable"))
            .set_skip_when_empty(),

        sm::make_histogram("read_latency", sm::description("Strong consistency read latency histogram"),
            {}, [this] { return to_metrics_histogram(linearizable_read.histogram()); })
            .aggregate({seastar::metrics::shard_label})(read_type_label("linearizable"))
            .set_skip_when_empty(),

        sm::make_summary("read_latency_summary", sm::description("Strong consistency read latency summary"),
            [this] { return to_metrics_summary(non_linearizable_read.summary()); })(read_type_label("non_linearizable"))
            .set_skip_when_empty(),

        sm::make_histogram("read_latency", sm::description("Strong consistency read latency histogram"),
            {}, [this] { return to_metrics_histogram(non_linearizable_read.histogram()); })
            .aggregate({seastar::metrics::shard_label})(read_type_label("non_linearizable"))
            .set_skip_when_empty(),

        sm::make_counter("read_errors", read_errors_timeout,
            sm::description("number of strong consistency read requests that failed"),
            {reason_label("timeout")})
            .set_skip_when_empty(),

        sm::make_counter("read_errors", read_errors_other,
            sm::description("number of strong consistency read requests that failed"),
            {reason_label("other")})
            .set_skip_when_empty(),

        sm::make_counter("read_node_bounces", read_node_bounces,
            sm::description("number of strong consistency read requests bounced to another node"))
            .set_skip_when_empty(),

        sm::make_counter("read_shard_bounces", read_shard_bounces,
            sm::description("number of strong consistency read requests bounced to another shard"))
            .set_skip_when_empty(),
    });
}

static const locator::tablet_replica* find_replica(const locator::tablet_info& tinfo, locator::host_id id) {
    const auto it = std::ranges::find_if(tinfo.replicas,
        [&] (const locator::tablet_replica& r) {
            return r.host == id;
        });
    return it == tinfo.replicas.end() ? nullptr : &*it;
}

// Subscribe target to sources and return an array of the corresponding
// subscriptions.
//
// The subscribing process will follow the order of the passed abort
// sources. The corresponding subscriptions in the returned array will
// also keep the same order.
//
// If some of the passed abort sources have already been triggered,
// they will immediately trigger target. This will be done in their
// relative order in the function's argument list.
template <std::same_as<abort_source>... Ts>
static auto chain_abort_sources(abort_source& target, Ts&... sources) {
    static_assert(sizeof...(Ts) > 0, "We need to chain at least one abort source!");
    auto source_array = std::array{std::ref(sources)...};

    for (abort_source& source : source_array) {
        if (source.abort_requested()) {
            target.request_abort_ex(source.abort_requested_exception_ptr());
        }
    }

    return std::array{
        sources.subscribe([&target] (const std::optional<std::exception_ptr>& eptr) noexcept {
            target.request_abort_ex(eptr.value_or(target.get_default_exception()));
        })...
    };
}

struct coordinator::operation_ctx {
    locator::effective_replication_map_ptr erm;
    raft_server raft_server;
    locator::tablet_id tablet_id;
    const locator::tablet_raft_info& raft_info;
    const locator::tablet_info& tablet_info;
};

// Select closest replica from a tablet replica set, preferring replicas in same rack
static locator::tablet_replica select_closest_replica(const gms::gossiper& gossiper,
                                               const locator::tablet_replica_set& replicas,
                                               const dht::token& token,
                                               const locator::topology& topo)
{
    // We need to convert tablet_replica_set to host_id_vector_replica_set first for sort_by_proximity
    auto hosts = replicas | std::views::filter([&gossiper] (const locator::tablet_replica& replica) {
        return gossiper.is_alive(replica.host);
    }) | std::views::transform([] (const locator::tablet_replica& replica) {
        return replica.host;
    }) | std::ranges::to<host_id_vector_replica_set>();

    if (hosts.empty()) {
        // If all replicas are down, there's no node worth forwarding to, so we return an exception
        throw exceptions::unavailable_exception(format("All replicas for token {} are down", token), db::consistency_level::ONE, 1, 0);
    }
    topo.sort_by_proximity(topo.my_host_id(), hosts);
    const auto& closest_host = hosts.front();
    const auto it = std::ranges::find_if(replicas,
        [&] (const locator::tablet_replica& r) {
            return r.host == closest_host;
        });
    return *it;
}

static need_redirect redirect_to_leader(locator::tablet_replica target, groups_manager& gm, raft::group_id group_id) {
    return {
        .target = target,
        // The `local()` here is needed to update the cache on the shard handling
        // the client request which may be different from the shard currently
        // executing the statement.
        .on_node_resolved = [container = &gm.container(), group_id] (locator::host_id leader) {
            container->local().leader_cache().put(group_id, leader);
        },
    };
}

static need_redirect redirect_to_replica(locator::tablet_replica target) {
    // When redirecting to a replica, there's no need to update the leader cache
    return { .target = target };
}

auto coordinator::create_operation_ctx(const schema& schema, const dht::token& token, abort_source& as, bool use_leader_cache)
    -> future<value_or_redirect<operation_ctx>>
{
    auto erm = schema.table().get_effective_replication_map();
    if (const auto* tablet_aware_rs = erm->get_replication_strategy().maybe_as_tablet_aware();
        !tablet_aware_rs || 
        tablet_aware_rs->get_consistency() != data_dictionary::consistency_config_option::global)
    {
        on_internal_error(logger,
            format("Unexpected replication strategy '{}' with consistency '{}' for table {}.{}",
                erm->get_replication_strategy().get_type(),
                tablet_aware_rs
                    ? consistency_config_option_to_string(tablet_aware_rs->get_consistency())
                    : "<undefined>",
                schema.ks_name(), schema.cf_name()));
    }
    const auto this_replica = locator::tablet_replica {
        .host = erm->get_token_metadata().get_my_id(),
        .shard = this_shard_id()
    };
    const auto& tablet_map = erm->get_token_metadata().tablets().get_tablet_map(schema.id());
    const auto tablet_id = tablet_map.get_tablet_id(token);
    const auto& tablet_info = tablet_map.get_tablet_info(tablet_id);
    const auto& raft_info = tablet_map.get_tablet_raft_info(tablet_id);

    if (!contains(tablet_info.replicas, this_replica)) {
        // For writes, check the leader cache to avoid an extra roundtrip.
        // For now, reads skip the cache because any replica can serve them.
        if (use_leader_cache) {
            if (const auto cached = _groups_manager.leader_cache().get(raft_info.group_id)) {
                if (const auto* target = find_replica(tablet_info, *cached)) {
                    co_return redirect_to_leader(*target, _groups_manager, raft_info.group_id);
                }
                // Cached leader is no longer a replica, evict it.
                _groups_manager.leader_cache().erase(raft_info.group_id);
            }
        }
        auto target = select_closest_replica(_gossiper, tablet_info.replicas, token,
                erm->get_token_metadata().get_topology());
        if (use_leader_cache) {
            co_return redirect_to_leader(target, _groups_manager, raft_info.group_id);
        }
        co_return redirect_to_replica(target);
    }

    co_await utils::get_local_injector().inject("sc_coordinator_wait_before_acquire_server",
            utils::wait_for_message(5min));

    auto raft_server = co_await _groups_manager.acquire_server(schema.id(), raft_info.group_id, as);

    co_return operation_ctx {
        .erm = std::move(erm),
        .raft_server = std::move(raft_server),
        .tablet_id = tablet_id,
        .raft_info = raft_info,
        .tablet_info = tablet_info
    };
}

coordinator::coordinator(groups_manager& groups_manager, replica::database& db, gms::gossiper& gossiper)
    : _groups_manager(groups_manager)
    , _db(db)
    , _gossiper(gossiper)
{
    _stats.register_stats();
}

future<value_or_redirect<>> coordinator::mutate(schema_ptr schema,
        const dht::token& token,
        mutation_gen&& mutation_gen,
        timeout_clock::time_point timeout,
        abort_source& as)
{
    auto aoe = abort_on_expiry<timeout_clock>(timeout);
    [[maybe_unused]] const auto subs = chain_abort_sources(aoe.abort_source(), as);

    utils::latency_counter lc;
    lc.start();
    auto mark_write_latency = defer([this, &lc] { _stats.write.mark(lc.stop().latency()); });
    bool commit_status_unknown_ex = false;

    try {
        auto op_result = co_await create_operation_ctx(*schema, token, aoe.abort_source(), true);
        if (auto* redirect = get_if<need_redirect>(&op_result)) {
            co_return std::move(*redirect);
        }
        auto& op = get<operation_ctx>(op_result);

        while (true) {
            co_await utils::get_local_injector().inject("sc_coordinator_wait_before_begin_mutate",
                utils::wait_for_message(5min));

            auto disposition = op.raft_server.begin_mutate(aoe.abort_source());
            if (const auto* not_a_leader = get_if<raft::not_a_leader>(&disposition)) {
                const auto leader_host_id = locator::host_id{not_a_leader->leader.uuid()};
                const auto* target = find_replica(op.tablet_info, leader_host_id);
                if (!target) {
                    on_internal_error(logger,
                        ::format("table {}.{}, tablet {}, current leader {} is not a replica, replicas {}",
                            schema->ks_name(), schema->cf_name(), op.tablet_id,
                            leader_host_id, op.tablet_info.replicas));
                }
                co_return redirect_to_leader(*target, _groups_manager, op.raft_info.group_id);
            }
            if (auto* wait_for_leader = get_if<raft_server::need_wait_for_leader>(&disposition)) {
                co_await std::move(wait_for_leader->future);
                continue;
            }
            const auto [ts, term] = get<raft_server::timestamp_with_term>(disposition);

            const raft_command command {
                .mutation{mutation_gen(ts)}
            };
            raft::command raft_cmd;
            ser::serialize(raft_cmd, command);

            logger.debug("mutate(): add_entry({}), term {}",
                command.mutation.pretty_printer(schema), term);

            co_await utils::get_local_injector().inject("sc_coordinator_wait_before_add_entry",
                utils::wait_for_message(5min));

            try {
                co_await op.raft_server.server().add_entry(std::move(raft_cmd),
                    raft::wait_type::committed,
                    &aoe.abort_source());
                co_return std::monostate{};
            } catch (...) {
                auto ex = std::current_exception();
                if (try_catch<raft::stopped_error>(ex)) {
                    // Holding raft_server.holder guarantees that the raft::server is not
                    // aborted until the holder is released.

                    on_internal_error(logger,
                        format("mutate(): add_entry, unexpected exception {}, table {}.{}, tablet {}, term {}",
                            ex, schema->ks_name(), schema->cf_name(), op.tablet_id, term));
                } else if (try_catch<raft::not_a_leader>(ex) || try_catch<raft::dropped_entry>(ex)) {
                    logger.debug("mutate(): add_entry, got retriable error {}, table {}.{}, tablet {}, term {}",
                        ex, schema->ks_name(), schema->cf_name(), op.tablet_id, term);

                    continue;
                } else if (try_catch<raft::commit_status_unknown>(ex)) {
                    logger.debug("mutate(): add_entry, got commit_status_unknown {}, table {}.{}, tablet {}, term {}",
                        ex, schema->ks_name(), schema->cf_name(), op.tablet_id, term);

                    ++_stats.write_errors_status_unknown;
                    // FIXME: use a dedicated ERROR_CODE instead of SERVER_ERROR
                    // FIXME: when a dedicated ERROR_CODE will be used,
                    //        we can get rid of the boolean flag
                    commit_status_unknown_ex = true;
                    throw exceptions::server_exception(
                        "The outcome of this statement is unknown. It may or may not have been applied. "
                        "Retrying the statement may be necessary.");
                }

                // Let the outer code handle other errors.
                throw;
            }
        }
    } catch (...) {
        auto ex = std::current_exception();
        // Unfortunately, timeouts can materialize in different forms depending
        // on which statement throws the exception.
        //
        // * raft::request_aborted: If the abort source passed to a raft::server's
        //     method was triggered.
        // * seastar::abort_requested_exception: Can be thrown by create_operation_ctx.
        // * timed_out_error: Can be thrown by the abort_on_expiry.
        // * condition_variable_timed_out: Can be thrown by begin_mutate.
        //
        // We handle them collectively here.
        if (try_catch<raft::request_aborted>(ex) || try_catch<seastar::abort_requested_exception>(ex)
                || try_catch<seastar::timed_out_error>(ex) || try_catch<seastar::condition_variable_timed_out>(ex)) {
            logger.trace("mutate(): request timed out with error {}, table {}.{}, token {}",
                ex, schema->ks_name(), schema->cf_name(), token);
            ++_stats.write_errors_timeout;
            co_return coroutine::return_exception(write_timeout(schema->ks_name(), schema->cf_name()));
        } else {
            if (!commit_status_unknown_ex) {
                ++_stats.write_errors_other;
            }
            logger.trace("mutate(): unknown exception {}, table {}.{}, token {}",
                ex, schema->ks_name(), schema->cf_name(), token);
            // We know nothing about other errors. Let the CQL server convert them to SERVER_ERROR.
            throw;
        }
    }
}

auto coordinator::query(schema_ptr schema,
        const query::read_command& cmd,
        const dht::partition_range_vector& ranges,
        read_type rtype,
        tracing::trace_state_ptr trace_state,
        timeout_clock::time_point timeout,
        abort_source& as
    ) -> future<query_result_type>
{
    auto aoe = abort_on_expiry<timeout_clock>(timeout);
    [[maybe_unused]] const auto subs = chain_abort_sources(aoe.abort_source(), as);

    utils::latency_counter lc;
    lc.start();
    auto& read_stats = (rtype == read_type::linearizable)
        ? _stats.linearizable_read : _stats.non_linearizable_read;
    auto mark_read_latency = defer([&read_stats, &lc] () mutable { read_stats.mark(lc.stop().latency()); });

    try {
        auto op_result = co_await create_operation_ctx(*schema, ranges[0].start()->value().token(), aoe.abort_source(), rtype == read_type::linearizable);
        if (auto* redirect = get_if<need_redirect>(&op_result)) {
            co_return std::move(*redirect);
        }
        auto& op = get<operation_ctx>(op_result);

        if (rtype == read_type::linearizable) {
            // For linearizable reads we may need to forward to the raft leader.
            while (true) {
                auto disposition = op.raft_server.begin_read(aoe.abort_source());
                if (const auto* not_a_leader = get_if<raft::not_a_leader>(&disposition)) {
                    const auto leader_host_id = locator::host_id{not_a_leader->leader.uuid()};
                    const auto* target = find_replica(op.tablet_info, leader_host_id);
                    if (!target) {
                        on_internal_error(logger,
                            ::format("query(): table {}.{}, tablet {}, current leader {} is not a replica, replicas {}",
                                schema->ks_name(), schema->cf_name(), op.tablet_id, leader_host_id, op.tablet_info.replicas));
                    }
                    co_return need_redirect{*target};
                }
                if (auto* wait_for_leader = get_if<raft_server::need_wait_for_leader>(&disposition)) {
                    co_await std::move(wait_for_leader->future);
                    continue;
                }
                break;
            }
        }
        // We're either a raft leader or it's a non-linearizable read. In both cases we can directly execute the read on this replica.

        if (rtype == read_type::linearizable) {
            co_await utils::get_local_injector().inject("sc_coordinator_wait_before_query_read_barrier",
                utils::wait_for_message(5min));

            co_await op.raft_server.server().read_barrier(&aoe.abort_source());
        }

        auto [result, cache_temp] = co_await _db.query(schema, cmd,
            query::result_options::only_result(), ranges, trace_state, timeout);

        co_return std::move(result);
    } catch (...) {
        auto ex = std::current_exception();
        // Unfortunately, timeouts can materialize in different forms depending
        // on which statement throws the exception.
        //
        // * raft::request_aborted: If the abort source passed to a raft::server's
        //     method was triggered.
        // * seastar::abort_requested_exception: Can be thrown by create_operation_ctx.
        // * timed_out_error: Can be thrown by the abort_on_expiry.
        // * seastar::condition_variable_timed_out: Can be thrown by begin_read's wait_for_leader.
        //
        // We handle them collectively here.
        if (try_catch<raft::request_aborted>(ex) || try_catch<seastar::abort_requested_exception>(ex)
                || try_catch<timed_out_error>(ex) || try_catch<seastar::condition_variable_timed_out>(ex)) {
            logger.trace("query(): request timed out with error {}, table {}.{}, read cmd {}",
                ex, schema->ks_name(), schema->cf_name(), cmd);
            ++_stats.read_errors_timeout;
            co_return coroutine::return_exception(read_timeout(schema->ks_name(), schema->cf_name()));
        } else {
            logger.trace("query(): unknown exception {}, table {}.{}, read cmd {}",
                ex, schema->ks_name(), schema->cf_name(), cmd);
            ++_stats.read_errors_other;
            // We know nothing about other errors. Let the CQL server convert them to SERVER_ERROR.
            throw;
        }
    }
}

future<> coordinator::wait_for_table_raft_groups_on_all_hosts(table_id table, lowres_clock::time_point timeout) {
    return _groups_manager.wait_for_table_raft_groups_on_all_hosts(table, timeout);
}

}
