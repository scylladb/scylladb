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
#include "locator/tablets.hh"
#include "schema/schema.hh"
#include "replica/database.hh"
#include "locator/tablet_replication_strategy.hh"
#include "service/strong_consistency/state_machine.hh"
#include "service/strong_consistency/groups_manager.hh"
#include "utils/error_injection.hh"
#include "idl/strong_consistency/state_machine.dist.hh"
#include "idl/strong_consistency/state_machine.dist.impl.hh"
#include "gms/gossiper.hh"
#include "utils/chain_abort_source.hh"
#include "utils/exponential_backoff_retry.hh"
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

static const locator::tablet_replica* find_replica(const locator::tablet_replica_set& replicas, locator::host_id id) {
    const auto it = std::ranges::find_if(replicas,
        [&] (const locator::tablet_replica& r) {
            return r.host == id;
        });
    return it == replicas.end() ? nullptr : &*it;
}

// Tablet replicas for leader redirection.
// This set must contain all tablet replicas that can potentially be the leader.
static locator::tablet_replica_set get_redirectable_tablet_replicas(const locator::tablet_info& tinfo, const locator::tablet_transition_info* trinfo) {
    using namespace locator;

    if (!trinfo) {
        return tinfo.replicas;
    }

    switch (trinfo->stage) {
        case tablet_transition_stage::start_migration:
        case tablet_transition_stage::sc_add_nonvoter:
            // The pending replica can't be a leader, because it can become a leader only in
            // sc_become_voter, and the transition from sc_snapshot_transfer to sc_become_voter
            // executes a barrier.
            return tinfo.replicas;
        case tablet_transition_stage::sc_snapshot_transfer:
            // in sc_snapshot_transfer the topology coordinator may have advanced to sc_become_voter,
            // so the pending replica can become a voter and a leader.
        case tablet_transition_stage::sc_become_voter: {
            auto replicas = tinfo.replicas;
            if (trinfo->pending_replica) {
                replicas.push_back(*trinfo->pending_replica);
            }
            return replicas;
        }
        case tablet_transition_stage::use_new:
            // the precondition of use_new guarantees that the leaving replica is not a member of the raft group.
        case tablet_transition_stage::cleanup:
        case tablet_transition_stage::end_migration:
            return trinfo->next;
        case tablet_transition_stage::sc_rollback: {
            // rollback may enter sc_rollback from sc_become_voter, thus the pending replica may be a leader.
            auto replicas = tinfo.replicas;
            if (trinfo->pending_replica) {
                replicas.push_back(*trinfo->pending_replica);
            }
            return replicas;
        }
        case tablet_transition_stage::cleanup_target:
            // the precondition of cleanup_target guarantees that the pending replica is not a member of the raft group.
        case tablet_transition_stage::revert_migration:
            return tinfo.replicas;
        case tablet_transition_stage::write_both_read_old_fallback_cleanup:
        case tablet_transition_stage::rebuild_repair:
            return tinfo.replicas;
        case tablet_transition_stage::repair:
        case tablet_transition_stage::end_repair:
        case tablet_transition_stage::restore:
            return tinfo.replicas;
    }
    on_internal_error(logger, format("get_redirectable_tablet_replicas: unknown tablet transition stage {}",
            static_cast<int>(trinfo->stage)));
}

// How long to wait for the locally reported leader to change before giving up on it
// being the stale side and rebuilding the request's view of the replica set instead.
static constexpr auto stale_leader_grace = std::chrono::seconds(1);

// Waits out a leader reported by the local raft server that is not among the replicas
// the tablet's current transition stage allows to be the leader.
//
// The condition is transient rather than impossible: current_leader() on a follower is
// the last leader it heard from, so a replica that a migration has just removed from the
// raft group keeps being reported until this replica's election timeout fires and it
// starts an election of its own. Redirecting the request to such a replica would send it
// to a node that doesn't host the tablet anymore, so wait for the reported leader to
// change and let the caller retry. The wait is bounded by the request's abort source.
//
// The leader is polled rather than waited on, because there is no event to wait for:
// wait_for_state_change() only fires when the local server changes role, and a follower
// that learns of a new leader stays a follower - raft updates current_leader() in place.
//
// The wait is also bounded by a short grace period, because the stale side may just as
// well be the caller's own replica set, which is a snapshot taken when its operation
// context was created. The caller rebuilds that context afterwards, which is what
// resolves the case where the reported leader is the one telling the truth.
//
// Always sleeps at least once, so that a caller which keeps being handed a leader it
// cannot use retries at the backoff's pace and reaches its deadline, rather than
// spinning. Nothing guarantees that the leader the caller was given is the one this
// server reports now - an error injection reports one that never matches - so the poll
// below decides only how long to wait, never whether to wait at all.
static future<> wait_out_stale_leader(raft::server& server, const schema& s,
        locator::tablet_id tablet_id, locator::host_id leader,
        const locator::tablet_replica_set& replicas, abort_source& as) {
    logger.debug("table {}.{}, tablet {}: reported leader {} cannot be the leader in the current "
        "transition stage, replicas {}, waiting for a new leader",
        s.ks_name(), s.cf_name(), tablet_id, leader, replicas);

    const auto stale = raft::server_id{leader.uuid()};
    const auto grace_deadline = lowres_clock::now() + stale_leader_grace;
    auto retry = exponential_backoff_retry(10ms, 100ms);

    do {
        co_await retry.retry(as);
    } while (server.current_leader() == stale && lowres_clock::now() < grace_deadline);
}

struct coordinator::operation_ctx {
    locator::effective_replication_map_ptr erm;
    raft_server raft_server;
    locator::tablet_id tablet_id;
    const locator::tablet_raft_info& raft_info;
    locator::tablet_replica_set replicas;
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
        .on_forwarding_finished = [container = &gm.container(), group_id] (locator::host_id_or_exception leader) {
            if (std::holds_alternative<locator::host_id>(leader)) {
                container->local().leader_cache().put(group_id, std::get<locator::host_id>(leader));
            } else {
                container->local().leader_cache().erase(group_id);
            }
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
    const auto* trinfo = tablet_map.get_tablet_transition_info(tablet_id);
    auto replicas = get_redirectable_tablet_replicas(tablet_info, trinfo);

    if (!contains(replicas, this_replica)) {
        // For writes, check the leader cache to avoid an extra roundtrip.
        // For now, reads skip the cache because any replica can serve them.
        if (use_leader_cache) {
            if (const auto cached = _groups_manager.leader_cache().get(raft_info.group_id)) {
                if (const auto* target = find_replica(replicas, *cached); target && _gossiper.is_alive(target->host)) {
                    return make_ready_future<value_or_redirect<operation_ctx>>(
                        redirect_to_leader(*target, _groups_manager, raft_info.group_id));
                }
                // Cached leader is no longer a replica/alive, evict it.
                _groups_manager.leader_cache().erase(raft_info.group_id);
            }
        }
        auto target = select_closest_replica(_gossiper, replicas, token,
                erm->get_token_metadata().get_topology());
        if (use_leader_cache) {
            return make_ready_future<value_or_redirect<operation_ctx>>(
                redirect_to_leader(target, _groups_manager, raft_info.group_id));
        }
        return make_ready_future<value_or_redirect<operation_ctx>>(redirect_to_replica(target));
    }

    return utils::get_local_injector().inject(
        "sc_coordinator_wait_before_acquire_server", utils::wait_for_message(5min)
    ).then([this, tid = schema.id(), &raft_info, &as] {
        return _groups_manager.acquire_server(tid, raft_info.group_id, as);
    }).then([erm = std::move(erm), tablet_id, &raft_info, replicas = std::move(replicas)] (raft_server server) mutable {
        return make_ready_future<value_or_redirect<operation_ctx>>(operation_ctx {
            .erm = std::move(erm),
            .raft_server = std::move(server),
            .tablet_id = tablet_id,
            .raft_info = raft_info,
            .replicas = std::move(replicas)
        });
    });
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
    [[maybe_unused]] const auto sub = utils::chain_abort_source(aoe.abort_source(), as);

    utils::latency_counter lc;
    lc.start();
    auto mark_write_latency = defer([this, &lc] noexcept { _stats.write.mark(lc.stop().latency()); });

    locator::tablet_id tid{-1};
    raft::term_t term;

    auto filter_error = [&] (std::exception_ptr ex) -> std::exception_ptr {
        // Unfortunately, timeouts can materialize in different forms depending
        // on which statement throws the exception.
        //
        // * raft::request_aborted: If the abort source passed to a raft::server's
        //     method was triggered.
        // * seastar::abort_requested_exception: Can be thrown by create_operation_ctx.
        // * timed_out_error: Can be thrown by the abort_on_expiry.
        // * raft::stopped_error: The raft server was aborted (e.g. table being dropped).
        //
        // We handle them collectively here.
        if (try_catch<raft::request_aborted>(ex)
                || try_catch<seastar::abort_requested_exception>(ex)
                || try_catch<seastar::timed_out_error>(ex)
                || try_catch<raft::stopped_error>(ex)) {
            if (!_db.column_family_exists(schema->id())) {
                return std::make_exception_ptr(replica::no_such_column_family(schema->ks_name(), schema->cf_name()));
            }
            logger.trace("mutate(): request timed out with error {}, table {}.{}, token {}",
                ex, schema->ks_name(), schema->cf_name(), token);
            ++_stats.write_errors_timeout;
            return std::make_exception_ptr(write_timeout(schema->ks_name(), schema->cf_name()));
        } else if (try_catch<raft::commit_status_unknown>(ex)) {
            logger.debug("mutate(): add_entry, got commit_status_unknown {}, table {}.{}, tablet {}, term {}",
                ex, schema->ks_name(), schema->cf_name(), tid, term);

            ++_stats.write_errors_status_unknown;
            // FIXME: use a dedicated ERROR_CODE instead of SERVER_ERROR
            return std::make_exception_ptr(exceptions::server_exception(
                "The outcome of this statement is unknown. It may or may not have been applied. "
                "Retrying the statement may be necessary."));
        } else {
            ++_stats.write_errors_other;
            logger.trace("mutate(): unknown exception {}, table {}.{}, token {}",
                ex, schema->ks_name(), schema->cf_name(), token);
            // We know nothing about other errors. Let the CQL server convert them to SERVER_ERROR.
            return ex;
        }
    };

    // The operation context snapshots the replica set when it is created, so it is
    // rebuilt whenever the request has to retry against a fresh view of the topology.
    std::optional<operation_ctx> op_storage;
    bool build_ctx = true;

    while (true) {
        if (build_ctx) {
            build_ctx = false;
            auto op_result_future = co_await coroutine::as_future(
                    create_operation_ctx(*schema, token, aoe.abort_source(), true));

            if (op_result_future.failed()) {
                co_await coroutine::return_exception_ptr(filter_error(std::move(op_result_future).get_exception()));
            }

            auto op_result = std::move(op_result_future).get();

            if (auto* redirect = get_if<need_redirect>(&op_result)) {
                co_return std::move(*redirect);
            }
            op_storage.emplace(std::move(get<operation_ctx>(op_result)));
        }
        auto& op = *op_storage;

        co_await utils::get_local_injector().inject("sc_coordinator_wait_before_begin_mutate",
            utils::wait_for_message(5min));

        auto disposition = op.raft_server.begin_mutate(aoe.abort_source());
        if (const auto* not_a_leader = get_if<raft::not_a_leader>(&disposition)) {
            const auto leader_host_id = locator::host_id{not_a_leader->leader.uuid()};
            const auto* target = find_replica(op.replicas, leader_host_id);
            if (!target) {
                auto f = co_await coroutine::as_future(wait_out_stale_leader(op.raft_server.server(),
                        *schema, op.tablet_id, leader_host_id, op.replicas, aoe.abort_source()));
                if (f.failed()) {
                    co_await coroutine::return_exception_ptr(filter_error(std::move(f).get_exception()));
                }
                // Either the leader was stale and has changed by now, or our own
                // replica set is what went stale - rebuilding decides which.
                build_ctx = true;
                continue;
            }
            co_return redirect_to_leader(*target, _groups_manager, op.raft_info.group_id);
        }
        if (auto* wait_for_leader = get_if<raft_server::need_wait_for_leader>(&disposition)) {
            auto f = co_await coroutine::as_future(std::move(wait_for_leader->future));
            if (f.failed()) {
                co_await coroutine::return_exception_ptr(filter_error(std::move(f).get_exception()));
            }
            continue;
        }

        api::timestamp_type ts;
        auto disposition_result = get<raft_server::timestamp_with_term>(disposition);
        std::tie(ts, term) = {disposition_result.timestamp, disposition_result.term};

        const raft_command command {
            .mutation{mutation_gen(ts)}
        };
        raft::command raft_cmd;
        ser::serialize(raft_cmd, command);

        logger.debug("mutate(): add_entry({}), term {}",
            command.mutation.pretty_printer(schema), term);

        co_await utils::get_local_injector().inject("sc_coordinator_wait_before_add_entry",
            utils::wait_for_message(5min));

        future<> add_entry_result = co_await coroutine::as_future(
            op.raft_server.server().add_entry(std::move(raft_cmd),
                raft::wait_type::committed,
                &aoe.abort_source()));

        if (!add_entry_result.failed()) {
            co_return std::monostate{};
        }

        auto ex = std::move(add_entry_result).get_exception();
        if (try_catch<raft::not_a_leader>(ex) || try_catch<raft::dropped_entry>(ex)) {
            logger.debug("mutate(): add_entry, got retriable error {}, table {}.{}, tablet {}, term {}",
                ex, schema->ks_name(), schema->cf_name(), op.tablet_id, term);

            continue;
        }

        co_await coroutine::return_exception_ptr(filter_error(std::move(ex)));
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
    [[maybe_unused]] const auto sub = utils::chain_abort_source(aoe.abort_source(), as);

    utils::latency_counter lc;
    lc.start();

    auto& read_stats = (rtype == read_type::linearizable)
        ? _stats.linearizable_read : _stats.non_linearizable_read;
    auto mark_read_latency = defer([&read_stats, &lc] () mutable noexcept { read_stats.mark(lc.stop().latency()); });

    auto filter_error = [&] (std::exception_ptr ex) -> std::exception_ptr {
        // Unfortunately, timeouts can materialize in different forms depending
        // on which statement throws the exception.
        //
        // * raft::request_aborted: If the abort source passed to a raft::server's
        //     method was triggered.
        // * seastar::abort_requested_exception: Can be thrown by create_operation_ctx.
        // * timed_out_error: Can be thrown by the abort_on_expiry.
        // * raft::stopped_error: The raft server was aborted (e.g. table being dropped).
        //
        // We handle them collectively here.
        if (try_catch<raft::request_aborted>(ex)
                || try_catch<seastar::abort_requested_exception>(ex)
                || try_catch<timed_out_error>(ex)
                || try_catch<raft::stopped_error>(ex)) {
            if (!_db.column_family_exists(schema->id())) {
                return std::make_exception_ptr(replica::no_such_column_family(schema->ks_name(), schema->cf_name()));
            }
            logger.trace("query(): request timed out with error {}, table {}.{}, read cmd {}",
                ex, schema->ks_name(), schema->cf_name(), cmd);
            ++_stats.read_errors_timeout;
            return std::make_exception_ptr(read_timeout(schema->ks_name(), schema->cf_name()));
        } else {
            logger.trace("query(): unknown exception {}, table {}.{}, read cmd {}",
                ex, schema->ks_name(), schema->cf_name(), cmd);
            ++_stats.read_errors_other;
            // We know nothing about other errors. Let the CQL server convert them to SERVER_ERROR.
            return ex;
        }
    };

    // The operation context snapshots the replica set when it is created, so it is
    // rebuilt whenever the request has to retry against a fresh view of the topology.
    std::optional<operation_ctx> op_storage;
    bool build_ctx = true;

    if (rtype == read_type::linearizable) {
        // For linearizable reads we may need to forward to the raft leader.
        while (true) {
            if (build_ctx) {
                build_ctx = false;
                auto f = co_await coroutine::as_future(create_operation_ctx(
                    *schema, ranges[0].start()->value().token(), aoe.abort_source(), true));
                if (f.failed()) {
                    co_await coroutine::return_exception_ptr(filter_error(std::move(f).get_exception()));
                }
                auto result = std::move(f).get();
                if (auto* redirect = get_if<need_redirect>(&result)) {
                    co_return std::move(*redirect);
                }
                op_storage.emplace(std::move(get<operation_ctx>(result)));
            }
            auto& op = *op_storage;

            auto disposition = op.raft_server.begin_read(aoe.abort_source());
            if (const auto* not_a_leader = get_if<raft::not_a_leader>(&disposition)) {
                const auto leader_host_id = locator::host_id{not_a_leader->leader.uuid()};
                const auto* target = find_replica(op.replicas, leader_host_id);
                if (!target) {
                    future<> f = co_await coroutine::as_future(wait_out_stale_leader(op.raft_server.server(),
                            *schema, op.tablet_id, leader_host_id, op.replicas, aoe.abort_source()));
                    if (f.failed()) {
                        co_await coroutine::return_exception_ptr(filter_error(std::move(f).get_exception()));
                    }
                    // Either the leader was stale and has changed by now, or our own
                    // replica set is what went stale - rebuilding decides which.
                    build_ctx = true;
                    continue;
                }
                co_return redirect_to_leader(*target, _groups_manager, op.raft_info.group_id);
            }
            if (auto* wait_for_leader = get_if<raft_server::need_wait_for_leader>(&disposition)) {
                future<> f = co_await coroutine::as_future(std::move(wait_for_leader->future));
                if (f.failed()) {
                    co_await coroutine::return_exception_ptr(filter_error(std::move(f).get_exception()));
                }
                continue;
            }
            break;
        }
    } else {
        auto f = co_await coroutine::as_future(create_operation_ctx(
            *schema, ranges[0].start()->value().token(), aoe.abort_source(), false));
        if (f.failed()) {
            co_await coroutine::return_exception_ptr(filter_error(std::move(f).get_exception()));
        }
        auto result = std::move(f).get();
        if (auto* redirect = get_if<need_redirect>(&result)) {
            co_return std::move(*redirect);
        }
        op_storage.emplace(std::move(get<operation_ctx>(result)));
    }
    auto& op = *op_storage;

    if (rtype == read_type::linearizable) {
        co_await utils::get_local_injector().inject("sc_coordinator_wait_before_query_read_barrier",
            utils::wait_for_message(5min));

        future<> f = co_await coroutine::as_future(op.raft_server.server().read_barrier(&aoe.abort_source()));
        if (f.failed()) {
            co_await coroutine::return_exception_ptr(filter_error(std::move(f).get_exception()));
        }
    }

    // We're either a raft leader or it's a non-linearizable read. In both cases we can directly execute the read on this replica.
    auto query_future = co_await coroutine::as_future(_db.query(schema, cmd,
        query::result_options::only_result(), ranges, trace_state, timeout));

    if (query_future.failed()) {
        co_await coroutine::return_exception_ptr(filter_error(std::move(query_future).get_exception()));
    }

    auto [result, cache_temp] = std::move(query_future).get();
    co_return std::move(result);
}

future<> coordinator::wait_for_table_raft_groups_on_all_hosts(table_id table, lowres_clock::time_point timeout) {
    return _groups_manager.wait_for_table_raft_groups_on_all_hosts(table, timeout);
}

}
