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
#include "utils/histogram_metrics_helper.hh"
#include "utils/abstract_formatter.hh"

#include <fmt/std.h>

#include <algorithm>
#include <concepts>
#include <span>

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

// Answers, for one request to a strongly consistent tablet, which replicas the request
// may be served by, at the stage the tablet's migration is currently in.
//
// The answer depends on what the request needs. A request that has to reach the raft
// leader may be sent to any replica that could currently be the leader: one that isn't
// answers not_a_leader and names it, and the request is redirected. A read served from
// local storage has no such second chance: a replica that doesn't have the data yet
// returns a wrong result rather than a redirect, so it may only go to a replica that holds
// the data. The two sets differ during a migration, and the second is a subset of the
// first: a replica that holds the data can always be redirected to.
//
// Either set is made of the replica lists the tablet's metadata already holds, so nothing
// is copied: the selector refers to those lists and looks replicas up in place. It owns
// the effective replication map it was built from, which keeps that metadata alive.
class coordinator::replica_selector {
    // One of the tablet's replica lists, optionally followed by the migration's pending
    // replica.
    struct replica_set_view {
        std::span<const locator::tablet_replica> base;
        const locator::tablet_replica* extra = nullptr;

        // The first replica that satisfies `pred`, or null.
        template <std::predicate<const locator::tablet_replica&> Pred>
        const locator::tablet_replica* find_if(Pred pred) const {
            if (const auto it = std::ranges::find_if(base, pred); it != base.end()) {
                return &*it;
            }
            return extra && pred(*extra) ? extra : nullptr;
        }

        template <std::invocable<const locator::tablet_replica&> Func>
        void for_each(Func func) const {
            std::ranges::for_each(base, func);
            if (extra) {
                func(*extra);
            }
        }

        locator::tablet_replica_set materialize() const {
            locator::tablet_replica_set replicas(base.begin(), base.end());
            if (extra) {
                replicas.push_back(*extra);
            }
            return replicas;
        }
    };

    static replica_set_view current(const locator::tablet_info& tinfo) {
        return {.base = tinfo.replicas};
    }

    static replica_set_view next(const locator::tablet_transition_info& trinfo) {
        return {.base = trinfo.next};
    }

    // A transition may have no pending replica: a rebuild that only drops a replica, which
    // is what lowering the replication factor schedules, goes through the same stages as
    // a migration.
    //
    // FIXME: of the transitions that aren't migrations, only a rebuild raising the
    // replication factor is tested with strongly consistent tablets. Rebuilds for
    // lowering it, for removenode and for replace have to be tested or refused.
    static replica_set_view current_plus_pending(const locator::tablet_info& tinfo,
            const locator::tablet_transition_info& trinfo) {
        return {
            .base = tinfo.replicas,
            .extra = trinfo.pending_replica ? &*trinfo.pending_replica : nullptr,
        };
    }

    [[noreturn]] static void on_unexpected_stage(const char* func, locator::tablet_transition_stage stage) {
        on_internal_error(logger, format("replica_selector::{}: unexpected transition stage {} "
                "of a strongly consistent tablet", func, stage));
    }

    // Replicas that may currently be the raft group's leader. This is the wider set,
    // because it has to name every replica a redirect could legitimately come from - a
    // leader missing from it would leave the request with nowhere to go.
    static replica_set_view leader_capable(const locator::tablet_info& tinfo,
            const locator::tablet_transition_info* trinfo) {
        if (!trinfo) {
            return current(tinfo);
        }

        // The pending replica joins the raft group as a non-voter at sc_add_nonvoter and
        // is promoted at sc_become_voter, so it can win an election only from that stage
        // on. A node that still sees an earlier stage can't be looking at a group where
        // the promotion has already happened: the topology coordinator runs it only after
        // the global barrier of sc_become_voter, which waits until no node holds a view
        // of an earlier stage.
        using enum locator::tablet_transition_stage;
        switch (trinfo->stage) {
            case start_migration:
            case sc_add_nonvoter:
            case sc_snapshot_transfer:
                return current(tinfo);

            case sc_become_voter:
                return current_plus_pending(tinfo, *trinfo);

            case use_new:
                // The precondition of use_new is that the leaving replica is no longer a
                // member of the raft group, so it can no longer be the leader.
            case cleanup:
            case end_migration:
                return next(*trinfo);

            case sc_rollback:
                // A rollback may be entered from sc_become_voter, where the pending
                // replica can already be a voter and the leader driving its own removal.
                return current_plus_pending(tinfo, *trinfo);

            case cleanup_target:
                // The precondition of cleanup_target is that the pending replica is no
                // longer a member of the raft group.
            case revert_migration:
                return current(tinfo);

            case write_both_read_old_fallback_cleanup:
                // A strongly consistent migration that fails at sc_become_voter rolls
                // back through sc_rollback instead.
            case rebuild_repair:
                // A strongly consistent rebuild transfers a raft snapshot at
                // sc_snapshot_transfer instead.
            case repair:
            case end_repair:
                // A strongly consistent tablet needs no repair: raft keeps its replicas
                // in sync.
            case restore:
                // FIXME: nothing refuses to schedule a repair or a restore of a strongly
                // consistent tablet yet.
                break;
        }
        on_unexpected_stage("leader_capable", trinfo->stage);
    }

    // Replicas that hold the tablet's data at this stage.
    //
    // The pending replica joins this set at sc_become_voter and not before. That stage
    // is published only after the snapshot transfer completed on it, which ends in a
    // raft read barrier, which ends in waiting for the local state machine to apply up
    // to the leader's read index. So from sc_become_voter on, the pending replica's
    // staleness is ordinary follower lag; before it, nothing bounds it.
    //
    // Always a subset of leader_capable() for the same stage.
    static replica_set_view readable(const locator::tablet_info& tinfo,
            const locator::tablet_transition_info* trinfo) {
        if (!trinfo) {
            return current(tinfo);
        }

        using enum locator::tablet_transition_stage;
        switch (trinfo->stage) {
            case start_migration:
            case sc_add_nonvoter:
            case sc_snapshot_transfer:
                // The pending replica is a member of the group by now, but nothing
                // bounds how far behind it is until the snapshot transfer of
                // sc_snapshot_transfer has completed - which is what publishing the next
                // stage attests to.
                return current(tinfo);

            case sc_become_voter:
            case use_new:
            case cleanup:
            case end_migration:
                // The leaving replica still holds the data until the cleanup, but it is
                // left out from here on so that this stays one set with the routing
                // information drivers are given, and because by use_new it has torn its
                // raft server down.
                return next(*trinfo);

            case sc_rollback:
            case cleanup_target:
            case revert_migration:
                // The rollback path keeps the old replica set, which never stopped
                // holding the data.
                return current(tinfo);

            case write_both_read_old_fallback_cleanup:
            case rebuild_repair:
            case repair:
            case end_repair:
            case restore:
                // See leader_capable().
                break;
        }
        on_unexpected_stage("readable", trinfo->stage);
    }

    locator::effective_replication_map_ptr _erm;
    const locator::tablet_map& _tablet_map;
    locator::tablet_id _tablet_id;
    const locator::tablet_info& _tablet_info;
    // Looked up once, because finding a tablet's transition is a hash lookup and a
    // request asks about its replicas more than once.
    const locator::tablet_transition_info* _trinfo;
    // The replicas the request may be served by.
    replica_set_view _serving;

public:
    // `needs_leader` says whether the request has to be executed by the raft group's
    // leader, which is true for writes and linearizable reads and false for a read that
    // is served from local storage.
    replica_selector(locator::effective_replication_map_ptr erm, table_id table, const dht::token& token,
            bool needs_leader)
        : _erm(std::move(erm))
        , _tablet_map(_erm->get_token_metadata().tablets().get_tablet_map(table))
        , _tablet_id(_tablet_map.get_tablet_id(token))
        , _tablet_info(_tablet_map.get_tablet_info(_tablet_id))
        , _trinfo(_tablet_map.get_tablet_transition_info(_tablet_id))
        , _serving(needs_leader ? leader_capable(_tablet_info, _trinfo) : readable(_tablet_info, _trinfo))
    {}

    locator::tablet_id tablet_id() const {
        return _tablet_id;
    }

    raft::group_id group_id() const {
        return _tablet_map.get_tablet_raft_info(_tablet_id).group_id;
    }

    // The tablet's migration stage, for log messages; nullopt when it isn't migrating.
    std::optional<locator::tablet_transition_stage> transition_stage() const {
        return _trinfo ? std::make_optional(_trinfo->stage) : std::nullopt;
    }

    locator::host_id my_host_id() const {
        return _erm->get_token_metadata().get_my_id();
    }

    // Whether the request may be served by this shard.
    bool may_serve_here() const {
        const auto this_replica = locator::tablet_replica{
            .host = _erm->get_token_metadata().get_my_id(),
            .shard = this_shard_id(),
        };
        return _serving.find_if([&] (const locator::tablet_replica& r) { return r == this_replica; }) != nullptr;
    }

    // The replica on `host` the request may be served by, or null if there is none.
    const locator::tablet_replica* find_replica(locator::host_id host) const {
        return _serving.find_if([host] (const locator::tablet_replica& r) { return r.host == host; });
    }

    // The live replica the request may be served by that is closest to this node,
    // preferring the same rack, other than one on `exclude`. Throws unavailable_exception
    // if none is alive: there is no node worth forwarding to.
    locator::tablet_replica closest_replica(const gms::gossiper& gossiper,
            std::optional<locator::host_id> exclude = std::nullopt) const {
        // sort_by_proximity() works on hosts, so the replica is looked up again after it.
        host_id_vector_replica_set hosts;
        _serving.for_each([&] (const locator::tablet_replica& replica) {
            if (replica.host != exclude && gossiper.is_alive(replica.host)) {
                hosts.push_back(replica.host);
            }
        });

        if (hosts.empty()) {
            throw exceptions::unavailable_exception(format("All replicas of tablet {} are down", _tablet_id),
                    db::consistency_level::ONE, 1, 0);
        }
        const auto& topo = _erm->get_token_metadata().get_topology();
        topo.sort_by_proximity(topo.my_host_id(), hosts);
        return *find_replica(hosts.front());
    }

    // The routing information to hand back to a driver that sent `block` with the
    // request, or nullopt if the block matches the tablet's current version. That version
    // is a hash of the replicas that hold the data, rotated to start with `leader`, so it
    // is the same set a read is willing to be served from locally, and where a driver is
    // told to go and where it will actually be answered agree. Nullopt as well when
    // `leader` doesn't hold the data, or isn't known.
    std::optional<locator::tablet_routing_info_v2> routing_info(raft::server_id leader,
            locator::tablet_version_block block) const {
        auto replicas = readable(_tablet_info, _trinfo).materialize();
        std::ranges::sort(replicas);
        const auto leader_it = std::ranges::find(replicas, locator::host_id{leader.uuid()}, &locator::tablet_replica::host);
        if (leader_it == replicas.end()) [[unlikely]] {
            return std::nullopt;
        }
        std::ranges::rotate(replicas, leader_it);

        const auto hash = locator::internal::hash_replica_list(replicas);
        if (locator::compare_tablet_version_block(hash, block)) [[likely]] {
            return std::nullopt;
        }

        const dht::token first_token = (_tablet_id == _tablet_map.first_tablet())
                ? dht::minimum_token()
                : _tablet_map.get_last_token(locator::tablet_id(size_t(_tablet_id) - 1));
        const dht::token last_token = _tablet_map.get_last_token(_tablet_id);

        return locator::tablet_routing_info_v2{
            .tablet_replicas = std::move(replicas),
            .token_range = std::make_pair(first_token, last_token),
            .hash = hash,
        };
    }
};

struct coordinator::operation_ctx {
    replica_selector replicas;
    raft_server raft_server;
};

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

auto coordinator::create_operation_ctx(const schema& schema, const dht::token& token, abort_source& as, bool needs_leader)
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

    replica_selector replicas(std::move(erm), schema.id(), token, needs_leader);

    if (!replicas.may_serve_here()) {
        return make_ready_future<value_or_redirect<operation_ctx>>(redirect_elsewhere(replicas, needs_leader));
    }

    return utils::get_local_injector().inject(
        "sc_coordinator_wait_before_acquire_server", utils::wait_for_message(5min)
    ).then([this, tid = schema.id(), group_id = replicas.group_id(), &as] {
        return _groups_manager.acquire_server(tid, group_id, as);
    }).then([replicas = std::move(replicas)] (raft_server server) mutable {
        return make_ready_future<value_or_redirect<operation_ctx>>(operation_ctx {
            .replicas = std::move(replicas),
            .raft_server = std::move(server),
        });
    });
}

need_redirect coordinator::redirect_elsewhere(const replica_selector& replicas, bool needs_leader,
        std::optional<locator::host_id> exclude) {
    const auto group_id = replicas.group_id();
    // For writes, check the leader cache to avoid an extra roundtrip.
    // For now, reads skip the cache because any replica holding the data can serve them.
    if (needs_leader) {
        if (const auto cached = _groups_manager.leader_cache().get(group_id)) {
            if (const auto* target = replicas.find_replica(*cached);
                    target && target->host != exclude && _gossiper.is_alive(target->host)) {
                return redirect_to_leader(*target, _groups_manager, group_id);
            }
            // Cached leader is no longer a replica/alive, evict it.
            _groups_manager.leader_cache().erase(group_id);
        }
        return redirect_to_leader(replicas.closest_replica(_gossiper, exclude), _groups_manager, group_id);
    }
    // Bounced to a replica that holds the data, never merely to one that could be
    // the leader - the target serves the read itself, so it has to be able to.
    return redirect_to_replica(replicas.closest_replica(_gossiper, exclude));
}

need_redirect coordinator::redirect_from_non_member(const schema& schema, const replica_selector& replicas) {
    const auto my_id = replicas.my_host_id();
    logger.debug("table {}.{}, tablet {}: this replica is not a member of the raft group, "
            "redirecting the request, transition stage {}",
            schema.ks_name(), schema.cf_name(), replicas.tablet_id(), replicas.transition_stage());
    return redirect_elsewhere(replicas, true, my_id);
}

std::optional<need_redirect> coordinator::reroute_after_teardown(std::exception_ptr ex, const schema& schema,
        const dht::token& token, bool needs_leader) {
    if (!try_catch<raft::stopped_error>(ex) || !_db.column_family_exists(schema.id())) {
        return std::nullopt;
    }
    replica_selector replicas(schema.table().get_effective_replication_map(), schema.id(), token, needs_leader);
    if (replicas.may_serve_here()) {
        return std::nullopt;
    }
    logger.debug("table {}.{}, tablet {}: the raft server was torn down while waiting for a leader, "
            "rerouting the request, transition stage {}",
            schema.ks_name(), schema.cf_name(), replicas.tablet_id(), replicas.transition_stage());
    return redirect_elsewhere(replicas, needs_leader);
}

coordinator::coordinator(groups_manager& groups_manager, replica::database& db, gms::gossiper& gossiper)
    : _groups_manager(groups_manager)
    , _db(db)
    , _gossiper(gossiper)
{
    _stats.register_stats();
}

auto coordinator::mutate(schema_ptr schema,
        const dht::token& token,
        mutation_gen&& mutation_gen,
        timeout_clock::time_point timeout,
        abort_source& as,
        std::optional<locator::tablet_version_block> tablet_version_block)
    -> future<value_or_redirect<mutate_result>>
{
    auto aoe = abort_on_expiry<timeout_clock>(timeout);
    [[maybe_unused]] const auto sub = utils::chain_abort_source(aoe.abort_source(), as);

    utils::latency_counter lc;
    lc.start();
    auto mark_write_latency = defer([this, &lc] noexcept { _stats.write.mark(lc.stop().latency()); });

    // State of the request as it advances through its stages. Each pointer
    // is null until the corresponding stage has been reached, so log messages
    // never print stale or uninitialized values.
    operation_ctx* op = nullptr;
    const raft_server::timestamp_with_term* ts_with_term = nullptr;

    // Rendered only when a log line is actually emitted.
    const auto state_fmt = lambda_formatter([&] (fmt::format_context& ctx) {
        if (!op) {
            fmt::format_to(ctx.out(), "no operation context yet");
        } else if (!ts_with_term) {
            fmt::format_to(ctx.out(), "tablet {}, no timestamp yet", op->replicas.tablet_id());
        } else {
            fmt::format_to(ctx.out(), "tablet {}, term {}, timestamp {}",
                    op->replicas.tablet_id(), ts_with_term->term, ts_with_term->timestamp);
        }
    });

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
            logger.trace("mutate(): request timed out with error {}, table {}.{}, token {}, {}",
                ex, schema->ks_name(), schema->cf_name(), token, state_fmt);
            ++_stats.write_errors_timeout;
            return std::make_exception_ptr(write_timeout(schema->ks_name(), schema->cf_name()));
        } else if (try_catch<raft::commit_status_unknown>(ex)) {
            logger.debug("mutate(): add_entry, got commit_status_unknown {}, table {}.{}, {}",
                ex, schema->ks_name(), schema->cf_name(), state_fmt);

            ++_stats.write_errors_status_unknown;
            // FIXME: use a dedicated ERROR_CODE instead of SERVER_ERROR
            return std::make_exception_ptr(exceptions::server_exception(
                "The outcome of this statement is unknown. It may or may not have been applied. "
                "Retrying the statement may be necessary."));
        } else if (auto* too_big = try_catch<raft::command_is_too_big_error>(ex)) {
            logger.trace("mutate(): command of {} bytes exceeds the limit of {}, table {}.{}, token {}",
                too_big->command_size, too_big->limit, schema->ks_name(), schema->cf_name(), token);
            ++_stats.write_errors_other;
            return std::make_exception_ptr(exceptions::invalid_request_exception(fmt::format(
                "Strongly consistent write of {} bytes exceeds the limit of {} bytes", too_big->command_size, too_big->limit)));
        } else {
            ++_stats.write_errors_other;
            logger.trace("mutate(): unknown exception {}, table {}.{}, token {}, {}",
                ex, schema->ks_name(), schema->cf_name(), token, state_fmt);
            // We know nothing about other errors. Let the CQL server convert them to SERVER_ERROR.
            return ex;
        }
    };

    auto op_result_future = co_await coroutine::as_future(
            create_operation_ctx(*schema, token, aoe.abort_source(), true));

    if (op_result_future.failed()) {
        co_await coroutine::return_exception_ptr(filter_error(std::move(op_result_future).get_exception()));
    }

    auto op_result = std::move(op_result_future).get();

    if (auto* redirect = get_if<need_redirect>(&op_result)) {
        co_return std::move(*redirect);
    }
    op = &get<operation_ctx>(op_result);

    while (true) {
        // `disposition` below is local to one iteration, so the pointer into
        // it must not survive into the next one.
        ts_with_term = nullptr;

        // A retry may come straight back here without suspending on anything that
        // observes the deadline, so observe it before retrying.
        if (aoe.abort_source().abort_requested()) {
            co_await coroutine::return_exception_ptr(filter_error(aoe.abort_source().abort_requested_exception_ptr()));
        }

        co_await utils::get_local_injector().inject("sc_coordinator_wait_before_begin_mutate",
            utils::wait_for_message(5min));

        auto disposition = op->raft_server.begin_mutate(aoe.abort_source());
        if (const auto* not_a_leader = get_if<raft::not_a_leader>(&disposition)) {
            const auto leader_host_id = locator::host_id{not_a_leader->leader.uuid()};
            const auto* target = op->replicas.find_replica(leader_host_id);
            if (!target) {
                // The leader the local raft server reports is not among the replicas the
                // tablet's current transition stage allows to be the leader, which is a
                // stale report rather than an error: current_leader() on a follower is
                // the last leader it heard from, so a replica that a migration has just
                // removed from the raft group keeps being named until the new leader
                // contacts this one. There is nowhere to redirect to, so tell the local
                // server to forget that leader and wait for the next one.
                //
                // The wait may well resolve with the same leader again: a message it sent
                // before it stepped down can still arrive and re-set current_leader(). We
                // retry with the same operation context, and the retries make progress
                // rather than spin. The context holds the effective replication map,
                // which blocks the global barrier of every later transition stage, and
                // with it the configuration change that could make a replica outside the
                // set the leader. So the change that removed the reported leader has
                // already committed, and all that is left is for it to reach this node.
                logger.debug("mutate(): table {}.{}, tablet {}, reported leader {} cannot be the leader "
                    "in transition stage {}, waiting for a new leader",
                    schema->ks_name(), schema->cf_name(), op->replicas.tablet_id(), leader_host_id,
                    op->replicas.transition_stage());

                auto f = co_await coroutine::as_future(
                        op->raft_server.server().wait_for_leader(&aoe.abort_source(), true));
                if (f.failed()) {
                    auto ex = std::move(f).get_exception();
                    if (auto redirect = reroute_after_teardown(ex, *schema, token, true)) {
                        co_return std::move(*redirect);
                    }
                    co_await coroutine::return_exception_ptr(filter_error(std::move(ex)));
                }
                continue;
            }
            co_return redirect_to_leader(*target, _groups_manager, op->replicas.group_id());
        }
        if (holds_alternative<raft_server::not_a_member>(disposition)) {
            co_return redirect_from_non_member(*schema, op->replicas);
        }
        if (auto* wait_for_leader = get_if<raft_server::need_wait_for_leader>(&disposition)) {
            logger.debug("mutate(): table {}.{}, {}: waiting for a leader", schema->ks_name(), schema->cf_name(), state_fmt);
            auto f = co_await coroutine::as_future(std::move(wait_for_leader->future));
            if (f.failed()) {
                auto ex = std::move(f).get_exception();
                if (auto redirect = reroute_after_teardown(ex, *schema, token, true)) {
                    co_return std::move(*redirect);
                }
                co_await coroutine::return_exception_ptr(filter_error(std::move(ex)));
            }
            continue;
        }

        ts_with_term = &get<raft_server::timestamp_with_term>(disposition);

        // Nothing between begin_mutate() above and add_entry() below may
        // suspend this coroutine, not even a co_await on a ready future
        // (Seastar suspends on those too when the task quota is exhausted).
        // begin_mutate() hands out timestamps in call order and raft appends
        // entries in add_entry() call order. If another write could slip in
        // between, the raft log order and the timestamp order would diverge:
        // a reader could observe the first log entry alone, and then, after
        // the second entry with the lower timestamp is applied, a state in
        // which cells from the second entry are visible while the first entry
        // still wins on the cells they share. Such a history is not
        // linearizable.
        const raft_command command {
            .mutation{mutation_gen(ts_with_term->timestamp)}
        };
        raft::command raft_cmd;
        ser::serialize(raft_cmd, command);

        logger.debug("mutate(): add_entry({}), {}",
            command.mutation.pretty_printer(schema), state_fmt);

        // CAUTION: If a preemption point gets added between `begin_mutate`
        // and `add_entry`, add an explicit check that the term has not
        // changed since `begin_mutate`.
        future<> add_entry_result = co_await coroutine::as_future(
            op->raft_server.server().add_entry(std::move(raft_cmd),
                raft::wait_type::committed,
                &aoe.abort_source()));

        if (!add_entry_result.failed()) {
            co_return mutate_result{
                .routing_info = tablet_version_block
                        ? op->replicas.routing_info(op->raft_server.server().current_leader(), *tablet_version_block)
                        : std::nullopt,
            };
        }

        auto ex = std::move(add_entry_result).get_exception();
        if (try_catch<raft::not_a_leader>(ex) || try_catch<raft::dropped_entry>(ex)) {
            logger.debug("mutate(): add_entry, got retriable error {}, table {}.{}, {}",
                ex, schema->ks_name(), schema->cf_name(), state_fmt);

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
        abort_source& as,
        std::optional<locator::tablet_version_block> tablet_version_block
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

    const auto& token = ranges[0].start()->value().token();
    auto op_result_future = co_await coroutine::as_future(create_operation_ctx(
        *schema,
        token,
        aoe.abort_source(),
        rtype == read_type::linearizable));

    if (op_result_future.failed()) {
        co_await coroutine::return_exception_ptr(filter_error(std::move(op_result_future).get_exception()));
    }

    auto op_result = std::move(op_result_future).get();

    if (auto* redirect = get_if<need_redirect>(&op_result)) {
        co_return std::move(*redirect);
    }
    auto& op = get<operation_ctx>(op_result);

    if (rtype == read_type::linearizable) {
        // For linearizable reads we may need to forward to the raft leader.
        while (true) {
            // See mutate() for why the deadline is checked here.
            if (aoe.abort_source().abort_requested()) {
                co_await coroutine::return_exception_ptr(filter_error(aoe.abort_source().abort_requested_exception_ptr()));
            }

            auto disposition = op.raft_server.begin_read(aoe.abort_source());
            if (const auto* not_a_leader = get_if<raft::not_a_leader>(&disposition)) {
                const auto leader_host_id = locator::host_id{not_a_leader->leader.uuid()};
                const auto* target = op.replicas.find_replica(leader_host_id);
                if (!target) {
                    // A leader outside the replica set the current transition stage allows
                    // is a stale report rather than an error: the local raft server keeps
                    // naming the leader a migration has just removed from the group until
                    // the new one contacts it. Forget it and wait for the next one. See
                    // mutate() for why retrying with the same operation context makes
                    // progress even when the wait resolves with the same leader again.
                    logger.debug("query(): table {}.{}, tablet {}, reported leader {} cannot be the leader "
                        "in transition stage {}, waiting for a new leader",
                        schema->ks_name(), schema->cf_name(), op.replicas.tablet_id(), leader_host_id,
                        op.replicas.transition_stage());

                    future<> f = co_await coroutine::as_future(
                            op.raft_server.server().wait_for_leader(&aoe.abort_source(), true));
                    if (f.failed()) {
                        auto ex = std::move(f).get_exception();
                        if (auto redirect = reroute_after_teardown(ex, *schema, token, true)) {
                            co_return std::move(*redirect);
                        }
                        co_await coroutine::return_exception_ptr(filter_error(std::move(ex)));
                    }
                    continue;
                }
                co_return redirect_to_leader(*target, _groups_manager, op.replicas.group_id());
            }
            if (holds_alternative<raft_server::not_a_member>(disposition)) {
                co_return redirect_from_non_member(*schema, op.replicas);
            }
            if (auto* wait_for_leader = get_if<raft_server::need_wait_for_leader>(&disposition)) {
                logger.debug("query(): table {}.{}, tablet {}: waiting for a leader",
                    schema->ks_name(), schema->cf_name(), op.replicas.tablet_id());
                future<> f = co_await coroutine::as_future(std::move(wait_for_leader->future));
                if (f.failed()) {
                    auto ex = std::move(f).get_exception();
                    if (auto redirect = reroute_after_teardown(ex, *schema, token, true)) {
                        co_return std::move(*redirect);
                    }
                    co_await coroutine::return_exception_ptr(filter_error(std::move(ex)));
                }
                continue;
            }
            break;
        }

        co_await utils::get_local_injector().inject("sc_coordinator_wait_before_query_read_barrier",
            utils::wait_for_message(5min));

        future<> f = co_await coroutine::as_future(op.raft_server.server().read_barrier(&aoe.abort_source()));
        if (f.failed()) {
            // read_barrier() finds the leader on its own, so it also waits for one when
            // the leader is unknown, e.g. after this replica, the leader, removed itself
            // from the group.
            auto ex = std::move(f).get_exception();
            if (auto redirect = reroute_after_teardown(ex, *schema, token, true)) {
                co_return std::move(*redirect);
            }
            co_await coroutine::return_exception_ptr(filter_error(std::move(ex)));
        }
    }

    // We're either a raft leader or it's a non-linearizable read. In both cases we can directly execute the read on this replica.
    auto query_future = co_await coroutine::as_future(_db.query(schema, cmd,
        query::result_options::only_result(), ranges, trace_state, timeout));

    if (query_future.failed()) {
        co_await coroutine::return_exception_ptr(filter_error(std::move(query_future).get_exception()));
    }

    auto [result, cache_temp] = std::move(query_future).get();
    co_return query_result{
        .result = std::move(result),
        .routing_info = tablet_version_block
                ? op.replicas.routing_info(op.raft_server.server().current_leader(), *tablet_version_block)
                : std::nullopt,
    };
}

future<> coordinator::wait_for_table_raft_groups_on_all_hosts(table_id table, lowres_clock::time_point timeout) {
    return _groups_manager.wait_for_table_raft_groups_on_all_hosts(table, timeout);
}

}
