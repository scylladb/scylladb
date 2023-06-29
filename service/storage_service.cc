/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "storage_service.hh"
#include "dht/boot_strapper.hh"
#include <seastar/core/distributed.hh>
#include <seastar/util/defer.hh>
#include <seastar/coroutine/as_future.hh>
#include "locator/snitch_base.hh"
#include "locator/production_snitch_base.hh"
#include "db/system_keyspace.hh"
#include "db/system_distributed_keyspace.hh"
#include "db/consistency_level.hh"
#include <seastar/core/smp.hh>
#include "utils/UUID.hh"
#include "gms/inet_address.hh"
#include "log.hh"
#include "service/migration_manager.hh"
#include "service/raft/raft_group0.hh"
#include "utils/to_string.hh"
#include "gms/gossiper.hh"
#include "gms/feature_service.hh"
#include <seastar/core/thread.hh>
#include <sstream>
#include <algorithm>
#include "locator/local_strategy.hh"
#include "version.hh"
#include "unimplemented.hh"
#include "streaming/stream_plan.hh"
#include "streaming/stream_state.hh"
#include "dht/range_streamer.hh"
#include <boost/range/adaptors.hpp>
#include <boost/range/algorithm.hpp>
#include "service/load_broadcaster.hh"
#include "transport/server.hh"
#include <seastar/core/rwlock.hh>
#include "db/batchlog_manager.hh"
#include "db/commitlog/commitlog.hh"
#include "db/hints/manager.hh"
#include "utils/exceptions.hh"
#include "message/messaging_service.hh"
#include "supervisor.hh"
#include "compaction/compaction_manager.hh"
#include "sstables/sstables.hh"
#include "db/config.hh"
#include "db/schema_tables.hh"
#include "replica/database.hh"
#include "replica/tablets.hh"
#include <seastar/core/metrics.hh>
#include "cdc/generation.hh"
#include "cdc/generation_service.hh"
#include "repair/repair.hh"
#include "repair/row_level.hh"
#include "gms/generation-number.hh"
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/coroutine/exception.hh>
#include "utils/stall_free.hh"
#include "utils/error_injection.hh"
#include "utils/fb_utilities.hh"
#include "locator/util.hh"
#include "idl/storage_service.dist.hh"
#include "service/storage_proxy.hh"
#include "service/raft/raft_address_map.hh"
#include "protocol_server.hh"
#include "types/set.hh"

#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/trim_all.hpp>

using token = dht::token;
using UUID = utils::UUID;
using inet_address = gms::inet_address;

extern logging::logger cdc_log;

namespace service {

static logging::logger slogger("storage_service");

storage_service::storage_service(abort_source& abort_source,
    distributed<replica::database>& db, gms::gossiper& gossiper,
    sharded<db::system_keyspace>& sys_ks,
    gms::feature_service& feature_service,
    sharded<service::migration_manager>& mm,
    locator::shared_token_metadata& stm,
    locator::effective_replication_map_factory& erm_factory,
    sharded<netw::messaging_service>& ms,
    sharded<repair_service>& repair,
    sharded<streaming::stream_manager>& stream_manager,
    endpoint_lifecycle_notifier& elc_notif,
    sharded<db::batchlog_manager>& bm,
    sharded<locator::snitch_ptr>& snitch)
        : _abort_source(abort_source)
        , _feature_service(feature_service)
        , _db(db)
        , _gossiper(gossiper)
        , _messaging(ms)
        , _migration_manager(mm)
        , _repair(repair)
        , _stream_manager(stream_manager)
        , _snitch(snitch)
        , _node_ops_abort_thread(node_ops_abort_thread())
        , _shared_token_metadata(stm)
        , _erm_factory(erm_factory)
        , _lifecycle_notifier(elc_notif)
        , _batchlog_manager(bm)
        , _sys_ks(sys_ks)
        , _snitch_reconfigure([this] {
            return container().invoke_on(0, [] (auto& ss) {
                return ss.snitch_reconfigured();
            });
        })
{
    register_metrics();

    _listeners.emplace_back(make_lw_shared(bs2::scoped_connection(sstable_read_error.connect([this] { do_isolate_on_error(disk_error::regular); }))));
    _listeners.emplace_back(make_lw_shared(bs2::scoped_connection(sstable_write_error.connect([this] { do_isolate_on_error(disk_error::regular); }))));
    _listeners.emplace_back(make_lw_shared(bs2::scoped_connection(general_disk_error.connect([this] { do_isolate_on_error(disk_error::regular); }))));
    _listeners.emplace_back(make_lw_shared(bs2::scoped_connection(commit_error.connect([this] { do_isolate_on_error(disk_error::commit); }))));

    if (_snitch.local_is_initialized()) {
        _listeners.emplace_back(make_lw_shared(_snitch.local()->when_reconfigured(_snitch_reconfigure)));
    }
}

enum class node_external_status {
    UNKNOWN        = 0,
    STARTING       = 1,
    JOINING        = 2,
    NORMAL         = 3,
    LEAVING        = 4,
    DECOMMISSIONED = 5,
    DRAINING       = 6,
    DRAINED        = 7,
    MOVING         = 8 //deprecated
};

static node_external_status map_operation_mode(storage_service::mode m) {
    switch (m) {
    case storage_service::mode::NONE: return node_external_status::STARTING;
    case storage_service::mode::STARTING: return node_external_status::STARTING;
    case storage_service::mode::BOOTSTRAP: return node_external_status::JOINING;
    case storage_service::mode::JOINING: return node_external_status::JOINING;
    case storage_service::mode::NORMAL: return node_external_status::NORMAL;
    case storage_service::mode::LEAVING: return node_external_status::LEAVING;
    case storage_service::mode::DECOMMISSIONED: return node_external_status::DECOMMISSIONED;
    case storage_service::mode::DRAINING: return node_external_status::DRAINING;
    case storage_service::mode::DRAINED: return node_external_status::DRAINED;
    case storage_service::mode::MOVING: return node_external_status::MOVING;
    }
    return node_external_status::UNKNOWN;
}

void storage_service::register_metrics() {
    if (this_shard_id() != 0) {
        // the relevant data is distributed between the shards,
        // We only need to register it once.
        return;
    }
    namespace sm = seastar::metrics;
    _metrics.add_group("node", {
            sm::make_gauge("operation_mode", sm::description("The operation mode of the current node. UNKNOWN = 0, STARTING = 1, JOINING = 2, NORMAL = 3, "
                    "LEAVING = 4, DECOMMISSIONED = 5, DRAINING = 6, DRAINED = 7, MOVING = 8"), [this] {
                return static_cast<std::underlying_type_t<node_external_status>>(map_operation_mode(_operation_mode));
            }),
    });
}

bool storage_service::is_replacing() {
    const auto& cfg = _db.local().get_config();
    if (!cfg.replace_node_first_boot().empty()) {
        if (_sys_ks.local().bootstrap_complete()) {
            slogger.info("Replace node on first boot requested; this node is already bootstrapped");
            return false;
        }
        return true;
    }
    if (!cfg.replace_address_first_boot().empty()) {
      if (_sys_ks.local().bootstrap_complete()) {
        slogger.info("Replace address on first boot requested; this node is already bootstrapped");
        return false;
      }
      return true;
    }
    // Returning true if cfg.replace_address is provided
    // will trigger an exception down the road if bootstrap_complete(),
    // as it is an error to use this option post bootstrap.
    // That said, we should just stop supporting it and force users
    // to move to the new, replace_node_first_boot config option.
    return !cfg.replace_address().empty();
}

bool storage_service::is_first_node() {
    if (is_replacing()) {
        return false;
    }
    auto seeds = _gossiper.get_seeds();
    if (seeds.empty()) {
        return false;
    }
    // Node with the smallest IP address is chosen as the very first node
    // in the cluster. The first node is the only node that does not
    // bootstrap in the cluser. All other nodes will bootstrap.
    std::vector<gms::inet_address> sorted_seeds(seeds.begin(), seeds.end());
    std::sort(sorted_seeds.begin(), sorted_seeds.end());
    if (sorted_seeds.front() == get_broadcast_address()) {
        slogger.info("I am the first node in the cluster. Skip bootstrap. Node={}", get_broadcast_address());
        return true;
    }
    return false;
}

bool storage_service::should_bootstrap() {
    return !_sys_ks.local().bootstrap_complete() && !is_first_node();
}

/* Broadcasts the chosen tokens through gossip,
 * together with a CDC generation timestamp and STATUS=NORMAL.
 *
 * Assumes that no other functions modify CDC_GENERATION_ID, TOKENS or STATUS
 * in the gossiper's local application state while this function runs.
 */
static future<> set_gossip_tokens(gms::gossiper& g,
        const std::unordered_set<dht::token>& tokens, std::optional<cdc::generation_id> cdc_gen_id) {
    assert(!tokens.empty());

    // Order is important: both the CDC streams timestamp and tokens must be known when a node handles our status.
    return g.add_local_application_state({
        { gms::application_state::TOKENS, gms::versioned_value::tokens(tokens) },
        { gms::application_state::CDC_GENERATION_ID, gms::versioned_value::cdc_generation_id(cdc_gen_id) },
        { gms::application_state::STATUS, gms::versioned_value::normal(tokens) }
    });
}

/*
 * The helper waits for two things
 *  1) for schema agreement
 *  2) there's no pending node operations
 * before proceeding with the bootstrap or replace.
 *
 * This function must only be called if we're not the first node
 * (i.e. booting into existing cluster).
 *
 * Precondition: gossiper observed at least one other live node;
 * see `gossiper::wait_for_live_nodes_to_show_up()`.
 */
future<> storage_service::wait_for_ring_to_settle() {
    auto t = gms::gossiper::clk::now();
    while (true) {
        slogger.info("waiting for schema information to complete");
        while (!_migration_manager.local().have_schema_agreement()) {
            co_await sleep_abortable(std::chrono::milliseconds(10), _abort_source);
        }
        co_await update_topology_change_info("joining");

        auto tmptr = get_token_metadata_ptr();
        if (!_db.local().get_config().consistent_rangemovement() ||
                (tmptr->get_bootstrap_tokens().empty() && tmptr->get_leaving_endpoints().empty())) {
            break;
        }
        auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(gms::gossiper::clk::now() - t).count();
        slogger.info("Checking bootstrapping/leaving nodes: tokens {}, leaving {}, sleep 1 second and check again ({} seconds elapsed)",
                tmptr->get_bootstrap_tokens().size(),
                tmptr->get_leaving_endpoints().size(),
                elapsed);

        if (gms::gossiper::clk::now() > t + std::chrono::seconds(60)) {
            throw std::runtime_error("Other bootstrapping/leaving nodes detected, cannot bootstrap while consistent_rangemovement is true");
        }
        co_await sleep_abortable(std::chrono::seconds(1), _abort_source);
    }
    slogger.info("Checking bootstrapping/leaving nodes: ok");
}

future<> storage_service::topology_state_load(cdc::generation_service& cdc_gen_svc) {
#ifdef SEASTAR_DEBUG
    static bool running = false;
    assert(!running); // The function is not re-entrant
    auto d = defer([] {
        running = false;
    });
    running = true;
#endif

    if (!_raft_topology_change_enabled) {
        co_return;
    }

    slogger.debug("raft topology: reload raft topology state");
    // read topology state from disk and recreate token_metadata from it
    _topology_state_machine._topology = co_await db::system_keyspace::load_topology_state();

    const auto& am = _group0->address_map();
    auto id2ip = [this, &am] (raft::server_id id) -> future<gms::inet_address> {
        auto ip = am.find(id);
        while (!ip) {
            static logger::rate_limit rate_limit{std::chrono::seconds(1)};
            slogger.log(log_level::warn, rate_limit, "raft topology: cannot map {} to ip, retrying.", id);
            // FIXME: https://github.com/scylladb/scylladb/issues/12279
            // Loop until gossiper figures the address
            // but the solution is to change token_metadata to work with server_ids instead of ips
            co_await sleep_abortable(std::chrono::milliseconds(5), _abort_source);
            ip = am.find(id);
        }
        co_return *ip;
    };

    for (const auto& id: _topology_state_machine._topology.left_nodes) {
        auto ip = co_await id2ip(id);
        if (_gossiper.get_live_members().contains(ip) || _gossiper.get_unreachable_members().contains(ip)) {
            co_await remove_endpoint(ip);
        }

        // FIXME: when removing a node from the cluster through `removenode`, we should ban it early,
        // at the beginning of the removal process (so it doesn't disrupt us in the middle of the process).
        // The node is only included in `left_nodes` at the end of the process.
        //
        // However if we do that, we need to also implement unbanning a node and do it if `removenode` is aborted.
        co_await _messaging.local().ban_host(locator::host_id{id.uuid()});
    }

    co_await mutate_token_metadata(seastar::coroutine::lambda([this, &id2ip, &am] (mutable_token_metadata_ptr tmptr) -> future<> {
        co_await tmptr->clear_gently(); // drop previous state

        tmptr->set_version(_topology_state_machine._topology.version);

        auto update_topology = [&] (inet_address ip, const replica_state& rs) {
            tmptr->update_topology(ip, locator::endpoint_dc_rack{rs.datacenter, rs.rack}, std::nullopt, rs.shard_count);
        };

        auto add_normal_node = [&] (raft::server_id id, const replica_state& rs) -> future<> {
            locator::host_id host_id{id.uuid()};
            auto ip = co_await id2ip(id);

            slogger.trace("raft topology: loading topology: raft id={} ip={} node state={} dc={} rack={} tokens state={} tokens={} shards={}",
                          id, ip, rs.state, rs.datacenter, rs.rack, _topology_state_machine._topology.tstate, rs.ring.value().tokens, rs.shard_count);
            // Save tokens, not needed for raft topology management, but needed by legacy
            // Also ip -> id mapping is needed for address map recreation on reboot
            if (!utils::fb_utilities::is_me(ip)) {
                // Some state that is used to fill in 'peeers' table is still propagated over gossiper.
                // Populate the table with the state from the gossiper here since storage_service::on_change()
                // (which is called each time gossiper state changes) may have skipped it because the tokens
                // for the node were not in the 'normal' state yet
                co_await update_peer_info(ip);
                // And then amend with the info from raft
                co_await _sys_ks.local().update_tokens(ip, rs.ring.value().tokens);
                co_await _sys_ks.local().update_peer_info(ip, "data_center", rs.datacenter);
                co_await _sys_ks.local().update_peer_info(ip, "rack", rs.rack);
                co_await _sys_ks.local().update_peer_info(ip, "host_id", id.uuid());
                co_await _sys_ks.local().update_peer_info(ip, "release_version", rs.release_version);
            } else {
                co_await _sys_ks.local().update_tokens(rs.ring.value().tokens);
                co_await _gossiper.add_local_application_state({{ gms::application_state::STATUS, gms::versioned_value::normal(rs.ring.value().tokens) }});
            }
            update_topology(ip, rs);
            co_await tmptr->update_normal_tokens(rs.ring.value().tokens, ip);
            tmptr->update_host_id(host_id, ip);
        };

        for (const auto& [id, rs]: _topology_state_machine._topology.normal_nodes) {
            co_await add_normal_node(id, rs);
        }

        tmptr->set_read_new(std::invoke([](std::optional<topology::transition_state> state) {
            using read_new_t = locator::token_metadata::read_new_t;
            if (!state.has_value()) {
                return read_new_t::no;
            }
            switch (*state) {
                case topology::transition_state::commit_cdc_generation:
                case topology::transition_state::publish_cdc_generation:
                case topology::transition_state::write_both_read_old:
                    return read_new_t::no;
                case topology::transition_state::write_both_read_new:
                    return read_new_t::yes;
            }
        }, _topology_state_machine._topology.tstate));

        for (const auto& [id, rs]: _topology_state_machine._topology.transition_nodes) {
            locator::host_id host_id{id.uuid()};
            auto ip = co_await id2ip(id);

            slogger.trace("raft topology: loading topology: raft id={} ip={} node state={} dc={} rack={} tokens state={} tokens={}",
                          id, ip, rs.state, rs.datacenter, rs.rack, _topology_state_machine._topology.tstate, rs.ring->tokens);

            switch (rs.state) {
            case node_state::bootstrapping:
                if (!utils::fb_utilities::is_me(ip)) {
                    // Save ip -> id mapping in peers table because we need it on restart, but do not save tokens until owned
                    co_await _sys_ks.local().update_tokens(ip, {});
                    co_await _sys_ks.local().update_peer_info(ip, "host_id", id.uuid());
                }
                update_topology(ip, rs);
                if (_topology_state_machine._topology.normal_nodes.empty()) {
                    // This is the first node in the cluster. Insert the tokens as normal to the token ring early
                    // so we can perform writes to regular 'distributed' tables during the bootstrap procedure
                    // (such as the CDC generation write).
                    // It doesn't break anything to set the tokens to normal early in this single-node case.
                    co_await tmptr->update_normal_tokens(rs.ring.value().tokens, ip);
                } else {
                    tmptr->add_bootstrap_tokens(rs.ring.value().tokens, ip);
                    co_await update_topology_change_info(tmptr, ::format("bootstrapping node {}/{}", id, ip));
                }
                break;
            case node_state::decommissioning:
            case node_state::removing:
                update_topology(ip, rs);
                co_await tmptr->update_normal_tokens(rs.ring.value().tokens, ip);
                tmptr->update_host_id(host_id, ip);
                tmptr->add_leaving_endpoint(ip);
                co_await update_topology_change_info(tmptr, ::format("{} {}/{}", rs.state, id, ip));
                break;
            case node_state::replacing: {
                assert(_topology_state_machine._topology.req_param.contains(id));
                auto replaced_id = std::get<raft::server_id>(_topology_state_machine._topology.req_param[id]);
                auto existing_ip = am.find(replaced_id);
                if (!existing_ip) {
                    // FIXME: What if not known?
                    on_fatal_internal_error(slogger, ::format("Cannot map id of a node being replaced {} to its ip", replaced_id));
                }
                assert(existing_ip);
                update_topology(ip, rs);
                tmptr->add_replacing_endpoint(*existing_ip, ip);
                co_await update_topology_change_info(tmptr, ::format("replacing {}/{} by {}/{}", replaced_id, *existing_ip, id, ip));
            }
                break;
            case node_state::rebuilding:
                // Rebuilding node is normal
                co_await add_normal_node(id, rs);
                break;
            case node_state::left_token_ring:
                break;
            default:
                on_fatal_internal_error(slogger, ::format("Unexpected state {} for node {}", rs.state, id));
            }
        }

        if (_db.local().get_config().check_experimental(db::experimental_features_t::feature::TABLETS)) {
            tmptr->set_tablets(co_await replica::read_tablet_metadata(*_qp));
        }
    }));

    if (auto gen_id = _topology_state_machine._topology.current_cdc_generation_id) {
        slogger.debug("topology_state_load: current CDC generation ID: {}", *gen_id);
        co_await cdc_gen_svc.handle_cdc_generation(*gen_id);
    }
}

future<> storage_service::topology_transition(storage_proxy& proxy, cdc::generation_service& cdc_gen_svc, gms::inet_address from, std::vector<canonical_mutation> cms) {
    assert(this_shard_id() == 0);
    // write new state into persistent storage
    std::vector<mutation> mutations;
    mutations.reserve(cms.size());
    try {
        for (const auto& cm : cms) {
            auto& tbl = _db.local().find_column_family(cm.column_family_id());
            mutations.emplace_back(cm.to_mutation(tbl.schema()));
        }
    } catch (replica::no_such_column_family& e) {
        slogger.error("Error while applying topology mutations from {}: {}", from, e);
        throw std::runtime_error(::format("Error while applying topology mutations: {}", e));
    }

    co_await proxy.mutate_locally(std::move(mutations), tracing::trace_state_ptr());

    co_await topology_state_load(cdc_gen_svc); // reload new state

    _topology_state_machine.event.signal();
}

future<> storage_service::merge_topology_snapshot(raft_topology_snapshot snp) {
   std::vector<mutation> muts;
   muts.reserve(snp.topology_mutations.size() + (snp.cdc_generation_mutation ? 1 : 0));
   {
       auto s = _db.local().find_schema(db::system_keyspace::NAME, db::system_keyspace::TOPOLOGY);
       boost::transform(snp.topology_mutations, std::back_inserter(muts), [s] (const canonical_mutation& m) {
           return m.to_mutation(s);
       });
   }
   if (snp.cdc_generation_mutation) {
       auto s = _db.local().find_schema(db::system_keyspace::NAME, db::system_keyspace::CDC_GENERATIONS_V3);
       muts.push_back(snp.cdc_generation_mutation->to_mutation(s));
   }
   co_await _db.local().apply(freeze(muts), db::no_timeout);
}

template<typename Builder>
class topology_mutation_builder_base {
private:
    Builder& self() {
        return *static_cast<Builder*>(this);
    }

protected:
    enum class collection_apply_mode {
        overwrite,
        update,
    };

    using builder_base = topology_mutation_builder_base<Builder>;

    Builder& apply_atomic(const char* cell, const data_value& value);
    template<std::ranges::range C>
    requires std::convertible_to<std::ranges::range_value_t<C>, data_value>
    Builder& apply_set(const char* cell, collection_apply_mode apply_mode, const C& c);
    Builder& del(const char* cell);
};

class topology_mutation_builder;

class topology_node_mutation_builder
        : public topology_mutation_builder_base<topology_node_mutation_builder> {

    friend builder_base;

    topology_mutation_builder& _builder;
    deletable_row& _r;

private:
    row& row();
    api::timestamp_type timestamp() const;
    const schema& schema() const;

public:
    topology_node_mutation_builder(topology_mutation_builder&, raft::server_id);

    topology_node_mutation_builder& set(const char* cell, node_state value);
    topology_node_mutation_builder& set(const char* cell, topology_request value);
    topology_node_mutation_builder& set(const char* cell, const sstring& value);
    topology_node_mutation_builder& set(const char* cell, const raft::server_id& value);
    topology_node_mutation_builder& set(const char* cell, const std::unordered_set<dht::token>& value);
    template<typename S>
    requires std::constructible_from<sstring, S>
    topology_node_mutation_builder& set(const char* cell, const std::set<S>& value);
    topology_node_mutation_builder& set(const char* cell, const uint32_t& value);
    topology_node_mutation_builder& set(const char* cell, const utils::UUID& value);
    topology_node_mutation_builder& del(const char* cell);
    canonical_mutation build();
};

class topology_mutation_builder
        : public topology_mutation_builder_base<topology_mutation_builder> {

    friend builder_base;
    friend class topology_node_mutation_builder;

    schema_ptr _s;
    mutation _m;
    api::timestamp_type _ts;

    std::optional<topology_node_mutation_builder> _node_builder;

private:
    row& row();
    api::timestamp_type timestamp() const;
    const schema& schema() const;

public:
    topology_mutation_builder(api::timestamp_type ts);
    topology_mutation_builder& set_transition_state(topology::transition_state);
    topology_mutation_builder& set_version(topology::version_t);
    topology_mutation_builder& set_current_cdc_generation_id(const cdc::generation_id_v2&);
    topology_mutation_builder& set_new_cdc_generation_data_uuid(const utils::UUID& value);
    topology_mutation_builder& set_global_topology_request(global_topology_request);
    template<typename S>
    requires std::constructible_from<sstring, S>
    topology_mutation_builder& add_enabled_features(const std::set<S>& value);
    topology_mutation_builder& del_transition_state();
    topology_mutation_builder& del_global_topology_request();
    topology_node_mutation_builder& with_node(raft::server_id);
    canonical_mutation build() { return canonical_mutation{std::move(_m)}; }
};

topology_mutation_builder::topology_mutation_builder(api::timestamp_type ts) :
        _s(db::system_keyspace::topology()),
        _m(_s, partition_key::from_singular(*_s, db::system_keyspace::TOPOLOGY)),
        _ts(ts) {
}

topology_node_mutation_builder::topology_node_mutation_builder(topology_mutation_builder& builder, raft::server_id id) :
        _builder(builder),
        _r(_builder._m.partition().clustered_row(*_builder._s, clustering_key::from_singular(*_builder._s, id.uuid()))) {
    _r.apply(row_marker(_builder._ts));
}

template<typename Builder>
Builder& topology_mutation_builder_base<Builder>::apply_atomic(const char* cell, const data_value& value) {
    const column_definition* cdef = self().schema().get_column_definition(cell);
    assert(cdef);
    self().row().apply(*cdef, atomic_cell::make_live(*cdef->type, self().timestamp(), cdef->type->decompose(value)));
    return self();
}

template<typename Builder>
template<std::ranges::range C>
requires std::convertible_to<std::ranges::range_value_t<C>, data_value>
Builder& topology_mutation_builder_base<Builder>::apply_set(const char* cell, collection_apply_mode apply_mode, const C& c) {
    const column_definition* cdef = self().schema().get_column_definition(cell);
    assert(cdef);
    auto vtype = static_pointer_cast<const set_type_impl>(cdef->type)->get_elements_type();

    std::set<bytes, serialized_compare> cset(vtype->as_less_comparator());
    for (const auto& v : c) {
        cset.insert(vtype->decompose(data_value(v)));
    }

    collection_mutation_description cm;
    cm.cells.reserve(cset.size());
    for (const bytes& raw : cset) {
        cm.cells.emplace_back(raw, atomic_cell::make_live(*bytes_type, self().timestamp(), bytes_view()));
    }

    if (apply_mode == collection_apply_mode::overwrite) {
        cm.tomb = tombstone(self().timestamp() - 1, gc_clock::now());
    }

    self().row().apply(*cdef, cm.serialize(*cdef->type));
    return self();
}

template<typename Builder>
Builder& topology_mutation_builder_base<Builder>::del(const char* cell) {
    auto cdef = self().schema().get_column_definition(cell);
    assert(cdef);
    if (!cdef->type->is_multi_cell()) {
        self().row().apply(*cdef, atomic_cell::make_dead(self().timestamp(), gc_clock::now()));
    } else {
        collection_mutation_description cm;
        cm.tomb = tombstone{self().timestamp(), gc_clock::now()};
        self().row().apply(*cdef, cm.serialize(*cdef->type));
    }
    return self();
}

row& topology_node_mutation_builder::row() {
    return _r.cells();
}

api::timestamp_type topology_node_mutation_builder::timestamp() const {
    return _builder._ts;
}

const schema& topology_node_mutation_builder::schema() const {
    return *_builder._s;
}

topology_node_mutation_builder& topology_node_mutation_builder::set(const char* cell, node_state value) {
    return apply_atomic(cell, sstring{::format("{}", value)});
}

topology_node_mutation_builder& topology_node_mutation_builder::set(const char* cell, topology_request value) {
    return apply_atomic(cell, sstring{::format("{}", value)});
}

topology_node_mutation_builder& topology_node_mutation_builder::set(const char* cell, const sstring& value) {
    return apply_atomic(cell, value);
}

topology_node_mutation_builder& topology_node_mutation_builder::set(const char* cell, const raft::server_id& value) {
    return apply_atomic(cell, value.uuid());
}

topology_node_mutation_builder& topology_node_mutation_builder::set(const char* cell, const uint32_t& value) {
    return apply_atomic(cell, int32_t(value));
}

topology_node_mutation_builder& topology_node_mutation_builder::set(
        const char* cell, const utils::UUID& value) {
    return apply_atomic(cell, value);
}

topology_node_mutation_builder& topology_node_mutation_builder::del(const char* cell) {
    return builder_base::del(cell);
}

topology_node_mutation_builder& topology_node_mutation_builder::set(const char* cell, const std::unordered_set<dht::token>& tokens) {
    return apply_set(cell, collection_apply_mode::overwrite, tokens | boost::adaptors::transformed([] (const auto& t) { return t.to_sstring(); }));
}

template<typename S>
requires std::constructible_from<sstring, S>
topology_node_mutation_builder& topology_node_mutation_builder::set(const char* cell, const std::set<S>& features) {
    return apply_set(cell, collection_apply_mode::overwrite, features | boost::adaptors::transformed([] (const auto& f) { return sstring(f); }));
}

canonical_mutation topology_node_mutation_builder::build() {
    return canonical_mutation{std::move(_builder._m)};
}

row& topology_mutation_builder::row() {
    return _m.partition().static_row().maybe_create();
}

api::timestamp_type topology_mutation_builder::timestamp() const {
    return _ts;
}

const schema& topology_mutation_builder::schema() const {
    return *_s;
}

topology_mutation_builder& topology_mutation_builder::set_transition_state(topology::transition_state value) {
    return apply_atomic("transition_state", ::format("{}", value));
}

topology_mutation_builder& topology_mutation_builder::set_version(topology::version_t value) {
    _m.set_static_cell("version", value, _ts);
    return *this;
}

topology_mutation_builder& topology_mutation_builder::del_transition_state() {
    return del("transition_state");
}

topology_mutation_builder& topology_mutation_builder::set_current_cdc_generation_id(
        const cdc::generation_id_v2& value) {
    apply_atomic("current_cdc_generation_timestamp", value.ts);
    apply_atomic("current_cdc_generation_uuid", value.id);
    return *this;
}

topology_mutation_builder& topology_mutation_builder::set_new_cdc_generation_data_uuid(
        const utils::UUID& value) {
    return apply_atomic("new_cdc_generation_data_uuid", value);
}

topology_mutation_builder& topology_mutation_builder::set_global_topology_request(global_topology_request value) {
    return apply_atomic("global_topology_request", ::format("{}", value));
}

template<typename S>
requires std::constructible_from<sstring, S>
topology_mutation_builder& topology_mutation_builder::add_enabled_features(const std::set<S>& features) {
    return apply_set("enabled_features", collection_apply_mode::update, features | boost::adaptors::transformed([] (const auto& f) { return sstring(f); }));
}

topology_mutation_builder& topology_mutation_builder::del_global_topology_request() {
    return del("global_topology_request");
}

topology_node_mutation_builder& topology_mutation_builder::with_node(raft::server_id n) {
    _node_builder.emplace(*this, n);
    return *_node_builder;
}

using raft_topology_cmd_handler_type = noncopyable_function<future<raft_topology_cmd_result>(
        sharded<db::system_distributed_keyspace>&, raft::term_t, uint64_t, const raft_topology_cmd&)>;

class topology_coordinator {
    sharded<db::system_distributed_keyspace>& _sys_dist_ks;
    netw::messaging_service& _messaging;
    locator::shared_token_metadata& _shared_tm;
    db::system_keyspace& _sys_ks;
    replica::database& _db;
    service::raft_group0& _group0;
    const service::raft_address_map& _address_map;
    service::topology_state_machine& _topo_sm;
    abort_source& _as;

    raft::server& _raft;
    const raft::term_t _term;
    uint64_t _last_cmd_index = 0;

    raft_topology_cmd_handler_type _raft_topology_cmd_handler;

    std::chrono::milliseconds _ring_delay;

    const locator::token_metadata& get_token_metadata() const noexcept {
        return *_shared_tm.get();
    }

    locator::token_metadata_ptr get_token_metadata_ptr() const noexcept {
        return _shared_tm.get();
    }

    // This is a topology snapshot for a given node. It contains pointers into the topology state machine
    // that may be outdated after guard is released so the structure is meant to be destroyed together
    // with the guard
    struct node_to_work_on {
        group0_guard guard;
        const topology_state_machine::topology_type* topology;
        raft::server_id id;
        const replica_state* rs;
        std::optional<topology_request> request;
        std::optional<request_param> req_param;
    };

    // The topology coordinator takes guard before operation start, but it releases it during various
    // RPC commands that it sends to make it possible to submit new requests to the state machine while
    // the coordinator drives current topology change. It is safe to do so since only the coordinator is
    // ever allowed to change node's state, others may only create requests. To make sure the coordinator did
    // not change while the lock was released, and hence the old coordinator does not work on old state, we check
    // that the raft term is still the same after the lock is re-acquired. Throw term_changed_error if it did.

    struct term_changed_error {};

    future<> cleanup_group0_config_if_needed() {
        auto& topo = _topo_sm._topology;
        auto rconf = _group0.group0_server().get_configuration();
        if (!rconf.is_joint()) {
            // Find nodes that 'left' but still in the config and remove them
            auto to_remove = boost::copy_range<std::vector<raft::server_id>>(
                    rconf.current
                    | boost::adaptors::transformed([&] (const raft::config_member& m) { return m.addr.id; })
                    | boost::adaptors::filtered([&] (const raft::server_id& id) { return topo.left_nodes.contains(id); }));
            if (!to_remove.empty()) {
                // Remove from group 0 nodes that left. They may failed to do so by themselves
                try {
                    slogger.trace("raft topology: topology coordinator fiber removing {}"
                                  " from raft since they are in `left` state", to_remove);
                    co_await _group0.group0_server().modify_config({}, to_remove, &_as);
                } catch (const raft::commit_status_unknown&) {
                    slogger.trace("raft topology: topology coordinator fiber got unknown status"
                                  " while removing {} from raft", to_remove);
                }
            }
        }
    }

    // Returns the guard back if no node to work on is found.
    std::variant<group0_guard, node_to_work_on> get_node_to_work_on_opt(group0_guard guard) {
        auto& topo = _topo_sm._topology;
        const std::pair<const raft::server_id, replica_state>* e = nullptr;

        std::optional<topology_request> req;
        if (topo.transition_nodes.size() != 0) {
            // If there is a node that is the middle of topology operation continue with it
            e = &*topo.transition_nodes.begin();
        } else if (topo.new_nodes.size() != 0) {
            // Otherwise check if there is a new node that wants to be joined
            e = &*topo.new_nodes.begin();
            req = topo.requests[e->first];
        } else if (!topo.requests.empty()) {
            // If there is no new node but request queue is not empty there is a request for normal node
            req = topo.requests.begin()->second;
            e = &*topo.normal_nodes.find(topo.requests.begin()->first);
        }

        if (!e) {
            return guard;
        }

        std::optional<request_param> req_param;
        auto rit = topo.req_param.find(e->first);
        if (rit != topo.req_param.end()) {
            req_param = rit->second;
        }
        return node_to_work_on{std::move(guard), &topo, e->first, &e->second, std::move(req), std::move(req_param)};
    };

    node_to_work_on get_node_to_work_on(group0_guard guard) {
        auto node_or_guard = get_node_to_work_on_opt(std::move(guard));
        if (auto* node = std::get_if<node_to_work_on>(&node_or_guard)) {
            return std::move(*node);
        }
        on_internal_error(slogger, ::format(
            "raft topology: could not find node to work on"
            " even though the state requires it (state: {})", _topo_sm._topology.tstate));
     };

    future<group0_guard> start_operation() {
        auto guard = co_await _group0.client().start_operation(&_as);

        if (_term != _raft.get_current_term()) {
            throw term_changed_error{};
        }

        co_return std::move(guard);
    }

    void release_node(std::optional<node_to_work_on> node) {
        // Leaving the scope destroys the object and releases the guard.
    }

    node_to_work_on retake_node(group0_guard guard, raft::server_id id) {
        auto& topo = _topo_sm._topology;

        auto it = topo.find(id);
        assert(it);

        std::optional<topology_request> req;
        auto rit = topo.requests.find(id);
        if (rit != topo.requests.end()) {
            req = rit->second;
        }
        std::optional<request_param> req_param;
        auto pit = topo.req_param.find(id);
        if (pit != topo.req_param.end()) {
            req_param = pit->second;
        }
        return node_to_work_on{std::move(guard), &topo, id, &it->second, std::move(req), std::move(req_param)};
    }

    group0_guard take_guard(node_to_work_on&& node) {
        return std::move(node.guard);
    }

    future<> update_topology_state(
            group0_guard guard, std::vector<canonical_mutation>&& updates, const sstring& reason) {
        try {
            slogger.trace("raft topology: do update {} reason {}", updates, reason);
            topology_change change{std::move(updates)};
            group0_command g0_cmd = _group0.client().prepare_command(std::move(change), guard, reason);
            co_await _group0.client().add_entry(std::move(g0_cmd), std::move(guard));
        } catch (group0_concurrent_modification&) {
            slogger.info("raft topology: race while changing state: {}. Retrying", reason);
            throw;
        }
    };

    raft::server_id parse_replaced_node(const node_to_work_on& node) {
        if (node.rs->state == node_state::replacing) {
            return std::get<raft::server_id>(node.req_param.value());
        }
        return {};
    }

    future<> exec_direct_command_helper(raft::server_id id, uint64_t cmd_index, const raft_topology_cmd& cmd) {
        auto ip = _address_map.find(id);
        if (!ip) {
            slogger.warn("raft topology: cannot send command {} with term {} and index {} "
                         "to {} because mapping to ip is not available",
                         cmd.cmd, _term, cmd_index, id);
            co_await coroutine::exception(std::make_exception_ptr(
                    std::runtime_error(::format("no ip address mapping for {}", id))));
        }
        slogger.trace("raft topology: send {} command with term {} and index {} to {}/{}",
            cmd.cmd, _term, cmd_index, id, *ip);
        auto result = utils::fb_utilities::is_me(*ip) ?
                    co_await _raft_topology_cmd_handler(_sys_dist_ks, _term, cmd_index, cmd) :
                    co_await ser::storage_service_rpc_verbs::send_raft_topology_cmd(
                            &_messaging, netw::msg_addr{*ip}, _term, cmd_index, cmd);
        if (result.status == raft_topology_cmd_result::command_status::fail) {
            co_await coroutine::exception(std::make_exception_ptr(
                    std::runtime_error(::format("failed status returned from {}/{}", id, *ip))));
        }
    };

    future<node_to_work_on> exec_direct_command(node_to_work_on&& node, const raft_topology_cmd& cmd) {
        auto id = node.id;
        release_node(std::move(node));
        const auto cmd_index = ++_last_cmd_index;
        co_await exec_direct_command_helper(id, cmd_index, cmd);
        co_return retake_node(co_await start_operation(), id);
    };

    future<> exec_global_command_helper(auto nodes, const raft_topology_cmd& cmd) {
        const auto cmd_index = ++_last_cmd_index;
        auto f = co_await coroutine::as_future(
                seastar::parallel_for_each(std::move(nodes), [this, &cmd, cmd_index] (raft::server_id id) {
            return exec_direct_command_helper(id, cmd_index, cmd);
        }));

        if (f.failed()) {
            co_await coroutine::return_exception(std::runtime_error(
                ::format("raft topology: exec_global_command({}) failed with {}",
                    cmd.cmd, f.get_exception())));
        }
    };

    future<group0_guard> exec_global_command(
            group0_guard guard, const raft_topology_cmd& cmd,
            const utils::small_vector<raft::server_id, 2>& exclude_nodes) {
        auto nodes = _topo_sm._topology.normal_nodes | boost::adaptors::filtered(
                [&exclude_nodes] (const std::pair<const raft::server_id, replica_state>& n) {
                    return std::none_of(exclude_nodes.begin(), exclude_nodes.end(),
                            [&n] (const raft::server_id& m) { return n.first == m; });
                }) | boost::adaptors::map_keys;
        {
            // release guard
            auto _ = std::move(guard);
        }
        co_await exec_global_command_helper(std::move(nodes), cmd);
        co_return co_await start_operation();
    }

    future<node_to_work_on> exec_global_command(
            node_to_work_on&& node, const raft_topology_cmd& cmd, bool include_local) {
        utils::small_vector<raft::server_id, 2> exclude_nodes{parse_replaced_node(node)};
        if (!include_local) {
            exclude_nodes.push_back(_raft.id());
        }
        auto guard = co_await exec_global_command(std::move(node.guard), cmd, exclude_nodes);
        co_return retake_node(std::move(guard), node.id);
    };

    future<> remove_from_group0(const raft::server_id& id) {
        slogger.info("raft topology: removing node {} from group 0 configuration...", id);
        co_await _group0.remove_from_raft_config(id);
        slogger.info("raft topology: node {} removed from group 0 configuration", id);
    }

    struct bootstrapping_info {
        const std::unordered_set<token>& bootstrap_tokens;
        const replica_state& rs;
    };

    // Returns data for a new CDC generation in the form of mutations for the CDC_GENERATIONS_V3 table
    // and the generation's UUID.
    //
    // If there's a bootstrapping node, its tokens should be included in the new generation.
    // Pass them and a reference to the bootstrapping node's replica_state through `binfo`.
    future<std::pair<utils::UUID, utils::chunked_vector<mutation>>> prepare_new_cdc_generation_data(
            locator::token_metadata_ptr tmptr, const group0_guard& guard, std::optional<bootstrapping_info> binfo) {
        auto get_sharding_info = [&] (dht::token end) -> std::pair<size_t, uint8_t> {
            if (binfo && binfo->bootstrap_tokens.contains(end)) {
                return {binfo->rs.shard_count, binfo->rs.ignore_msb};
            } else {
                // FIXME: token metadata should directly return host ID for given token. See #12279
                auto ep = tmptr->get_endpoint(end);
                if (!ep) {
                    // get_sharding_info is only called for bootstrap tokens
                    // or for tokens present in token_metadata
                    on_internal_error(slogger, ::format(
                        "raft topology: make_new_cdc_generation_data: get_sharding_info:"
                        " can't find endpoint for token {}", end));
                }

                auto id = tmptr->get_host_id_if_known(*ep);
                if (!id) {
                    on_internal_error(slogger, ::format(
                        "raft topology: make_new_cdc_generation_data: get_sharding_info:"
                        " can't find host ID for endpoint {}, owner of token {}", *ep, end));
                }

                auto ptr = _topo_sm._topology.find(raft::server_id{id->uuid()});
                if (!ptr) {
                    on_internal_error(slogger, ::format(
                        "raft topology: make_new_cdc_generation_data: get_sharding_info:"
                        " couldn't find node {} in topology, owner of token {}", *id, end));
                }

                auto& rs = ptr->second;
                return {rs.shard_count, rs.ignore_msb};
            }
        };

        auto [gen_uuid, gen_desc] = cdc::make_new_generation_data(
            binfo ? binfo->bootstrap_tokens : std::unordered_set<token>{}, get_sharding_info, tmptr);
        auto gen_table_schema = _db.find_schema(
            db::system_keyspace::NAME, db::system_keyspace::CDC_GENERATIONS_V3);

        // FIXME: the CDC generation data can be large and not fit in a single command
        // (for large clusters, it will introduce reactor stalls and go over commitlog entry
        // size limit). We need to split it into multiple mutations by smartly picking
        // a `mutation_size_threshold` and sending each mutation as a separate group 0 command.
        // We also don't want to serialize the commands - there may be many of them,
        // and we don't want to wait for a network round-trip to a quorum between each command.
        // So we need to introduce a mechanism for group 0 to send a sequence of commands
        // that can be committed concurrently. Also we need to be careful with memory consumption
        // with many large mutations.
        // See `system_distributed_keyspace::insert_cdc_generation` for inspiration how it
        // was done when the mutations were stored in a regular distributed table.
        const size_t mutation_size_threshold = 2'000'000;
        auto gen_mutations = co_await cdc::get_cdc_generation_mutations(
            gen_table_schema, gen_uuid, gen_desc, mutation_size_threshold, guard.write_timestamp());

        co_return std::pair{gen_uuid, std::move(gen_mutations)};
    }

    // Precondition: there is no node request and no ongoing topology transition
    // (checked under the guard we're holding).
    future<> handle_global_request(group0_guard guard) {
        switch (_topo_sm._topology.global_request.value()) {
        case global_topology_request::new_cdc_generation: {
            slogger.info("raft topology: new CDC generation requested");

            auto tmptr = get_token_metadata_ptr();
            auto [gen_uuid, gen_mutations] = co_await prepare_new_cdc_generation_data(tmptr, guard, std::nullopt);

            std::vector<canonical_mutation> updates{gen_mutations.begin(), gen_mutations.end()};
            topology_mutation_builder builder(guard.write_timestamp());
            builder.set_transition_state(topology::transition_state::commit_cdc_generation)
                   .set_new_cdc_generation_data_uuid(gen_uuid)
                   .del_global_topology_request();
            updates.push_back(builder.build());
            auto reason = ::format(
                "insert CDC generation data (UUID: {})", gen_uuid);
            co_await update_topology_state(std::move(guard), {std::move(updates)}, reason);
        }
            break;
        }
    }

    future<node_to_work_on> global_token_metadata_barrier(node_to_work_on&& node) {
        node = co_await exec_global_command(std::move(node),
            raft_topology_cmd::command::barrier_and_drain,
            true);
        node = co_await exec_global_command(std::move(node),
            raft_topology_cmd::command::fence,
            true);
        co_return std::move(node);
    }

    // Returns `true` iff there was work to do.
    future<bool> handle_topology_transition(group0_guard guard) {
        auto tstate = _topo_sm._topology.tstate;
        if (!tstate) {
            auto node_or_guard = get_node_to_work_on_opt(std::move(guard));
            if (auto* node = std::get_if<node_to_work_on>(&node_or_guard)) {
                co_await handle_node_transition(std::move(*node));
                co_return true;
            }

            guard = std::get<group0_guard>(std::move(node_or_guard));
            if (_topo_sm._topology.global_request) {
                co_await handle_global_request(std::move(guard));
                co_return true;
            }
            co_return false;
        }

        switch (*tstate) {
            case topology::transition_state::commit_cdc_generation: {
                // make sure all nodes know about new topology and have the new CDC generation data
                // (we require all nodes to be alive for topo change for now)
                // Note: if there was a replace or removenode going on, we'd need to put the replaced/removed
                // node into `exclude_nodes` parameter in `exec_global_command`, but CDC generations are never
                // introduced during replace/remove.
                {
                    auto f = co_await coroutine::as_future(exec_global_command(std::move(guard),
                        raft_topology_cmd::command::barrier,
                        {_raft.id()}));
                    if (f.failed()) {
                        slogger.error("raft topology: transition_state::commit_cdc_generation, "
                                      "raft_topology_cmd::command::barrier failed, error {}", f.get_exception());
                        break;
                    }
                    guard = std::move(f).get();
                }

                // We don't need to add delay to the generation timestamp if this is the first generation.
                bool add_ts_delay = bool(_topo_sm._topology.current_cdc_generation_id);

                // Begin the race.
                // See the large FIXME below.
                auto cdc_gen_ts = cdc::new_generation_timestamp(add_ts_delay, _ring_delay);
                auto cdc_gen_uuid = _topo_sm._topology.new_cdc_generation_data_uuid;
                if (!cdc_gen_uuid) {
                    on_internal_error(slogger,
                        "raft topology: new CDC generation data UUID missing in `commit_cdc_generation` state");
                }

                cdc::generation_id_v2 cdc_gen_id {
                    .ts = cdc_gen_ts,
                    .id = *cdc_gen_uuid,
                };

                {
                    // Sanity check.
                    // This could happen if the topology coordinator's clock is broken.
                    auto curr_gen_id = _topo_sm._topology.current_cdc_generation_id;
                    if (curr_gen_id && curr_gen_id->ts >= cdc_gen_ts) {
                        on_internal_error(slogger, ::format(
                            "raft topology: new CDC generation has smaller timestamp than the previous one."
                            " Old generation ID: {}, new generation ID: {}", *curr_gen_id, cdc_gen_id));
                    }
                }

                // Tell all nodes to start using the new CDC generation by updating the topology
                // with the generation's ID and timestamp.
                // At the same time move the topology change procedure to the next step.
                //
                // FIXME: as in previous implementation with gossiper and ring_delay, this assumes that all nodes
                // will learn about the new CDC generation before their clocks reach the generation's timestamp.
                // With this group 0 based implementation, it means that the command must be committed,
                // replicated and applied on all nodes before their clocks reach the generation's timestamp
                // (i.e. within 2 * ring_delay = 60 seconds by default if clocks are synchronized). If this
                // doesn't hold some coordinators might use the wrong CDC streams for some time and CDC stream
                // readers will miss some data. It's likely that Raft replication doesn't converge as quickly
                // as gossiping does.
                //
                // We could use a two-phase algorithm instead: first tell all nodes to prepare for using
                // the new generation, then tell all nodes to commit. If some nodes don't manage to prepare
                // in time, we abort the generation switch. If all nodes prepare, we commit. If a node prepares
                // but doesn't receive a commit in time, it stops coordinating CDC-enabled writes until it
                // receives a commit or abort. This solution does not have a safety problem like the one
                // above, but it has an availability problem when nodes get disconnected from group 0 majority
                // in the middle of a CDC generation switch (when they are prepared to switch but not
                // committed) - they won't coordinate CDC-enabled writes until they reconnect to the
                // majority and commit.
                topology_mutation_builder builder(guard.write_timestamp());
                builder.set_transition_state(topology::transition_state::publish_cdc_generation)
                       .set_current_cdc_generation_id(cdc_gen_id)
                       .set_version(_topo_sm._topology.version + 1);
                auto str = ::format("committed new CDC generation, ID: {}", cdc_gen_id);
                co_await update_topology_state(std::move(guard), {builder.build()}, std::move(str));
            }
                break;
            case topology::transition_state::publish_cdc_generation: {
                // We just committed a new CDC generation in the commit_cdc_generation step.
                // Publish it to the user-facing distributed CDC description tables.
                auto curr_gen_id = _topo_sm._topology.current_cdc_generation_id.value();
                auto gen_data = co_await _sys_ks.read_cdc_generation(curr_gen_id.id);

                co_await _sys_dist_ks.local().create_cdc_desc(
                    curr_gen_id.ts, gen_data, { get_token_metadata().count_normal_token_owners() });

                topology_mutation_builder builder(guard.write_timestamp());
                if (_topo_sm._topology.transition_nodes.empty()) {
                    builder.del_transition_state();
                } else {
                    builder.set_transition_state(topology::transition_state::write_both_read_old);
                }
                auto str = ::format("published CDC generation, ID: {}", curr_gen_id);
                co_await update_topology_state(std::move(guard), {builder.build()}, std::move(str));
            }
                break;
            case topology::transition_state::write_both_read_old: {
                auto node = get_node_to_work_on(std::move(guard));

                // make sure all nodes know about new topology (we require all nodes to be alive for topo change for now)
                {
                    auto f = co_await coroutine::as_future(global_token_metadata_barrier(std::move(node)));
                    if (f.failed()) {
                        slogger.error("raft topology: transition_state::write_both_read_old, "
                                      "global_token_metadata_barrier failed, error {}",
                                      f.get_exception());
                        break;
                    }
                    node = std::move(f).get();
                }

                raft_topology_cmd cmd{raft_topology_cmd::command::stream_ranges};
                if (node.rs->state == node_state::removing) {
                    // tell all nodes to stream data of the removed node to new range owners
                    auto f = co_await coroutine::as_future(exec_global_command(std::move(node), cmd, true));
                    if (f.failed()) {
                        slogger.error("raft topology: send_raft_topology_cmd(stream_ranges) failed "
                                      "during removenode, error {}", f.get_exception());
                        break;
                    }
                    node = std::move(f).get();
                } else {
                    // Tell joining/leaving/replacing node to stream its ranges
                    try {
                        node = co_await exec_direct_command(std::move(node), cmd);
                    } catch (term_changed_error&) {
                        throw;
                    } catch (...) {
                        slogger.error("raft topology: send_raft_topology_cmd(stream_ranges) failed with exception"
                                      " (node state is {}): {}", node.rs->state, std::current_exception());
                        break;
                    }
                }
                // Streaming completed. We can now move tokens state to topology::transition_state::write_both_read_new
                topology_mutation_builder builder(node.guard.write_timestamp());
                builder
                    .set_transition_state(topology::transition_state::write_both_read_new)
                    .set_version(_topo_sm._topology.version + 1);
                auto str = ::format("{}: streaming completed for node {}", node.rs->state, node.id);
                co_await update_topology_state(take_guard(std::move(node)), {builder.build()}, std::move(str));
            }
                break;
            case topology::transition_state::write_both_read_new: {
                auto node = get_node_to_work_on(std::move(guard));

                // In this state writes goes to old and new replicas but reads start to be done from new replicas
                // Before we stop writing to old replicas we need to wait for all previous reads to complete
                {
                    auto f = co_await coroutine::as_future(global_token_metadata_barrier(std::move(node)));
                    if (f.failed()) {
                        slogger.error("raft topology: transition_state::write_both_read_new, "
                                      "global_token_metadata_barrier failed, error {}",
                                      f.get_exception());
                        break;
                    }
                    node = std::move(f).get();
                }
                switch(node.rs->state) {
                case node_state::bootstrapping: {
                    topology_mutation_builder builder(node.guard.write_timestamp());
                    builder.del_transition_state()
                           .set_version(_topo_sm._topology.version + 1)
                           .with_node(node.id)
                           .set("node_state", node_state::normal);
                    co_await update_topology_state(take_guard(std::move(node)), {builder.build()},
                                                   "bootstrap: read fence completed");
                    }
                    break;
                case node_state::removing:
                    co_await remove_from_group0(node.id);
                case node_state::decommissioning: {
                    topology_mutation_builder builder(node.guard.write_timestamp());
                    auto next_state = node.rs->state == node_state::decommissioning
                                        ? node_state::left_token_ring : node_state::left;
                    builder.del_transition_state()
                           .set_version(_topo_sm._topology.version + 1)
                           .with_node(node.id)
                           .del("tokens")
                           .set("node_state", next_state);
                    auto str = ::format("{}: read fence completed", node.rs->state);
                    co_await update_topology_state(take_guard(std::move(node)), {builder.build()}, std::move(str));
                }
                    break;
                case node_state::replacing: {
                    topology_mutation_builder builder1(node.guard.write_timestamp());
                    // Move new node to 'normal'
                    builder1.del_transition_state()
                            .set_version(_topo_sm._topology.version + 1)
                            .with_node(node.id)
                            .set("node_state", node_state::normal);

                    // Move old node to 'left'
                    topology_mutation_builder builder2(node.guard.write_timestamp());
                    builder2.with_node(parse_replaced_node(node))
                            .del("tokens")
                            .set("node_state", node_state::left);
                    co_await update_topology_state(take_guard(std::move(node)), {builder1.build(), builder2.build()},
                                                  "replace: read fence completed");
                    }
                    break;
                default:
                    on_fatal_internal_error(slogger, ::format(
                            "Ring state on node {} is write_both_read_new while the node is in state {}",
                            node.id, node.rs->state));
                }
                // Reads are fenced. We can now remove topology::transition_state and move node state to normal
            }
                break;
        }
        co_return true;
    };

    // Called when there is no ongoing topology transition.
    // Used to start new topology transitions using node requests or perform node operations
    // that don't change the topology (like rebuild).
    future<> handle_node_transition(node_to_work_on&& node) {
        slogger.info("raft topology: coordinator fiber found a node to work on id={} state={}", node.id, node.rs->state);

        switch (node.rs->state) {
            case node_state::normal:
            case node_state::none: {
                // if the state is none there have to be either 'join' or 'replace' request
                // if the state is normal there have to be either 'leave', 'remove' or 'rebuild' request
                topology_mutation_builder builder(node.guard.write_timestamp());
                switch (node.request.value()) {
                    case topology_request::join: {
                        assert(!node.rs->ring);
                        auto num_tokens = std::get<uint32_t>(node.req_param.value());
                        // A node just joined and does not have tokens assigned yet
                        // Need to assign random tokens to the node
                        auto tmptr = get_token_metadata_ptr();
                        auto bootstrap_tokens = dht::boot_strapper::get_random_bootstrap_tokens(
                                tmptr, num_tokens, dht::check_token_endpoint::yes);

                        auto [gen_uuid, gen_mutations] = co_await prepare_new_cdc_generation_data(
                                tmptr, node.guard, bootstrapping_info{bootstrap_tokens, *node.rs});
                        std::vector<canonical_mutation> updates{gen_mutations.begin(), gen_mutations.end()};

                        // Write chosen tokens and CDC generation data through raft.
                        builder.set_transition_state(topology::transition_state::commit_cdc_generation)
                               .set_new_cdc_generation_data_uuid(gen_uuid)
                               .with_node(node.id)
                               .set("node_state", node_state::bootstrapping)
                               .del("topology_request")
                               .set("tokens", bootstrap_tokens);
                        updates.push_back(builder.build());
                        auto reason = ::format(
                            "bootstrap: assign tokens and insert CDC generation data (UUID: {})", gen_uuid);
                        co_await update_topology_state(take_guard(std::move(node)), {std::move(updates)}, reason);
                        break;
                        }
                    case topology_request::leave:
                        assert(node.rs->ring);
                        // start decommission and put tokens of decommissioning nodes into write_both_read_old state
                        // meaning that reads will go to the replica being decommissioned
                        // but writes will go to new owner as well
                        builder.set_transition_state(topology::transition_state::write_both_read_old)
                               .set_version(_topo_sm._topology.version + 1)
                               .with_node(node.id)
                               .set("node_state", node_state::decommissioning)
                               .del("topology_request");
                        co_await update_topology_state(take_guard(std::move(node)), {builder.build()},
                                                       "start decommission");
                        break;
                    case topology_request::remove:
                        assert(node.rs->ring);
                        // start removing and put tokens of a node been removed into write_both_read_old state
                        // meaning that reads will go to the replica being removed (it is dead though)
                        // but writes will go to new owner as well
                        builder.set_transition_state(topology::transition_state::write_both_read_old)
                               .set_version(_topo_sm._topology.version + 1)
                               .with_node(node.id)
                               .set("node_state", node_state::removing)
                               .del("topology_request");
                        co_await update_topology_state(take_guard(std::move(node)), {builder.build()},
                                                       "start removenode");
                        break;
                    case topology_request::replace: {
                        assert(!node.rs->ring);
                        auto replaced_id = std::get<raft::server_id>(node.req_param.value());
                        auto it = _topo_sm._topology.normal_nodes.find(replaced_id);
                        assert(it != _topo_sm._topology.normal_nodes.end());
                        assert(it->second.ring && it->second.state == node_state::normal);
                        // start replacing and take ownership of the tokens of a node been replaced
                        // and put them into write_both_read_old state meaning that reads will go
                        // to the replica being removed (it is dead though) but writes will go to new owner as well
                        builder.set_transition_state(topology::transition_state::write_both_read_old)
                               .set_version(_topo_sm._topology.version + 1)
                               .with_node(node.id)
                               .set("node_state", node_state::replacing)
                               .del("topology_request")
                               .set("tokens", it->second.ring->tokens);
                        co_await update_topology_state(take_guard(std::move(node)), {builder.build()}, "start replace");
                        break;
                        }
                    case topology_request::rebuild: {
                        topology_mutation_builder builder(node.guard.write_timestamp());
                        builder.with_node(node.id)
                               .set("node_state", node_state::rebuilding)
                               .del("topology_request");
                        co_await update_topology_state(take_guard(std::move(node)), {builder.build()},
                                                       "start rebuilding");
                        break;
                    }
                }
                break;
            }
            case node_state::rebuilding: {
                node = co_await exec_direct_command(
                        std::move(node), raft_topology_cmd::command::stream_ranges);
                topology_mutation_builder builder(node.guard.write_timestamp());
                builder.with_node(node.id)
                       .set("node_state", node_state::normal)
                       .del("rebuild_option");
                co_await update_topology_state(take_guard(std::move(node)), {builder.build()}, "rebuilding completed");
            }
                break;
            case node_state::left_token_ring: {
                if (node.id == _raft.id()) {
                    // Someone else needs to coordinate the rest of the decommission process,
                    // because the decommissioning node is going to shut down in the middle of this state.
                    slogger.info("raft topology: coordinator is decommissioning; giving up leadership");
                    // Become a nonvoter which triggers a leader stepdown.
                    co_await _group0.become_nonvoter();
                    if (_raft.is_leader()) {
                        co_await _raft.wait_for_state_change(&_as);
                    }

                    // throw term_changed_error so we leave the coordinator loop instead of trying another
                    // read_barrier which may fail with an (harmless, but unnecessary and annoying) error
                    // telling us we're not in the configuration anymore (we'll get removed by the new
                    // coordinator)
                    throw term_changed_error{};

                    // Note: if we restart after this point and become a voter
                    // and then a coordinator again, it's fine - we'll just repeat this step.
                    // (If we're in `left` state when we try to restart we won't
                    // be able to become a voter - we'll be banned from the cluster.)
                }

                // Wait until other nodes observe the new token ring and stop sending writes to this node.
                {
                    auto id = node.id;
                    auto f = co_await coroutine::as_future(global_token_metadata_barrier(std::move(node)));
                    if (f.failed()) {
                        slogger.error("raft topology: node_state::left_token_ring (node: {}), "
                                      "global_token_metadata_barrier failed, error {}",
                                      id, f.get_exception());
                        break;
                    }
                    node = std::move(f).get();
                }

                // Tell the node to shut down.
                // This is done to improve user experience when there are no failures.
                // In the next state (`node_state::left`), the node will be banned by the rest of the cluster,
                // so there's no guarantee that it would learn about entering that state even if it was still
                // a member of group0, hence we use a separate direct RPC in this state to shut it down.
                //
                // There is the possibility that the node will never get the message
                // and decommission will hang on that node.
                // This is fine for the rest of the cluster - we will still remove, ban the node and continue.
                auto node_id = node.id;
                bool shutdown_failed = false;
                try {
                    node = co_await exec_direct_command(std::move(node), raft_topology_cmd::command::shutdown);
                } catch (...) {
                    slogger.warn("raft topology: failed to tell node {} to shut down - it may hang."
                                 " It's safe to shut it down manually now. (Exception: {})",
                                 node.id, std::current_exception());
                    shutdown_failed = true;
                }
                if (shutdown_failed) {
                    node = retake_node(co_await start_operation(), node_id);
                }

                // Remove the node from group0 here - in general, it won't be able to leave on its own
                // because we'll ban it as soon as we tell it to shut down.
                co_await remove_from_group0(node.id);

                topology_mutation_builder builder(node.guard.write_timestamp());
                builder.with_node(node.id)
                       .set("node_state", node_state::left);
                auto str = ::format("finished decommissioning node {}", node.id);
                co_await update_topology_state(take_guard(std::move(node)), {builder.build()}, std::move(str));
            }
                break;
            case node_state::bootstrapping:
            case node_state::decommissioning:
            case node_state::removing:
            case node_state::replacing:
                // Should not get here
                on_fatal_internal_error(slogger, ::format(
                    "Found node {} in state {} but there is no ongoing topology transition",
                    node.id, node.rs->state));
            case node_state::left:
                // Should not get here
                on_fatal_internal_error(slogger, ::format(
                        "Topology coordinator is called for node {} in state 'left'", node.id));
                break;
        }
    };

public:
    topology_coordinator(
            sharded<db::system_distributed_keyspace>& sys_dist_ks,
            netw::messaging_service& messaging, locator::shared_token_metadata& shared_tm,
            db::system_keyspace& sys_ks, replica::database& db, service::raft_group0& group0,
            service::topology_state_machine& topo_sm, abort_source& as, raft::server& raft_server,
            raft_topology_cmd_handler_type raft_topology_cmd_handler,
            std::chrono::milliseconds ring_delay)
        : _sys_dist_ks(sys_dist_ks), _messaging(messaging), _shared_tm(shared_tm), _sys_ks(sys_ks), _db(db)
        , _group0(group0), _address_map(_group0.address_map()), _topo_sm(topo_sm), _as(as)
        , _raft(raft_server), _term(raft_server.get_current_term())
        , _raft_topology_cmd_handler(std::move(raft_topology_cmd_handler))
        , _ring_delay(ring_delay)
    {}

    future<> run();
};

future<> topology_coordinator::run() {
    slogger.info("raft topology: start topology coordinator fiber");

    auto abort = _as.subscribe([this] () noexcept {
        _topo_sm.event.signal();
    });

    bool wait_for_event = false;

    while (!_as.abort_requested()) {
        try {
            if (wait_for_event) {
                slogger.trace("raft topology: topology coordinator fiber has nothing to do. Sleeping.");
                co_await _topo_sm.event.when();
                slogger.trace("raft topology: topology coordinator fiber got an event");
                wait_for_event = false;
            }

            auto guard = co_await start_operation();
            co_await cleanup_group0_config_if_needed();

            bool had_work = co_await handle_topology_transition(std::move(guard));
            if (!had_work) {
                // Nothing to work on. Wait for topology change event.
                wait_for_event = true;
                continue;
            }
        } catch (raft::request_aborted&) {
            slogger.debug("raft topology: topology change coordinator fiber aborted");
        } catch (group0_concurrent_modification&) {
        } catch (term_changed_error&) {
            // Term changed. We may no longer be a leader
            slogger.debug("raft topology: topology change coordinator fiber notices term change {} -> {}", _term, _raft.get_current_term());
        } catch (...) {
            slogger.error("raft topology: topology change coordinator fiber got error {}", std::current_exception());
        }
        co_await coroutine::maybe_yield();
    }
}

future<> storage_service::raft_state_monitor_fiber(raft::server& raft, cdc::generation_service& cdc_gen_svc, sharded<db::system_distributed_keyspace>& sys_dist_ks) {
    std::optional<abort_source> as;
    try {
        while (!_abort_source.abort_requested()) {
            // Wait for a state change in case we are not a leader yet, or we are are the leader
            // and coordinator work is running (in which case 'as' is engaged)
            while (!raft.is_leader() || as) {
                co_await raft.wait_for_state_change(&_abort_source);
                if (as) {
                    as->request_abort(); // we are no longer a leader, so abort the coordinator
                    co_await std::exchange(_topology_change_coordinator, make_ready_future<>());
                    as = std::nullopt;
                }
            }
            // We are the leader now but that can change any time!
            as.emplace();
            // start topology change coordinator in the background
            _topology_change_coordinator = do_with(
                std::make_unique<topology_coordinator>(
                    sys_dist_ks, _messaging.local(), _shared_token_metadata,
                    _sys_ks.local(), _db.local(), *_group0, _topology_state_machine, *as, raft,
                    std::bind_front(&storage_service::raft_topology_cmd_handler, this), get_ring_delay()),
                [] (std::unique_ptr<topology_coordinator>& coordinator) { return coordinator->run(); });
        }
    } catch (...) {
        slogger.info("raft_state_monitor_fiber aborted with {}", std::current_exception());
    }
    if (as) {
        as->request_abort(); // abort current coordinator if running
        co_await std::move(_topology_change_coordinator);
    }
}


future<> storage_service::raft_replace(raft::server& raft_server, raft::server_id replaced_id, gms::inet_address replaced_ip) {
    // Read barrier to access the latest topology. Quorum of nodes has to be alive.
    co_await raft_server.read_barrier(&_abort_source);

    auto it = _topology_state_machine._topology.find(raft_server.id());
    if (it && it->second.state != node_state::replacing) {
        throw std::runtime_error(::format("Cannot do \"replace address\" operation with a node that is in state: {}", it->second.state));
    }

    // add myself to topology with request to replace
    while (!_topology_state_machine._topology.contains(raft_server.id())) {
        slogger.info("raft topology: adding myself to topology for replace: {} replacing {}", raft_server.id(), replaced_id);
        auto guard = co_await _group0->client().start_operation(&_abort_source);

        auto it = _topology_state_machine._topology.normal_nodes.find(replaced_id);
        if (it == _topology_state_machine._topology.normal_nodes.end()) {
            throw std::runtime_error(::format("Cannot replace node {}/{} because it is not in the 'normal' state", replaced_ip, replaced_id));
        }

        auto& rs = it->second;
        topology_mutation_builder builder(guard.write_timestamp());
        builder.with_node(raft_server.id())
               .set("node_state", node_state::none)
               .set("datacenter", rs.datacenter)
               .set("rack", rs.rack)
               .set("release_version", version::release())
               .set("topology_request", topology_request::replace)
               .set("replaced_id", replaced_id)
               .set("num_tokens", _db.local().get_config().num_tokens())
               .set("shard_count", smp::count)
               .set("ignore_msb", _db.local().get_config().murmur3_partitioner_ignore_msb_bits())
               .set("supported_features", _feature_service.supported_feature_set());
        topology_change change{{builder.build()}};
        group0_command g0_cmd = _group0->client().prepare_command(std::move(change), guard, ::format("replace {}/{}: add myself ({}) to topology", replaced_id, replaced_ip, raft_server.id()));
        try {
            co_await _group0->client().add_entry(std::move(g0_cmd), std::move(guard), &_abort_source);
        } catch (group0_concurrent_modification&) {
            slogger.info("raft topology: replace: concurrent operation is detected, retrying.");
        }
    }

    co_return;
}

future<> storage_service::raft_bootstrap(raft::server& raft_server) {
    // We try to find ourself in the topology without doing read barrier
    // first to not require quorum of live nodes during regular boot. But
    // if we are not in the topology it either means this is the first boot
    // or we failed during bootstrap so do a read barrier (which requires
    // quorum to be alive) and re-check.
    if (!_topology_state_machine._topology.contains(raft_server.id())) {
        co_await raft_server.read_barrier(&_abort_source);
    }

    while (!_topology_state_machine._topology.contains(raft_server.id())) {
        slogger.info("raft topology: adding myself to topology: {}", raft_server.id());
        // Current topology does not contains this node. Bootstrap is needed!
        auto guard = co_await _group0->client().start_operation(&_abort_source);
        topology_mutation_builder builder(guard.write_timestamp());
        builder.with_node(raft_server.id())
               .set("node_state", node_state::none)
               .set("datacenter", _snitch.local()->get_datacenter())
               .set("rack", _snitch.local()->get_rack())
               .set("release_version", version::release())
               .set("topology_request", topology_request::join)
               .set("num_tokens", _db.local().get_config().num_tokens())
               .set("shard_count", smp::count)
               .set("ignore_msb", _db.local().get_config().murmur3_partitioner_ignore_msb_bits())
               .set("supported_features", _feature_service.supported_feature_set());
        topology_change change{{builder.build()}};
        group0_command g0_cmd = _group0->client().prepare_command(std::move(change), guard, "bootstrap: add myself to topology");
        try {
            co_await _group0->client().add_entry(std::move(g0_cmd), std::move(guard), &_abort_source);
        } catch (group0_concurrent_modification&) {
            slogger.info("raft topology: bootstrap: concurrent operation is detected, retrying.");
        }
    }
}

future<> storage_service::update_topology_with_local_metadata(raft::server& raft_server) {
    // TODO: include more metadata here
    auto local_shard_count = smp::count;
    auto local_ignore_msb = _db.local().get_config().murmur3_partitioner_ignore_msb_bits();
    auto local_release_version = version::release();
    auto local_supported_features = boost::copy_range<std::set<sstring>>(_feature_service.supported_feature_set());

    auto synchronized = [&] () {
        auto it = _topology_state_machine._topology.find(raft_server.id());
        if (!it) {
            throw std::runtime_error{"Removed from topology while performing metadata update"};
        }

        auto& replica_state = it->second;

        return replica_state.shard_count == local_shard_count
            && replica_state.ignore_msb == local_ignore_msb
            && replica_state.release_version == local_release_version
            && replica_state.supported_features == local_supported_features;
    };

    // We avoid performing a read barrier if we're sure that our metadata stored in topology
    // is the same as local metadata. Note that only we can update our metadata, other nodes cannot.
    //
    // We use a persisted flag `must_update_topology` to avoid the following scenario:
    // 1. the node restarts and its metadata changes
    // 2. the node commits the new metadata to topology, but before the update is applied
    //    to the local state machine, the node crashes
    // 3. then the metadata changes back to old values and node restarts again
    // 4. the local state machine tells us that we're in sync, which is wrong
    // If the persisted flag is true, it tells us that we attempted a metadata change earlier,
    // forcing us to perform a read barrier even when the local state machine tells us we're in sync.

    if (synchronized() && !(co_await _sys_ks.local().get_must_synchronize_topology())) {
        co_return;
    }

    while (true) {
        slogger.info("raft topology: refreshing topology to check if it's synchronized with local metadata");

        auto guard = co_await _group0->client().start_operation(&_abort_source);

        if (synchronized()) {
            break;
        }

        slogger.info("raft topology: updating topology with local metadata");

        co_await _sys_ks.local().set_must_synchronize_topology(true);

        topology_mutation_builder builder(guard.write_timestamp());
        builder.with_node(raft_server.id())
               .set("shard_count", local_shard_count)
               .set("ignore_msb", local_ignore_msb)
               .set("release_version", local_release_version)
               .set("supported_features", local_supported_features);
        topology_change change{{builder.build()}};
        group0_command g0_cmd = _group0->client().prepare_command(
                std::move(change), guard, ::format("{}: update topology with local metadata", raft_server.id()));

        try {
            co_await _group0->client().add_entry(std::move(g0_cmd), std::move(guard), &_abort_source);
        } catch (group0_concurrent_modification&) {
            slogger.info("raft topology: update topology with local metadata:"
                         " concurrent operation is detected, retrying.");
        }
    }

    co_await _sys_ks.local().set_must_synchronize_topology(false);
}

future<> storage_service::join_token_ring(cdc::generation_service& cdc_gen_service,
        sharded<db::system_distributed_keyspace>& sys_dist_ks,
        sharded<service::storage_proxy>& proxy,
        std::unordered_set<gms::inet_address> initial_contact_nodes,
        std::unordered_set<gms::inet_address> loaded_endpoints,
        std::unordered_map<gms::inet_address, sstring> loaded_peer_features,
        std::chrono::milliseconds delay,
        cql3::query_processor& qp) {
    std::unordered_set<token> bootstrap_tokens;
    std::map<gms::application_state, gms::versioned_value> app_states;
    /* The timestamp of the CDC streams generation that this node has proposed when joining.
     * This value is nullopt only when:
     * 1. this node is being upgraded from a non-CDC version,
     * 2. this node is starting for the first time or restarting with CDC previously disabled,
     *    in which case the value should become populated before we leave the join_token_ring procedure.
     *
     * Important: this variable is using only during the startup procedure. It is moved out from
     * at the end of `join_token_ring`; the responsibility handling of CDC generations is passed
     * to cdc::generation_service.
     *
     * DO NOT use this variable after `join_token_ring` (i.e. after we call `generation_service::after_join`
     * and pass it the ownership of the timestamp.
     */
    std::optional<cdc::generation_id> cdc_gen_id;

    if (_sys_ks.local().was_decommissioned()) {
        if (_db.local().get_config().override_decommission() && !_db.local().get_config().consistent_cluster_management()) {
            slogger.warn("This node was decommissioned, but overriding by operator request.");
            co_await _sys_ks.local().set_bootstrap_state(db::system_keyspace::bootstrap_state::COMPLETED);
        } else {
            auto msg = sstring("This node was decommissioned and will not rejoin the ring unless override_decommission=true has been set and consistent cluster management is not in use,"
                               "or all existing data is removed and the node is bootstrapped again");
            slogger.error("{}", msg);
            throw std::runtime_error(msg);
        }
    }

    bool replacing_a_node_with_same_ip = false;
    bool replacing_a_node_with_diff_ip = false;
    std::optional<replacement_info> ri;
    std::optional<gms::inet_address> replace_address;
    std::optional<locator::host_id> replaced_host_id;
    std::optional<raft_group0::replace_info> raft_replace_info;
    auto tmlock = std::make_unique<token_metadata_lock>(co_await get_token_metadata_lock());
    auto tmptr = co_await get_mutable_token_metadata_ptr();
    if (is_replacing()) {
        if (_sys_ks.local().bootstrap_complete()) {
            throw std::runtime_error("Cannot replace address with a node that is already bootstrapped");
        }
        ri = co_await prepare_replacement_info(initial_contact_nodes, loaded_peer_features);
        replace_address = ri->address;
        raft_replace_info = raft_group0::replace_info {
            .ip_addr = *replace_address,
            .raft_id = raft::server_id{ri->host_id.uuid()},
        };
        if (!_raft_topology_change_enabled) {
            bootstrap_tokens = std::move(ri->tokens);
            replacing_a_node_with_same_ip = *replace_address == get_broadcast_address();
            replacing_a_node_with_diff_ip = *replace_address != get_broadcast_address();

            slogger.info("Replacing a node with {} IP address, my address={}, node being replaced={}",
                get_broadcast_address() == *replace_address ? "the same" : "a different",
                get_broadcast_address(), *replace_address);
            tmptr->update_topology(*replace_address, std::move(ri->dc_rack), locator::node::state::leaving);
            co_await tmptr->update_normal_tokens(bootstrap_tokens, *replace_address);
            replaced_host_id = ri->host_id;
        }
    } else if (should_bootstrap()) {
        co_await check_for_endpoint_collision(initial_contact_nodes, loaded_peer_features);
    } else {
        auto local_features = _feature_service.supported_feature_set();
        slogger.info("Checking remote features with gossip, initial_contact_nodes={}", initial_contact_nodes);
        co_await _gossiper.do_shadow_round(initial_contact_nodes);
        _gossiper.check_knows_remote_features(local_features, loaded_peer_features);
        _gossiper.check_snitch_name_matches(_snitch.local()->get_name());
        // Check if the node is already removed from the cluster
        auto local_host_id = _db.local().get_config().host_id;
        auto my_ip = get_broadcast_address();
        if (!_gossiper.is_safe_for_restart(my_ip, local_host_id)) {
            throw std::runtime_error(::format("The node {} with host_id {} is removed from the cluster. Can not restart the removed node to join the cluster again!",
                    my_ip, local_host_id));
        }
        co_await _gossiper.reset_endpoint_state_map();
        for (auto ep : loaded_endpoints) {
            co_await _gossiper.add_saved_endpoint(ep);
        }
    }
    auto features = _feature_service.supported_feature_set();
    slogger.info("Save advertised features list in the 'system.{}' table", db::system_keyspace::LOCAL);
    // Save the advertised feature set to system.local table after
    // all remote feature checks are complete and after gossip shadow rounds are done.
    // At this point, the final feature set is already determined before the node joins the ring.
    co_await _sys_ks.local().save_local_supported_features(features);

    // If this is a restarting node, we should update tokens before gossip starts
    auto my_tokens = co_await _sys_ks.local().get_saved_tokens();
    bool restarting_normal_node = _sys_ks.local().bootstrap_complete() && !is_replacing() && !my_tokens.empty();
    if (restarting_normal_node) {
        slogger.info("Restarting a node in NORMAL status");
        // This node must know about its chosen tokens before other nodes do
        // since they may start sending writes to this node after it gossips status = NORMAL.
        // Therefore we update _token_metadata now, before gossip starts.
        tmptr->update_topology(get_broadcast_address(), _sys_ks.local().local_dc_rack(), locator::node::state::normal);
        co_await tmptr->update_normal_tokens(my_tokens, get_broadcast_address());

        cdc_gen_id = co_await _sys_ks.local().get_cdc_generation_id();
        if (!cdc_gen_id) {
            // We could not have completed joining if we didn't generate and persist a CDC streams timestamp,
            // unless we are restarting after upgrading from non-CDC supported version.
            // In that case we won't begin a CDC generation: it should be done by one of the nodes
            // after it learns that it everyone supports the CDC feature.
            cdc_log.warn(
                    "Restarting node in NORMAL status with CDC enabled, but no streams timestamp was proposed"
                    " by this node according to its local tables. Are we upgrading from a non-CDC supported version?");
        }
    }

    // have to start the gossip service before we can see any info on other nodes.  this is necessary
    // for bootstrap to get the load info it needs.
    // (we won't be part of the storage ring though until we add a counterId to our state, below.)
    // Seed the host ID-to-endpoint map with our own ID.
    auto local_host_id = _db.local().get_config().host_id;
    if (!replacing_a_node_with_diff_ip) {
        auto endpoint = get_broadcast_address();
        auto eps = _gossiper.get_endpoint_state_for_endpoint_ptr(endpoint);
        if (eps) {
            auto replace_host_id = _gossiper.get_host_id(get_broadcast_address());
            slogger.info("Host {}/{} is replacing {}/{} using the same address", local_host_id, endpoint, replace_host_id, endpoint);
        }
        tmptr->update_host_id(local_host_id, get_broadcast_address());
    }

    // Replicate the tokens early because once gossip runs other nodes
    // might send reads/writes to this node. Replicate it early to make
    // sure the tokens are valid on all the shards.
    co_await replicate_to_all_cores(std::move(tmptr));
    tmlock.reset();

    auto broadcast_rpc_address = utils::fb_utilities::get_broadcast_rpc_address();
    // Ensure we know our own actual Schema UUID in preparation for updates
    co_await db::schema_tables::recalculate_schema_version(_sys_ks, proxy, _feature_service);

    app_states.emplace(gms::application_state::NET_VERSION, versioned_value::network_version());
    app_states.emplace(gms::application_state::HOST_ID, versioned_value::host_id(local_host_id));
    app_states.emplace(gms::application_state::RPC_ADDRESS, versioned_value::rpcaddress(broadcast_rpc_address));
    app_states.emplace(gms::application_state::RELEASE_VERSION, versioned_value::release_version());
    app_states.emplace(gms::application_state::SUPPORTED_FEATURES, versioned_value::supported_features(features));
    app_states.emplace(gms::application_state::CACHE_HITRATES, versioned_value::cache_hitrates(""));
    app_states.emplace(gms::application_state::SCHEMA_TABLES_VERSION, versioned_value(db::schema_tables::version));
    app_states.emplace(gms::application_state::RPC_READY, versioned_value::cql_ready(false));
    app_states.emplace(gms::application_state::VIEW_BACKLOG, versioned_value(""));
    app_states.emplace(gms::application_state::SCHEMA, versioned_value::schema(_db.local().get_version()));
    if (restarting_normal_node) {
        // Order is important: both the CDC streams timestamp and tokens must be known when a node handles our status.
        // Exception: there might be no CDC streams timestamp proposed by us if we're upgrading from a non-CDC version.
        app_states.emplace(gms::application_state::TOKENS, versioned_value::tokens(my_tokens));
        app_states.emplace(gms::application_state::CDC_GENERATION_ID, versioned_value::cdc_generation_id(cdc_gen_id));
        app_states.emplace(gms::application_state::STATUS, versioned_value::normal(my_tokens));
    }
    if (replacing_a_node_with_same_ip || replacing_a_node_with_diff_ip) {
        app_states.emplace(gms::application_state::TOKENS, versioned_value::tokens(bootstrap_tokens));
    }
    app_states.emplace(gms::application_state::SNITCH_NAME, versioned_value::snitch_name(_snitch.local()->get_name()));
    app_states.emplace(gms::application_state::SHARD_COUNT, versioned_value::shard_count(smp::count));
    app_states.emplace(gms::application_state::IGNORE_MSB_BITS, versioned_value::ignore_msb_bits(_db.local().get_config().murmur3_partitioner_ignore_msb_bits()));

    for (auto&& s : _snitch.local()->get_app_states()) {
        app_states.emplace(s.first, std::move(s.second));
    }

    slogger.info("Starting up server gossip");

    auto generation_number = gms::generation_type(co_await _sys_ks.local().increment_and_get_generation());
    auto advertise = gms::advertise_myself(!replacing_a_node_with_same_ip);
    co_await _gossiper.start_gossiping(generation_number, app_states, advertise);

    if (!_raft_topology_change_enabled && should_bootstrap()) {
        // Wait for NORMAL state handlers to finish for existing nodes now, so that connection dropping
        // (happening at the end of `handle_state_normal`: `notify_joined`) doesn't interrupt
        // group 0 joining or repair. (See #12764, #12956, #12972, #13302)
        //
        // But before we can do that, we must make sure that gossip sees at least one other node
        // and fetches the list of peers from it; otherwise `wait_for_normal_state_handled_on_boot`
        // may trivially finish without waiting for anyone.
        co_await _gossiper.wait_for_live_nodes_to_show_up(2);

        auto ignore_nodes = ri
                ? parse_node_list(_db.local().get_config().ignore_dead_nodes_for_replace(), get_token_metadata())
                // TODO: specify ignore_nodes for bootstrap
                : std::unordered_set<gms::inet_address>{};
        auto sync_nodes = co_await get_nodes_to_sync_with(ignore_nodes);
        if (ri) {
            sync_nodes.erase(ri->address);
        }

        // Note: in Raft topology mode this is unnecessary.
        // Node state changes are propagated to the cluster through explicit global barriers.
        co_await wait_for_normal_state_handled_on_boot(sync_nodes);

        // NORMAL doesn't necessarily mean UP (#14042). Wait for these nodes to be UP as well
        // to reduce flakiness (we need them to be UP to perform CDC generation write and for repair/streaming).
        //
        // This could be done in Raft topology mode as well, but the calculation of nodes to sync with
        // has to be done based on topology state machine instead of gossiper as it is here;
        // furthermore, the place in the code where we do this has to be different (it has to be coordinated
        // by the topology coordinator after it joins the node to the cluster).
        std::vector<gms::inet_address> sync_nodes_vec{sync_nodes.begin(), sync_nodes.end()};
        slogger.info("Waiting for nodes {} to be alive", sync_nodes_vec);
        co_await _gossiper.wait_alive(sync_nodes_vec, std::chrono::seconds{30});
        slogger.info("Nodes {} are alive", sync_nodes_vec);
    }

    assert(_group0);
    // if the node is bootstrapped the functin will do nothing since we already created group0 in main.cc
    co_await _group0->setup_group0(_sys_ks.local(), initial_contact_nodes, raft_replace_info, *this, qp, _migration_manager.local(), cdc_gen_service);

    raft::server* raft_server = co_await [this] () -> future<raft::server*> {
        if (!_raft_topology_change_enabled) {
            co_return nullptr;
        } else if (_sys_ks.local().bootstrap_complete()) {
            auto [upgrade_lock_holder, upgrade_state] = co_await _group0->client().get_group0_upgrade_state();
            co_return upgrade_state == group0_upgrade_state::use_post_raft_procedures ? &_group0->group0_server() : nullptr;
        } else {
            auto upgrade_state = (co_await _group0->client().get_group0_upgrade_state()).second;
            if (upgrade_state != group0_upgrade_state::use_post_raft_procedures) {
                on_internal_error(slogger, "raft topology: cluster not upgraded to use group 0 after setup_group0");
            }
            co_return &_group0->group0_server();
        }
    } ();

    auto schema_change_announce = _db.local().observable_schema_version().observe([this] (table_schema_version schema_version) mutable {
        _migration_manager.local().passive_announce(std::move(schema_version));
    });
    _listeners.emplace_back(make_lw_shared(std::move(schema_change_announce)));
    co_await _gossiper.wait_for_gossip_to_settle();
    co_await _feature_service.enable_features_on_join(_gossiper, _sys_ks.local());

    set_mode(mode::JOINING);

    if (raft_server) { // Raft is enabled. Check if we need to bootstrap ourself using raft
        slogger.info("topology changes are using raft");

        // start topology coordinator fiber
        _raft_state_monitor = raft_state_monitor_fiber(*raft_server, cdc_gen_service, sys_dist_ks);

        // Need to start system_distributed_keyspace before bootstrap because bootstraping
        // process may access those tables.
        supervisor::notify("starting system distributed keyspace");
        co_await sys_dist_ks.invoke_on_all(&db::system_distributed_keyspace::start);

        if (is_replacing()) {
            assert(raft_replace_info);
            co_await raft_replace(*raft_server, raft_replace_info->raft_id, raft_replace_info->ip_addr);
        } else {
            co_await raft_bootstrap(*raft_server);
        }

        // Wait until we enter one of the final states
        co_await _topology_state_machine.event.when([this, raft_server] {
            return _topology_state_machine._topology.normal_nodes.contains(raft_server->id()) ||
            _topology_state_machine._topology.left_nodes.contains(raft_server->id());
        });

        if (_topology_state_machine._topology.left_nodes.contains(raft_server->id())) {
            throw std::runtime_error("A node that already left the cluster cannot be restarted");
        }

        co_await update_topology_with_local_metadata(*raft_server);

        // Node state is enough to know that bootstrap has completed, but to make legacy code happy
        // let it know that the bootstrap is completed as well
        co_await _sys_ks.local().set_bootstrap_state(db::system_keyspace::bootstrap_state::COMPLETED);
        set_mode(mode::NORMAL);

        if (get_token_metadata().sorted_tokens().empty()) {
            auto err = ::format("join_token_ring: Sorted token in token_metadata is empty");
            slogger.error("{}", err);
            throw std::runtime_error(err);
        }

        co_await _group0->finish_setup_after_join(*this, qp, _migration_manager.local(), cdc_gen_service);
        co_return;
    }

    // We bootstrap if we haven't successfully bootstrapped before, as long as we are not a seed.
    // If we are a seed, or if the user manually sets auto_bootstrap to false,
    // we'll skip streaming data from other nodes and jump directly into the ring.
    //
    // The seed check allows us to skip the RING_DELAY sleep for the single-node cluster case,
    // which is useful for both new users and testing.
    //
    // We attempted to replace this with a schema-presence check, but you need a meaningful sleep
    // to get schema info from gossip which defeats the purpose.  See CASSANDRA-4427 for the gory details.
    if (should_bootstrap()) {
        bool resume_bootstrap = _sys_ks.local().bootstrap_in_progress();
        if (resume_bootstrap) {
            slogger.warn("Detected previous bootstrap failure; retrying");
        } else {
            co_await _sys_ks.local().set_bootstrap_state(db::system_keyspace::bootstrap_state::IN_PROGRESS);
        }
        slogger.info("waiting for ring information");

        // if our schema hasn't matched yet, keep sleeping until it does
        // (post CASSANDRA-1391 we don't expect this to be necessary very often, but it doesn't hurt to be careful)
        co_await wait_for_ring_to_settle();

        if (!replace_address) {
            auto tmptr = get_token_metadata_ptr();

            if (tmptr->is_normal_token_owner(get_broadcast_address())) {
                throw std::runtime_error("This node is already a member of the token ring; bootstrap aborted. (If replacing a dead node, remove the old one from the ring first.)");
            }
            slogger.info("getting bootstrap token");
            if (resume_bootstrap) {
                bootstrap_tokens = co_await _sys_ks.local().get_saved_tokens();
                if (!bootstrap_tokens.empty()) {
                    slogger.info("Using previously saved tokens = {}", bootstrap_tokens);
                } else {
                    bootstrap_tokens = boot_strapper::get_bootstrap_tokens(tmptr, _db.local().get_config(), dht::check_token_endpoint::yes);
                }
            } else {
                bootstrap_tokens = boot_strapper::get_bootstrap_tokens(tmptr, _db.local().get_config(), dht::check_token_endpoint::yes);
            }
        } else {
            if (*replace_address != get_broadcast_address()) {
                // Sleep additionally to make sure that the server actually is not alive
                // and giving it more time to gossip if alive.
                slogger.info("Sleeping before replacing {}...", *replace_address);
                co_await sleep_abortable(2 * get_ring_delay(), _abort_source);

                // check for operator errors...
                const auto tmptr = get_token_metadata_ptr();
                for (auto token : bootstrap_tokens) {
                    auto existing = tmptr->get_endpoint(token);
                    if (existing) {
                        auto* eps = _gossiper.get_endpoint_state_for_endpoint_ptr(*existing);
                        if (eps && eps->get_update_timestamp() > gms::gossiper::clk::now() - delay) {
                            throw std::runtime_error("Cannot replace a live node...");
                        }
                    } else {
                        throw std::runtime_error(::format("Cannot replace token {} which does not exist!", token));
                    }
                }
            } else {
                slogger.info("Sleeping before replacing {}...", *replace_address);
                co_await sleep_abortable(get_ring_delay(), _abort_source);
            }
            slogger.info("Replacing a node with token(s): {}", bootstrap_tokens);
            // bootstrap_tokens was previously set using tokens gossiped by the replaced node
        }
        co_await sys_dist_ks.invoke_on_all(&db::system_distributed_keyspace::start);
        co_await mark_existing_views_as_built(sys_dist_ks);
        co_await _sys_ks.local().update_tokens(bootstrap_tokens);
        co_await bootstrap(cdc_gen_service, bootstrap_tokens, cdc_gen_id, ri);
    } else {
        supervisor::notify("starting system distributed keyspace");
        co_await sys_dist_ks.invoke_on_all(&db::system_distributed_keyspace::start);
        bootstrap_tokens = co_await _sys_ks.local().get_saved_tokens();
        if (bootstrap_tokens.empty()) {
            bootstrap_tokens = boot_strapper::get_bootstrap_tokens(get_token_metadata_ptr(), _db.local().get_config(), dht::check_token_endpoint::no);
            co_await _sys_ks.local().update_tokens(bootstrap_tokens);
        } else {
            size_t num_tokens = _db.local().get_config().num_tokens();
            if (bootstrap_tokens.size() != num_tokens) {
                throw std::runtime_error(::format("Cannot change the number of tokens from {:d} to {:d}", bootstrap_tokens.size(), num_tokens));
            } else {
                slogger.info("Using saved tokens {}", bootstrap_tokens);
            }
        }
    }

    slogger.debug("Setting tokens to {}", bootstrap_tokens);
    co_await mutate_token_metadata([this, &bootstrap_tokens] (mutable_token_metadata_ptr tmptr) {
        // This node must know about its chosen tokens before other nodes do
        // since they may start sending writes to this node after it gossips status = NORMAL.
        // Therefore, in case we haven't updated _token_metadata with our tokens yet, do it now.
        tmptr->update_topology(get_broadcast_address(), _sys_ks.local().local_dc_rack(), locator::node::state::normal);
        return tmptr->update_normal_tokens(bootstrap_tokens, get_broadcast_address());
    });

    if (!_sys_ks.local().bootstrap_complete()) {
        // If we're not bootstrapping then we shouldn't have chosen a CDC streams timestamp yet.
        assert(should_bootstrap() || !cdc_gen_id);

        // Don't try rewriting CDC stream description tables.
        // See cdc.md design notes, `Streams description table V1 and rewriting` section, for explanation.
        co_await _sys_ks.local().cdc_set_rewritten(std::nullopt);
    }

    if (!cdc_gen_id) {
        // If we didn't observe any CDC generation at this point, then either
        // 1. we're replacing a node,
        // 2. we've already bootstrapped, but are upgrading from a non-CDC version,
        // 3. we're the first node, starting a fresh cluster.

        // In the replacing case we won't create any CDC generation: we're not introducing any new tokens,
        // so the current generation used by the cluster is fine.

        // In the case of an upgrading cluster, one of the nodes is responsible for creating
        // the first CDC generation. We'll check if it's us.

        // Finally, if we're the first node, we'll create the first generation.

        if (!is_replacing()
                && (!_sys_ks.local().bootstrap_complete()
                    || cdc::should_propose_first_generation(get_broadcast_address(), _gossiper))) {
            try {
                cdc_gen_id = co_await cdc_gen_service.legacy_make_new_generation(bootstrap_tokens, !is_first_node());
            } catch (...) {
                cdc_log.warn(
                    "Could not create a new CDC generation: {}. This may make it impossible to use CDC or cause performance problems."
                    " Use nodetool checkAndRepairCdcStreams to fix CDC.", std::current_exception());
            }
        }
    }

    // Persist the CDC streams timestamp before we persist bootstrap_state = COMPLETED.
    if (cdc_gen_id) {
        co_await _sys_ks.local().update_cdc_generation_id(*cdc_gen_id);
    }
    // If we crash now, we will choose a new CDC streams timestamp anyway (because we will also choose a new set of tokens).
    // But if we crash after setting bootstrap_state = COMPLETED, we will keep using the persisted CDC streams timestamp after restarting.

    co_await _sys_ks.local().set_bootstrap_state(db::system_keyspace::bootstrap_state::COMPLETED);
    // At this point our local tokens and CDC streams timestamp are chosen (bootstrap_tokens, cdc_gen_id) and will not be changed.

    // start participating in the ring.
    co_await set_gossip_tokens(_gossiper, bootstrap_tokens, cdc_gen_id);

    set_mode(mode::NORMAL);

    if (get_token_metadata().sorted_tokens().empty()) {
        auto err = ::format("join_token_ring: Sorted token in token_metadata is empty");
        slogger.error("{}", err);
        throw std::runtime_error(err);
    }

    assert(_group0);
    co_await _group0->finish_setup_after_join(*this, qp, _migration_manager.local(), cdc_gen_service);
    co_await cdc_gen_service.after_join(std::move(cdc_gen_id));
}

future<> storage_service::mark_existing_views_as_built(sharded<db::system_distributed_keyspace>& sys_dist_ks) {
    assert(this_shard_id() == 0);
    auto views = _db.local().get_views();
    co_await coroutine::parallel_for_each(views, [this, &sys_dist_ks] (view_ptr& view) -> future<> {
        co_await _sys_ks.local().mark_view_as_built(view->ks_name(), view->cf_name());
        co_await sys_dist_ks.local().finish_view_build(view->ks_name(), view->cf_name());
    });
}

std::unordered_set<gms::inet_address> storage_service::parse_node_list(sstring comma_separated_list, const token_metadata& tm) {
    std::vector<sstring> ignore_nodes_strs;
    std::unordered_set<gms::inet_address> ignore_nodes;
    boost::split(ignore_nodes_strs, comma_separated_list, boost::is_any_of(","));
    for (std::string n : ignore_nodes_strs) {
        try {
            std::replace(n.begin(), n.end(), '\"', ' ');
            std::replace(n.begin(), n.end(), '\'', ' ');
            boost::trim_all(n);
            if (!n.empty()) {
                auto ep_and_id = tm.parse_host_id_and_endpoint(n);
                ignore_nodes.insert(ep_and_id.endpoint);
            }
        } catch (...) {
            throw std::runtime_error(::format("Failed to parse node list: {}: invalid node={}: {}", ignore_nodes_strs, n, std::current_exception()));
        }
    }
    return ignore_nodes;
}

future<std::unordered_set<gms::inet_address>> storage_service::get_nodes_to_sync_with(
        const std::unordered_set<gms::inet_address>& ignore_nodes) {
    std::unordered_set<gms::inet_address> result;
    for (const auto& node :_gossiper.get_endpoints()) {
        co_await coroutine::maybe_yield();
        slogger.info("Check node={}, status={}", node, _gossiper.get_gossip_status(node));
        if (node != get_broadcast_address() &&
                _gossiper.is_normal_ring_member(node) &&
                !ignore_nodes.contains(node)) {
            result.insert(node);
        }
    }
    co_return result;
}

// Runs inside seastar::async context
future<> storage_service::bootstrap(cdc::generation_service& cdc_gen_service, std::unordered_set<token>& bootstrap_tokens, std::optional<cdc::generation_id>& cdc_gen_id, const std::optional<replacement_info>& replacement_info) {
    return seastar::async([this, &bootstrap_tokens, &cdc_gen_id, &cdc_gen_service, &replacement_info] {
        auto bootstrap_rbno = is_repair_based_node_ops_enabled(streaming::stream_reason::bootstrap);

        set_mode(mode::BOOTSTRAP);
        slogger.debug("bootstrap: rbno={} replacing={}", bootstrap_rbno, is_replacing());

        // Wait until we know tokens of existing node before announcing replacing status.
        slogger.info("Wait until local node knows tokens of peer nodes");
        _gossiper.wait_for_range_setup().get();

        _db.invoke_on_all([] (replica::database& db) {
            for (auto& cf : db.get_non_system_column_families()) {
                cf->notify_bootstrap_or_replace_start();
            }
        }).get();

        {
            int retry = 0;
            while (get_token_metadata_ptr()->count_normal_token_owners() == 0) {
                if (retry++ < 500) {
                    sleep_abortable(std::chrono::milliseconds(10), _abort_source).get();
                    continue;
                }
                // We're joining an existing cluster, so there are normal nodes in the cluster.
                // We've waited for tokens to arrive.
                // But we didn't see any normal token owners. Something's wrong, we cannot proceed.
                throw std::runtime_error{
                        "Failed to learn about other nodes' tokens during bootstrap or replace. Make sure that:\n"
                        " - the node can contact other nodes in the cluster,\n"
                        " - the `ring_delay` parameter is large enough (the 30s default should be enough for small-to-middle-sized clusters),\n"
                        " - a node with this IP didn't recently leave the cluster. If it did, wait for some time first (the IP is quarantined),\n"
                        "and retry the bootstrap/replace."};
            }
        }

        if (!replacement_info) {
            // Even if we reached this point before but crashed, we will make a new CDC generation.
            // It doesn't hurt: other nodes will (potentially) just do more generation switches.
            // We do this because with this new attempt at bootstrapping we picked a different set of tokens.

            // Update pending ranges now, so we correctly count ourselves as a pending replica
            // when inserting the new CDC generation.
            if (!bootstrap_rbno) {
                // When is_repair_based_node_ops_enabled is true, the bootstrap node
                // will use node_ops_cmd to bootstrap, node_ops_cmd will update the pending ranges.
                slogger.debug("bootstrap: update pending ranges: endpoint={} bootstrap_tokens={}", get_broadcast_address(), bootstrap_tokens);
                mutate_token_metadata([this, &bootstrap_tokens] (mutable_token_metadata_ptr tmptr) {
                    auto endpoint = get_broadcast_address();
                    tmptr->update_topology(endpoint, _sys_ks.local().local_dc_rack(), locator::node::state::joining);
                    tmptr->add_bootstrap_tokens(bootstrap_tokens, endpoint);
                    return update_topology_change_info(std::move(tmptr), ::format("bootstrapping node {}", endpoint));
                }).get();
            }

            // After we pick a generation timestamp, we start gossiping it, and we stick with it.
            // We don't do any other generation switches (unless we crash before complecting bootstrap).
            assert(!cdc_gen_id);

            cdc_gen_id = cdc_gen_service.legacy_make_new_generation(bootstrap_tokens, !is_first_node()).get0();

            if (!bootstrap_rbno) {
                // When is_repair_based_node_ops_enabled is true, the bootstrap node
                // will use node_ops_cmd to bootstrap, bootstrapping gossip status is not needed for bootstrap.
                _gossiper.add_local_application_state({
                    { gms::application_state::TOKENS, versioned_value::tokens(bootstrap_tokens) },
                    { gms::application_state::CDC_GENERATION_ID, versioned_value::cdc_generation_id(cdc_gen_id) },
                    { gms::application_state::STATUS, versioned_value::bootstrapping(bootstrap_tokens) },
                }).get();

                slogger.info("sleeping {} ms for pending range setup", get_ring_delay().count());
                _gossiper.wait_for_range_setup().get();
                dht::boot_strapper bs(_db, _stream_manager, _abort_source, get_broadcast_address(), _sys_ks.local().local_dc_rack(), bootstrap_tokens, get_token_metadata_ptr());
                slogger.info("Starting to bootstrap...");
                bs.bootstrap(streaming::stream_reason::bootstrap, _gossiper).get();
            } else {
                // Even with RBNO bootstrap we need to announce the new CDC generation immediately after it's created.
                _gossiper.add_local_application_state({
                    { gms::application_state::CDC_GENERATION_ID, versioned_value::cdc_generation_id(cdc_gen_id) },
                }).get();
                slogger.info("Starting to bootstrap...");
                run_bootstrap_ops(bootstrap_tokens);
            }
        } else {
            auto replace_addr = replacement_info->address;
            auto replaced_host_id = replacement_info->host_id;

            slogger.debug("Removing replaced endpoint {} from system.peers", replace_addr);
            _sys_ks.local().remove_endpoint(replace_addr).get();

            assert(replaced_host_id);
            auto raft_id = raft::server_id{replaced_host_id.uuid()};
            assert(_group0);
            bool raft_available = _group0->wait_for_raft().get();
            if (raft_available) {
                slogger.info("Replace: removing {}/{} from group 0...", replace_addr, raft_id);
                _group0->remove_from_group0(raft_id).get();
            }

            slogger.info("Starting to bootstrap...");
            run_replace_ops(bootstrap_tokens, *replacement_info);
        }

        _db.invoke_on_all([] (replica::database& db) {
            for (auto& cf : db.get_non_system_column_families()) {
                cf->notify_bootstrap_or_replace_end();
            }
        }).get();

        slogger.info("Bootstrap completed! for the tokens {}", bootstrap_tokens);
    });
}

sstring
storage_service::get_rpc_address(const inet_address& endpoint) const {
    if (endpoint != get_broadcast_address()) {
        auto* v = _gossiper.get_application_state_ptr(endpoint, gms::application_state::RPC_ADDRESS);
        if (v) {
            return v->value();
        }
    }
    return fmt::to_string(endpoint);
}

future<std::unordered_map<dht::token_range, inet_address_vector_replica_set>>
storage_service::get_range_to_address_map(const sstring& keyspace) const {
    return get_range_to_address_map(_db.local().find_keyspace(keyspace).get_effective_replication_map());
}

future<std::unordered_map<dht::token_range, inet_address_vector_replica_set>>
storage_service::get_range_to_address_map(locator::vnode_effective_replication_map_ptr erm) const {
    return get_range_to_address_map(erm, erm->get_token_metadata_ptr()->sorted_tokens());
}

// Caller is responsible to hold token_metadata valid until the returned future is resolved
future<std::unordered_map<dht::token_range, inet_address_vector_replica_set>>
storage_service::get_range_to_address_map(locator::vnode_effective_replication_map_ptr erm,
        const std::vector<token>& sorted_tokens) const {
    co_return co_await construct_range_to_endpoint_map(erm, co_await get_all_ranges(sorted_tokens));
}

future<> storage_service::handle_state_replacing_update_pending_ranges(mutable_token_metadata_ptr tmptr, inet_address replacing_node) {
    try {
        slogger.info("handle_state_replacing: Waiting for replacing node {} to be alive on all shards", replacing_node);
        co_await _gossiper.wait_alive({replacing_node}, std::chrono::milliseconds(5 * 1000));
        slogger.info("handle_state_replacing: Replacing node {} is now alive on all shards", replacing_node);
    } catch (...) {
        slogger.warn("handle_state_replacing: Failed to wait for replacing node {} to be alive on all shards: {}",
                replacing_node, std::current_exception());
    }
    slogger.info("handle_state_replacing: Update pending ranges for replacing node {}", replacing_node);
    co_await update_topology_change_info(tmptr, ::format("handle_state_replacing {}", replacing_node));
}

future<> storage_service::handle_state_bootstrap(inet_address endpoint) {
    slogger.debug("endpoint={} handle_state_bootstrap", endpoint);
    // explicitly check for TOKENS, because a bootstrapping node might be bootstrapping in legacy mode; that is, not using vnodes and no token specified
    auto tokens = get_tokens_for(endpoint);

    slogger.debug("Node {} state bootstrapping, token {}", endpoint, tokens);

    // if this node is present in token metadata, either we have missed intermediate states
    // or the node had crashed. Print warning if needed, clear obsolete stuff and
    // continue.
    auto tmlock = co_await get_token_metadata_lock();
    auto tmptr = co_await get_mutable_token_metadata_ptr();
    if (tmptr->is_normal_token_owner(endpoint)) {
        // If isLeaving is false, we have missed both LEAVING and LEFT. However, if
        // isLeaving is true, we have only missed LEFT. Waiting time between completing
        // leave operation and rebootstrapping is relatively short, so the latter is quite
        // common (not enough time for gossip to spread). Therefore we report only the
        // former in the log.
        if (!tmptr->is_leaving(endpoint)) {
            slogger.info("Node {} state jump to bootstrap", endpoint);
        }
        tmptr->remove_endpoint(endpoint);
    }

    tmptr->update_topology(endpoint, get_dc_rack_for(endpoint), locator::node::state::joining);
    tmptr->add_bootstrap_tokens(tokens, endpoint);
    if (_gossiper.uses_host_id(endpoint)) {
        tmptr->update_host_id(_gossiper.get_host_id(endpoint), endpoint);
    }
    co_await update_topology_change_info(tmptr, ::format("handle_state_bootstrap {}", endpoint));
    co_await replicate_to_all_cores(std::move(tmptr));
}

future<> storage_service::handle_state_normal(inet_address endpoint) {
    slogger.debug("endpoint={} handle_state_normal", endpoint);

    if (_raft_topology_change_enabled) {
        slogger.debug("ignore handle_state_normal since topology change are using raft");
        co_return;
    }

    auto tokens = get_tokens_for(endpoint);

    slogger.debug("Node {} state normal, token {}", endpoint, tokens);

    auto tmlock = std::make_unique<token_metadata_lock>(co_await get_token_metadata_lock());
    auto tmptr = co_await get_mutable_token_metadata_ptr();
    if (tmptr->is_normal_token_owner(endpoint)) {
        slogger.info("Node {} state jump to normal", endpoint);
    }
    std::unordered_set<inet_address> endpoints_to_remove;

    auto do_remove_node = [&] (gms::inet_address node) {
        tmptr->remove_endpoint(node);
        endpoints_to_remove.insert(node);
    };
    // Order Matters, TM.updateHostID() should be called before TM.updateNormalToken(), (see CASSANDRA-4300).
    if (_gossiper.uses_host_id(endpoint)) {
        auto host_id = _gossiper.get_host_id(endpoint);
        auto existing = tmptr->get_endpoint_for_host_id(host_id);
        if (existing && *existing != endpoint) {
            if (*existing == get_broadcast_address()) {
                slogger.warn("Not updating host ID {} for {} because it's mine", host_id, endpoint);
                do_remove_node(endpoint);
            } else if (_gossiper.compare_endpoint_startup(endpoint, *existing) > 0) {
                slogger.warn("Host ID collision for {} between {} and {}; {} is the new owner", host_id, *existing, endpoint, endpoint);
                do_remove_node(*existing);
                slogger.info("Set host_id={} to be owned by node={}, existing={}", host_id, endpoint, *existing);
                tmptr->update_host_id(host_id, endpoint);
            } else {
                slogger.warn("Host ID collision for {} between {} and {}; ignored {}", host_id, *existing, endpoint, endpoint);
                do_remove_node(endpoint);
            }
        } else if (existing && *existing == endpoint) {
            tmptr->del_replacing_endpoint(endpoint);
        } else {
            tmptr->del_replacing_endpoint(endpoint);
            auto nodes = _gossiper.get_nodes_with_host_id(host_id);
            bool left = std::any_of(nodes.begin(), nodes.end(), [this] (const gms::inet_address& node) { return _gossiper.is_left(node); });
            if (left) {
                slogger.info("Skip to set host_id={} to be owned by node={}, because the node is removed from the cluster, nodes {} used to own the host_id", host_id, endpoint, nodes);
                _normal_state_handled_on_boot.insert(endpoint);
                co_return;
            }
            slogger.info("Set host_id={} to be owned by node={}", host_id, endpoint);
            tmptr->update_host_id(host_id, endpoint);
        }
    }

    // Tokens owned by the handled endpoint.
    // The endpoint broadcasts its set of chosen tokens. If a token was also chosen by another endpoint,
    // the collision is resolved by assigning the token to the endpoint which started later.
    std::unordered_set<token> owned_tokens;

    // token_to_endpoint_map is used to track the current token owners for the purpose of removing replaced endpoints.
    // when any token is replaced by a new owner, we track the existing owner in `candidates_for_removal`
    // and eventually, if any candidate for removal ends up owning no tokens, it is removed from token_metadata.
    std::unordered_map<token, inet_address> token_to_endpoint_map = get_token_metadata().get_token_to_endpoint();
    std::unordered_set<inet_address> candidates_for_removal;

    for (auto t : tokens) {
        // we don't want to update if this node is responsible for the token and it has a later startup time than endpoint.
        auto current = token_to_endpoint_map.find(t);
        if (current == token_to_endpoint_map.end()) {
            slogger.debug("handle_state_normal: New node {} at token {}", endpoint, t);
            owned_tokens.insert(t);
            continue;
        }
        auto current_owner = current->second;
        if (endpoint == current_owner) {
            slogger.debug("handle_state_normal: endpoint={} == current_owner={} token {}", endpoint, current_owner, t);
            // set state back to normal, since the node may have tried to leave, but failed and is now back up
            owned_tokens.insert(t);
        } else if (_gossiper.compare_endpoint_startup(endpoint, current_owner) > 0) {
            slogger.debug("handle_state_normal: endpoint={} > current_owner={}, token {}", endpoint, current_owner, t);
            owned_tokens.insert(t);
            slogger.info("handle_state_normal: remove endpoint={} token={}", current_owner, t);
            // currentOwner is no longer current, endpoint is.  Keep track of these moves, because when
            // a host no longer has any tokens, we'll want to remove it.
            token_to_endpoint_map.erase(current);
            candidates_for_removal.insert(current_owner);
            slogger.info("handle_state_normal: Nodes {} and {} have the same token {}. {} is the new owner", endpoint, current_owner, t, endpoint);
        } else {
            // current owner of this token is kept and endpoint attempt to own it is rejected.
            // Keep track of these moves, because when a host no longer has any tokens, we'll want to remove it.
            token_to_endpoint_map.erase(current);
            candidates_for_removal.insert(endpoint);
            slogger.info("handle_state_normal: Nodes {} and {} have the same token {}. Ignoring {}", endpoint, current_owner, t, endpoint);
        }
    }

    // After we replace all tokens owned by current_owner
    // We check for each candidate for removal if it still owns any tokens,
    // and remove it if it doesn't anymore.
    if (!candidates_for_removal.empty()) {
        for (const auto& [t, ep] : token_to_endpoint_map) {
            if (candidates_for_removal.contains(ep)) {
                slogger.debug("handle_state_normal: endpoint={} still owns tokens, will not be removed", ep);
                candidates_for_removal.erase(ep);
                if (candidates_for_removal.empty()) {
                    break;
                }
            }
        }
    }

    for (const auto& ep : candidates_for_removal) {
        slogger.info("handle_state_normal: endpoints_to_remove endpoint={}", ep);
        endpoints_to_remove.insert(ep);
    }

    bool is_normal_token_owner = tmptr->is_normal_token_owner(endpoint);
    bool do_notify_joined = false;

    if (endpoints_to_remove.contains(endpoint)) [[unlikely]] {
        if (!owned_tokens.empty()) {
            on_fatal_internal_error(slogger, ::format("endpoint={} is marked for removal but still owns {} tokens", endpoint, owned_tokens.size()));
        }
    } else {
        if (owned_tokens.empty()) {
            on_internal_error(slogger, ::format("endpoint={} is not marked for removal but owns no tokens", endpoint));
        }

        if (!is_normal_token_owner) {
            do_notify_joined = true;
        }

        tmptr->update_topology(endpoint, get_dc_rack_for(endpoint), locator::node::state::normal);
        co_await tmptr->update_normal_tokens(owned_tokens, endpoint);
    }

    co_await update_topology_change_info(tmptr, ::format("handle_state_normal {}", endpoint));
    co_await replicate_to_all_cores(std::move(tmptr));
    tmlock.reset();

    for (auto ep : endpoints_to_remove) {
        co_await remove_endpoint(ep);
    }
    slogger.debug("handle_state_normal: endpoint={} is_normal_token_owner={} endpoint_to_remove={} owned_tokens={}", endpoint, is_normal_token_owner, endpoints_to_remove.contains(endpoint), owned_tokens);
    if (!owned_tokens.empty() && !endpoints_to_remove.count(endpoint)) {
        co_await update_peer_info(endpoint);
        try {
            co_await _sys_ks.local().update_tokens(endpoint, owned_tokens);
        } catch (...) {
            slogger.error("handle_state_normal: fail to update tokens for {}: {}", endpoint, std::current_exception());
        }
    }

    // Send joined notification only when this node was not a member prior to this
    if (do_notify_joined) {
        co_await notify_joined(endpoint);
    }

    if (slogger.is_enabled(logging::log_level::debug)) {
        const auto& tm = get_token_metadata();
        auto ver = tm.get_ring_version();
        for (auto& x : tm.get_token_to_endpoint()) {
            slogger.debug("handle_state_normal: token_metadata.ring_version={}, token={} -> endpoint={}", ver, x.first, x.second);
        }
    }
    _normal_state_handled_on_boot.insert(endpoint);
}

future<> storage_service::handle_state_leaving(inet_address endpoint) {
    slogger.debug("endpoint={} handle_state_leaving", endpoint);

    auto tokens = get_tokens_for(endpoint);

    slogger.debug("Node {} state leaving, tokens {}", endpoint, tokens);

    // If the node is previously unknown or tokens do not match, update tokenmetadata to
    // have this node as 'normal' (it must have been using this token before the
    // leave). This way we'll get pending ranges right.
    auto tmlock = co_await get_token_metadata_lock();
    auto tmptr = co_await get_mutable_token_metadata_ptr();
    if (!tmptr->is_normal_token_owner(endpoint)) {
        // FIXME: this code should probably resolve token collisions too, like handle_state_normal
        slogger.info("Node {} state jump to leaving", endpoint);

        tmptr->update_topology(endpoint, get_dc_rack_for(endpoint), locator::node::state::leaving);
        co_await tmptr->update_normal_tokens(tokens, endpoint);
    } else {
        auto tokens_ = tmptr->get_tokens(endpoint);
        std::set<token> tmp(tokens.begin(), tokens.end());
        if (!std::includes(tokens_.begin(), tokens_.end(), tmp.begin(), tmp.end())) {
            slogger.warn("Node {} 'leaving' token mismatch. Long network partition?", endpoint);
            slogger.debug("tokens_={}, tokens={}", tokens_, tmp);

            co_await tmptr->update_normal_tokens(tokens, endpoint);
        }
    }

    // at this point the endpoint is certainly a member with this token, so let's proceed
    // normally
    tmptr->add_leaving_endpoint(endpoint);

    co_await update_topology_change_info(tmptr, ::format("handle_state_leaving", endpoint));
    co_await replicate_to_all_cores(std::move(tmptr));
}

future<> storage_service::handle_state_left(inet_address endpoint, std::vector<sstring> pieces) {

    if (_raft_topology_change_enabled) {
        slogger.debug("ignore handle_state_left since topology change are using raft");
        co_return;
    }

    slogger.debug("endpoint={} handle_state_left", endpoint);
    if (pieces.size() < 2) {
        slogger.warn("Fail to handle_state_left endpoint={} pieces={}", endpoint, pieces);
        co_return;
    }
    auto tokens = get_tokens_for(endpoint);
    slogger.debug("Node {} state left, tokens {}", endpoint, tokens);
    if (tokens.empty()) {
        auto eps = _gossiper.get_endpoint_state_for_endpoint_ptr(endpoint);
        if (eps) {
            slogger.warn("handle_state_left: Tokens for node={} are empty, endpoint_state={}", endpoint, *eps);
        } else {
            slogger.warn("handle_state_left: Couldn't find endpoint state for node={}", endpoint);
        }
        auto tokens_from_tm = get_token_metadata().get_tokens(endpoint);
        slogger.warn("handle_state_left: Get tokens from token_metadata, node={}, tokens={}", endpoint, tokens_from_tm);
        tokens = std::unordered_set<dht::token>(tokens_from_tm.begin(), tokens_from_tm.end());
    }
    co_await excise(tokens, endpoint, extract_expire_time(pieces));
}

void storage_service::handle_state_moving(inet_address endpoint, std::vector<sstring> pieces) {
    throw std::runtime_error(::format("Move operation is not supported anymore, endpoint={}", endpoint));
}

future<> storage_service::handle_state_removed(inet_address endpoint, std::vector<sstring> pieces) {
    slogger.debug("endpoint={} handle_state_removed", endpoint);

    if (endpoint == get_broadcast_address()) {
        slogger.info("Received removenode gossip about myself. Is this node rejoining after an explicit removenode?");
        try {
            co_await drain();
        } catch (...) {
            slogger.error("Fail to drain: {}", std::current_exception());
            throw;
        }
        co_return;
    }
    if (get_token_metadata().is_normal_token_owner(endpoint)) {
        auto state = pieces[0];
        auto remove_tokens = get_token_metadata().get_tokens(endpoint);
        std::unordered_set<token> tmp(remove_tokens.begin(), remove_tokens.end());
        co_await excise(std::move(tmp), endpoint, extract_expire_time(pieces));
    } else { // now that the gossiper has told us about this nonexistent member, notify the gossiper to remove it
        add_expire_time_if_found(endpoint, extract_expire_time(pieces));
        co_await remove_endpoint(endpoint);
    }
}

future<> storage_service::on_join(gms::inet_address endpoint, gms::endpoint_state ep_state) {
    slogger.debug("endpoint={} on_join", endpoint);
    for (const auto& e : ep_state.get_application_state_map()) {
        co_await on_change(endpoint, e.first, e.second);
    }
}

future<> storage_service::on_alive(gms::inet_address endpoint, gms::endpoint_state state) {
    slogger.debug("endpoint={} on_alive", endpoint);
    bool is_normal_token_owner = get_token_metadata().is_normal_token_owner(endpoint);
    if (is_normal_token_owner) {
        co_await notify_up(endpoint);
    }
    bool replacing_pending_ranges = _replacing_nodes_pending_ranges_updater.contains(endpoint);
    if (replacing_pending_ranges) {
        _replacing_nodes_pending_ranges_updater.erase(endpoint);
    }

    if (!is_normal_token_owner || replacing_pending_ranges) {
        auto tmlock = co_await get_token_metadata_lock();
        auto tmptr = co_await get_mutable_token_metadata_ptr();
        if (replacing_pending_ranges) {
            slogger.info("Trigger pending ranges updater for replacing node {}", endpoint);
            co_await handle_state_replacing_update_pending_ranges(tmptr, endpoint);
        }
        if (!is_normal_token_owner) {
            tmptr->update_topology(endpoint, get_dc_rack_for(endpoint));
        }
        co_await replicate_to_all_cores(std::move(tmptr));
    }
}

future<> storage_service::before_change(gms::inet_address endpoint, gms::endpoint_state current_state, gms::application_state new_state_key, const gms::versioned_value& new_value) {
    slogger.debug("endpoint={} before_change: new app_state={}, new versioned_value={}", endpoint, new_state_key, new_value);
    return make_ready_future();
}

future<> storage_service::on_change(inet_address endpoint, application_state state, const versioned_value& value) {
    slogger.debug("endpoint={} on_change:     app_state={}, versioned_value={}", endpoint, state, value);
    if (state == application_state::STATUS) {
        std::vector<sstring> pieces;
        boost::split(pieces, value.value(), boost::is_any_of(sstring(versioned_value::DELIMITER_STR)));
        if (pieces.empty()) {
            slogger.warn("Fail to split status in on_change: endpoint={}, app_state={}, value={}", endpoint, state, value);
            co_return;
        }
        const sstring& move_name = pieces[0];
        if (move_name == sstring(versioned_value::STATUS_BOOTSTRAPPING)) {
            co_await handle_state_bootstrap(endpoint);
        } else if (move_name == sstring(versioned_value::STATUS_NORMAL) ||
                   move_name == sstring(versioned_value::SHUTDOWN)) {
            co_await handle_state_normal(endpoint);
        } else if (move_name == sstring(versioned_value::REMOVED_TOKEN)) {
            co_await handle_state_removed(endpoint, std::move(pieces));
        } else if (move_name == sstring(versioned_value::STATUS_LEAVING)) {
            co_await handle_state_leaving(endpoint);
        } else if (move_name == sstring(versioned_value::STATUS_LEFT)) {
            co_await handle_state_left(endpoint, std::move(pieces));
        } else if (move_name == sstring(versioned_value::STATUS_MOVING)) {
            handle_state_moving(endpoint, std::move(pieces));
        } else if (move_name == sstring(versioned_value::HIBERNATE)) {
            slogger.warn("endpoint={} went into HIBERNATE state, this is no longer supported.  Use a new version to perform the replace operation.", endpoint);
        } else {
            co_return; // did nothing.
        }
    } else {
        auto* ep_state = _gossiper.get_endpoint_state_for_endpoint_ptr(endpoint);
        if (!ep_state || _gossiper.is_dead_state(*ep_state)) {
            slogger.debug("Ignoring state change for dead or unknown endpoint: {}", endpoint);
            co_return;
        }
        if (get_token_metadata().is_normal_token_owner(endpoint)) {
            slogger.debug("endpoint={} on_change:     updating system.peers table", endpoint);
            co_await do_update_system_peers_table(endpoint, state, value);
            if (state == application_state::RPC_READY) {
                slogger.debug("Got application_state::RPC_READY for node {}, is_cql_ready={}", endpoint, ep_state->is_cql_ready());
                co_await notify_cql_change(endpoint, ep_state->is_cql_ready());
            } else if (state == application_state::INTERNAL_IP) {
                co_await maybe_reconnect_to_preferred_ip(endpoint, inet_address(value.value()));
            }
        }
    }
}

future<> storage_service::maybe_reconnect_to_preferred_ip(inet_address ep, inet_address local_ip) {
    if (!_snitch.local()->prefer_local()) {
        co_return;
    }

    const auto& topo = get_token_metadata().get_topology();
    if (topo.get_datacenter() == topo.get_datacenter(ep) && _messaging.local().get_preferred_ip(ep) != local_ip) {
        slogger.debug("Initiated reconnect to an Internal IP {} for the {}", local_ip, ep);
        co_await _messaging.invoke_on_all([ep, local_ip] (auto& local_ms) {
            local_ms.cache_preferred_ip(ep, local_ip);
        });
    }
}


future<> storage_service::on_remove(gms::inet_address endpoint) {
    slogger.debug("endpoint={} on_remove", endpoint);
    auto tmlock = co_await get_token_metadata_lock();
    auto tmptr = co_await get_mutable_token_metadata_ptr();
    tmptr->remove_endpoint(endpoint);
    co_await update_topology_change_info(tmptr, ::format("on_remove {}", endpoint));
    co_await replicate_to_all_cores(std::move(tmptr));
}

future<> storage_service::on_dead(gms::inet_address endpoint, gms::endpoint_state state) {
    slogger.debug("endpoint={} on_dead", endpoint);
    return notify_down(endpoint);
}

future<> storage_service::on_restart(gms::inet_address endpoint, gms::endpoint_state state) {
    slogger.debug("endpoint={} on_restart", endpoint);
    // If we have restarted before the node was even marked down, we need to reset the connection pool
    if (state.is_alive()) {
        return on_dead(endpoint, state);
    }
    return make_ready_future();
}

template <typename T>
future<> storage_service::update_table(gms::inet_address endpoint, sstring col, T value) {
    try {
        co_await _sys_ks.local().update_peer_info(endpoint, col, value);
    } catch (...) {
        slogger.error("fail to update {} for {}: {}", col, endpoint, std::current_exception());
    }
}

future<> storage_service::do_update_system_peers_table(gms::inet_address endpoint, const application_state& state, const versioned_value& value) {
    slogger.debug("Update system.peers table: endpoint={}, app_state={}, versioned_value={}", endpoint, state, value);
    if (state == application_state::RELEASE_VERSION) {
        co_await update_table(endpoint, "release_version", value.value());
    } else if (state == application_state::DC) {
        co_await update_table(endpoint, "data_center", value.value());
    } else if (state == application_state::RACK) {
        co_await update_table(endpoint, "rack", value.value());
    } else if (state == application_state::INTERNAL_IP) {
        auto col = sstring("preferred_ip");
        inet_address ep;
        try {
            ep = gms::inet_address(value.value());
        } catch (...) {
            slogger.error("fail to update {} for {}: invalid address {}", col, endpoint, value.value());
            co_return;
        }
        co_await update_table(endpoint, col, ep.addr());
    } else if (state == application_state::RPC_ADDRESS) {
        auto col = sstring("rpc_address");
        inet_address ep;
        try {
            ep = gms::inet_address(value.value());
        } catch (...) {
            slogger.error("fail to update {} for {}: invalid rcpaddr {}", col, endpoint, value.value());
            co_return;
        }
        co_await update_table(endpoint, col, ep.addr());
    } else if (state == application_state::SCHEMA) {
        co_await update_table(endpoint, "schema_version", utils::UUID(value.value()));
    } else if (state == application_state::HOST_ID) {
        co_await update_table(endpoint, "host_id", utils::UUID(value.value()));
    } else if (state == application_state::SUPPORTED_FEATURES) {
        co_await update_table(endpoint, "supported_features", value.value());
    }
}

future<> storage_service::update_peer_info(gms::inet_address endpoint) {
    slogger.debug("Update peer info: endpoint={}", endpoint);
    using namespace gms;
    auto* ep_state = _gossiper.get_endpoint_state_for_endpoint_ptr(endpoint);
    if (!ep_state) {
        co_return;
    }
    for (auto& entry : ep_state->get_application_state_map()) {
        auto& app_state = entry.first;
        auto& value = entry.second;
        co_await do_update_system_peers_table(endpoint, app_state, value);
    }
}

std::unordered_set<locator::token> storage_service::get_tokens_for(inet_address endpoint) {
    auto tokens_string = _gossiper.get_application_state_value(endpoint, application_state::TOKENS);
    slogger.trace("endpoint={}, tokens_string={}", endpoint, tokens_string);
    auto ret = versioned_value::tokens_from_string(tokens_string);
    slogger.trace("endpoint={}, tokens={}", endpoint, ret);
    return ret;
}

locator::endpoint_dc_rack storage_service::get_dc_rack_for(inet_address endpoint) {
    auto* dc = _gossiper.get_application_state_ptr(endpoint, gms::application_state::DC);
    auto* rack = _gossiper.get_application_state_ptr(endpoint, gms::application_state::RACK);
    return locator::endpoint_dc_rack{
        .dc = dc ? dc->value() : locator::endpoint_dc_rack::default_location.dc,
        .rack = rack ? rack->value() : locator::endpoint_dc_rack::default_location.rack,
    };
}

void endpoint_lifecycle_notifier::register_subscriber(endpoint_lifecycle_subscriber* subscriber)
{
    _subscribers.add(subscriber);
}

future<> endpoint_lifecycle_notifier::unregister_subscriber(endpoint_lifecycle_subscriber* subscriber) noexcept
{
    return _subscribers.remove(subscriber);
}

future<> storage_service::stop_transport() {
    if (!_transport_stopped.has_value()) {
        promise<> stopped;
        _transport_stopped = stopped.get_future();

        seastar::async([this] {
            slogger.info("Stop transport: starts");

            slogger.debug("shutting down migration manager");
            _migration_manager.invoke_on_all(&service::migration_manager::drain).get();

            shutdown_protocol_servers().get();
            slogger.info("Stop transport: shutdown rpc and cql server done");

            _gossiper.container().invoke_on_all(&gms::gossiper::shutdown).get();
            slogger.info("Stop transport: stop_gossiping done");

            do_stop_ms().get();
            slogger.info("Stop transport: shutdown messaging_service done");

            _stream_manager.invoke_on_all(&streaming::stream_manager::shutdown).get();
            slogger.info("Stop transport: shutdown stream_manager done");

            slogger.info("Stop transport: done");
        }).forward_to(std::move(stopped));
    }

    return _transport_stopped.value();
}

future<> storage_service::drain_on_shutdown() {
    assert(this_shard_id() == 0);
    return (_operation_mode == mode::DRAINING || _operation_mode == mode::DRAINED) ?
        _drain_finished.get_future() : do_drain();
}

future<> storage_service::init_messaging_service_part(sharded<service::storage_proxy>& proxy, sharded<db::system_distributed_keyspace>& sys_dist_ks) {
    return container().invoke_on_all([&proxy, &sys_dist_ks] (storage_service& local) {
        return local.init_messaging_service(proxy, sys_dist_ks);
    });
}

future<> storage_service::uninit_messaging_service_part() {
    return container().invoke_on_all(&service::storage_service::uninit_messaging_service);
}

void storage_service::set_group0(raft_group0& group0) {
    _group0 = &group0;
    _raft_topology_change_enabled = _group0->is_raft_enabled() && _db.local().get_config().check_experimental(db::experimental_features_t::feature::RAFT);
}

future<> storage_service::join_cluster(cdc::generation_service& cdc_gen_service,
        sharded<db::system_distributed_keyspace>& sys_dist_ks, sharded<service::storage_proxy>& proxy, cql3::query_processor& qp) {
    assert(this_shard_id() == 0);

    set_mode(mode::STARTING);

    std::unordered_set<inet_address> loaded_endpoints;
    if (_db.local().get_config().load_ring_state() && !_raft_topology_change_enabled) {
        slogger.info("Loading persisted ring state");
        auto loaded_tokens = co_await _sys_ks.local().load_tokens();
        auto loaded_host_ids = co_await _sys_ks.local().load_host_ids();
        auto loaded_dc_rack = co_await _sys_ks.local().load_dc_rack_info();

        auto get_dc_rack = [&loaded_dc_rack] (inet_address ep) {
            if (loaded_dc_rack.contains(ep)) {
                return loaded_dc_rack[ep];
            } else {
                return locator::endpoint_dc_rack::default_location;
            }
        };

        if (slogger.is_enabled(logging::log_level::debug)) {
            for (auto& x : loaded_tokens) {
                slogger.debug("Loaded tokens: endpoint={}, tokens={}", x.first, x.second);
            }

            for (auto& x : loaded_host_ids) {
                slogger.debug("Loaded host_id: endpoint={}, uuid={}", x.first, x.second);
            }
        }

        auto tmlock = co_await get_token_metadata_lock();
        auto tmptr = co_await get_mutable_token_metadata_ptr();
        for (auto x : loaded_tokens) {
            auto ep = x.first;
            auto tokens = x.second;
            if (ep == get_broadcast_address()) {
                // entry has been mistakenly added, delete it
                co_await _sys_ks.local().remove_endpoint(ep);
            } else {
                tmptr->update_topology(ep, get_dc_rack(ep), locator::node::state::normal);
                co_await tmptr->update_normal_tokens(tokens, ep);
                if (loaded_host_ids.contains(ep)) {
                    tmptr->update_host_id(loaded_host_ids.at(ep), ep);
                }
                loaded_endpoints.insert(ep);
                co_await _gossiper.add_saved_endpoint(ep);
            }
        }
        co_await replicate_to_all_cores(std::move(tmptr));
    }

    // Seeds are now only used as the initial contact point nodes. If the
    // loaded_endpoints are empty which means this node is a completely new
    // node, we use the nodes specified in seeds as the initial contact
    // point nodes, otherwise use the peer nodes persisted in system table.
    auto seeds = _gossiper.get_seeds();
    auto initial_contact_nodes = loaded_endpoints.empty() ?
        std::unordered_set<gms::inet_address>(seeds.begin(), seeds.end()) :
        loaded_endpoints;
    auto loaded_peer_features = co_await _sys_ks.local().load_peer_features();
    slogger.info("initial_contact_nodes={}, loaded_endpoints={}, loaded_peer_features={}",
            initial_contact_nodes, loaded_endpoints, loaded_peer_features.size());
    for (auto& x : loaded_peer_features) {
        slogger.info("peer={}, supported_features={}", x.first, x.second);
    }
    co_return co_await join_token_ring(cdc_gen_service, sys_dist_ks, proxy, std::move(initial_contact_nodes), std::move(loaded_endpoints), std::move(loaded_peer_features), get_ring_delay(), qp);
}

future<> storage_service::replicate_to_all_cores(mutable_token_metadata_ptr tmptr) noexcept {
    assert(this_shard_id() == 0);

    slogger.debug("Replicating token_metadata to all cores");
    std::exception_ptr ex;

    std::vector<mutable_token_metadata_ptr> pending_token_metadata_ptr;
    pending_token_metadata_ptr.resize(smp::count);
    std::vector<std::unordered_map<sstring, locator::vnode_effective_replication_map_ptr>> pending_effective_replication_maps;
    pending_effective_replication_maps.resize(smp::count);
    std::vector<std::unordered_map<table_id, locator::effective_replication_map_ptr>> pending_table_erms;
    pending_table_erms.resize(smp::count);

    try {
        auto base_shard = this_shard_id();
        pending_token_metadata_ptr[base_shard] = tmptr;
        // clone a local copy of updated token_metadata on all other shards
        co_await smp::invoke_on_others(base_shard, [&, tmptr] () -> future<> {
            pending_token_metadata_ptr[this_shard_id()] = make_token_metadata_ptr(co_await tmptr->clone_async());
        });

        // Precalculate new effective_replication_map for all keyspaces
        // and clone to all shards;
        //
        // TODO: at the moment create on shard 0 first
        // but in the future we may want to use hash() % smp::count
        // to evenly distribute the load.
        auto& db = _db.local();
        auto keyspaces = db.get_all_keyspaces();
        for (auto& ks_name : keyspaces) {
            auto rs = db.find_keyspace(ks_name).get_replication_strategy_ptr();
            if (rs->is_per_table()) {
                continue;
            }
            auto erm = co_await get_erm_factory().create_effective_replication_map(rs, tmptr);
            pending_effective_replication_maps[base_shard].emplace(ks_name, std::move(erm));
        }
        co_await container().invoke_on_others([&] (storage_service& ss) -> future<> {
            auto& db = ss._db.local();
            for (auto& ks_name : keyspaces) {
                auto rs = db.find_keyspace(ks_name).get_replication_strategy_ptr();
                if (rs->is_per_table()) {
                    continue;
                }
                auto tmptr = pending_token_metadata_ptr[this_shard_id()];
                auto erm = co_await ss.get_erm_factory().create_effective_replication_map(rs, std::move(tmptr));
                pending_effective_replication_maps[this_shard_id()].emplace(ks_name, std::move(erm));
            }
        });
        // Prepare per-table erms.
        co_await container().invoke_on_all([&] (storage_service& ss) {
            auto& db = ss._db.local();
            auto tmptr = pending_token_metadata_ptr[this_shard_id()];
            for (auto&& [id, cf] : db.get_column_families()) { // Safe because we iterate without preemption
                auto rs = db.find_keyspace(cf->schema()->keypace_name()).get_replication_strategy_ptr();
                locator::effective_replication_map_ptr erm;
                if (auto pt_rs = rs->maybe_as_per_table()) {
                    erm = pt_rs->make_replication_map(id, tmptr);
                } else {
                    erm = pending_effective_replication_maps[this_shard_id()][cf->schema()->keypace_name()];
                }
                pending_table_erms[this_shard_id()].emplace(id, std::move(erm));
            }
        });
    } catch (...) {
        ex = std::current_exception();
    }

    // Rollback on metadata replication error
    if (ex) {
        try {
            co_await smp::invoke_on_all([&] () -> future<> {
                auto tmptr = std::move(pending_token_metadata_ptr[this_shard_id()]);
                auto erms = std::move(pending_effective_replication_maps[this_shard_id()]);
                auto table_erms = std::move(pending_table_erms[this_shard_id()]);

                co_await utils::clear_gently(erms);
                co_await utils::clear_gently(tmptr);
            });
        } catch (...) {
            slogger.warn("Failure to reset pending token_metadata in cleanup path: {}. Ignored.", std::current_exception());
        }

        std::rethrow_exception(std::move(ex));
    }

    // Apply changes on all shards
    try {
        co_await container().invoke_on_all([&] (storage_service& ss) {
            ss._shared_token_metadata.set(std::move(pending_token_metadata_ptr[this_shard_id()]));
            auto& db = ss._db.local();

            auto& erms = pending_effective_replication_maps[this_shard_id()];
            for (auto it = erms.begin(); it != erms.end(); ) {
                auto& ks = db.find_keyspace(it->first);
                ks.update_effective_replication_map(std::move(it->second));
                it = erms.erase(it);
            }

            auto& table_erms = pending_table_erms[this_shard_id()];
            for (auto it = table_erms.begin(); it != table_erms.end(); ) {
                auto& cf = db.find_column_family(it->first);
                cf.update_effective_replication_map(std::move(it->second));
                it = table_erms.erase(it);
            }
        });
    } catch (...) {
        // applying the changes on all shards should never fail
        // it will end up in an inconsistent state that we can't recover from.
        slogger.error("Failed to apply token_metadata changes: {}. Aborting.", std::current_exception());
        abort();
    }
}

future<> storage_service::stop() {
    // make sure nobody uses the semaphore
    node_ops_signal_abort(std::nullopt);
    _listeners.clear();
     _topology_state_machine.event.broken(make_exception_ptr(abort_requested_exception()));
    co_await _async_gate.close();
    co_await when_all(std::move(_node_ops_abort_thread), std::move(_raft_state_monitor));
}

future<> storage_service::check_for_endpoint_collision(std::unordered_set<gms::inet_address> initial_contact_nodes, const std::unordered_map<gms::inet_address, sstring>& loaded_peer_features) {
    slogger.debug("Starting shadow gossip round to check for endpoint collision");

    return seastar::async([this, initial_contact_nodes, loaded_peer_features] {
        auto t = gms::gossiper::clk::now();
        bool found_bootstrapping_node = false;
        auto local_features = _feature_service.supported_feature_set();
        do {
            slogger.info("Checking remote features with gossip");
            _gossiper.do_shadow_round(initial_contact_nodes).get();
            _gossiper.check_knows_remote_features(local_features, loaded_peer_features);
            _gossiper.check_snitch_name_matches(_snitch.local()->get_name());
            auto addr = get_broadcast_address();
            if (!_gossiper.is_safe_for_bootstrap(addr)) {
                throw std::runtime_error(::format("A node with address {} already exists, cancelling join. "
                    "Use replace_address if you want to replace this node.", addr));
            }
            if (_db.local().get_config().consistent_rangemovement() &&
                // Raft is responsible for consistency, so in case it is enable no need to check here
                !_raft_topology_change_enabled) {
                found_bootstrapping_node = false;
                for (const auto& addr : _gossiper.get_endpoints()) {
                    auto state = _gossiper.get_gossip_status(addr);
                    if (state == sstring(versioned_value::STATUS_UNKNOWN)) {
                        throw std::runtime_error(::format("Node {} has gossip status=UNKNOWN. Try fixing it before adding new node to the cluster.", addr));
                    }
                    slogger.debug("Checking bootstrapping/leaving/moving nodes: node={}, status={} (check_for_endpoint_collision)", addr, state);
                    if (state == sstring(versioned_value::STATUS_BOOTSTRAPPING) ||
                        state == sstring(versioned_value::STATUS_LEAVING) ||
                        state == sstring(versioned_value::STATUS_MOVING)) {
                        if (gms::gossiper::clk::now() > t + std::chrono::seconds(60)) {
                            throw std::runtime_error("Other bootstrapping/leaving/moving nodes detected, cannot bootstrap while consistent_rangemovement is true (check_for_endpoint_collision)");
                        } else {
                            sstring saved_state(state);
                            _gossiper.goto_shadow_round();
                            _gossiper.reset_endpoint_state_map().get();
                            found_bootstrapping_node = true;
                            auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(gms::gossiper::clk::now() - t).count();
                            slogger.info("Checking bootstrapping/leaving/moving nodes: node={}, status={}, sleep 1 second and check again ({} seconds elapsed) (check_for_endpoint_collision)", addr, saved_state, elapsed);
                            sleep_abortable(std::chrono::seconds(1), _abort_source).get();
                            break;
                        }
                    }
                }
            }
        } while (found_bootstrapping_node);
        slogger.info("Checking bootstrapping/leaving/moving nodes: ok (check_for_endpoint_collision)");
        _gossiper.reset_endpoint_state_map().get();
    });
}

future<> storage_service::remove_endpoint(inet_address endpoint) {
    co_await _gossiper.remove_endpoint(endpoint);
    try {
        co_await _sys_ks.local().remove_endpoint(endpoint);
    } catch (...) {
        slogger.error("fail to remove endpoint={}: {}", endpoint, std::current_exception());
    }
}

future<storage_service::replacement_info>
storage_service::prepare_replacement_info(std::unordered_set<gms::inet_address> initial_contact_nodes, const std::unordered_map<gms::inet_address, sstring>& loaded_peer_features) {
    locator::host_id replace_host_id;
    gms::inet_address replace_address;

    auto& cfg = _db.local().get_config();
    if (!cfg.replace_node_first_boot().empty()) {
        replace_host_id = locator::host_id(utils::UUID(cfg.replace_node_first_boot()));
    } else if (!cfg.replace_address_first_boot().empty()) {
        replace_address = gms::inet_address(cfg.replace_address_first_boot());
        slogger.warn("The replace_address_first_boot={} option is deprecated. Please use the replace_node_first_boot option", replace_address);
    } else if (!cfg.replace_address().empty()) {
        replace_address = gms::inet_address(cfg.replace_address());
        slogger.warn("The replace_address={} option is deprecated. Please use the replace_node_first_boot option", replace_address);
    } else {
        on_internal_error(slogger, "No replace_node or replace_address configuration options found");
    }

    slogger.info("Gathering node replacement information for {}/{}", replace_host_id, replace_address);

    auto seeds = _gossiper.get_seeds();
    if (seeds.size() == 1 && seeds.contains(replace_address)) {
        throw std::runtime_error(::format("Cannot replace_address {} because no seed node is up", replace_address));
    }

    // make magic happen
    slogger.info("Checking remote features with gossip");
    co_await _gossiper.do_shadow_round(initial_contact_nodes);
    auto local_features = _feature_service.supported_feature_set();
    _gossiper.check_knows_remote_features(local_features, loaded_peer_features);

    // now that we've gossiped at least once, we should be able to find the node we're replacing
    if (replace_host_id) {
        auto nodes = _gossiper.get_nodes_with_host_id(replace_host_id);
        if (nodes.empty()) {
            throw std::runtime_error(::format("Replaced node with Host ID {} not found", replace_host_id));
        }
        if (nodes.size() > 1) {
            throw std::runtime_error(::format("Found multiple nodes with Host ID {}: {}", replace_host_id, nodes));
        }
        replace_address = *nodes.begin();
    }

    auto* state = _gossiper.get_endpoint_state_for_endpoint_ptr(replace_address);
    if (!state) {
        throw std::runtime_error(::format("Cannot replace_address {} because it doesn't exist in gossip", replace_address));
    }

    // Reject to replace a node that has left the ring
    auto status = _gossiper.get_gossip_status(replace_address);
    if (status == gms::versioned_value::STATUS_LEFT || status == gms::versioned_value::REMOVED_TOKEN) {
        throw std::runtime_error(::format("Cannot replace_address {} because it has left the ring, status={}", replace_address, status));
    }

    std::unordered_set<dht::token> tokens;
    if (!_raft_topology_change_enabled) {
        tokens = get_tokens_for(replace_address);
        if (tokens.empty()) {
            throw std::runtime_error(::format("Could not find tokens for {} to replace", replace_address));
        }
    }

    auto dc_rack = get_dc_rack_for(replace_address);

    if (!replace_host_id) {
        replace_host_id = _gossiper.get_host_id(replace_address);
    }
    slogger.info("Host {}/{} is replacing {}/{}", _db.local().get_config().host_id, get_broadcast_address(), replace_host_id, replace_address);
    co_await _gossiper.reset_endpoint_state_map();

    co_return replacement_info {
        .tokens = std::move(tokens),
        .dc_rack = std::move(dc_rack),
        .host_id = std::move(replace_host_id),
        .address = replace_address,
    };
}

future<std::map<gms::inet_address, float>> storage_service::get_ownership() {
    return run_with_no_api_lock([] (storage_service& ss) {
        const auto& tm = ss.get_token_metadata();
        auto token_map = dht::token::describe_ownership(tm.sorted_tokens());
        // describeOwnership returns tokens in an unspecified order, let's re-order them
        std::map<gms::inet_address, float> ownership;
        for (auto entry : token_map) {
            gms::inet_address endpoint = tm.get_endpoint(entry.first).value();
            auto token_ownership = entry.second;
            ownership[endpoint] += token_ownership;
        }
        return ownership;
    });
}

future<std::map<gms::inet_address, float>> storage_service::effective_ownership(sstring keyspace_name) {
    return run_with_no_api_lock([keyspace_name] (storage_service& ss) mutable -> future<std::map<gms::inet_address, float>> {
        locator::vnode_effective_replication_map_ptr erm;
        if (keyspace_name != "") {
            //find throws no such keyspace if it is missing
            const replica::keyspace& ks = ss._db.local().find_keyspace(keyspace_name);
            // This is ugly, but it follows origin
            auto&& rs = ks.get_replication_strategy();  // clang complains about typeid(ks.get_replication_strategy());
            if (typeid(rs) == typeid(locator::local_strategy)) {
                throw std::runtime_error("Ownership values for keyspaces with LocalStrategy are meaningless");
            }
            erm = ks.get_effective_replication_map();
        } else {
            auto non_system_keyspaces = ss._db.local().get_non_system_keyspaces();

            //system_traces is a non-system keyspace however it needs to be counted as one for this process
            size_t special_table_count = 0;
            if (std::find(non_system_keyspaces.begin(), non_system_keyspaces.end(), "system_traces") !=
                    non_system_keyspaces.end()) {
                special_table_count += 1;
            }
            if (non_system_keyspaces.size() > special_table_count) {
                throw std::runtime_error("Non-system keyspaces don't have the same replication settings, effective ownership information is meaningless");
            }
            keyspace_name = "system_traces";
            const auto& ks = ss._db.local().find_keyspace(keyspace_name);
            erm = ks.get_effective_replication_map();
        }

        // The following loops seems computationally heavy, but it's not as bad.
        // The upper two simply iterate over all the endpoints by iterating over all the
        // DC and all the instances in each DC.
        //
        // The call for get_range_for_endpoint is done once per endpoint
        const auto& tm = *erm->get_token_metadata_ptr();
        const auto token_ownership = dht::token::describe_ownership(tm.sorted_tokens());
        const auto datacenter_endpoints = tm.get_topology().get_datacenter_endpoints();
        std::map<gms::inet_address, float> final_ownership;

        for (const auto& [dc, endpoints_map] : datacenter_endpoints) {
            for (auto endpoint : endpoints_map) {
                // calculate the ownership with replication and add the endpoint to the final ownership map
                try {
                    float ownership = 0.0f;
                    auto ranges = ss.get_ranges_for_endpoint(erm, endpoint);
                    for (auto& r : ranges) {
                        // get_ranges_for_endpoint will unwrap the first range.
                        // With t0 t1 t2 t3, the first range (t3,t0] will be splitted
                        // as (min,t0] and (t3,max]. Skippping the range (t3,max]
                        // we will get the correct ownership number as if the first
                        // range were not splitted.
                        if (!r.end()) {
                            continue;
                        }
                        auto end_token = r.end()->value();
                        auto loc = token_ownership.find(end_token);
                        if (loc != token_ownership.end()) {
                            ownership += loc->second;
                        }
                    }
                    final_ownership[endpoint] = ownership;
                }  catch (replica::no_such_keyspace&) {
                    // In case ss.get_ranges_for_endpoint(keyspace_name, endpoint) is not found, just mark it as zero and continue
                    final_ownership[endpoint] = 0;
                }
            }
        }
        co_return final_ownership;
    });
}

void storage_service::set_mode(mode m) {
    if (m != _operation_mode) {
        slogger.info("entering {} mode", m);
        _operation_mode = m;
    } else {
        // This shouldn't happen, but it's too much for an assert,
        // so -- just emit a warning in the hope that it will be
        // noticed, reported and fixed
        slogger.warn("re-entering {} mode", m);
    }
}

sstring storage_service::get_release_version() {
    return version::release();
}

sstring storage_service::get_schema_version() {
    return _db.local().get_version().to_sstring();
}

static constexpr auto UNREACHABLE = "UNREACHABLE";

future<std::unordered_map<sstring, std::vector<sstring>>> storage_service::describe_schema_versions() {
    auto live_hosts = _gossiper.get_live_members();
    std::unordered_map<sstring, std::vector<sstring>> results;
    netw::messaging_service& ms = _messaging.local();
    return map_reduce(std::move(live_hosts), [&ms] (auto host) {
        auto f0 = ms.send_schema_check(netw::msg_addr{ host, 0 });
        return std::move(f0).then_wrapped([host] (auto f) {
            if (f.failed()) {
                f.ignore_ready_future();
                return std::pair<gms::inet_address, std::optional<table_schema_version>>(host, std::nullopt);
            }
            return std::pair<gms::inet_address, std::optional<table_schema_version>>(host, f.get0());
        });
    }, std::move(results), [] (auto results, auto host_and_version) {
        auto version = host_and_version.second ? host_and_version.second->to_sstring() : UNREACHABLE;
        results.try_emplace(version).first->second.emplace_back(host_and_version.first.to_sstring());
        return results;
    }).then([this] (auto results) {
        // we're done: the results map is ready to return to the client.  the rest is just debug logging:
        auto it_unreachable = results.find(UNREACHABLE);
        if (it_unreachable != results.end()) {
            slogger.debug("Hosts not in agreement. Didn't get a response from everybody: {}", fmt::join(it_unreachable->second, ","));
        }
        auto my_version = get_schema_version();
        for (auto&& entry : results) {
            // check for version disagreement. log the hosts that don't agree.
            if (entry.first == UNREACHABLE || entry.first == my_version) {
                continue;
            }
            for (auto&& host : entry.second) {
                slogger.debug("{} disagrees ({})", host, entry.first);
            }
        }
        if (results.size() == 1) {
            slogger.debug("Schemas are in agreement.");
        }
        return results;
    });
};

future<storage_service::mode> storage_service::get_operation_mode() {
    return run_with_no_api_lock([] (storage_service& ss) {
        return make_ready_future<mode>(ss._operation_mode);
    });
}

future<bool> storage_service::is_gossip_running() {
    return run_with_no_api_lock([] (storage_service& ss) {
        return ss._gossiper.is_enabled();
    });
}

future<> storage_service::start_gossiping() {
    return run_with_api_lock(sstring("start_gossiping"), [] (storage_service& ss) -> future<> {
        if (!ss._gossiper.is_enabled()) {
            slogger.warn("Starting gossip by operator request");
            co_await ss._gossiper.container().invoke_on_all(&gms::gossiper::start);
            bool should_stop_gossiper = false; // undo action
            try {
                auto cdc_gen_ts = co_await ss._sys_ks.local().get_cdc_generation_id();
                if (!cdc_gen_ts) {
                    cdc_log.warn("CDC generation timestamp missing when starting gossip");
                }
                co_await set_gossip_tokens(ss._gossiper,
                        co_await ss._sys_ks.local().get_local_tokens(),
                        cdc_gen_ts);
                ss._gossiper.force_newer_generation();
                co_await ss._gossiper.start_gossiping(gms::get_generation_number());
            } catch (...) {
                should_stop_gossiper = true;
            }
            if (should_stop_gossiper) {
                co_await ss._gossiper.container().invoke_on_all(&gms::gossiper::stop);
            }
        }
    });
}

future<> storage_service::stop_gossiping() {
    return run_with_api_lock(sstring("stop_gossiping"), [] (storage_service& ss) {
        if (ss._gossiper.is_enabled()) {
            slogger.warn("Stopping gossip by operator request");
            return ss._gossiper.container().invoke_on_all(&gms::gossiper::stop);
        }
        return make_ready_future<>();
    });
}

future<> storage_service::do_stop_ms() {
    return _messaging.invoke_on_all([] (auto& ms) {
        return ms.shutdown();
    }).then([] {
        slogger.info("messaging_service stopped");
    });
}

class node_ops_ctl {
    std::unordered_set<gms::inet_address> nodes_unknown_verb;
    std::unordered_set<gms::inet_address> nodes_down;
    std::unordered_set<gms::inet_address> nodes_failed;

public:
    const storage_service& ss;
    sstring desc;
    locator::host_id host_id;   // Host ID of the node operand (i.e. added, replaced, or leaving node)
    inet_address endpoint;      // IP address of the node operand (i.e. added, replaced, or leaving node)
    locator::token_metadata_ptr tmptr;
    std::unordered_set<gms::inet_address> sync_nodes;
    std::unordered_set<gms::inet_address> ignore_nodes;
    node_ops_cmd_request req;
    std::chrono::seconds heartbeat_interval;
    abort_source as;
    std::optional<future<>> heartbeat_updater_done_fut;

    explicit node_ops_ctl(const storage_service& ss_, node_ops_cmd cmd, locator::host_id id, gms::inet_address ep, node_ops_id uuid = node_ops_id::create_random_id())
        : ss(ss_)
        , host_id(id)
        , endpoint(ep)
        , tmptr(ss.get_token_metadata_ptr())
        , req(cmd, uuid)
        , heartbeat_interval(ss._db.local().get_config().nodeops_heartbeat_interval_seconds())
    {}

    ~node_ops_ctl() {
        if (heartbeat_updater_done_fut) {
            on_internal_error_noexcept(slogger, "node_ops_ctl destroyed without stopping");
        }
    }

    const node_ops_id& uuid() const noexcept {
        return req.ops_uuid;
    }

    // may be called multiple times
    void start(sstring desc_, std::function<bool(gms::inet_address)> sync_to_node = [] (gms::inet_address) { return true; }) {
        desc = std::move(desc_);

        slogger.info("{}[{}]: Started {} operation: node={}/{}", desc, uuid(), desc, host_id, endpoint);

        refresh_sync_nodes(std::move(sync_to_node));
    }

    void refresh_sync_nodes(std::function<bool(gms::inet_address)> sync_to_node = [] (gms::inet_address) { return true; }) {
        // sync data with all normal token owners
        sync_nodes.clear();
        const auto& topo = tmptr->get_topology();
        topo.for_each_node([&] (const locator::node* np) {
            seastar::thread::maybe_yield();
            // FIXME: use node* rather than endpoint
            auto node = np->endpoint();
            if (!ignore_nodes.contains(node) && sync_to_node(node)) {
                sync_nodes.insert(node);
            }
        });

        for (auto& node : sync_nodes) {
            if (!ss.gossiper().is_alive(node)) {
                nodes_down.emplace(node);
            }
        }
        if (!nodes_down.empty()) {
            auto msg = ::format("{}[{}]: Cannot start: nodes={} needed for {} operation are down. It is highly recommended to fix the down nodes and try again.", desc, uuid(), nodes_down, desc);
            slogger.warn("{}", msg);
            throw std::runtime_error(msg);
        }

        slogger.info("{}[{}]: sync_nodes={}, ignore_nodes={}", desc, uuid(), desc, host_id, endpoint, sync_nodes, ignore_nodes);
    }

    future<> stop() noexcept {
        co_await stop_heartbeat_updater();
    }

    // Caller should set the required req members before prepare
    future<> prepare(node_ops_cmd cmd) noexcept {
        return send_to_all(cmd);
    }

    void start_heartbeat_updater(node_ops_cmd cmd) {
        if (heartbeat_updater_done_fut) {
            on_internal_error(slogger, "heartbeat_updater already started");
        }
        heartbeat_updater_done_fut = heartbeat_updater(cmd);
    }

    future<> query_pending_op() {
        req.cmd = node_ops_cmd::query_pending_ops;
        co_await coroutine::parallel_for_each(sync_nodes, [this] (const gms::inet_address& node) -> future<> {
            auto resp = co_await ss._messaging.local().send_node_ops_cmd(netw::msg_addr(node), req);
            slogger.debug("{}[{}]: Got query_pending_ops response from node={}, resp.pending_ops={}", desc, uuid(), node, resp.pending_ops);
            if (boost::find(resp.pending_ops, uuid()) == resp.pending_ops.end()) {
                throw std::runtime_error(::format("{}[{}]: Node {} no longer tracks the operation", desc, uuid(), node));
            }
        });
    }

    future<> stop_heartbeat_updater() noexcept {
        if (heartbeat_updater_done_fut) {
            as.request_abort();
            co_await *std::exchange(heartbeat_updater_done_fut, std::nullopt);
        }
    }

    future<> done(node_ops_cmd cmd) noexcept {
        co_await stop_heartbeat_updater();
        co_await send_to_all(cmd);
    }

    future<> abort(node_ops_cmd cmd) noexcept {
        co_await stop_heartbeat_updater();
        co_await send_to_all(cmd);
    }

    future<> abort_on_error(node_ops_cmd cmd, std::exception_ptr ex) noexcept {
        slogger.error("{}[{}]: Operation failed, sync_nodes={}: {}", desc, uuid(), sync_nodes, ex);
        try {
            co_await abort(cmd);
        } catch (...) {
            slogger.warn("{}[{}]: The {} command failed while handling a previous error, sync_nodes={}: {}. Ignoring", desc, uuid(), cmd, sync_nodes, std::current_exception());
        }
        co_await coroutine::return_exception_ptr(std::move(ex));
    }

    future<> send_to_all(node_ops_cmd cmd) {
        req.cmd = cmd;
        req.ignore_nodes = boost::copy_range<std::list<gms::inet_address>>(ignore_nodes);
        sstring op_desc = ::format("{}", cmd);
        slogger.info("{}[{}]: Started {}", desc, uuid(), req);
        auto cmd_category = categorize_node_ops_cmd(cmd);
        co_await coroutine::parallel_for_each(sync_nodes, [&] (const gms::inet_address& node) -> future<> {
            if (nodes_unknown_verb.contains(node) || nodes_down.contains(node) ||
                    (nodes_failed.contains(node) && (cmd_category != node_ops_cmd_category::abort))) {
                // Note that we still send abort commands to failed nodes.
                co_return;
            }
            try {
                co_await ss._messaging.local().send_node_ops_cmd(netw::msg_addr(node), req);
                slogger.debug("{}[{}]: Got {} response from node={}", desc, uuid(), op_desc, node);
            } catch (const seastar::rpc::unknown_verb_error&) {
                if (cmd_category == node_ops_cmd_category::prepare) {
                    slogger.warn("{}[{}]: Node {} does not support the {} verb", desc, uuid(), node, op_desc);
                } else {
                    slogger.warn("{}[{}]: Node {} did not find ops_uuid={} or does not support the {} verb", desc, uuid(), node, uuid(), op_desc);
                }
                nodes_unknown_verb.emplace(node);
            } catch (const seastar::rpc::closed_error&) {
                slogger.warn("{}[{}]: Node {} is down for {} verb", desc, uuid(), op_desc, node);
                nodes_down.emplace(node);
            } catch (...) {
                slogger.warn("{}[{}]: Node {} failed {} verb: {}", desc, uuid(), node, op_desc, std::current_exception());
                nodes_failed.emplace(node);
            }
        });
        std::vector<sstring> errors;
        if (!nodes_failed.empty()) {
            errors.emplace_back(::format("The {} command failed for nodes={}", op_desc, nodes_failed));
        }
        if (!nodes_unknown_verb.empty()) {
            if (cmd_category == node_ops_cmd_category::prepare) {
                errors.emplace_back(::format("The {} command is unsupported on nodes={}. Please upgrade your cluster and run operation again", op_desc, nodes_unknown_verb));
            } else {
                errors.emplace_back(::format("The ops_uuid={} was not found or the {} command is unsupported on nodes={}", uuid(), op_desc, nodes_unknown_verb));
            }
        }
        if (!nodes_down.empty()) {
            errors.emplace_back(::format("The {} command failed for nodes={}: the needed nodes are down. It is highly recommended to fix the down nodes and try again", op_desc, nodes_down));
        }
        if (!errors.empty()) {
            co_await coroutine::return_exception(std::runtime_error(fmt::to_string(fmt::join(errors, "; "))));
        }
        slogger.info("{}[{}]: Finished {}", desc, uuid(), req);
    }

    future<> heartbeat_updater(node_ops_cmd cmd) {
        slogger.info("{}[{}]: Started heartbeat_updater (interval={}s)", desc, uuid(), heartbeat_interval.count());
        while (!as.abort_requested()) {
            auto req = node_ops_cmd_request{cmd, uuid(), {}, {}, {}};
            co_await coroutine::parallel_for_each(sync_nodes, [&] (const gms::inet_address& node) -> future<> {
                try {
                    co_await ss._messaging.local().send_node_ops_cmd(netw::msg_addr(node), req);
                    slogger.debug("{}[{}]: Got heartbeat response from node={}", desc, uuid(), node);
                } catch (...) {
                    slogger.warn("{}[{}]: Failed to get heartbeat response from node={}", desc, uuid(), node);
                };
            });
            co_await sleep_abortable(heartbeat_interval, as).handle_exception([] (std::exception_ptr) {});
        }
        slogger.info("{}[{}]: Stopped heartbeat_updater", desc, uuid());
    }
};

static
void on_streaming_finished() {
    utils::get_local_injector().inject("storage_service_streaming_sleep3", std::chrono::seconds{3}).get();
}

future<> storage_service::raft_decomission() {
    auto& raft_server = _group0->group0_server();

    auto shutdown_request_future = make_ready_future<>();
    auto disengage_shutdown_promise = defer([this] {
        _shutdown_request_promise = std::nullopt;
    });

    while (true) {
        auto guard = co_await _group0->client().start_operation(&_abort_source);

        auto it = _topology_state_machine._topology.find(raft_server.id());
        if (!it) {
            throw std::runtime_error(::format("local node {} is not a member of the cluster", raft_server.id()));
        }

        const auto& rs = it->second;

        if (rs.state != node_state::normal) {
            throw std::runtime_error(::format("local node is not in the normal state (current state: {})", rs.state));
        }

        if (_topology_state_machine._topology.normal_nodes.size() == 1) {
            throw std::runtime_error("Cannot decomission last node in the cluster");
        }

        shutdown_request_future = _shutdown_request_promise.emplace().get_future();

        slogger.info("raft topology: request decomission for: {}", raft_server.id());
        topology_mutation_builder builder(guard.write_timestamp());
        builder.with_node(raft_server.id())
               .set("topology_request", topology_request::leave);
        topology_change change{{builder.build()}};
        group0_command g0_cmd = _group0->client().prepare_command(std::move(change), guard, ::format("decomission: request decomission for {}", raft_server.id()));

        try {
            co_await _group0->client().add_entry(std::move(g0_cmd), std::move(guard), &_abort_source);
        } catch (group0_concurrent_modification&) {
            slogger.info("raft topology: decomission: concurrent operation is detected, retrying.");
            continue;
        }
        break;
    }

    // Wait for the coordinator to tell us to shut down.
    co_await std::move(shutdown_request_future);

    // Need to set it otherwise gossiper will try to send shutdown on exit
    co_await _gossiper.add_local_application_state({{ gms::application_state::STATUS, gms::versioned_value::left({}, _gossiper.now().time_since_epoch().count()) }});
}

future<> storage_service::decommission() {
    return run_with_api_lock(sstring("decommission"), [] (storage_service& ss) {
        return seastar::async([&ss] {
            std::exception_ptr leave_group0_ex;
            if (ss._raft_topology_change_enabled) {
                ss.raft_decomission().get();
            } else {
                bool left_token_ring = false;
                auto uuid = node_ops_id::create_random_id();
                auto& db = ss._db.local();
                node_ops_ctl ctl(ss, node_ops_cmd::decommission_prepare, db.get_config().host_id, ss.get_broadcast_address());
                auto stop_ctl = deferred_stop(ctl);

                // Step 1: Decide who needs to sync data
                // TODO: wire ignore_nodes provided by user
                ctl.start("decommission");

                uuid = ctl.uuid();
                auto endpoint = ctl.endpoint;
                const auto& tmptr = ctl.tmptr;
                if (!tmptr->is_normal_token_owner(endpoint)) {
                    throw std::runtime_error("local node is not a member of the token ring yet");
                }
                // We assume that we're a member of group 0 if we're in decommission()` and Raft is enabled.
                // We have no way to check that we're not a member: attempting to perform group 0 operations
                // would simply hang in that case, the leader would refuse to talk to us.
                // If we aren't a member then we shouldn't be here anyway, since it means that either
                // an earlier decommission finished (leave_group0 is the last operation in decommission)
                // or that we were removed using `removenode`.
                //
                // For handling failure scenarios such as a group 0 member that is not a token ring member,
                // there's `removenode`.

                auto temp = tmptr->clone_after_all_left().get0();
                auto num_tokens_after_all_left = temp.sorted_tokens().size();
                temp.clear_gently().get();
                if (num_tokens_after_all_left < 2) {
                    throw std::runtime_error("no other normal nodes in the ring; decommission would be pointless");
                }

                if (ss._operation_mode != mode::NORMAL) {
                    throw std::runtime_error(::format("Node in {} state; wait for status to become normal or restart", ss._operation_mode));
                }

                ss.update_topology_change_info(::format("decommission {}", endpoint)).get();

                auto non_system_keyspaces = db.get_non_local_vnode_based_strategy_keyspaces();
                for (const auto& keyspace_name : non_system_keyspaces) {
                    if (ss._db.local().find_keyspace(keyspace_name).get_effective_replication_map()->has_pending_ranges(ss.get_broadcast_address())) {
                        throw std::runtime_error("data is currently moving to this node; unable to leave the ring");
                    }
                }

                slogger.info("DECOMMISSIONING: starts");
                ctl.req.leaving_nodes = std::list<gms::inet_address>{endpoint};

                assert(ss._group0);
                bool raft_available = ss._group0->wait_for_raft().get();

                try {
                    // Step 2: Start heartbeat updater
                    ctl.start_heartbeat_updater(node_ops_cmd::decommission_heartbeat);

                    // Step 3: Prepare to sync data
                    ctl.prepare(node_ops_cmd::decommission_prepare).get();

                    // Step 4: Start to sync data
                    slogger.info("DECOMMISSIONING: unbootstrap starts");
                    ss.unbootstrap().get();
                    on_streaming_finished();
                    slogger.info("DECOMMISSIONING: unbootstrap done");

                    // Step 5: Become a group 0 non-voter before leaving the token ring.
                    //
                    // Thanks to this, even if we fail after leaving the token ring but before leaving group 0,
                    // group 0's availability won't be reduced.
                    if (raft_available) {
                        slogger.info("decommission[{}]: becoming a group 0 non-voter", uuid);
                        ss._group0->become_nonvoter().get();
                        slogger.info("decommission[{}]: became a group 0 non-voter", uuid);
                    }

                    // Step 6: Verify that other nodes didn't abort in the meantime.
                    // See https://github.com/scylladb/scylladb/issues/12989.
                    ctl.query_pending_op().get();

                    // Step 7: Leave the token ring
                    slogger.info("decommission[{}]: leaving token ring", uuid);
                    ss.leave_ring().get();
                    left_token_ring = true;
                    slogger.info("decommission[{}]: left token ring", uuid);

                    // Step 8: Finish token movement
                    ctl.done(node_ops_cmd::decommission_done).get();
                } catch (...) {
                    ctl.abort_on_error(node_ops_cmd::decommission_abort, std::current_exception()).get();
                }

                // Step 8: Leave group 0
                //
                // If the node failed to leave the token ring, don't remove it from group 0
                // --- hence the `left_token_ring` check.
                try {
                    utils::get_local_injector().inject("decommission_fail_before_leave_group0",
                        [] { throw std::runtime_error("decommission_fail_before_leave_group0"); });

                    if (raft_available && left_token_ring) {
                        slogger.info("decommission[{}]: leaving Raft group 0", uuid);
                        assert(ss._group0);
                        ss._group0->leave_group0().get();
                        slogger.info("decommission[{}]: left Raft group 0", uuid);
                    }
                } catch (...) {
                    // Even though leave_group0 failed, we will finish decommission and shut down everything.
                    // There's nothing smarter we could do. We should not continue operating in this broken
                    // state (we're not a member of the token ring any more).
                    //
                    // If we didn't manage to leave group 0, we will stay as a non-voter
                    // (which is not too bad - non-voters at least do not reduce group 0's availability).
                    // It's possible to remove the garbage member using `removenode`.
                    slogger.error(
                        "decommission[{}]: FAILED when trying to leave Raft group 0: \"{}\". This node"
                        " is no longer a member of the token ring, so it will finish shutting down its services."
                        " It may still be a member of Raft group 0. To remove it, shut it down and use `removenode`."
                        " Consult the `decommission` and `removenode` documentation for more details.",
                        uuid, std::current_exception());
                    leave_group0_ex = std::current_exception();
                }
            }

            ss.stop_transport().get();
            slogger.info("DECOMMISSIONING: stopped transport");

            ss.get_batchlog_manager().invoke_on_all([] (auto& bm) {
                return bm.drain();
            }).get();
            slogger.info("DECOMMISSIONING: stop batchlog_manager done");

            // StageManager.shutdownNow();
            ss._sys_ks.local().set_bootstrap_state(db::system_keyspace::bootstrap_state::DECOMMISSIONED).get();
            slogger.info("DECOMMISSIONING: set_bootstrap_state done");
            ss.set_mode(mode::DECOMMISSIONED);

            if (leave_group0_ex) {
                std::rethrow_exception(leave_group0_ex);
            }

            slogger.info("DECOMMISSIONING: done");
            // let op be responsible for killing the process
        });
    });
}

// Runs inside seastar::async context
void storage_service::run_bootstrap_ops(std::unordered_set<token>& bootstrap_tokens) {
    auto& db = _db.local();
    node_ops_ctl ctl(*this, node_ops_cmd::bootstrap_prepare, db.get_config().host_id, get_broadcast_address());
    auto stop_ctl = deferred_stop(ctl);
    const auto& uuid = ctl.uuid();

    // Step 1: Decide who needs to sync data for bootstrap operation
    // TODO: Specify ignore_nodes
    ctl.start("bootstrap");

    auto start_time = std::chrono::steady_clock::now();
    for (;;) {
        ctl.sync_nodes.insert(get_broadcast_address());

        // Step 2: Wait until no pending node operations
        std::unordered_map<gms::inet_address, std::list<node_ops_id>> pending_ops;
        auto req = node_ops_cmd_request(node_ops_cmd::query_pending_ops, uuid);
        parallel_for_each(ctl.sync_nodes, [this, req, uuid, &pending_ops] (const gms::inet_address& node) {
            return _messaging.local().send_node_ops_cmd(netw::msg_addr(node), req).then([uuid, node, &pending_ops] (node_ops_cmd_response resp) {
                slogger.debug("bootstrap[{}]: Got query_pending_ops response from node={}, resp.pending_ops={}", uuid, node, resp.pending_ops);
                if (!resp.pending_ops.empty()) {
                    pending_ops.emplace(node, resp.pending_ops);
                }
                return make_ready_future<>();
            });
        }).handle_exception([uuid] (std::exception_ptr ep) {
            slogger.warn("bootstrap[{}]: Failed to query_pending_ops : {}", uuid, ep);
        }).get();
        if (pending_ops.empty()) {
            break;
        } else {
            if (std::chrono::steady_clock::now() > start_time + std::chrono::seconds(60)) {
                throw std::runtime_error(::format("bootstrap[{}]: Found pending node ops = {}, reject bootstrap", uuid, pending_ops));
            }
            slogger.warn("bootstrap[{}]: Found pending node ops = {}, sleep 5 seconds and check again", uuid, pending_ops);
            sleep_abortable(std::chrono::seconds(5), _abort_source).get();
            ctl.refresh_sync_nodes();
            // the bootstrapping node will be added back when we loop
        }
    }

    auto tokens = std::list<dht::token>(bootstrap_tokens.begin(), bootstrap_tokens.end());
    ctl.req.bootstrap_nodes = {
        {get_broadcast_address(), tokens},
    };
    try {
        // Step 2: Start heartbeat updater
        ctl.start_heartbeat_updater(node_ops_cmd::bootstrap_heartbeat);

        // Step 3: Prepare to sync data
        ctl.prepare(node_ops_cmd::bootstrap_prepare).get();

        // Step 5: Sync data for bootstrap
        _repair.local().bootstrap_with_repair(get_token_metadata_ptr(), bootstrap_tokens).get();
        on_streaming_finished();

        // Step 6: Finish
        ctl.done(node_ops_cmd::bootstrap_done).get();
    } catch (...) {
        ctl.abort_on_error(node_ops_cmd::bootstrap_abort, std::current_exception()).get();
    }
}

// Runs inside seastar::async context
void storage_service::run_replace_ops(std::unordered_set<token>& bootstrap_tokens, replacement_info replace_info) {
    node_ops_ctl ctl(*this, node_ops_cmd::replace_prepare, replace_info.host_id, replace_info.address);
    auto stop_ctl = deferred_stop(ctl);
    const auto& uuid = ctl.uuid();
    gms::inet_address replace_address = replace_info.address;
    ctl.ignore_nodes = parse_node_list(_db.local().get_config().ignore_dead_nodes_for_replace(), *ctl.tmptr);
    // Step 1: Decide who needs to sync data for replace operation
    // The replacing node is not a normal token owner yet
    // Add it back explicitly after checking all other nodes.
    ctl.start("replace", [&] (gms::inet_address node) {
        return node != replace_address;
    });
    ctl.sync_nodes.insert(get_broadcast_address());

    auto sync_nodes_generations = _gossiper.get_generation_for_nodes(ctl.sync_nodes).get();
    // Map existing nodes to replacing nodes
    ctl.req.replace_nodes = {
        {replace_address, get_broadcast_address()},
    };
    try {
        // Step 2: Start heartbeat updater
        ctl.start_heartbeat_updater(node_ops_cmd::replace_heartbeat);

        // Step 3: Prepare to sync data
        ctl.prepare(node_ops_cmd::replace_prepare).get();

        // Step 4: Allow nodes in sync_nodes list to mark the replacing node as alive
        _gossiper.advertise_to_nodes(sync_nodes_generations).get();
        slogger.info("replace[{}]: Allow nodes={} to mark replacing node={} as alive", uuid, ctl.sync_nodes, get_broadcast_address());

        // Step 5: Wait for nodes to finish marking the replacing node as live
        ctl.send_to_all(node_ops_cmd::replace_prepare_mark_alive).get();

        // Step 6: Update pending ranges on nodes
        ctl.send_to_all(node_ops_cmd::replace_prepare_pending_ranges).get();

        // Step 7: Sync data for replace
        if (is_repair_based_node_ops_enabled(streaming::stream_reason::replace)) {
            slogger.info("replace[{}]: Using repair based node ops to sync data", uuid);
            _repair.local().replace_with_repair(get_token_metadata_ptr(), bootstrap_tokens, ctl.ignore_nodes).get();
        } else {
            slogger.info("replace[{}]: Using streaming based node ops to sync data", uuid);
            dht::boot_strapper bs(_db, _stream_manager, _abort_source, get_broadcast_address(), _sys_ks.local().local_dc_rack(), bootstrap_tokens, get_token_metadata_ptr());
            bs.bootstrap(streaming::stream_reason::replace, _gossiper, replace_address).get();
        }
        on_streaming_finished();

        // Step 8: Finish
        ctl.done(node_ops_cmd::replace_done).get();

        // Allow any nodes to mark the replacing node as alive
        _gossiper.advertise_to_nodes({}).get();
        slogger.info("replace[{}]: Allow any nodes to mark replacing node={} as alive", uuid,  get_broadcast_address());
    } catch (...) {
        // we need to revert the effect of prepare verb the replace ops is failed
        ctl.abort_on_error(node_ops_cmd::replace_abort, std::current_exception()).get();
    }
}

future<> storage_service::raft_removenode(locator::host_id host_id) {
    auto id = raft::server_id{host_id.uuid()};
    while (true) {
        auto guard = co_await _group0->client().start_operation(&_abort_source);

        auto it = _topology_state_machine._topology.find(id);

        if (!it) {
            throw std::runtime_error(::format("raft topology removenode: host id {} is not found in the cluster", host_id));
        }

        auto& rs = it->second; // not usable after yeild

        if (rs.state != node_state::normal) {
            throw std::runtime_error(::format("raft topology removenode: node {} is in '{}' state. Wait for it to be in 'normal' state", id, rs.state));
        }
        const auto& am = _group0->address_map();
        auto ip = am.find(id);
        if (!ip) {
            // What to do if there is no mapping? Wait and retry?
            on_fatal_internal_error(slogger, ::format("Remove node cannot find a mapping from node id {} to its ip", id));
        }

        if (_gossiper.is_alive(*ip)) {
            const std::string message = ::format(
                "raft topology removenode: Rejected removenode operation for node {} ip {} "
                "the node being removed is alive, maybe you should use decommission instead?",
                id, *ip);
            slogger.warn(std::string_view(message));
            throw std::runtime_error(message);
        }

        slogger.info("raft topology: request removenode for: {}", id);
        topology_mutation_builder builder(guard.write_timestamp());
        builder.with_node(id)
               .set("topology_request", topology_request::remove);
        topology_change change{{builder.build()}};
        group0_command g0_cmd = _group0->client().prepare_command(std::move(change), guard, ::format("removenode: request remove for {}", id));

        try {
            co_await _group0->client().add_entry(std::move(g0_cmd), std::move(guard), &_abort_source);
        } catch (group0_concurrent_modification&) {
            slogger.info("raft topology: removenode: concurrent operation is detected, retrying.");
            continue;
        }

        break;
    }

    // Wait the node we are removing to enter left state
    co_await _topology_state_machine.event.when([this, id] {
        return _topology_state_machine._topology.left_nodes.contains(id);
    });

    try {
        co_await _group0->remove_from_raft_config(id);
    } catch (raft::not_a_member&) {
        slogger.info("raft topology removenode: already removed from the raft config by the topology coordinator");
    }
}

future<> storage_service::removenode(locator::host_id host_id, std::list<locator::host_id_or_endpoint> ignore_nodes_params) {
    return run_with_api_lock(sstring("removenode"), [host_id, ignore_nodes_params = std::move(ignore_nodes_params)] (storage_service& ss) mutable {
        return seastar::async([&ss, host_id, ignore_nodes_params = std::move(ignore_nodes_params)] () mutable {
            if (ss._raft_topology_change_enabled) {
                ss.raft_removenode(host_id).get();
                return;
            }
            node_ops_ctl ctl(ss, node_ops_cmd::removenode_prepare, host_id, gms::inet_address());
            auto stop_ctl = deferred_stop(ctl);
            auto uuid = ctl.uuid();
            const auto& tmptr = ctl.tmptr;
            auto endpoint_opt = tmptr->get_endpoint_for_host_id(host_id);
            assert(ss._group0);
            auto raft_id = raft::server_id{host_id.uuid()};
            bool raft_available = ss._group0->wait_for_raft().get();
            bool is_group0_member = raft_available && ss._group0->is_member(raft_id, false);
            if (!endpoint_opt && !is_group0_member) {
                throw std::runtime_error(::format("removenode[{}]: Node {} not found in the cluster", uuid, host_id));
            }

            // If endpoint_opt is engaged, the node is a member of the token ring.
            // is_group0_member indicates whether the node is a member of Raft group 0.
            // A node might be a member of group 0 but not a member of the token ring, e.g. due to a
            // previously failed removenode/decommission. The code is written to handle this
            // situation. Parts related to removing this node from the token ring are conditioned on
            // endpoint_opt, while parts related to removing from group 0 are conditioned on
            // is_group0_member.

            if (endpoint_opt && ss._gossiper.is_alive(*endpoint_opt)) {
                const std::string message = ::format(
                    "removenode[{}]: Rejected removenode operation (node={}); "
                    "the node being removed is alive, maybe you should use decommission instead?",
                    uuid, *endpoint_opt);
                slogger.warn(std::string_view(message));
                throw std::runtime_error(message);
            }

            for (auto& hoep : ignore_nodes_params) {
                hoep.resolve(*tmptr);
                ctl.ignore_nodes.insert(hoep.endpoint);
            }

            bool removed_from_token_ring = !endpoint_opt;
            if (endpoint_opt) {
                auto endpoint = *endpoint_opt;
                ctl.endpoint = endpoint;

                // Step 1: Make the node a group 0 non-voter before removing it from the token ring.
                //
                // Thanks to this, even if we fail after removing the node from the token ring
                // but before removing it group 0, group 0's availability won't be reduced.
                if (is_group0_member && ss._group0->is_member(raft_id, true)) {
                    slogger.info("removenode[{}]: making node {} a non-voter in group 0", uuid, raft_id);
                    ss._group0->make_nonvoter(raft_id).get();
                    slogger.info("removenode[{}]: made node {} a non-voter in group 0", uuid, raft_id);
                }

                // Step 2: Decide who needs to sync data
                //
                // By default, we require all nodes in the cluster to participate
                // the removenode operation and sync data if needed. We fail the
                // removenode operation if any of them is down or fails.
                //
                // If the user want the removenode opeartion to succeed even if some of the nodes
                // are not available, the user has to explicitly pass a list of
                // node that can be skipped for the operation.
                ctl.start("removenode", [&] (gms::inet_address node) {
                    return node != endpoint;
                });

                auto tokens = tmptr->get_tokens(endpoint);

                try {
                    // Step 3: Start heartbeat updater
                    ctl.start_heartbeat_updater(node_ops_cmd::removenode_heartbeat);

                    // Step 4: Prepare to sync data
                    ctl.req.leaving_nodes = {endpoint};
                    ctl.prepare(node_ops_cmd::removenode_prepare).get();

                    // Step 5: Start to sync data
                    ctl.send_to_all(node_ops_cmd::removenode_sync_data).get();
                    on_streaming_finished();

                    // Step 6: Finish token movement
                    ctl.done(node_ops_cmd::removenode_done).get();

                    // Step 7: Announce the node has left
                    slogger.info("removenode[{}]: Advertising that the node left the ring", uuid);
                    ss._gossiper.advertise_token_removed(endpoint, host_id).get();
                    std::unordered_set<token> tmp(tokens.begin(), tokens.end());
                    ss.excise(std::move(tmp), endpoint).get();
                    removed_from_token_ring = true;
                    slogger.info("removenode[{}]: Finished removing the node from the ring", uuid);
                } catch (...) {
                    // we need to revert the effect of prepare verb the removenode ops is failed
                    ctl.abort_on_error(node_ops_cmd::removenode_abort, std::current_exception()).get();
                }
            }

            // Step 8: Remove the node from group 0
            //
            // If the node was a token ring member but we failed to remove it,
            // don't remove it from group 0 -- hence the `removed_from_token_ring` check.
            try {
                utils::get_local_injector().inject("removenode_fail_before_remove_from_group0",
                    [] { throw std::runtime_error("removenode_fail_before_remove_from_group0"); });

                if (is_group0_member && removed_from_token_ring) {
                    slogger.info("removenode[{}]: removing node {} from Raft group 0", uuid, raft_id);
                    ss._group0->remove_from_group0(raft_id).get();
                    slogger.info("removenode[{}]: removed node {} from Raft group 0", uuid, raft_id);
                }
            } catch (...) {
                slogger.error(
                    "removenode[{}]: FAILED when trying to remove the node from Raft group 0: \"{}\". The node"
                    " is no longer a member of the token ring, but it may still be a member of Raft group 0."
                    " Please retry `removenode`. Consult the `removenode` documentation for more details.",
                    uuid, std::current_exception());
                throw;
            }

            slogger.info("removenode[{}]: Finished removenode operation, host id={}", uuid, host_id);
        });
    });
}

future<> storage_service::check_and_repair_cdc_streams(cdc::generation_service& cdc_gen_svc) {
    assert(this_shard_id() == 0);

    if (_raft_topology_change_enabled) {
        return raft_check_and_repair_cdc_streams();
    }

    return cdc_gen_svc.check_and_repair_cdc_streams();
}

class node_ops_meta_data {
    node_ops_id _ops_uuid;
    gms::inet_address _coordinator;
    std::function<future<> ()> _abort;
    shared_ptr<abort_source> _abort_source;
    std::function<void ()> _signal;
    shared_ptr<node_ops_info> _ops;
    seastar::timer<lowres_clock> _watchdog;
    std::chrono::seconds _watchdog_interval;
public:
    explicit node_ops_meta_data(
            node_ops_id ops_uuid,
            gms::inet_address coordinator,
            std::list<gms::inet_address> ignore_nodes,
            std::chrono::seconds watchdog_interval,
            std::function<future<> ()> abort_func,
            std::function<void ()> signal_func);
    shared_ptr<node_ops_info> get_ops_info();
    shared_ptr<abort_source> get_abort_source();
    future<> abort();
    void update_watchdog();
    void cancel_watchdog();
};

void storage_service::node_ops_cmd_check(gms::inet_address coordinator, const node_ops_cmd_request& req) {
    auto ops_uuids = boost::copy_range<std::vector<node_ops_id>>(_node_ops| boost::adaptors::map_keys);
    std::string msg;
    if (req.cmd == node_ops_cmd::removenode_prepare || req.cmd == node_ops_cmd::replace_prepare ||
            req.cmd == node_ops_cmd::decommission_prepare || req.cmd == node_ops_cmd::bootstrap_prepare) {
        // Peer node wants to start a new node operation. Make sure no pending node operation is in progress.
        if (!_node_ops.empty()) {
            msg = ::format("node_ops_cmd_check: Node {} rejected node_ops_cmd={} from node={} with ops_uuid={}, pending_node_ops={}, pending node ops is in progress",
                    get_broadcast_address(), req.cmd, coordinator, req.ops_uuid, ops_uuids);
        }
    } else if (req.cmd == node_ops_cmd::decommission_heartbeat || req.cmd == node_ops_cmd::removenode_heartbeat ||
            req.cmd == node_ops_cmd::replace_heartbeat || req.cmd == node_ops_cmd::bootstrap_heartbeat) {
        // We allow node_ops_cmd heartbeat to be sent before prepare cmd
    } else {
        if (ops_uuids.size() == 1 && ops_uuids.front() == req.ops_uuid) {
            // Check is good, since we know this ops_uuid and this is the only ops_uuid we are working on.
        } else if (ops_uuids.size() == 0) {
            // The ops_uuid received is unknown. Fail the request.
            msg = ::format("node_ops_cmd_check: Node {} rejected node_ops_cmd={} from node={} with ops_uuid={}, pending_node_ops={}, the node ops is unknown",
                    get_broadcast_address(), req.cmd, coordinator, req.ops_uuid, ops_uuids);
        } else {
            // Other node ops is in progress. Fail the request.
            msg = ::format("node_ops_cmd_check: Node {} rejected node_ops_cmd={} from node={} with ops_uuid={}, pending_node_ops={}, pending node ops is in progress",
                    get_broadcast_address(), req.cmd, coordinator, req.ops_uuid, ops_uuids);
        }
    }
    if (!msg.empty()) {
        slogger.warn("{}", msg);
        throw std::runtime_error(msg);
    }
}

void storage_service::on_node_ops_registered(node_ops_id ops_uuid) {
    utils::get_local_injector().inject("storage_service_nodeops_prepare_handler_sleep3", std::chrono::seconds{3}).get();
    utils::get_local_injector().inject("storage_service_nodeops_abort_after_1s", [this, ops_uuid] {
        (void)with_gate(_async_gate, [this, ops_uuid] {
            return seastar::sleep_abortable(std::chrono::seconds(1), _abort_source).then([this, ops_uuid] {
                node_ops_signal_abort(ops_uuid);
            });
        });
    });
}

void storage_service::node_ops_insert(node_ops_id ops_uuid,
                                      gms::inet_address coordinator,
                                      std::list<inet_address> ignore_nodes,
                                      std::function<future<>()> abort_func) {
    auto watchdog_interval = std::chrono::seconds(_db.local().get_config().nodeops_watchdog_timeout_seconds());
    auto meta = node_ops_meta_data(ops_uuid, coordinator, std::move(ignore_nodes), watchdog_interval, std::move(abort_func),
                                   [this, ops_uuid]() mutable { node_ops_signal_abort(ops_uuid); });
    _node_ops.emplace(ops_uuid, std::move(meta));
    on_node_ops_registered(ops_uuid);
}

future<node_ops_cmd_response> storage_service::node_ops_cmd_handler(gms::inet_address coordinator, node_ops_cmd_request req) {
    return seastar::async([this, coordinator, req = std::move(req)] () mutable {
        auto ops_uuid = req.ops_uuid;
        slogger.debug("node_ops_cmd_handler cmd={}, ops_uuid={}", req.cmd, ops_uuid);

        if (req.cmd == node_ops_cmd::query_pending_ops) {
            bool ok = true;
            auto ops_uuids = boost::copy_range<std::list<node_ops_id>>(_node_ops| boost::adaptors::map_keys);
            node_ops_cmd_response resp(ok, ops_uuids);
            slogger.debug("node_ops_cmd_handler: Got query_pending_ops request from {}, pending_ops={}", coordinator, ops_uuids);
            return resp;
        } else if (req.cmd == node_ops_cmd::repair_updater) {
            slogger.debug("repair[{}]: Got repair_updater request from {}", ops_uuid, coordinator);
            _db.invoke_on_all([coordinator, ops_uuid, tables = req.repair_tables] (replica::database &db) {
                for (const auto& table_id : tables) {
                    try {
                        auto& table = db.find_column_family(table_id);
                        table.update_off_strategy_trigger();
                        slogger.debug("repair[{}]: Updated off_strategy_trigger for table {}.{} by node {}",
                                ops_uuid, table.schema()->ks_name(), table.schema()->cf_name(), coordinator);
                    } catch (replica::no_such_column_family&) {
                        // The table could be dropped by user, ignore it.
                    } catch (...) {
                        throw;
                    }
                }
            }).get();
            bool ok = true;
            return node_ops_cmd_response(ok);
        }

        node_ops_cmd_check(coordinator, req);

        if (req.cmd == node_ops_cmd::removenode_prepare) {
            if (req.leaving_nodes.size() > 1) {
                auto msg = ::format("removenode[{}]: Could not removenode more than one node at a time: leaving_nodes={}", req.ops_uuid, req.leaving_nodes);
                slogger.warn("{}", msg);
                throw std::runtime_error(msg);
            }
            mutate_token_metadata([coordinator, &req, this] (mutable_token_metadata_ptr tmptr) mutable {
                for (auto& node : req.leaving_nodes) {
                    slogger.info("removenode[{}]: Added node={} as leaving node, coordinator={}", req.ops_uuid, node, coordinator);
                    tmptr->add_leaving_endpoint(node);
                }
                return update_topology_change_info(tmptr, ::format("removenode {}", req.leaving_nodes));
            }).get();
            node_ops_insert(ops_uuid, coordinator, std::move(req.ignore_nodes), [this, coordinator, req = std::move(req)] () mutable {
                return mutate_token_metadata([this, coordinator, req = std::move(req)] (mutable_token_metadata_ptr tmptr) mutable {
                    for (auto& node : req.leaving_nodes) {
                        slogger.info("removenode[{}]: Removed node={} as leaving node, coordinator={}", req.ops_uuid, node, coordinator);
                        tmptr->del_leaving_endpoint(node);
                    }
                    return update_topology_change_info(tmptr, ::format("removenode {}", req.leaving_nodes));
                });
            });
        } else if (req.cmd == node_ops_cmd::removenode_heartbeat) {
            slogger.debug("removenode[{}]: Updated heartbeat from coordinator={}", req.ops_uuid,  coordinator);
            node_ops_update_heartbeat(ops_uuid).get();
        } else if (req.cmd == node_ops_cmd::removenode_done) {
            slogger.info("removenode[{}]: Marked ops done from coordinator={}", req.ops_uuid, coordinator);
            node_ops_done(ops_uuid).get();
        } else if (req.cmd == node_ops_cmd::removenode_sync_data) {
            auto it = _node_ops.find(ops_uuid);
            if (it == _node_ops.end()) {
                throw std::runtime_error(::format("removenode[{}]: Can not find ops_uuid={}", ops_uuid, ops_uuid));
            }
            auto ops = it->second.get_ops_info();
            auto as = it->second.get_abort_source();
            for (auto& node : req.leaving_nodes) {
                if (is_repair_based_node_ops_enabled(streaming::stream_reason::removenode)) {
                    slogger.info("removenode[{}]: Started to sync data for removing node={} using repair, coordinator={}", req.ops_uuid, node, coordinator);
                    _repair.local().removenode_with_repair(get_token_metadata_ptr(), node, ops).get();
                } else {
                    slogger.info("removenode[{}]: Started to sync data for removing node={} using stream, coordinator={}", req.ops_uuid, node, coordinator);
                    removenode_with_stream(node, as).get();
                }
            }
        } else if (req.cmd == node_ops_cmd::removenode_abort) {
            node_ops_abort(ops_uuid).get();
        } else if (req.cmd == node_ops_cmd::decommission_prepare) {
            utils::get_local_injector().inject(
                "storage_service_decommission_prepare_handler_sleep", std::chrono::milliseconds{1500}).get();
            if (req.leaving_nodes.size() > 1) {
                auto msg = ::format("decommission[{}]: Could not decommission more than one node at a time: leaving_nodes={}", req.ops_uuid, req.leaving_nodes);
                slogger.warn("{}", msg);
                throw std::runtime_error(msg);
            }
            mutate_token_metadata([coordinator, &req, this] (mutable_token_metadata_ptr tmptr) mutable {
                for (auto& node : req.leaving_nodes) {
                    slogger.info("decommission[{}]: Added node={} as leaving node, coordinator={}", req.ops_uuid, node, coordinator);
                    tmptr->add_leaving_endpoint(node);
                }
                return update_topology_change_info(tmptr, ::format("decommission {}", req.leaving_nodes));
            }).get();
            node_ops_insert(ops_uuid, coordinator, std::move(req.ignore_nodes), [this, coordinator, req = std::move(req)] () mutable {
                return mutate_token_metadata([this, coordinator, req = std::move(req)] (mutable_token_metadata_ptr tmptr) mutable {
                    for (auto& node : req.leaving_nodes) {
                        slogger.info("decommission[{}]: Removed node={} as leaving node, coordinator={}", req.ops_uuid, node, coordinator);
                        tmptr->del_leaving_endpoint(node);
                    }
                    return update_topology_change_info(tmptr, ::format("decommission {}", req.leaving_nodes));
                });
            });
        } else if (req.cmd == node_ops_cmd::decommission_heartbeat) {
            slogger.debug("decommission[{}]: Updated heartbeat from coordinator={}", req.ops_uuid,  coordinator);
            node_ops_update_heartbeat(ops_uuid).get();
        } else if (req.cmd == node_ops_cmd::decommission_done) {
            bool check_again = false;
            auto start_time = std::chrono::steady_clock::now();
            slogger.info("decommission[{}]: Started to check if nodes={} have left the cluster, coordinator={}", req.ops_uuid, req.leaving_nodes, coordinator);
            do {
                check_again = false;
                for (auto& node : req.leaving_nodes) {
                    auto tmptr = get_token_metadata_ptr();
                    if (tmptr->is_normal_token_owner(node)) {
                        check_again = true;
                        if (std::chrono::steady_clock::now() > start_time + std::chrono::seconds(60)) {
                            auto msg = ::format("decommission[{}]: Node {} is still in the cluster", req.ops_uuid, node);
                            throw std::runtime_error(msg);
                        }
                        slogger.warn("decommission[{}]: Node {} is still in the cluster, sleep and check again", req.ops_uuid, node);
                        sleep_abortable(std::chrono::milliseconds(500), _abort_source).get();
                        break;
                    }
                }
            } while (check_again);
            slogger.info("decommission[{}]: Finished to check if nodes={} have left the cluster, coordinator={}", req.ops_uuid, req.leaving_nodes, coordinator);
            slogger.info("decommission[{}]: Marked ops done from coordinator={}", req.ops_uuid, coordinator);
            slogger.debug("Triggering off-strategy compaction for all non-system tables on decommission completion");
            _db.invoke_on_all([](replica::database &db) {
                for (auto& table : db.get_non_system_column_families()) {
                    table->trigger_offstrategy_compaction();
                }
            }).get();
            node_ops_done(ops_uuid).get();
        } else if (req.cmd == node_ops_cmd::decommission_abort) {
            node_ops_abort(ops_uuid).get();
        } else if (req.cmd == node_ops_cmd::replace_prepare) {
            // Mark the replacing node as replacing
            if (req.replace_nodes.size() > 1) {
                auto msg = ::format("replace[{}]: Could not replace more than one node at a time: replace_nodes={}", req.ops_uuid, req.replace_nodes);
                slogger.warn("{}", msg);
                throw std::runtime_error(msg);
            }
            mutate_token_metadata([coordinator, &req, this] (mutable_token_metadata_ptr tmptr) mutable {
                for (auto& x: req.replace_nodes) {
                    auto existing_node = x.first;
                    auto replacing_node = x.second;
                    slogger.info("replace[{}]: Added replacing_node={} to replace existing_node={}, coordinator={}", req.ops_uuid, replacing_node, existing_node, coordinator);
                    tmptr->update_topology(replacing_node, get_dc_rack_for(replacing_node), locator::node::state::joining);
                    tmptr->add_replacing_endpoint(existing_node, replacing_node);
                }
                return make_ready_future<>();
            }).get();
            node_ops_insert(ops_uuid, coordinator, std::move(req.ignore_nodes), [this, coordinator, req = std::move(req)] () mutable {
                return mutate_token_metadata([this, coordinator, req = std::move(req)] (mutable_token_metadata_ptr tmptr) mutable {
                    for (auto& x: req.replace_nodes) {
                        auto existing_node = x.first;
                        auto replacing_node = x.second;
                        slogger.info("replace[{}]: Removed replacing_node={} to replace existing_node={}, coordinator={}", req.ops_uuid, replacing_node, existing_node, coordinator);
                        tmptr->del_replacing_endpoint(existing_node);
                    }
                    return update_topology_change_info(tmptr, ::format("replace {}", req.replace_nodes));
                });
            });
        } else if (req.cmd == node_ops_cmd::replace_prepare_mark_alive) {
            // Wait for local node has marked replacing node as alive
            auto nodes = boost::copy_range<std::vector<inet_address>>(req.replace_nodes| boost::adaptors::map_values);
            try {
                _gossiper.wait_alive(nodes, std::chrono::milliseconds(120 * 1000)).get();
            } catch (...) {
                slogger.warn("replace[{}]: Failed to wait for marking replacing node as up, replace_nodes={}: {}",
                        req.ops_uuid, req.replace_nodes, std::current_exception());
                throw;
            }
        } else if (req.cmd == node_ops_cmd::replace_prepare_pending_ranges) {
            // Update the pending_ranges for the replacing node
            slogger.debug("replace[{}]: Updated pending_ranges from coordinator={}", req.ops_uuid, coordinator);
            mutate_token_metadata([&req, this] (mutable_token_metadata_ptr tmptr) mutable {
                return update_topology_change_info(tmptr, ::format("replace {}", req.replace_nodes));
            }).get();
        } else if (req.cmd == node_ops_cmd::replace_heartbeat) {
            slogger.debug("replace[{}]: Updated heartbeat from coordinator={}", req.ops_uuid, coordinator);
            node_ops_update_heartbeat(ops_uuid).get();
        } else if (req.cmd == node_ops_cmd::replace_done) {
            slogger.info("replace[{}]: Marked ops done from coordinator={}", req.ops_uuid, coordinator);
            node_ops_done(ops_uuid).get();
        } else if (req.cmd == node_ops_cmd::replace_abort) {
            node_ops_abort(ops_uuid).get();
        } else if (req.cmd == node_ops_cmd::bootstrap_prepare) {
            // Mark the bootstrap node as bootstrapping
            if (req.bootstrap_nodes.size() > 1) {
                auto msg = ::format("bootstrap[{}]: Could not bootstrap more than one node at a time: bootstrap_nodes={}", req.ops_uuid, req.bootstrap_nodes);
                slogger.warn("{}", msg);
                throw std::runtime_error(msg);
            }
            mutate_token_metadata([coordinator, &req, this] (mutable_token_metadata_ptr tmptr) mutable {
                for (auto& x: req.bootstrap_nodes) {
                    auto& endpoint = x.first;
                    auto tokens = std::unordered_set<dht::token>(x.second.begin(), x.second.end());
                    slogger.info("bootstrap[{}]: Added node={} as bootstrap, coordinator={}", req.ops_uuid, endpoint, coordinator);
                    tmptr->update_topology(endpoint, get_dc_rack_for(endpoint), locator::node::state::joining);
                    tmptr->add_bootstrap_tokens(tokens, endpoint);
                }
                return update_topology_change_info(tmptr, ::format("bootstrap {}", req.bootstrap_nodes));
            }).get();
            node_ops_insert(ops_uuid, coordinator, std::move(req.ignore_nodes), [this, coordinator, req = std::move(req)] () mutable {
                return mutate_token_metadata([this, coordinator, req = std::move(req)] (mutable_token_metadata_ptr tmptr) mutable {
                    for (auto& x: req.bootstrap_nodes) {
                        auto& endpoint = x.first;
                        auto tokens = std::unordered_set<dht::token>(x.second.begin(), x.second.end());
                        slogger.info("bootstrap[{}]: Removed node={} as bootstrap, coordinator={}", req.ops_uuid, endpoint, coordinator);
                        tmptr->remove_bootstrap_tokens(tokens);
                    }
                    return update_topology_change_info(tmptr, ::format("bootstrap {}", req.bootstrap_nodes));
                });
            });
        } else if (req.cmd == node_ops_cmd::bootstrap_heartbeat) {
            slogger.debug("bootstrap[{}]: Updated heartbeat from coordinator={}", req.ops_uuid, coordinator);
            node_ops_update_heartbeat(ops_uuid).get();
        } else if (req.cmd == node_ops_cmd::bootstrap_done) {
            slogger.info("bootstrap[{}]: Marked ops done from coordinator={}", req.ops_uuid, coordinator);
            node_ops_done(ops_uuid).get();
        } else if (req.cmd == node_ops_cmd::bootstrap_abort) {
            node_ops_abort(ops_uuid).get();
        } else {
            auto msg = ::format("node_ops_cmd_handler: ops_uuid={}, unknown cmd={}", req.ops_uuid, req.cmd);
            slogger.warn("{}", msg);
            throw std::runtime_error(msg);
        }
        bool ok = true;
        node_ops_cmd_response resp(ok);
        return resp;
    });
}

future<> storage_service::drain() {
    return run_with_api_lock(sstring("drain"), [] (storage_service& ss) {
        if (ss._operation_mode == mode::DRAINED) {
            slogger.warn("Cannot drain node (did it already happen?)");
            return make_ready_future<>();
        }

        ss.set_mode(mode::DRAINING);
        return ss.do_drain().then([&ss] {
            ss._drain_finished.set_value();
            ss.set_mode(mode::DRAINED);
        });
    });
}

future<> storage_service::do_drain() {
    co_await stop_transport();

    co_await tracing::tracing::tracing_instance().invoke_on_all(&tracing::tracing::shutdown);

    co_await get_batchlog_manager().invoke_on_all([] (auto& bm) {
        return bm.drain();
    });

    co_await _db.invoke_on_all(&replica::database::drain);
    co_await _sys_ks.invoke_on_all(&db::system_keyspace::shutdown);
}

future<> storage_service::raft_rebuild(sstring source_dc) {
    auto& raft_server = _group0->group0_server();

    while (true) {
        auto guard = co_await _group0->client().start_operation(&_abort_source);

        auto it = _topology_state_machine._topology.find(raft_server.id());
        if (!it) {
            throw std::runtime_error(::format("local node {} is not a member of the cluster", raft_server.id()));
        }

        const auto& rs = it->second;

        if (rs.state != node_state::normal) {
            throw std::runtime_error(::format("local node is not in the normal state (current state: {})", rs.state));
        }

        if (_topology_state_machine._topology.normal_nodes.size() == 1) {
            throw std::runtime_error("Cannot rebuild a single node");
        }

        slogger.info("raft topology: request rebuild for: {}", raft_server.id());
        topology_mutation_builder builder(guard.write_timestamp());
        builder.with_node(raft_server.id())
               .set("topology_request", topology_request::rebuild)
               .set("rebuild_option", source_dc);
        topology_change change{{builder.build()}};
        group0_command g0_cmd = _group0->client().prepare_command(std::move(change), guard, ::format("rebuild: request rebuild for {} ({})", raft_server.id(), source_dc));

        try {
            co_await _group0->client().add_entry(std::move(g0_cmd), std::move(guard), &_abort_source);
        } catch (group0_concurrent_modification&) {
            slogger.info("raft topology: rebuild: concurrent operation is detected, retrying.");
            continue;
        }
        break;
    }

    // Wait until rebuild completes. We know it completes when the request parameter is empty
    co_await _topology_state_machine.event.when([this, &raft_server] {
        return !_topology_state_machine._topology.req_param.contains(raft_server.id());
    });
}

future<> storage_service::raft_check_and_repair_cdc_streams() {
    std::optional<cdc::generation_id_v2> curr_gen;

    while (true) {
        slogger.info("raft topology: request check_and_repair_cdc_streams, refreshing topology");
        auto guard = co_await _group0->client().start_operation(&_abort_source);
        auto curr_req = _topology_state_machine._topology.global_request;
        if (curr_req && *curr_req != global_topology_request::new_cdc_generation) {
            // FIXME: replace this with a queue
            throw std::runtime_error{
                "check_and_repair_cdc_streams: a different topology request is already pending, try again later"};
        }

        curr_gen = _topology_state_machine._topology.current_cdc_generation_id;

        // FIXME: check if the current generation is optimal, don't request new one if it isn't

        topology_mutation_builder builder(guard.write_timestamp());
        builder.set_global_topology_request(global_topology_request::new_cdc_generation);
        topology_change change{{builder.build()}};
        group0_command g0_cmd = _group0->client().prepare_command(std::move(change), guard,
                ::format("request check+repair CDC generation from {}", _group0->group0_server().id()));
        try {
            co_await _group0->client().add_entry(std::move(g0_cmd), std::move(guard), &_abort_source);
        } catch (group0_concurrent_modification&) {
            slogger.info("raft topology: request check+repair CDC: concurrent operation is detected, retrying.");
            continue;
        }
        break;
    }

    // Wait until the current CDC generation changes.
    // This might happen due to a different reason than our request but we don't care.
    co_await _topology_state_machine.event.when([this, &curr_gen] {
        return curr_gen != _topology_state_machine._topology.current_cdc_generation_id;
    });
}

future<> storage_service::rebuild(sstring source_dc) {
    return run_with_api_lock(sstring("rebuild"), [source_dc] (storage_service& ss) -> future<> {
        if (ss._raft_topology_change_enabled) {
            co_await ss.raft_rebuild(source_dc);
        } else {
            slogger.info("rebuild from dc: {}", source_dc == "" ? "(any dc)" : source_dc);
            auto tmptr = ss.get_token_metadata_ptr();
            if (ss.is_repair_based_node_ops_enabled(streaming::stream_reason::rebuild)) {
                co_await ss._repair.local().rebuild_with_repair(tmptr, std::move(source_dc));
            } else {
                auto streamer = make_lw_shared<dht::range_streamer>(ss._db, ss._stream_manager, tmptr, ss._abort_source,
                        ss.get_broadcast_address(), ss._sys_ks.local().local_dc_rack(), "Rebuild", streaming::stream_reason::rebuild);
                streamer->add_source_filter(std::make_unique<dht::range_streamer::failure_detector_source_filter>(ss._gossiper.get_unreachable_members()));
                if (source_dc != "") {
                    streamer->add_source_filter(std::make_unique<dht::range_streamer::single_datacenter_filter>(source_dc));
                }
                auto ks_erms = ss._db.local().get_non_local_strategy_keyspaces_erms();
                for (const auto& [keyspace_name, erm] : ks_erms) {
                    co_await streamer->add_ranges(keyspace_name, erm, ss.get_ranges_for_endpoint(erm, utils::fb_utilities::get_broadcast_address()), ss._gossiper, false);
                }
                try {
                    co_await streamer->stream_async();
                    slogger.info("Streaming for rebuild successful");
                } catch (...) {
                    auto ep = std::current_exception();
                    // This is used exclusively through JMX, so log the full trace but only throw a simple RTE
                    slogger.warn("Error while rebuilding node: {}", ep);
                    std::rethrow_exception(std::move(ep));
                }
            }
        }
    });
}

int32_t storage_service::get_exception_count() {
    // FIXME
    // We return 0 for no exceptions, it should probably be
    // replaced by some general exception handling that would count
    // the unhandled exceptions.
    //return (int)StorageMetrics.exceptions.count();
    return 0;
}

future<std::unordered_multimap<dht::token_range, inet_address>>
storage_service::get_changed_ranges_for_leaving(locator::vnode_effective_replication_map_ptr erm, inet_address endpoint) {
    // First get all ranges the leaving endpoint is responsible for
    auto ranges = get_ranges_for_endpoint(erm, endpoint);

    slogger.debug("Node {} ranges [{}]", endpoint, ranges);

    std::unordered_map<dht::token_range, inet_address_vector_replica_set> current_replica_endpoints;

    // Find (for each range) all nodes that store replicas for these ranges as well
    for (auto& r : ranges) {
        auto end_token = r.end() ? r.end()->value() : dht::maximum_token();
        auto eps = erm->get_natural_endpoints(end_token);
        current_replica_endpoints.emplace(r, std::move(eps));
        co_await coroutine::maybe_yield();
    }

    auto temp = co_await get_token_metadata_ptr()->clone_after_all_left();

    // endpoint might or might not be 'leaving'. If it was not leaving (that is, removenode
    // command was used), it is still present in temp and must be removed.
    if (temp.is_normal_token_owner(endpoint)) {
        temp.remove_endpoint(endpoint);
    }

    std::unordered_multimap<dht::token_range, inet_address> changed_ranges;

    // Go through the ranges and for each range check who will be
    // storing replicas for these ranges when the leaving endpoint
    // is gone. Whoever is present in newReplicaEndpoints list, but
    // not in the currentReplicaEndpoints list, will be needing the
    // range.
    const auto& rs = erm->get_replication_strategy();
    for (auto& r : ranges) {
        auto end_token = r.end() ? r.end()->value() : dht::maximum_token();
        auto new_replica_endpoints = co_await rs.calculate_natural_endpoints(end_token, temp);

        auto rg = current_replica_endpoints.equal_range(r);
        for (auto it = rg.first; it != rg.second; it++) {
            const dht::token_range& range_ = it->first;
            inet_address_vector_replica_set& current_eps = it->second;
            slogger.debug("range={}, current_replica_endpoints={}, new_replica_endpoints={}", range_, current_eps, new_replica_endpoints);
            for (auto ep : it->second) {
                auto beg = new_replica_endpoints.begin();
                auto end = new_replica_endpoints.end();
                new_replica_endpoints.erase(std::remove(beg, end, ep), end);
            }
        }

        if (slogger.is_enabled(logging::log_level::debug)) {
            if (new_replica_endpoints.empty()) {
                slogger.debug("Range {} already in all replicas", r);
            } else {
                slogger.debug("Range {} will be responsibility of {}", r, new_replica_endpoints);
            }
        }
        for (auto& ep : new_replica_endpoints) {
            changed_ranges.emplace(r, ep);
        }
        // Replication strategy doesn't necessarily yield in calculate_natural_endpoints.
        // E.g. everywhere_replication_strategy
        co_await coroutine::maybe_yield();
    }
    co_await temp.clear_gently();

    co_return changed_ranges;
}

future<> storage_service::unbootstrap() {
    slogger.info("Started batchlog replay for decommission");
    co_await get_batchlog_manager().local().do_batch_log_replay();
    slogger.info("Finished batchlog replay for decommission");

    if (is_repair_based_node_ops_enabled(streaming::stream_reason::decommission)) {
        co_await _repair.local().decommission_with_repair(get_token_metadata_ptr());
    } else {
        std::unordered_map<sstring, std::unordered_multimap<dht::token_range, inet_address>> ranges_to_stream;

        auto ks_erms = _db.local().get_non_local_strategy_keyspaces_erms();
        for (const auto& [keyspace_name, erm] : ks_erms) {
            auto ranges_mm = co_await get_changed_ranges_for_leaving(erm, get_broadcast_address());
            if (slogger.is_enabled(logging::log_level::debug)) {
                std::vector<range<token>> ranges;
                for (auto& x : ranges_mm) {
                    ranges.push_back(x.first);
                }
                slogger.debug("Ranges needing transfer for keyspace={} are [{}]", keyspace_name, ranges);
            }
            ranges_to_stream.emplace(keyspace_name, std::move(ranges_mm));
        }

        set_mode(mode::LEAVING);

        auto stream_success = stream_ranges(std::move(ranges_to_stream));

        // wait for the transfer runnables to signal the latch.
        slogger.debug("waiting for stream acks.");
        try {
            co_await std::move(stream_success);
        } catch (...) {
            slogger.warn("unbootstrap fails to stream : {}", std::current_exception());
            throw;
        }
        slogger.debug("stream acks all received.");
    }
}

future<> storage_service::removenode_add_ranges(lw_shared_ptr<dht::range_streamer> streamer, gms::inet_address leaving_node) {
    auto my_address = get_broadcast_address();
    auto ks_erms = _db.local().get_non_local_strategy_keyspaces_erms();
    for (const auto& [keyspace_name, erm] : ks_erms) {
        std::unordered_multimap<dht::token_range, inet_address> changed_ranges = co_await get_changed_ranges_for_leaving(erm, leaving_node);
        dht::token_range_vector my_new_ranges;
        for (auto& x : changed_ranges) {
            if (x.second == my_address) {
                my_new_ranges.emplace_back(x.first);
            }
        }
        std::unordered_multimap<inet_address, dht::token_range> source_ranges = co_await get_new_source_ranges(erm, my_new_ranges);
        std::unordered_map<inet_address, dht::token_range_vector> ranges_per_endpoint;
        for (auto& x : source_ranges) {
            ranges_per_endpoint[x.first].emplace_back(x.second);
        }
        streamer->add_rx_ranges(keyspace_name, std::move(ranges_per_endpoint));
    }
}

future<> storage_service::removenode_with_stream(gms::inet_address leaving_node, shared_ptr<abort_source> as_ptr) {
    return seastar::async([this, leaving_node, as_ptr] {
        auto tmptr = get_token_metadata_ptr();
        abort_source as;
        auto sub = _abort_source.subscribe([&as] () noexcept {
            if (!as.abort_requested()) {
                as.request_abort();
            }
        });
        if (!as_ptr) {
            throw std::runtime_error("removenode_with_stream: abort_source is nullptr");
        }
        auto as_ptr_sub = as_ptr->subscribe([&as] () noexcept {
            if (!as.abort_requested()) {
                as.request_abort();
            }
        });
        auto streamer = make_lw_shared<dht::range_streamer>(_db, _stream_manager, tmptr, as, get_broadcast_address(), _sys_ks.local().local_dc_rack(), "Removenode", streaming::stream_reason::removenode);
        removenode_add_ranges(streamer, leaving_node).get();
        try {
            streamer->stream_async().get();
        } catch (...) {
            slogger.warn("removenode_with_stream: stream failed: {}", std::current_exception());
            throw;
        }
    });
}

future<> storage_service::excise(std::unordered_set<token> tokens, inet_address endpoint) {
    slogger.info("Removing tokens {} for {}", tokens, endpoint);
    // FIXME: HintedHandOffManager.instance.deleteHintsForEndpoint(endpoint);
    co_await remove_endpoint(endpoint);
    auto tmlock = std::make_optional(co_await get_token_metadata_lock());
    auto tmptr = co_await get_mutable_token_metadata_ptr();
    tmptr->remove_endpoint(endpoint);
    tmptr->remove_bootstrap_tokens(tokens);

    co_await update_topology_change_info(tmptr, ::format("excise {}", endpoint));
    co_await replicate_to_all_cores(std::move(tmptr));
    tmlock.reset();

    co_await notify_left(endpoint);
}

future<> storage_service::excise(std::unordered_set<token> tokens, inet_address endpoint, int64_t expire_time) {
    add_expire_time_if_found(endpoint, expire_time);
    return excise(tokens, endpoint);
}

future<> storage_service::leave_ring() {
    co_await _sys_ks.local().set_bootstrap_state(db::system_keyspace::bootstrap_state::NEEDS_BOOTSTRAP);
    co_await mutate_token_metadata([this] (mutable_token_metadata_ptr tmptr) {
        auto endpoint = get_broadcast_address();
        tmptr->remove_endpoint(endpoint);
        return update_topology_change_info(std::move(tmptr), ::format("leave_ring {}", endpoint));
    });

    auto expire_time = _gossiper.compute_expire_time().time_since_epoch().count();
    co_await _gossiper.add_local_application_state(gms::application_state::STATUS,
            versioned_value::left(co_await _sys_ks.local().get_local_tokens(), expire_time));
    auto delay = std::max(get_ring_delay(), gms::gossiper::INTERVAL);
    slogger.info("Announcing that I have left the ring for {}ms", delay.count());
    co_await sleep_abortable(delay, _abort_source);
}

future<>
storage_service::stream_ranges(std::unordered_map<sstring, std::unordered_multimap<dht::token_range, inet_address>> ranges_to_stream_by_keyspace) {
    auto streamer = dht::range_streamer(_db, _stream_manager, get_token_metadata_ptr(), _abort_source, get_broadcast_address(), _sys_ks.local().local_dc_rack(), "Unbootstrap", streaming::stream_reason::decommission);
    for (auto& entry : ranges_to_stream_by_keyspace) {
        const auto& keyspace = entry.first;
        auto& ranges_with_endpoints = entry.second;

        if (ranges_with_endpoints.empty()) {
            continue;
        }

        std::unordered_map<inet_address, dht::token_range_vector> ranges_per_endpoint;
        for (auto& end_point_entry : ranges_with_endpoints) {
            dht::token_range r = end_point_entry.first;
            inet_address endpoint = end_point_entry.second;
            ranges_per_endpoint[endpoint].emplace_back(r);
            co_await coroutine::maybe_yield();
        }
        streamer.add_tx_ranges(keyspace, std::move(ranges_per_endpoint));
    }
    try {
        co_await streamer.stream_async();
        slogger.info("stream_ranges successful");
    } catch (...) {
        auto ep = std::current_exception();
        slogger.warn("stream_ranges failed: {}", ep);
        std::rethrow_exception(std::move(ep));
    }
}

void storage_service::add_expire_time_if_found(inet_address endpoint, int64_t expire_time) {
    if (expire_time != 0L) {
        using clk = gms::gossiper::clk;
        auto time = clk::time_point(clk::duration(expire_time));
        _gossiper.add_expire_time_for_endpoint(endpoint, time);
    }
}

future<> storage_service::shutdown_protocol_servers() {
    for (auto& server : _protocol_servers) {
        slogger.info("Shutting down {} server", server->name());
        try {
            co_await server->stop_server();
        } catch (...) {
            slogger.error("Unexpected error shutting down {} server: {}",
                    server->name(), std::current_exception());
            throw;
        }
        slogger.info("Shutting down {} server was successful", server->name());
    }
}

future<std::unordered_multimap<inet_address, dht::token_range>>
storage_service::get_new_source_ranges(locator::vnode_effective_replication_map_ptr erm, const dht::token_range_vector& ranges) const {
    auto my_address = get_broadcast_address();
    std::unordered_map<dht::token_range, inet_address_vector_replica_set> range_addresses = co_await erm->get_range_addresses();
    std::unordered_multimap<inet_address, dht::token_range> source_ranges;

    // find alive sources for our new ranges
    auto tmptr = erm->get_token_metadata_ptr();
    for (auto r : ranges) {
        inet_address_vector_replica_set sources;
        auto it = range_addresses.find(r);
        if (it != range_addresses.end()) {
            sources = it->second;
        }

        tmptr->get_topology().sort_by_proximity(my_address, sources);

        if (std::find(sources.begin(), sources.end(), my_address) != sources.end()) {
            auto err = ::format("get_new_source_ranges: sources={}, my_address={}", sources, my_address);
            slogger.warn("{}", err);
            throw std::runtime_error(err);
        }


        for (auto& source : sources) {
            if (_gossiper.is_alive(source)) {
                source_ranges.emplace(source, r);
                break;
            }
        }

        co_await coroutine::maybe_yield();
    }
    co_return source_ranges;
}

future<> storage_service::move(token new_token) {
    return run_with_api_lock(sstring("move"), [] (storage_service& ss) mutable {
        return make_exception_future<>(std::runtime_error("Move opeartion is not supported only more"));
    });
}

future<std::vector<storage_service::token_range_endpoints>>
storage_service::describe_ring(const sstring& keyspace, bool include_only_local_dc) const {
    return locator::describe_ring(_db.local(), _gossiper, keyspace, include_only_local_dc);
}

future<std::unordered_map<dht::token_range, inet_address_vector_replica_set>>
storage_service::construct_range_to_endpoint_map(
        locator::vnode_effective_replication_map_ptr erm,
        const dht::token_range_vector& ranges) const {
    std::unordered_map<dht::token_range, inet_address_vector_replica_set> res;
    res.reserve(ranges.size());
    for (auto r : ranges) {
        res[r] = erm->get_natural_endpoints(
                r.end() ? r.end()->value() : dht::maximum_token());
        co_await coroutine::maybe_yield();
    }
    co_return res;
}


std::map<token, inet_address> storage_service::get_token_to_endpoint_map() {
    return get_token_metadata().get_normal_and_bootstrapping_token_to_endpoint_map();
}

std::chrono::milliseconds storage_service::get_ring_delay() {
    auto ring_delay = _db.local().get_config().ring_delay_ms();
    slogger.trace("Get RING_DELAY: {}ms", ring_delay);
    return std::chrono::milliseconds(ring_delay);
}

future<locator::token_metadata_lock> storage_service::get_token_metadata_lock() noexcept {
    assert(this_shard_id() == 0);
    return _shared_token_metadata.get_lock();
}

// Acquire the token_metadata lock and get a mutable_token_metadata_ptr.
// Pass that ptr to \c func, and when successfully done,
// replicate it to all cores.
//
// By default the merge_lock (that is unified with the token_metadata_lock)
// is acquired for mutating the token_metadata.  Pass acquire_merge_lock::no
// when called from paths that already acquire the merge_lock, like
// db::schema_tables::do_merge_schema.
//
// Note: must be called on shard 0.
future<> storage_service::mutate_token_metadata(std::function<future<> (mutable_token_metadata_ptr)> func, acquire_merge_lock acquire_merge_lock) noexcept {
    assert(this_shard_id() == 0);
    std::optional<token_metadata_lock> tmlock;

    if (acquire_merge_lock) {
        tmlock.emplace(co_await get_token_metadata_lock());
    }
    auto tmptr = co_await get_mutable_token_metadata_ptr();
    co_await func(tmptr);
    co_await replicate_to_all_cores(std::move(tmptr));
}

future<> storage_service::update_topology_change_info(mutable_token_metadata_ptr tmptr, sstring reason) {
    assert(this_shard_id() == 0);

    try {
        locator::dc_rack_fn get_dc_rack_from_gossiper([this] (inet_address ep) { return get_dc_rack_for(ep); });
        co_await tmptr->update_topology_change_info(get_dc_rack_from_gossiper);
    } catch (...) {
        auto ep = std::current_exception();
        slogger.error("Failed to update topology change info for {}: {}", reason, ep);
        std::rethrow_exception(std::move(ep));
    }
}

future<> storage_service::update_topology_change_info(sstring reason, acquire_merge_lock acquire_merge_lock) {
    return mutate_token_metadata([this, reason = std::move(reason)] (mutable_token_metadata_ptr tmptr) mutable {
        return update_topology_change_info(std::move(tmptr), std::move(reason));
    }, acquire_merge_lock);
}

future<> storage_service::keyspace_changed(const sstring& ks_name) {
    // The keyspace_changed notification is called on all shards
    // after any keyspace schema change, but we need to mutate_token_metadata
    // once after all shards are done with database::update_keyspace.
    // mutate_token_metadata (via update_topology_change_info) will update the
    // token metadata and effective_replication_map on all shards.
    if (this_shard_id() != 0) {
        return make_ready_future<>();
    }
    // Update pending ranges since keyspace can be changed after we calculate pending ranges.
    sstring reason = ::format("keyspace {}", ks_name);
    return update_topology_change_info(reason, acquire_merge_lock::no);
}

void storage_service::on_update_tablet_metadata() {
    if (this_shard_id() != 0) {
        // replicate_to_all_cores() takes care of other shards.
        return;
    }
    // FIXME: Avoid reading whole tablet metadata on partial changes.
    load_tablet_metadata().get();
}

future<> storage_service::load_tablet_metadata() {
    if (!_db.local().get_config().check_experimental(db::experimental_features_t::feature::TABLETS)) {
        return make_ready_future<>();
    }
    return mutate_token_metadata([this] (mutable_token_metadata_ptr tmptr) -> future<> {
        tmptr->set_tablets(co_await replica::read_tablet_metadata(*_qp));
    }, acquire_merge_lock::no);
}

future<> storage_service::snitch_reconfigured() {
    assert(this_shard_id() == 0);
    auto& snitch = _snitch.local();
    co_await mutate_token_metadata([&snitch] (mutable_token_metadata_ptr tmptr) -> future<> {
        // re-read local rack and DC info
        auto endpoint = utils::fb_utilities::get_broadcast_address();
        auto dr = locator::endpoint_dc_rack {
            .dc = snitch->get_datacenter(),
            .rack = snitch->get_rack(),
        };
        tmptr->update_topology(endpoint, std::move(dr));
        return make_ready_future<>();
    });

    if (_gossiper.is_enabled()) {
        co_await _gossiper.add_local_application_state(snitch->get_app_states());
    }
}

future<raft_topology_cmd_result> storage_service::raft_topology_cmd_handler(sharded<db::system_distributed_keyspace>& sys_dist_ks, raft::term_t term, uint64_t cmd_index, const raft_topology_cmd& cmd) {
    raft_topology_cmd_result result;
    slogger.trace("raft topology: topology cmd rpc {} is called", cmd.cmd);

    // The retrier does:
    // If no operation was previously started - start it now
    // If previous operation still running - wait for it an return its result
    // If previous operation completed sucessfully - return immediately
    // If previous opertaion failed - restart it
    auto retrier = [] (std::optional<shared_future<>>& f, auto&& func) -> future<> {
        if (!f || f->failed()) {
            if (f) {
                slogger.info("raft topology: retry streaming after previous attempt failed with {}", f->get_future().get_exception());
            } else {
                slogger.info("raft topology: start streaming");
            }
            f = func();
        } else {
            slogger.debug("raft topology: already streaming");
        }
        co_await f.value().get_future();
        slogger.info("raft topology: streaming completed");
    };

    try {
        auto& raft_server = _group0->group0_server();
        // do barrier to make sure we always see the latest topology
        co_await raft_server.read_barrier(&_abort_source);
        if (raft_server.get_current_term() != term) {
           // Return an error since the command is from outdated leader
           co_return result;
        }

        {
            auto& state = _raft_topology_cmd_handler_state;
            if (state.term != term) {
                state.term = term;
            } else if (cmd_index <= state.last_index) {
                // Return an error since the command is outdated
                co_return result;
            }
            state.last_index = cmd_index;
        }

        // We capture the topology version right after the checks
        // above, before any yields. This is crucial since _topology_state_machine._topology
        // might be altered concurrently while this method is running,
        // which can cause the fence command to apply an invalid fence version.
        const auto version = _topology_state_machine._topology.version;

        switch (cmd.cmd) {
            case raft_topology_cmd::command::barrier:
                // we already did read barrier above
                result.status = raft_topology_cmd_result::command_status::success;
            break;
            case raft_topology_cmd::command::barrier_and_drain: {
                co_await container().invoke_on_all([version] (storage_service& ss) -> future<> {
                    const auto current_version = ss._shared_token_metadata.get()->get_version();
                    slogger.debug("Got raft_topology_cmd::barrier_and_drain, version {}, current version {}",
                        version, current_version);

                    // This shouldn't happen under normal operation, it's only plausible
                    // if the topology change coordinator has
                    // moved to another node and managed to update the topology
                    // parallel to this method. The previous coordinator
                    // should be inactive now, so it won't observe this
                    // exception. By returning exception we aim
                    // to reveal any other conditions where this may arise.
                    if (current_version != version) {
                        co_await coroutine::return_exception(std::runtime_error(
                            ::format("raft topology: command::barrier_and_drain, the version has changed, "
                                     "version {}, current_version {}, the topology change coordinator "
                                     " had probably migrated to another node",
                                version, current_version)));
                    }

                    co_await ss._shared_token_metadata.stale_versions_in_use();
                    slogger.debug("raft_topology_cmd::barrier_and_drain done");
                });
                result.status = raft_topology_cmd_result::command_status::success;
            }
            break;
            case raft_topology_cmd::command::stream_ranges: {
                const auto& rs = _topology_state_machine._topology.find(raft_server.id())->second;
                auto tstate = _topology_state_machine._topology.tstate;
                if (!rs.ring ||
                    (tstate != topology::transition_state::write_both_read_old && rs.state != node_state::normal && rs.state != node_state::rebuilding)) {
                    slogger.warn("raft topology: got stream_ranges request while my tokens state is {} and node state is {}", tstate, rs.state);
                    break;
                }
                switch(rs.state) {
                case node_state::bootstrapping:
                case node_state::replacing: {
                    set_mode(mode::BOOTSTRAP);
                    // See issue #4001
                    co_await mark_existing_views_as_built(sys_dist_ks);
                    co_await _db.invoke_on_all([] (replica::database& db) {
                        for (auto& cf : db.get_non_system_column_families()) {
                            cf->notify_bootstrap_or_replace_start();
                        }
                    });
                    if (rs.state == node_state::bootstrapping) {
                        if (!_topology_state_machine._topology.normal_nodes.empty()) { // stream only if there is a node in normal state
                            co_await retrier(_bootstrap_result, coroutine::lambda([&] () -> future<> {
                                if (is_repair_based_node_ops_enabled(streaming::stream_reason::bootstrap)) {
                                    co_await _repair.local().bootstrap_with_repair(get_token_metadata_ptr(), rs.ring.value().tokens);
                                } else {
                                    dht::boot_strapper bs(_db, _stream_manager, _abort_source, get_broadcast_address(),
                                        locator::endpoint_dc_rack{rs.datacenter, rs.rack}, rs.ring.value().tokens, get_token_metadata_ptr());
                                    co_await bs.bootstrap(streaming::stream_reason::bootstrap, _gossiper);
                                }
                            }));
                        }
                        // Bootstrap did not complete yet, but streaming did
                    } else {
                        co_await retrier(_bootstrap_result, coroutine::lambda([&] () ->future<> {
                            if (is_repair_based_node_ops_enabled(streaming::stream_reason::replace)) {
                                co_await _repair.local().replace_with_repair(get_token_metadata_ptr(), rs.ring.value().tokens, {});
                            } else {
                                dht::boot_strapper bs(_db, _stream_manager, _abort_source, get_broadcast_address(),
                                                      locator::endpoint_dc_rack{rs.datacenter, rs.rack}, rs.ring.value().tokens, get_token_metadata_ptr());
                                assert(_topology_state_machine._topology.req_param.contains(raft_server.id()));
                                auto replaced_id = std::get<raft::server_id>(_topology_state_machine._topology.req_param[raft_server.id()]);
                                auto existing_ip = _group0->address_map().find(replaced_id);
                                assert(existing_ip);
                                co_await bs.bootstrap(streaming::stream_reason::replace, _gossiper, *existing_ip);
                            }
                        }));
                    }
                    co_await _db.invoke_on_all([] (replica::database& db) {
                        for (auto& cf : db.get_non_system_column_families()) {
                            cf->notify_bootstrap_or_replace_end();
                        }
                    });
                    result.status = raft_topology_cmd_result::command_status::success;
                }
                break;
                case node_state::decommissioning:
                    co_await retrier(_decomission_result, coroutine::lambda([&] () { return unbootstrap(); }));
                    result.status = raft_topology_cmd_result::command_status::success;
                break;
                case node_state::normal: {
                    // If asked to stream a node in normal state it means that remove operation is running
                    // Find the node that is been removed
                    auto it = boost::find_if(_topology_state_machine._topology.transition_nodes, [] (auto& e) { return e.second.state == node_state::removing; });
                    if (it == _topology_state_machine._topology.transition_nodes.end()) {
                        slogger.warn("raft topology: got stream_ranges request while my state is normal but cannot find a node that is been removed");
                        break;
                    }
                    slogger.debug("raft topology: streaming to remove node {}", it->first);
                    const auto& am = _group0->address_map();
                    auto ip = am.find(it->first); // map node id to ip
                    assert (ip); // what to do if address is unknown?
                    co_await retrier(_remove_result[it->first], coroutine::lambda([&] () {
                        auto as = make_shared<abort_source>();
                        auto sub = _abort_source.subscribe([as] () noexcept {
                            if (!as->abort_requested()) {
                                as->request_abort();
                            }
                        });
                        if (is_repair_based_node_ops_enabled(streaming::stream_reason::removenode)) {
                            auto ops = seastar::make_shared<node_ops_info>(node_ops_id::create_random_id(), as, std::list<gms::inet_address>());
                            // FIXME: ignore node list support
                            return _repair.local().removenode_with_repair(get_token_metadata_ptr(), *ip, ops);
                        } else {
                            return removenode_with_stream(*ip, as);
                        }
                    }));
                    result.status = raft_topology_cmd_result::command_status::success;
                }
                break;
                case node_state::rebuilding: {
                    auto source_dc = std::get<sstring>(_topology_state_machine._topology.req_param[raft_server.id()]);
                    slogger.info("raft topology: rebuild from dc: {}", source_dc == "" ? "(any dc)" : source_dc);
                    co_await retrier(_rebuild_result, [&] () -> future<> {
                        auto tmptr = get_token_metadata_ptr();
                        if (is_repair_based_node_ops_enabled(streaming::stream_reason::rebuild)) {
                            co_await _repair.local().rebuild_with_repair(tmptr, std::move(source_dc));
                        } else {
                            auto streamer = make_lw_shared<dht::range_streamer>(_db, _stream_manager, tmptr, _abort_source,
                                    get_broadcast_address(), _sys_ks.local().local_dc_rack(), "Rebuild", streaming::stream_reason::rebuild);
                            streamer->add_source_filter(std::make_unique<dht::range_streamer::failure_detector_source_filter>(_gossiper.get_unreachable_members()));
                            if (source_dc != "") {
                                streamer->add_source_filter(std::make_unique<dht::range_streamer::single_datacenter_filter>(source_dc));
                            }
                            auto ks_erms = _db.local().get_non_local_strategy_keyspaces_erms();
                            for (const auto& [keyspace_name, erm] : ks_erms) {
                                co_await streamer->add_ranges(keyspace_name, erm, get_ranges_for_endpoint(erm, utils::fb_utilities::get_broadcast_address()), _gossiper, false);
                            }
                            try {
                                co_await streamer->stream_async();
                                slogger.info("raft topology: streaming for rebuild successful");
                            } catch (...) {
                                auto ep = std::current_exception();
                                // This is used exclusively through JMX, so log the full trace but only throw a simple RTE
                                slogger.warn("raft topology: error while rebuilding node: {}", ep);
                                std::rethrow_exception(std::move(ep));
                            }
                        }
                    });
                    _rebuild_result.reset();
                    result.status = raft_topology_cmd_result::command_status::success;
                }
                break;
                case node_state::left_token_ring:
                case node_state::left:
                case node_state::none:
                case node_state::removing:
                    on_fatal_internal_error(slogger, ::format("Node {} got streaming request in state {}. It should be either dead or not part of the cluster",
                                     raft_server.id(), rs.state));
                break;
                }
            }
            break;
            case raft_topology_cmd::command::fence: {
                // We can have several concurrent fence commands in case topology change
                // coordinator migrated to another node. The update_fence_version function
                // checks that the version doesn't decrease, we do the check and persist
                // the new version under the same lock to avoid raises.
                auto holder = co_await get_units(_raft_topology_cmd_handler_state._operation_mutex, 1);
                co_await update_fence_version(version);
                co_await _sys_ks.local().update_topology_fence_version(version);
                result.status = raft_topology_cmd_result::command_status::success;
                break;
            }
            case raft_topology_cmd::command::shutdown:
                if (_shutdown_request_promise) {
                    std::exchange(_shutdown_request_promise, std::nullopt)->set_value();
                } else {
                    slogger.warn("raft topology: got shutdown request while not decommissioning");
                }
                break;
        }
    } catch (...) {
        slogger.error("raft topology: raft_topology_cmd failed with: {}", std::current_exception());
    }
    co_return result;
}

future<> storage_service::update_fence_version(token_metadata::version_t new_version) {
    return container().invoke_on_all([new_version] (storage_service& ss) {
        slogger.debug("update_fence_version, version {}", new_version);
        ss._shared_token_metadata.update_fence_version(new_version);
    });
}

void storage_service::init_messaging_service(sharded<service::storage_proxy>& proxy, sharded<db::system_distributed_keyspace>& sys_dist_ks) {
    _messaging.local().register_node_ops_cmd([this] (const rpc::client_info& cinfo, node_ops_cmd_request req) {
        auto coordinator = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        return container().invoke_on(0, [coordinator, req = std::move(req)] (auto& ss) mutable {
            return ss.node_ops_cmd_handler(coordinator, std::move(req));
        });
    });
    ser::storage_service_rpc_verbs::register_raft_topology_cmd(&_messaging.local(), [this, &sys_dist_ks] (raft::term_t term, uint64_t cmd_index, raft_topology_cmd cmd) {
        return container().invoke_on(0, [&sys_dist_ks, cmd = std::move(cmd), term, cmd_index] (auto& ss) {
            return ss.raft_topology_cmd_handler(sys_dist_ks, term, cmd_index, cmd);
        });
    });
    ser::storage_service_rpc_verbs::register_raft_pull_topology_snapshot(&_messaging.local(), [this, &proxy] (raft_topology_pull_params params) {
        return container().invoke_on(0, [&proxy] (auto& ss) -> future<raft_topology_snapshot> {
            if (!ss._raft_topology_change_enabled) {
               co_return raft_topology_snapshot{};
            }

            auto& db = proxy.local().get_db();

            std::vector<canonical_mutation> topology_mutations;
            std::optional<cdc::generation_id_v2> curr_cdc_gen_id;
            {
                // FIXME: make it an rwlock, here we only need to lock for reads,
                // might be useful if multiple nodes are trying to pull concurrently.
                auto read_apply_mutex_holder = co_await ss._group0->client().hold_read_apply_mutex();
                auto rs = co_await db::system_keyspace::query_mutations(
                    db, db::system_keyspace::NAME, db::system_keyspace::TOPOLOGY);
                auto s = ss._db.local().find_schema(db::system_keyspace::NAME, db::system_keyspace::TOPOLOGY);
                topology_mutations.reserve(rs->partitions().size());
                boost::range::transform(
                        rs->partitions(), std::back_inserter(topology_mutations), [s] (const partition& p) {
                    return canonical_mutation{p.mut().unfreeze(s)};
                });

                curr_cdc_gen_id = ss._topology_state_machine._topology.current_cdc_generation_id;
            }

            std::optional<canonical_mutation> cdc_generation_mutation;
            if (curr_cdc_gen_id) {
                // We don't need to fetch this data under group0 apply mutex, it's immutable.
                // We only need to include the current CDC generation data in the snapshot,
                // because nodes only load whatever `current_cdc_generation_id` points to in topology.
                //
                // FIXME: when we bootstrap nodes in quick succession, the timestamp of the newest CDC generation
                // may be for some time larger than the clocks of our nodes. The last bootstrapped node
                // will only receive the newest CDC generation and not earlier ones, so it will only be able
                // to coordinate writes to CDC-enabled tables after its clock advances to reach the newest
                // generation's timestamp. In other words, it may not be able to coordinate writes for some
                // time after bootstrapping and drivers connecting to it will receive errors.
                // To fix that, we could store in topology a small history of recent CDC generation IDs
                // (garbage-collected with time) instead of just the last one, and load all of them.
                // Alternatively, a node would wait for some time before switching to normal state.
                auto s = ss._db.local().find_schema(db::system_keyspace::NAME, db::system_keyspace::CDC_GENERATIONS_V3);
                auto key = dht::decorate_key(*s, partition_key::from_singular(*s, curr_cdc_gen_id->id));
                auto partition_range = dht::partition_range::make_singular(key);
                auto rs = co_await db::system_keyspace::query_mutations(
                    db, db::system_keyspace::NAME, db::system_keyspace::CDC_GENERATIONS_V3, partition_range);
                if (rs->partitions().size() != 1) {
                    on_internal_error(slogger, ::format(
                        "pull_raft_topology_snapshot: expected a single partition in CDC generation query,"
                        ", got {} (generation ID: {})", rs->partitions().size(), *curr_cdc_gen_id));
                }
                cdc_generation_mutation.emplace(rs->partitions().begin()->mut().unfreeze(s));
            }

            co_return raft_topology_snapshot{
                .topology_mutations = std::move(topology_mutations),
                .cdc_generation_mutation = std::move(cdc_generation_mutation),
            };
        });
    });
}

future<> storage_service::uninit_messaging_service() {
    return when_all_succeed(
        _messaging.local().unregister_node_ops_cmd(),
        ser::storage_service_rpc_verbs::unregister(&_messaging.local())
    ).discard_result();
}

void storage_service::do_isolate_on_error(disk_error type)
{
    static std::atomic<bool> isolated = { false };

    if (!isolated.exchange(true)) {
        slogger.error("Shutting down communications due to I/O errors until operator intervention: {} error: {}", type == disk_error::commit ? "Commitlog" : "Disk", std::current_exception());
        // isolated protect us against multiple stops
        //FIXME: discarded future.
        (void)isolate();
    }
}

future<> storage_service::isolate() {
    return run_with_no_api_lock([] (storage_service& ss) {
        return ss.stop_transport();
    });
}

future<sstring> storage_service::get_removal_status() {
    return run_with_no_api_lock([] (storage_service& ss) {
        return make_ready_future<sstring>(sstring("No token removals in process."));
    });
}

future<> storage_service::force_remove_completion() {
    return run_with_no_api_lock([] (storage_service& ss) -> future<> {
        while (!ss._operation_in_progress.empty()) {
            if (ss._operation_in_progress != sstring("removenode")) {
                throw std::runtime_error(::format("Operation {} is in progress, try again", ss._operation_in_progress));
            }

            // This flag will make removenode stop waiting for the confirmation,
            // wait it to complete
            slogger.info("Operation removenode is in progress, wait for it to complete");
            co_await sleep_abortable(std::chrono::seconds(1), ss._abort_source);
        }
        ss._operation_in_progress = sstring("removenode_force");

        try {
            const auto& tm = ss.get_token_metadata();
            if (!tm.get_leaving_endpoints().empty()) {
                auto leaving = tm.get_leaving_endpoints();
                slogger.warn("Removal not confirmed, Leaving={}", leaving);
                for (auto endpoint : leaving) {
                    locator::host_id host_id;
                    auto tokens = tm.get_tokens(endpoint);
                    try {
                        host_id = tm.get_host_id(endpoint);
                    } catch (...) {
                        slogger.warn("No host_id is found for endpoint {}", endpoint);
                        continue;
                    }
                    co_await ss._gossiper.advertise_token_removed(endpoint, host_id);
                    std::unordered_set<token> tokens_set(tokens.begin(), tokens.end());
                    co_await ss.excise(tokens_set, endpoint);

                    slogger.info("force_remove_completion: removing endpoint {} from group 0", endpoint);
                    assert(ss._group0);
                    bool raft_available = co_await ss._group0->wait_for_raft();
                    if (raft_available) {
                        co_await ss._group0->remove_from_group0(raft::server_id{host_id.uuid()});
                    }
                }
            } else {
                slogger.warn("No tokens to force removal on, call 'removenode' first");
            }
            ss._operation_in_progress = {};
        } catch (...) {
            ss._operation_in_progress = {};
            throw;
        }
    });
}

/**
 * Takes an ordered list of adjacent tokens and divides them in the specified number of ranges.
 */
static std::vector<std::pair<dht::token_range, uint64_t>>
calculate_splits(std::vector<dht::token> tokens, uint64_t split_count, replica::column_family& cf) {
    auto sstables = cf.get_sstables();
    const double step = static_cast<double>(tokens.size() - 1) / split_count;
    auto prev_token_idx = 0;
    std::vector<std::pair<dht::token_range, uint64_t>> splits;
    splits.reserve(split_count);
    for (uint64_t i = 1; i <= split_count; ++i) {
        auto index = static_cast<uint32_t>(std::round(i * step));
        dht::token_range range({{ std::move(tokens[prev_token_idx]), false }}, {{ tokens[index], true }});
        // always return an estimate > 0 (see CASSANDRA-7322)
        uint64_t estimated_keys_for_range = 0;
        for (auto&& sst : *sstables) {
            estimated_keys_for_range += sst->estimated_keys_for_range(range);
        }
        splits.emplace_back(std::move(range), std::max(static_cast<uint64_t>(cf.schema()->min_index_interval()), estimated_keys_for_range));
        prev_token_idx = index;
    }
    return splits;
};

std::vector<std::pair<dht::token_range, uint64_t>>
storage_service::get_splits(const sstring& ks_name, const sstring& cf_name, range<dht::token> range, uint32_t keys_per_split) {
    using range_type = dht::token_range;
    auto& cf = _db.local().find_column_family(ks_name, cf_name);
    auto schema = cf.schema();
    auto sstables = cf.get_sstables();
    uint64_t total_row_count_estimate = 0;
    std::vector<dht::token> tokens;
    std::vector<range_type> unwrapped;
    if (range.is_wrap_around(dht::token_comparator())) {
        auto uwr = range.unwrap();
        unwrapped.emplace_back(std::move(uwr.second));
        unwrapped.emplace_back(std::move(uwr.first));
    } else {
        unwrapped.emplace_back(std::move(range));
    }
    tokens.push_back(std::move(unwrapped[0].start().value_or(range_type::bound(dht::minimum_token()))).value());
    for (auto&& r : unwrapped) {
        std::vector<dht::token> range_tokens;
        for (auto &&sst : *sstables) {
            total_row_count_estimate += sst->estimated_keys_for_range(r);
            auto keys = sst->get_key_samples(*cf.schema(), r);
            std::transform(keys.begin(), keys.end(), std::back_inserter(range_tokens), [](auto&& k) { return std::move(k.token()); });
        }
        std::sort(range_tokens.begin(), range_tokens.end());
        std::move(range_tokens.begin(), range_tokens.end(), std::back_inserter(tokens));
    }
    tokens.push_back(std::move(unwrapped[unwrapped.size() - 1].end().value_or(range_type::bound(dht::maximum_token()))).value());

    // split_count should be much smaller than number of key samples, to avoid huge sampling error
    constexpr uint32_t min_samples_per_split = 4;
    uint64_t max_split_count = tokens.size() / min_samples_per_split + 1;
    uint64_t split_count = std::max(uint64_t(1), std::min(max_split_count, total_row_count_estimate / keys_per_split));

    return calculate_splits(std::move(tokens), split_count, cf);
};

dht::token_range_vector
storage_service::get_ranges_for_endpoint(const locator::vnode_effective_replication_map_ptr& erm, const gms::inet_address& ep) const {
    return erm->get_ranges(ep);
}

// Caller is responsible to hold token_metadata valid until the returned future is resolved
future<dht::token_range_vector>
storage_service::get_all_ranges(const std::vector<token>& sorted_tokens) const {
    if (sorted_tokens.empty())
        co_return dht::token_range_vector();
    int size = sorted_tokens.size();
    dht::token_range_vector ranges;
    ranges.reserve(size);
    ranges.push_back(dht::token_range::make_ending_with(range_bound<token>(sorted_tokens[0], true)));
    co_await coroutine::maybe_yield();
    for (int i = 1; i < size; ++i) {
        dht::token_range r(range<token>::bound(sorted_tokens[i - 1], false), range<token>::bound(sorted_tokens[i], true));
        ranges.push_back(r);
        co_await coroutine::maybe_yield();
    }
    ranges.push_back(dht::token_range::make_starting_with(range_bound<token>(sorted_tokens[size-1], false)));

    co_return ranges;
}

inet_address_vector_replica_set
storage_service::get_natural_endpoints(const sstring& keyspace,
        const sstring& cf, const sstring& key) const {
    auto schema = _db.local().find_schema(keyspace, cf);
    partition_key pk = partition_key::from_nodetool_style_string(schema, key);
    dht::token token = schema->get_partitioner().get_token(*schema, pk.view());
    return get_natural_endpoints(keyspace, token);
}

inet_address_vector_replica_set
storage_service::get_natural_endpoints(const sstring& keyspace, const token& pos) const {
    return _db.local().find_keyspace(keyspace).get_effective_replication_map()->get_natural_endpoints(pos);
}

future<> endpoint_lifecycle_notifier::notify_down(gms::inet_address endpoint) {
    return seastar::async([this, endpoint] {
        _subscribers.thread_for_each([endpoint] (endpoint_lifecycle_subscriber* subscriber) {
            try {
                subscriber->on_down(endpoint);
            } catch (...) {
                slogger.warn("Down notification failed {}: {}", endpoint, std::current_exception());
            }
        });
    });
}

future<> storage_service::notify_down(inet_address endpoint) {
    co_await container().invoke_on_all([endpoint] (auto&& ss) {
        ss._messaging.local().remove_rpc_client(netw::msg_addr{endpoint, 0});
        return ss._lifecycle_notifier.notify_down(endpoint);
    });
    slogger.debug("Notify node {} has been down", endpoint);
}

future<> endpoint_lifecycle_notifier::notify_left(gms::inet_address endpoint) {
    return seastar::async([this, endpoint] {
        _subscribers.thread_for_each([endpoint] (endpoint_lifecycle_subscriber* subscriber) {
            try {
                subscriber->on_leave_cluster(endpoint);
            } catch (...) {
                slogger.warn("Leave cluster notification failed {}: {}", endpoint, std::current_exception());
            }
        });
    });
}

future<> storage_service::notify_left(inet_address endpoint) {
    co_await container().invoke_on_all([endpoint] (auto&& ss) {
        return ss._lifecycle_notifier.notify_left(endpoint);
    });
    slogger.debug("Notify node {} has left the cluster", endpoint);
}

future<> endpoint_lifecycle_notifier::notify_up(gms::inet_address endpoint) {
    return seastar::async([this, endpoint] {
        _subscribers.thread_for_each([endpoint] (endpoint_lifecycle_subscriber* subscriber) {
            try {
                subscriber->on_up(endpoint);
            } catch (...) {
                slogger.warn("Up notification failed {}: {}", endpoint, std::current_exception());
            }
        });
    });
}

future<> storage_service::notify_up(inet_address endpoint) {
    if (!_gossiper.is_cql_ready(endpoint) || !_gossiper.is_alive(endpoint)) {
        co_return;
    }
    co_await container().invoke_on_all([endpoint] (auto&& ss) {
        return ss._lifecycle_notifier.notify_up(endpoint);
    });
    slogger.debug("Notify node {} has been up", endpoint);
}

future<> endpoint_lifecycle_notifier::notify_joined(gms::inet_address endpoint) {
    return seastar::async([this, endpoint] {
        _subscribers.thread_for_each([endpoint] (endpoint_lifecycle_subscriber* subscriber) {
            try {
                subscriber->on_join_cluster(endpoint);
            } catch (...) {
                slogger.warn("Join cluster notification failed {}: {}", endpoint, std::current_exception());
            }
        });
    });
}

future<> storage_service::notify_joined(inet_address endpoint) {
    if (!_gossiper.is_normal(endpoint)) {
        co_return;
    }

    co_await utils::get_local_injector().inject(
        "storage_service_notify_joined_sleep", std::chrono::milliseconds{500});

    co_await container().invoke_on_all([endpoint] (auto&& ss) {
        ss._messaging.local().remove_rpc_client_with_ignored_topology(netw::msg_addr{endpoint, 0});
        return ss._lifecycle_notifier.notify_joined(endpoint);
    });
    slogger.debug("Notify node {} has joined the cluster", endpoint);
}

future<> storage_service::notify_cql_change(inet_address endpoint, bool ready) {
    if (ready) {
        co_await notify_up(endpoint);
    } else {
        co_await notify_down(endpoint);
    }
}

bool storage_service::is_normal_state_handled_on_boot(gms::inet_address node) {
    return _normal_state_handled_on_boot.contains(node);
}

// Wait for normal state handler to finish on boot
future<> storage_service::wait_for_normal_state_handled_on_boot(const std::unordered_set<gms::inet_address>& nodes) {
    slogger.info("Started waiting for normal state handler for nodes {}", nodes);
    auto start_time = std::chrono::steady_clock::now();
    for (auto& node: nodes) {
        while (!is_normal_state_handled_on_boot(node)) {
            slogger.debug("Waiting for normal state handler for node {}", node);
            co_await sleep_abortable(std::chrono::milliseconds(100), _abort_source);
            if (std::chrono::steady_clock::now() > start_time + std::chrono::seconds(60)) {
                throw std::runtime_error(::format("Node {} did not finish normal state handler, reject the node ops", node));
            }
        }
    }
    slogger.info("Finished waiting for normal state handler for nodes {}", nodes);
    co_return;
}

future<bool> storage_service::is_cleanup_allowed(sstring keyspace) {
    return container().invoke_on(0, [keyspace = std::move(keyspace)] (storage_service& ss) {
        auto my_address = ss.get_broadcast_address();
        auto pending_ranges = ss._db.local().find_keyspace(keyspace).get_effective_replication_map()->has_pending_ranges(my_address);
        bool is_bootstrap_mode = ss._operation_mode == mode::BOOTSTRAP;
        slogger.debug("is_cleanup_allowed: keyspace={}, is_bootstrap_mode={}, pending_ranges={}",
                keyspace, is_bootstrap_mode, pending_ranges);
        return !is_bootstrap_mode && !pending_ranges;
    });
}

bool storage_service::is_repair_based_node_ops_enabled(streaming::stream_reason reason) {
    static const std::unordered_map<sstring, streaming::stream_reason> reason_map{
        {"replace", streaming::stream_reason::replace},
        {"bootstrap", streaming::stream_reason::bootstrap},
        {"decommission", streaming::stream_reason::decommission},
        {"removenode", streaming::stream_reason::removenode},
        {"rebuild", streaming::stream_reason::rebuild},
    };
    std::vector<sstring> enabled_list;
    std::unordered_set<streaming::stream_reason> enabled_set;
    auto enabled_list_str = _db.local().get_config().allowed_repair_based_node_ops();
    boost::trim_all(enabled_list_str);
    std::replace(enabled_list_str.begin(), enabled_list_str.end(), '\"', ' ');
    std::replace(enabled_list_str.begin(), enabled_list_str.end(), '\'', ' ');
    boost::split(enabled_list, enabled_list_str, boost::is_any_of(","));
    for (sstring op : enabled_list) {
        try {
            if (!op.empty()) {
                auto it = reason_map.find(op);
                if (it != reason_map.end()) {
                    enabled_set.insert(it->second);
                } else {
                    throw std::invalid_argument(::format("unsupported operation name: {}", op));
                }
            }
        } catch (...) {
            throw std::invalid_argument(::format("Failed to parse allowed_repair_based_node_ops parameter [{}]: {}",
                    enabled_list_str, std::current_exception()));
        }
    }
    bool global_enabled = _db.local().get_config().enable_repair_based_node_ops();
    slogger.info("enable_repair_based_node_ops={}, allowed_repair_based_node_ops={{{}}}", global_enabled, fmt::join(enabled_set, " ,"));
    return global_enabled && enabled_set.contains(reason);
}

node_ops_meta_data::node_ops_meta_data(
        node_ops_id ops_uuid,
        gms::inet_address coordinator,
        std::list<gms::inet_address> ignore_nodes,
        std::chrono::seconds watchdog_interval,
        std::function<future<> ()> abort_func,
        std::function<void ()> signal_func)
    : _ops_uuid(std::move(ops_uuid))
    , _coordinator(std::move(coordinator))
    , _abort(std::move(abort_func))
    , _abort_source(seastar::make_shared<abort_source>())
    , _signal(std::move(signal_func))
    , _ops(seastar::make_shared<node_ops_info>(_ops_uuid, _abort_source, std::move(ignore_nodes)))
    , _watchdog([sig = _signal] { sig(); })
    , _watchdog_interval(watchdog_interval)
{
    slogger.debug("node_ops_meta_data: ops_uuid={} arm interval={}", _ops_uuid, _watchdog_interval.count());
    _watchdog.arm(_watchdog_interval);
}

future<> node_ops_meta_data::abort() {
    slogger.debug("node_ops_meta_data: ops_uuid={} abort", _ops_uuid);
    _watchdog.cancel();
    return _abort();
}

void node_ops_meta_data::update_watchdog() {
    slogger.debug("node_ops_meta_data: ops_uuid={} update_watchdog", _ops_uuid);
    if (_abort_source->abort_requested()) {
        return;
    }
    _watchdog.cancel();
    _watchdog.arm(_watchdog_interval);
}

void node_ops_meta_data::cancel_watchdog() {
    slogger.debug("node_ops_meta_data: ops_uuid={} cancel_watchdog", _ops_uuid);
    _watchdog.cancel();
}

shared_ptr<node_ops_info> node_ops_meta_data::get_ops_info() {
    return _ops;
}

shared_ptr<abort_source> node_ops_meta_data::get_abort_source() {
    return _abort_source;
}

future<> storage_service::node_ops_update_heartbeat(node_ops_id ops_uuid) {
    slogger.debug("node_ops_update_heartbeat: ops_uuid={}", ops_uuid);
    auto permit = co_await seastar::get_units(_node_ops_abort_sem, 1);
    auto it = _node_ops.find(ops_uuid);
    if (it != _node_ops.end()) {
        node_ops_meta_data& meta = it->second;
        meta.update_watchdog();
    }
}

future<> storage_service::node_ops_done(node_ops_id ops_uuid) {
    slogger.debug("node_ops_done: ops_uuid={}", ops_uuid);
    auto permit = co_await seastar::get_units(_node_ops_abort_sem, 1);
    auto it = _node_ops.find(ops_uuid);
    if (it != _node_ops.end()) {
        node_ops_meta_data& meta = it->second;
        meta.cancel_watchdog();
        _node_ops.erase(it);
    }
}

future<> storage_service::node_ops_abort(node_ops_id ops_uuid) {
    slogger.debug("node_ops_abort: ops_uuid={}", ops_uuid);
    auto permit = co_await seastar::get_units(_node_ops_abort_sem, 1);

    if (!ops_uuid) {
        for (auto& [uuid, meta] : _node_ops) {
            co_await meta.abort();
            auto as = meta.get_abort_source();
            if (as && !as->abort_requested()) {
                as->request_abort();
            }
        }
        _node_ops.clear();
        co_return;
    }

    auto it = _node_ops.find(ops_uuid);
    if (it != _node_ops.end()) {
        node_ops_meta_data& meta = it->second;
        slogger.info("aborting node operation ops_uuid={}", ops_uuid);
        co_await meta.abort();
        auto as = meta.get_abort_source();
        if (as && !as->abort_requested()) {
            as->request_abort();
        }
        _node_ops.erase(it);
    } else {
        slogger.info("aborting node operation ops_uuid={}: operation not found", ops_uuid);
    }
}

void storage_service::node_ops_signal_abort(std::optional<node_ops_id> ops_uuid) {
    if (ops_uuid) {
        slogger.warn("Node operation ops_uuid={} watchdog expired. Signaling the operation to abort", ops_uuid);
    }
    _node_ops_abort_queue.push_back(ops_uuid);
    _node_ops_abort_cond.signal();
}

future<> storage_service::node_ops_abort_thread() {
    slogger.info("Started node_ops_abort_thread");
    for (;;) {
        co_await _node_ops_abort_cond.wait([this] { return !_node_ops_abort_queue.empty(); });
        slogger.debug("Awoke node_ops_abort_thread: node_ops_abort_queue={}", _node_ops_abort_queue);
        while (!_node_ops_abort_queue.empty()) {
            auto uuid_opt = _node_ops_abort_queue.front();
            _node_ops_abort_queue.pop_front();
            try {
                co_await node_ops_abort(uuid_opt.value_or(node_ops_id::create_null_id()));
            } catch (...) {
                slogger.warn("Failed to abort node operation ops_uuid={}: {}", *uuid_opt, std::current_exception());
            }
            if (!uuid_opt) {
                slogger.info("Stopped node_ops_abort_thread");
                co_return;
            }
        }
    }
    __builtin_unreachable();
}

} // namespace service

