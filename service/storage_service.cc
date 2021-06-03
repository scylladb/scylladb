/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 *
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "storage_service.hh"
#include "dht/boot_strapper.hh"
#include <seastar/core/distributed.hh>
#include "locator/snitch_base.hh"
#include "db/system_keyspace.hh"
#include "db/system_distributed_keyspace.hh"
#include "utils/UUID.hh"
#include "gms/inet_address.hh"
#include "log.hh"
#include "service/migration_manager.hh"
#include "service/storage_proxy.hh"
#include "to_string.hh"
#include "gms/gossiper.hh"
#include "gms/failure_detector.hh"
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
#include <seastar/net/tls.hh>
#include "utils/exceptions.hh"
#include "message/messaging_service.hh"
#include "supervisor.hh"
#include "sstables/compaction_manager.hh"
#include "sstables/sstables.hh"
#include "db/config.hh"
#include "db/schema_tables.hh"
#include "distributed_loader.hh"
#include "database.hh"
#include <seastar/core/metrics.hh>
#include "cdc/generation.hh"
#include "cdc/generation_service.hh"
#include "repair/repair.hh"
#include "repair/row_level.hh"
#include "service/priority_manager.hh"
#include "utils/generation-number.hh"
#include <seastar/core/coroutine.hh>

#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>

using token = dht::token;
using UUID = utils::UUID;
using inet_address = gms::inet_address;

extern logging::logger cdc_log;

namespace service {

static logging::logger slogger("storage_service");

distributed<storage_service> _the_storage_service;

storage_service::storage_service(abort_source& abort_source, distributed<database>& db, gms::gossiper& gossiper, sharded<db::system_distributed_keyspace>& sys_dist_ks,
        sharded<db::view::view_update_generator>& view_update_generator, gms::feature_service& feature_service, storage_service_config config,
        sharded<service::migration_manager>& mm,
        locator::shared_token_metadata& stm, sharded<netw::messaging_service>& ms, sharded<cdc::generation_service>& cdc_gen_service,
        sharded<repair_service>& repair, bool for_testing)
        : _abort_source(abort_source)
        , _feature_service(feature_service)
        , _db(db)
        , _gossiper(gossiper)
        , _messaging(ms)
        , _migration_manager(mm)
        , _repair(repair)
        , _for_testing(for_testing)
        , _node_ops_abort_thread(node_ops_abort_thread())
        , _shared_token_metadata(stm)
        , _cdc_gen_service(cdc_gen_service)
        , _sys_dist_ks(sys_dist_ks)
        , _view_update_generator(view_update_generator)
        , _snitch_reconfigure([this] { return snitch_reconfigured(); })
        , _schema_version_publisher([this] { return publish_schema_version(); }) {
    register_metrics();
    sstable_read_error.connect([this] { do_isolate_on_error(disk_error::regular); });
    sstable_write_error.connect([this] { do_isolate_on_error(disk_error::regular); });
    general_disk_error.connect([this] { do_isolate_on_error(disk_error::regular); });
    commit_error.connect([this] { do_isolate_on_error(disk_error::commit); });

    auto& snitch = locator::i_endpoint_snitch::snitch_instance();
    if (snitch.local_is_initialized()) {
        _listeners.emplace_back(make_lw_shared(snitch.local()->when_reconfigured(_snitch_reconfigure)));
    }
}

void storage_service::enable_all_features() {
    auto features = _feature_service.known_feature_set();
    _feature_service.enable(features);
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
    case storage_service::mode::STARTING: return node_external_status::STARTING;
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

bool storage_service::is_auto_bootstrap() const {
    return _db.local().get_config().auto_bootstrap();
}

std::unordered_set<token> storage_service::get_replace_tokens() {
    std::unordered_set<token> ret;
    std::unordered_set<sstring> tokens;
    auto tokens_string = _db.local().get_config().replace_token();
    try {
        boost::split(tokens, tokens_string, boost::is_any_of(sstring(",")));
    } catch (...) {
        throw std::runtime_error(format("Unable to parse replace_token={}", tokens_string));
    }
    tokens.erase("");
    for (auto token_string : tokens) {
        auto token = dht::token::from_sstring(token_string);
        ret.insert(token);
    }
    return ret;
}

std::optional<UUID> storage_service::get_replace_node() {
    auto replace_node = _db.local().get_config().replace_node();
    if (replace_node.empty()) {
        return std::nullopt;
    }
    try {
        return utils::UUID(replace_node);
    } catch (...) {
        auto msg = format("Unable to parse {} as host-id", replace_node);
        slogger.error("{}", msg);
        throw std::runtime_error(msg);
    }
}

bool storage_service::is_first_node() {
    if (_db.local().is_replacing()) {
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
    return !db::system_keyspace::bootstrap_complete() && !is_first_node();
}

void storage_service::install_schema_version_change_listener() {
    _listeners.emplace_back(make_lw_shared(_db.local().observable_schema_version().observe([this] (utils::UUID schema_version) {
        (void)_schema_version_publisher.trigger();
    })));
}

future<> storage_service::publish_schema_version() {
    return _migration_manager.local().passive_announce(_db.local().get_version());
}

future<> storage_service::snitch_reconfigured() {
    return update_topology(utils::fb_utilities::get_broadcast_address());
}

// Runs inside seastar::async context
void storage_service::prepare_to_join(
        std::unordered_set<gms::inet_address> initial_contact_nodes,
        std::unordered_set<gms::inet_address> loaded_endpoints,
        std::unordered_map<gms::inet_address, sstring> loaded_peer_features,
        bind_messaging_port do_bind) {
    std::map<gms::application_state, gms::versioned_value> app_states;
    if (db::system_keyspace::was_decommissioned()) {
        if (_db.local().get_config().override_decommission()) {
            slogger.warn("This node was decommissioned, but overriding by operator request.");
            db::system_keyspace::set_bootstrap_state(db::system_keyspace::bootstrap_state::COMPLETED).get();
        } else {
            auto msg = sstring("This node was decommissioned and will not rejoin the ring unless override_decommission=true has been set,"
                               "or all existing data is removed and the node is bootstrapped again");
            slogger.error("{}", msg);
            throw std::runtime_error(msg);
        }
    }
    if (get_replace_tokens().size() > 0 || get_replace_node()) {
         throw std::runtime_error("Replace method removed; use replace_address instead");
    }
    bool replacing_a_node_with_same_ip = false;
    bool replacing_a_node_with_diff_ip = false;
    auto tmlock = std::make_unique<token_metadata_lock>(get_token_metadata_lock().get0());
    auto tmptr = get_mutable_token_metadata_ptr().get0();
    if (_db.local().is_replacing()) {
        if (db::system_keyspace::bootstrap_complete()) {
            throw std::runtime_error("Cannot replace address with a node that is already bootstrapped");
        }
        _bootstrap_tokens = prepare_replacement_info(initial_contact_nodes, loaded_peer_features, do_bind).get0();
        auto replace_address = _db.local().get_replace_address();
        replacing_a_node_with_same_ip = replace_address && *replace_address == get_broadcast_address();
        replacing_a_node_with_diff_ip = replace_address && *replace_address != get_broadcast_address();

        slogger.info("Replacing a node with {} IP address, my address={}, node being replaced={}",
            get_broadcast_address() == *replace_address ? "the same" : "a different",
            get_broadcast_address(), *replace_address);
        tmptr->update_normal_tokens(_bootstrap_tokens, *replace_address).get();
    } else if (should_bootstrap()) {
        check_for_endpoint_collision(initial_contact_nodes, loaded_peer_features, do_bind).get();
    } else {
        auto local_features = _feature_service.known_feature_set();
        slogger.info("Checking remote features with gossip, initial_contact_nodes={}", initial_contact_nodes);
        _gossiper.do_shadow_round(initial_contact_nodes, gms::bind_messaging_port(bool(do_bind))).get();
        _gossiper.check_knows_remote_features(local_features, loaded_peer_features);
        _gossiper.check_snitch_name_matches();
        _gossiper.reset_endpoint_state_map().get();
        for (auto ep : loaded_endpoints) {
            _gossiper.add_saved_endpoint(ep);
        }
    }

    // If this is a restarting node, we should update tokens before gossip starts
    auto my_tokens = db::system_keyspace::get_saved_tokens().get0();
    bool restarting_normal_node = db::system_keyspace::bootstrap_complete() && !_db.local().is_replacing() && !my_tokens.empty();
    if (restarting_normal_node) {
        slogger.info("Restarting a node in NORMAL status");
        // This node must know about its chosen tokens before other nodes do
        // since they may start sending writes to this node after it gossips status = NORMAL.
        // Therefore we update _token_metadata now, before gossip starts.
        tmptr->update_normal_tokens(my_tokens, get_broadcast_address()).get();

        _cdc_gen_id = db::system_keyspace::get_cdc_generation_id().get0();
        if (!_cdc_gen_id) {
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
    auto local_host_id = db::system_keyspace::get_local_host_id().get0();
    _db.invoke_on_all([local_host_id] (auto& db) {
        db.set_local_id(local_host_id);
    }).get();
    auto features = _feature_service.supported_feature_set();
    if (!replacing_a_node_with_diff_ip) {
        // Replacing node with a different ip should own the host_id only after
        // the replacing node becomes NORMAL status. It is updated in
        // handle_state_normal().
        tmptr->update_host_id(local_host_id, get_broadcast_address());
    }

    // Replicate the tokens early because once gossip runs other nodes
    // might send reads/writes to this node. Replicate it early to make
    // sure the tokens are valid on all the shards.
    replicate_to_all_cores(std::move(tmptr)).get();
    tmlock.reset();

    auto broadcast_rpc_address = utils::fb_utilities::get_broadcast_rpc_address();
    auto& proxy = service::get_storage_proxy();
    // Ensure we know our own actual Schema UUID in preparation for updates
    db::schema_tables::recalculate_schema_version(proxy, _feature_service).get0();
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
        app_states.emplace(gms::application_state::CDC_GENERATION_ID, versioned_value::cdc_generation_id(_cdc_gen_id));
        app_states.emplace(gms::application_state::STATUS, versioned_value::normal(my_tokens));
    }
    if (replacing_a_node_with_same_ip || replacing_a_node_with_diff_ip) {
        app_states.emplace(gms::application_state::TOKENS, versioned_value::tokens(_bootstrap_tokens));
    }
    const auto& snitch_name = locator::i_endpoint_snitch::get_local_snitch_ptr()->get_name();
    app_states.emplace(gms::application_state::SNITCH_NAME, versioned_value::snitch_name(snitch_name));

    slogger.info("Starting up server gossip");

    auto generation_number = db::system_keyspace::increment_and_get_generation().get0();
    auto advertise = gms::advertise_myself(!replacing_a_node_with_same_ip);
    _gossiper.start_gossiping(generation_number, app_states, gms::bind_messaging_port(bool(do_bind)), advertise).get();

    install_schema_version_change_listener();

    // gossip local partitioner information (shard count and ignore_msb_bits)
    gossip_sharder().get();

    // gossip Schema.emptyVersion forcing immediate check for schema updates (see MigrationManager#maybeScheduleSchemaPull)

    // Wait for gossip to settle so that the fetures will be enabled
    if (do_bind) {
        gms::get_local_gossiper().wait_for_gossip_to_settle().get();
    }
}

void storage_service::maybe_start_sys_dist_ks() {
    supervisor::notify("starting system distributed keyspace");
    _sys_dist_ks.invoke_on_all(&db::system_distributed_keyspace::start).get();
}

/* Broadcasts the chosen tokens through gossip,
 * together with a CDC generation timestamp and STATUS=NORMAL.
 *
 * Assumes that no other functions modify CDC_GENERATION_ID, TOKENS or STATUS
 * in the gossiper's local application state while this function runs.
 */
// Runs inside seastar::async context
static void set_gossip_tokens(gms::gossiper& g,
        const std::unordered_set<dht::token>& tokens, std::optional<cdc::generation_id> cdc_gen_id) {
    assert(!tokens.empty());

    // Order is important: both the CDC streams timestamp and tokens must be known when a node handles our status.
    g.add_local_application_state({
        { gms::application_state::TOKENS, gms::versioned_value::tokens(tokens) },
        { gms::application_state::CDC_GENERATION_ID, gms::versioned_value::cdc_generation_id(cdc_gen_id) },
        { gms::application_state::STATUS, gms::versioned_value::normal(tokens) }
    }).get();
}

// Runs inside seastar::async context
void storage_service::join_token_ring(int delay) {
    // This function only gets called on shard 0, but we want to set _joined
    // on all shards, so this variable can be later read locally.
    container().invoke_on_all([] (auto&& ss) {
        ss._joined = true;
    }).get();
    // We bootstrap if we haven't successfully bootstrapped before, as long as we are not a seed.
    // If we are a seed, or if the user manually sets auto_bootstrap to false,
    // we'll skip streaming data from other nodes and jump directly into the ring.
    //
    // The seed check allows us to skip the RING_DELAY sleep for the single-node cluster case,
    // which is useful for both new users and testing.
    //
    // We attempted to replace this with a schema-presence check, but you need a meaningful sleep
    // to get schema info from gossip which defeats the purpose.  See CASSANDRA-4427 for the gory details.
    std::unordered_set<inet_address> current;
    if (should_bootstrap()) {
        bool resume_bootstrap = db::system_keyspace::bootstrap_in_progress();
        if (resume_bootstrap) {
            slogger.warn("Detected previous bootstrap failure; retrying");
        } else {
            db::system_keyspace::set_bootstrap_state(db::system_keyspace::bootstrap_state::IN_PROGRESS).get();
        }
        set_mode(mode::JOINING, "waiting for ring information", true);
        auto& gossiper = gms::get_gossiper().local();
        // first sleep the delay to make sure we see *at least* one other node
        for (int i = 0; i < delay && gossiper.get_live_members().size() < 2; i += 1000) {
            sleep_abortable(std::chrono::seconds(1), _abort_source).get();
        }
        // if our schema hasn't matched yet, keep sleeping until it does
        // (post CASSANDRA-1391 we don't expect this to be necessary very often, but it doesn't hurt to be careful)
        while (!_migration_manager.local().have_schema_agreement()) {
            set_mode(mode::JOINING, "waiting for schema information to complete", true);
            sleep_abortable(std::chrono::seconds(1), _abort_source).get();
        }
        set_mode(mode::JOINING, "schema complete, ready to bootstrap", true);
        set_mode(mode::JOINING, "waiting for pending range calculation", true);
        update_pending_ranges("joining").get();
        set_mode(mode::JOINING, "calculation complete, ready to bootstrap", true);
        slogger.debug("... got ring + schema info");

        auto t = gms::gossiper::clk::now();
        auto tmptr = get_token_metadata_ptr();
        while (_db.local().get_config().consistent_rangemovement() &&
            (!tmptr->get_bootstrap_tokens().empty() ||
             !tmptr->get_leaving_endpoints().empty())) {
            auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(gms::gossiper::clk::now() - t).count();
            slogger.info("Checking bootstrapping/leaving nodes: tokens {}, leaving {}, sleep 1 second and check again ({} seconds elapsed)",
                tmptr->get_bootstrap_tokens().size(),
                tmptr->get_leaving_endpoints().size(),
                elapsed);

            sleep_abortable(std::chrono::seconds(1), _abort_source).get();

            if (gms::gossiper::clk::now() > t + std::chrono::seconds(60)) {
                throw std::runtime_error("Other bootstrapping/leaving nodes detected, cannot bootstrap while consistent_rangemovement is true");
            }

            // Check the schema and pending range again
            while (!_migration_manager.local().have_schema_agreement()) {
                set_mode(mode::JOINING, "waiting for schema information to complete", true);
                sleep_abortable(std::chrono::seconds(1), _abort_source).get();
            }
            update_pending_ranges("bootstrapping/leaving nodes while joining").get();
            tmptr = get_token_metadata_ptr();
        }
        slogger.info("Checking bootstrapping/leaving nodes: ok");

        if (!_db.local().is_replacing()) {
            if (tmptr->is_member(get_broadcast_address())) {
                throw std::runtime_error("This node is already a member of the token ring; bootstrap aborted. (If replacing a dead node, remove the old one from the ring first.)");
            }
            set_mode(mode::JOINING, "getting bootstrap token", true);
            if (resume_bootstrap) {
                _bootstrap_tokens = db::system_keyspace::get_saved_tokens().get0();
                if (!_bootstrap_tokens.empty()) {
                    slogger.info("Using previously saved tokens = {}", _bootstrap_tokens);
                } else {
                    _bootstrap_tokens = boot_strapper::get_bootstrap_tokens(tmptr, _db.local());
                    slogger.info("Using newly generated tokens = {}", _bootstrap_tokens);
                }
            } else {
                _bootstrap_tokens = boot_strapper::get_bootstrap_tokens(tmptr, _db.local());
                slogger.info("Using newly generated tokens = {}", _bootstrap_tokens);
            }
        } else {
            auto replace_addr = _db.local().get_replace_address();
            if (replace_addr && *replace_addr != get_broadcast_address()) {
                // Sleep additionally to make sure that the server actually is not alive
                // and giving it more time to gossip if alive.
                sleep_abortable(service::load_broadcaster::BROADCAST_INTERVAL, _abort_source).get();

                // check for operator errors...
                const auto tmptr = get_token_metadata_ptr();
                for (auto token : _bootstrap_tokens) {
                    auto existing = tmptr->get_endpoint(token);
                    if (existing) {
                        auto* eps = _gossiper.get_endpoint_state_for_endpoint_ptr(*existing);
                        if (eps && eps->get_update_timestamp() > gms::gossiper::clk::now() - std::chrono::milliseconds(delay)) {
                            throw std::runtime_error("Cannot replace a live node...");
                        }
                        current.insert(*existing);
                    } else {
                        throw std::runtime_error(format("Cannot replace token {} which does not exist!", token));
                    }
                }
            } else {
                sleep_abortable(get_ring_delay(), _abort_source).get();
            }
            set_mode(mode::JOINING, format("Replacing a node with token(s): {}", _bootstrap_tokens), true);
            // _bootstrap_tokens was previously set in prepare_to_join using tokens gossiped by the replaced node
        }
        maybe_start_sys_dist_ks();
        mark_existing_views_as_built();
        db::system_keyspace::update_tokens(_bootstrap_tokens).get();
        bootstrap(); // blocks until finished
    } else {
        maybe_start_sys_dist_ks();
        size_t num_tokens = _db.local().get_config().num_tokens();
        _bootstrap_tokens = db::system_keyspace::get_saved_tokens().get0();
        if (_bootstrap_tokens.empty()) {
            auto initial_tokens = _db.local().get_initial_tokens();
            if (initial_tokens.size() < 1) {
                _bootstrap_tokens = boot_strapper::get_random_tokens(get_token_metadata_ptr(), num_tokens);
                if (num_tokens == 1) {
                    slogger.warn("Generated random token {}. Random tokens will result in an unbalanced ring; see http://wiki.apache.org/cassandra/Operations", _bootstrap_tokens);
                } else {
                    slogger.info("Generated random tokens. tokens are {}", _bootstrap_tokens);
                }
            } else {
                for (auto token_string : initial_tokens) {
                    auto token = dht::token::from_sstring(token_string);
                    _bootstrap_tokens.insert(token);
                }
                slogger.info("Saved tokens not found. Using configuration value: {}", _bootstrap_tokens);
            }
            db::system_keyspace::update_tokens(_bootstrap_tokens).get();
        } else {
            if (_bootstrap_tokens.size() != num_tokens) {
                throw std::runtime_error(format("Cannot change the number of tokens from {:d} to {:d}", _bootstrap_tokens.size(), num_tokens));
            } else {
                slogger.info("Using saved tokens {}", _bootstrap_tokens);
            }
        }
    }

    slogger.debug("Setting tokens to {}", _bootstrap_tokens);
    mutate_token_metadata([this] (mutable_token_metadata_ptr tmptr) {
        // This node must know about its chosen tokens before other nodes do
        // since they may start sending writes to this node after it gossips status = NORMAL.
        // Therefore, in case we haven't updated _token_metadata with our tokens yet, do it now.
        return tmptr->update_normal_tokens(_bootstrap_tokens, get_broadcast_address());
    }).get();

    if (!db::system_keyspace::bootstrap_complete()) {
        // If we're not bootstrapping then we shouldn't have chosen a CDC streams timestamp yet.
        assert(should_bootstrap() || !_cdc_gen_id);

        // Don't try rewriting CDC stream description tables.
        // See cdc.md design notes, `Streams description table V1 and rewriting` section, for explanation.
        db::system_keyspace::cdc_set_rewritten(std::nullopt).get();
    }

    if (!_cdc_gen_id) {
        // If we didn't observe any CDC generation at this point, then either
        // 1. we're replacing a node,
        // 2. we've already bootstrapped, but are upgrading from a non-CDC version,
        // 3. we're the first node, starting a fresh cluster.

        // In the replacing case we won't create any CDC generation: we're not introducing any new tokens,
        // so the current generation used by the cluster is fine.

        // In the case of an upgrading cluster, one of the nodes is responsible for creating
        // the first CDC generation. We'll check if it's us.

        // Finally, if we're the first node, we'll create the first generation.

        if (!_db.local().is_replacing()
                && (!db::system_keyspace::bootstrap_complete()
                    || cdc::should_propose_first_generation(get_broadcast_address(), _gossiper))) {
            try {
                _cdc_gen_id = cdc::make_new_cdc_generation(_db.local().get_config(),
                        _bootstrap_tokens, get_token_metadata_ptr(), _gossiper,
                        _sys_dist_ks.local(), get_ring_delay(),
                        !_for_testing && !is_first_node() /* add_delay */,
                        _feature_service.cluster_supports_cdc_generations_v2()).get0();
            } catch (...) {
                cdc_log.warn(
                    "Could not create a new CDC generation: {}. This may make it impossible to use CDC or cause performance problems."
                    " Use nodetool checkAndRepairCdcStreams to fix CDC.", std::current_exception());
            }
        }
    }

    // Persist the CDC streams timestamp before we persist bootstrap_state = COMPLETED.
    if (_cdc_gen_id) {
        db::system_keyspace::update_cdc_generation_id(*_cdc_gen_id).get();
    }
    // If we crash now, we will choose a new CDC streams timestamp anyway (because we will also choose a new set of tokens).
    // But if we crash after setting bootstrap_state = COMPLETED, we will keep using the persisted CDC streams timestamp after restarting.

    db::system_keyspace::set_bootstrap_state(db::system_keyspace::bootstrap_state::COMPLETED).get();
    // At this point our local tokens and CDC streams timestamp are chosen (_bootstrap_tokens, _cdc_gen_id) and will not be changed.

    // start participating in the ring.
    set_gossip_tokens(_gossiper, _bootstrap_tokens, _cdc_gen_id);
    set_mode(mode::NORMAL, "node is now in normal status", true);

    if (get_token_metadata().sorted_tokens().empty()) {
        auto err = format("join_token_ring: Sorted token in token_metadata is empty");
        slogger.error("{}", err);
        throw std::runtime_error(err);
    }

    _cdc_gen_service.local().after_join(std::move(_cdc_gen_id)).get();

    // Ensure that the new CDC stream description table has all required streams.
    // See the function's comment for details.
    cdc::maybe_rewrite_streams_descriptions(
            _db.local(), _sys_dist_ks.local_shared(),
            [tm = get_token_metadata_ptr()] { return tm->count_normal_token_owners(); },
            _abort_source).get();
}

void storage_service::mark_existing_views_as_built() {
    _db.invoke_on(0, [this] (database& db) {
        return do_with(db.get_views(), [this] (std::vector<view_ptr>& views) {
            return parallel_for_each(views, [this] (view_ptr& view) {
                return db::system_keyspace::mark_view_as_built(view->ks_name(), view->cf_name()).then([this, view] {
                    return _sys_dist_ks.local().finish_view_build(view->ks_name(), view->cf_name());
                });
            });
        });
    }).get();
}

// Runs inside seastar::async context
void storage_service::bootstrap() {
    _is_bootstrap_mode = true;
    auto x = seastar::defer([this] { _is_bootstrap_mode = false; });

    if (!_db.local().is_replacing()) {
        // Wait until we know tokens of existing node before announcing join status.
        _gossiper.wait_for_range_setup().get();

        // Even if we reached this point before but crashed, we will make a new CDC generation.
        // It doesn't hurt: other nodes will (potentially) just do more generation switches.
        // We do this because with this new attempt at bootstrapping we picked a different set of tokens.

        // Update pending ranges now, so we correctly count ourselves as a pending replica
        // when inserting the new CDC generation.
      if (!is_repair_based_node_ops_enabled()) {
        // When is_repair_based_node_ops_enabled is true, the bootstrap node
        // will use node_ops_cmd to bootstrap, node_ops_cmd will update the pending ranges.
        mutate_token_metadata([this] (mutable_token_metadata_ptr tmptr) {
            auto endpoint = get_broadcast_address();
            tmptr->add_bootstrap_tokens(_bootstrap_tokens, endpoint);
            return update_pending_ranges(std::move(tmptr), format("bootstrapping node {}", endpoint));
        }).get();
      }

        // After we pick a generation timestamp, we start gossiping it, and we stick with it.
        // We don't do any other generation switches (unless we crash before complecting bootstrap).
        assert(!_cdc_gen_id);

        _cdc_gen_id = cdc::make_new_cdc_generation(_db.local().get_config(),
                _bootstrap_tokens, get_token_metadata_ptr(), _gossiper,
                _sys_dist_ks.local(), get_ring_delay(),
                !_for_testing && !is_first_node() /* add_delay */,
                _feature_service.cluster_supports_cdc_generations_v2()).get0();

      if (!is_repair_based_node_ops_enabled()) {
        // When is_repair_based_node_ops_enabled is true, the bootstrap node
        // will use node_ops_cmd to bootstrap, bootstrapping gossip status is not needed for bootstrap.
        _gossiper.add_local_application_state({
            // Order is important: both the CDC streams timestamp and tokens must be known when a node handles our status.
            { gms::application_state::TOKENS, versioned_value::tokens(_bootstrap_tokens) },
            { gms::application_state::CDC_GENERATION_ID, versioned_value::cdc_generation_id(_cdc_gen_id) },
            { gms::application_state::STATUS, versioned_value::bootstrapping(_bootstrap_tokens) },
        }).get();

        set_mode(mode::JOINING, format("sleeping {} ms for pending range setup", get_ring_delay().count()), true);
        _gossiper.wait_for_range_setup().get();
     }
    } else {
        // Wait until we know tokens of existing node before announcing replacing status.
        set_mode(mode::JOINING, sprint("Wait until local node knows tokens of peer nodes"), true);
        _gossiper.wait_for_range_setup().get();
        auto replace_addr = _db.local().get_replace_address();
        if (replace_addr) {
            slogger.debug("Removing replaced endpoint {} from system.peers", *replace_addr);
            db::system_keyspace::remove_endpoint(*replace_addr).get();
        }
    }

    _db.invoke_on_all([this] (database& db) {
        for (auto& cf : db.get_non_system_column_families()) {
            cf->notify_bootstrap_or_replace_start();
        }
    }).get();

    set_mode(mode::JOINING, "Starting to bootstrap...", true);
    if (is_repair_based_node_ops_enabled()) {
        if (_db.local().is_replacing()) {
            run_replace_ops();
        } else {
            run_bootstrap_ops();
        }
    } else {
        // Does the actual streaming of newly replicated token ranges.
        if (_db.local().is_replacing()) {
            run_replace_ops();
        } else {
            dht::boot_strapper bs(_db, _abort_source, get_broadcast_address(), _bootstrap_tokens, get_token_metadata_ptr());
            bs.bootstrap(streaming::stream_reason::bootstrap).get();
        }
    }
    _db.invoke_on_all([this] (database& db) {
        for (auto& cf : db.get_non_system_column_families()) {
            cf->notify_bootstrap_or_replace_end();
        }
    }).get();


    slogger.info("Bootstrap completed! for the tokens {}", _bootstrap_tokens);
}

sstring
storage_service::get_rpc_address(const inet_address& endpoint) const {
    if (endpoint != get_broadcast_address()) {
        auto* v = _gossiper.get_application_state_ptr(endpoint, gms::application_state::RPC_ADDRESS);
        if (v) {
            return v->value;
        }
    }
    return boost::lexical_cast<std::string>(endpoint);
}

std::unordered_map<dht::token_range, inet_address_vector_replica_set>
storage_service::get_range_to_address_map(const sstring& keyspace) const {
    return get_range_to_address_map(keyspace, get_token_metadata().sorted_tokens());
}

std::unordered_map<dht::token_range, inet_address_vector_replica_set>
storage_service::get_range_to_address_map_in_local_dc(
        const sstring& keyspace) const {
    std::function<bool(const inet_address&)> filter =  [this](const inet_address& address) {
        return is_local_dc(address);
    };

    auto orig_map = get_range_to_address_map(keyspace, get_tokens_in_local_dc());
    std::unordered_map<dht::token_range, inet_address_vector_replica_set> filtered_map;
    for (auto entry : orig_map) {
        auto& addresses = filtered_map[entry.first];
        addresses.reserve(entry.second.size());
        std::copy_if(entry.second.begin(), entry.second.end(), std::back_inserter(addresses), filter);
    }

    return filtered_map;
}

std::vector<token>
storage_service::get_tokens_in_local_dc() const {
    std::vector<token> filtered_tokens;
    const auto& tm = get_token_metadata();
    for (auto token : tm.sorted_tokens()) {
        auto endpoint = tm.get_endpoint(token);
        if (is_local_dc(*endpoint))
            filtered_tokens.push_back(token);
    }
    return filtered_tokens;
}

bool
storage_service::is_local_dc(const inet_address& targetHost) const {
    auto remote_dc = locator::i_endpoint_snitch::get_local_snitch_ptr()->get_datacenter(targetHost);
    auto local_dc = locator::i_endpoint_snitch::get_local_snitch_ptr()->get_datacenter(get_broadcast_address());
    return remote_dc == local_dc;
}

std::unordered_map<dht::token_range, inet_address_vector_replica_set>
storage_service::get_range_to_address_map(const sstring& keyspace,
        const std::vector<token>& sorted_tokens) const {
    sstring ks = keyspace;
    // some people just want to get a visual representation of things. Allow null and set it to the first
    // non-system keyspace.
    if (keyspace == "") {
        auto keyspaces = _db.local().get_non_system_keyspaces();
        if (keyspaces.empty()) {
            throw std::runtime_error("No keyspace provided and no non system kespace exist");
        }
        ks = keyspaces[0];
    }
    return construct_range_to_endpoint_map(ks, get_all_ranges(sorted_tokens));
}

void storage_service::handle_state_replacing_update_pending_ranges(mutable_token_metadata_ptr tmptr, inet_address replacing_node) {
    try {
        slogger.info("handle_state_replacing: Waiting for replacing node {} to be alive on all shards", replacing_node);
        _gossiper.wait_alive({replacing_node}, std::chrono::milliseconds(5 * 1000));
        slogger.info("handle_state_replacing: Replacing node {} is now alive on all shards", replacing_node);
    } catch (...) {
        slogger.warn("handle_state_replacing: Failed to wait for replacing node {} to be alive on all shards: {}",
                replacing_node, std::current_exception());
    }
    slogger.info("handle_state_replacing: Update pending ranges for replacing node {}", replacing_node);
    update_pending_ranges(tmptr, format("handle_state_replacing {}", replacing_node)).get();
}

void storage_service::handle_state_replacing(inet_address replacing_node) {
    slogger.debug("endpoint={} handle_state_replacing", replacing_node);
    auto host_id = _gossiper.get_host_id(replacing_node);
    auto tmlock = get_token_metadata_lock().get0();
    auto tmptr = get_mutable_token_metadata_ptr().get0();
    auto existing_node_opt = tmptr->get_endpoint_for_host_id(host_id);
    auto replace_addr = _db.local().get_replace_address();
    if (replacing_node == get_broadcast_address() && replace_addr && *replace_addr == get_broadcast_address()) {
        existing_node_opt = replacing_node;
    }
    if (!existing_node_opt) {
        slogger.warn("Can not find the existing node for the replacing node {}", replacing_node);
        return;
    }
    auto existing_node = *existing_node_opt;
    auto existing_tokens = get_tokens_for(existing_node);
    auto replacing_tokens = get_tokens_for(replacing_node);
    slogger.info("Node {} is replacing existing node {} with host_id={}, existing_tokens={}, replacing_tokens={}",
            replacing_node, existing_node, host_id, existing_tokens, replacing_tokens);
    tmptr->add_replacing_endpoint(existing_node, replacing_node);
    if (_gossiper.is_alive(replacing_node)) {
        slogger.info("handle_state_replacing: Replacing node {} is already alive, update pending ranges", replacing_node);
        handle_state_replacing_update_pending_ranges(tmptr, replacing_node);
    } else {
        slogger.info("handle_state_replacing: Replacing node {} is not alive yet, delay update pending ranges", replacing_node);
        _replacing_nodes_pending_ranges_updater.insert(replacing_node);
    }
    replicate_to_all_cores(std::move(tmptr)).get();
}

void storage_service::handle_state_bootstrap(inet_address endpoint) {
    slogger.debug("endpoint={} handle_state_bootstrap", endpoint);
    // explicitly check for TOKENS, because a bootstrapping node might be bootstrapping in legacy mode; that is, not using vnodes and no token specified
    auto tokens = get_tokens_for(endpoint);

    slogger.debug("Node {} state bootstrapping, token {}", endpoint, tokens);

    // if this node is present in token metadata, either we have missed intermediate states
    // or the node had crashed. Print warning if needed, clear obsolete stuff and
    // continue.
    auto tmlock = get_token_metadata_lock().get0();
    auto tmptr = get_mutable_token_metadata_ptr().get0();
    if (tmptr->is_member(endpoint)) {
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

    tmptr->add_bootstrap_tokens(tokens, endpoint);
    if (_gossiper.uses_host_id(endpoint)) {
        tmptr->update_host_id(_gossiper.get_host_id(endpoint), endpoint);
    }
    update_pending_ranges(tmptr, format("handle_state_bootstrap {}", endpoint)).get();
    replicate_to_all_cores(std::move(tmptr)).get();
}

void storage_service::handle_state_normal(inet_address endpoint) {
    slogger.debug("endpoint={} handle_state_normal", endpoint);
    auto tokens = get_tokens_for(endpoint);

    slogger.debug("Node {} state normal, token {}", endpoint, tokens);

    auto tmlock = std::make_unique<token_metadata_lock>(get_token_metadata_lock().get0());
    auto tmptr = get_mutable_token_metadata_ptr().get0();
    if (tmptr->is_member(endpoint)) {
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
            slogger.info("Set host_id={} to be owned by node={}", host_id, endpoint);
            tmptr->update_host_id(host_id, endpoint);
        }
    }

    // Tokens owned by the handled endpoint.
    // The endpoint broadcasts its set of chosen tokens. If a token was also chosen by another endpoint,
    // the collision is resolved by assigning the token to the endpoint which started later.
    std::unordered_set<token> owned_tokens;

    for (auto t : tokens) {
        // we don't want to update if this node is responsible for the token and it has a later startup time than endpoint.
        auto current_owner = tmptr->get_endpoint(t);
        if (!current_owner) {
            slogger.debug("handle_state_normal: New node {} at token {}", endpoint, t);
            owned_tokens.insert(t);
        } else if (endpoint == *current_owner) {
            slogger.debug("handle_state_normal: endpoint={} == current_owner={} token {}", endpoint, *current_owner, t);
            // set state back to normal, since the node may have tried to leave, but failed and is now back up
            owned_tokens.insert(t);
        } else if (_gossiper.compare_endpoint_startup(endpoint, *current_owner) > 0) {
            slogger.debug("handle_state_normal: endpoint={} > current_owner={}, token {}", endpoint, *current_owner, t);
            owned_tokens.insert(t);
            // currentOwner is no longer current, endpoint is.  Keep track of these moves, because when
            // a host no longer has any tokens, we'll want to remove it.
            std::multimap<inet_address, token> ep_to_token_copy = get_token_metadata().get_endpoint_to_token_map_for_reading();
            auto rg = ep_to_token_copy.equal_range(*current_owner);
            for (auto it = rg.first; it != rg.second; it++) {
                if (it->second == t) {
                    slogger.info("handle_state_normal: remove endpoint={} token={}", *current_owner, t);
                    ep_to_token_copy.erase(it);
                }
            }
            if (!ep_to_token_copy.contains(*current_owner)) {
                slogger.info("handle_state_normal: endpoints_to_remove endpoint={}", *current_owner);
                endpoints_to_remove.insert(*current_owner);
            }
            slogger.info("handle_state_normal: Nodes {} and {} have the same token {}. {} is the new owner", endpoint, *current_owner, t, endpoint);
        } else {
            slogger.info("handle_state_normal: Nodes {} and {} have the same token {}. Ignoring {}", endpoint, *current_owner, t, endpoint);
        }
    }

    bool is_member = tmptr->is_member(endpoint);
    // Update pending ranges after update of normal tokens immediately to avoid
    // a race where natural endpoint was updated to contain node A, but A was
    // not yet removed from pending endpoints
    tmptr->update_normal_tokens(owned_tokens, endpoint).get();
    update_pending_ranges(tmptr, format("handle_state_normal {}", endpoint)).get();
    replicate_to_all_cores(std::move(tmptr)).get();
    tmlock.reset();

    for (auto ep : endpoints_to_remove) {
        remove_endpoint(ep);
    }
    slogger.debug("handle_state_normal: endpoint={} owned_tokens = {}", endpoint, owned_tokens);
    if (!owned_tokens.empty() && !endpoints_to_remove.count(endpoint)) {
        update_peer_info(endpoint);
        db::system_keyspace::update_tokens(endpoint, owned_tokens).then_wrapped([endpoint] (auto&& f) {
            try {
                f.get();
            } catch (...) {
                slogger.error("handle_state_normal: fail to update tokens for {}: {}", endpoint, std::current_exception());
            }
            return make_ready_future<>();
        }).get();
    }

    // Send joined notification only when this node was not a member prior to this
    if (!is_member) {
        notify_joined(endpoint);
    }

    if (slogger.is_enabled(logging::log_level::debug)) {
        auto tm = get_token_metadata();
        auto ver = tm.get_ring_version();
        for (auto& x : tm.get_token_to_endpoint()) {
            slogger.debug("handle_state_normal: token_metadata.ring_version={}, token={} -> endpoint={}", ver, x.first, x.second);
        }
    }
}

void storage_service::handle_state_leaving(inet_address endpoint) {
    slogger.debug("endpoint={} handle_state_leaving", endpoint);

    auto tokens = get_tokens_for(endpoint);

    slogger.debug("Node {} state leaving, tokens {}", endpoint, tokens);

    // If the node is previously unknown or tokens do not match, update tokenmetadata to
    // have this node as 'normal' (it must have been using this token before the
    // leave). This way we'll get pending ranges right.
    auto tmlock = get_token_metadata_lock().get0();
    auto tmptr = get_mutable_token_metadata_ptr().get0();
    if (!tmptr->is_member(endpoint)) {
        // FIXME: this code should probably resolve token collisions too, like handle_state_normal
        slogger.info("Node {} state jump to leaving", endpoint);

        tmptr->update_normal_tokens(tokens, endpoint).get();
    } else {
        auto tokens_ = tmptr->get_tokens(endpoint);
        std::set<token> tmp(tokens.begin(), tokens.end());
        if (!std::includes(tokens_.begin(), tokens_.end(), tmp.begin(), tmp.end())) {
            slogger.warn("Node {} 'leaving' token mismatch. Long network partition?", endpoint);
            slogger.debug("tokens_={}, tokens={}", tokens_, tmp);

            tmptr->update_normal_tokens(tokens, endpoint).get();
        }
    }

    // at this point the endpoint is certainly a member with this token, so let's proceed
    // normally
    tmptr->add_leaving_endpoint(endpoint);

    update_pending_ranges(tmptr, format("handle_state_leaving", endpoint)).get();
    replicate_to_all_cores(std::move(tmptr)).get();
}

void storage_service::handle_state_left(inet_address endpoint, std::vector<sstring> pieces) {
    slogger.debug("endpoint={} handle_state_left", endpoint);
    if (pieces.size() < 2) {
        slogger.warn("Fail to handle_state_left endpoint={} pieces={}", endpoint, pieces);
        return;
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
    excise(tokens, endpoint, extract_expire_time(pieces));
}

void storage_service::handle_state_moving(inet_address endpoint, std::vector<sstring> pieces) {
    throw std::runtime_error(format("Move operation is not supported anymore, endpoint={}", endpoint));
}

void storage_service::handle_state_removing(inet_address endpoint, std::vector<sstring> pieces) {
    slogger.debug("endpoint={} handle_state_removing", endpoint);
    if (pieces.empty()) {
        slogger.warn("Fail to handle_state_removing endpoint={} pieces={}", endpoint, pieces);
        return;
    }
    if (endpoint == get_broadcast_address()) {
        slogger.info("Received removenode gossip about myself. Is this node rejoining after an explicit removenode?");
        try {
            drain().get();
        } catch (...) {
            slogger.error("Fail to drain: {}", std::current_exception());
            throw;
        }
        return;
    }
    if (get_token_metadata().is_member(endpoint)) {
        auto state = pieces[0];
        auto remove_tokens = get_token_metadata().get_tokens(endpoint);
        if (sstring(gms::versioned_value::REMOVED_TOKEN) == state) {
            std::unordered_set<token> tmp(remove_tokens.begin(), remove_tokens.end());
            excise(std::move(tmp), endpoint, extract_expire_time(pieces));
        } else if (sstring(gms::versioned_value::REMOVING_TOKEN) == state) {
            mutate_token_metadata([this, remove_tokens = std::move(remove_tokens), endpoint] (mutable_token_metadata_ptr tmptr) mutable {
                slogger.debug("Tokens {} removed manually (endpoint was {})", remove_tokens, endpoint);
                // Note that the endpoint is being removed
                tmptr->add_leaving_endpoint(endpoint);
                return update_pending_ranges(std::move(tmptr), format("handle_state_removing {}", endpoint));
            }).get();
            // find the endpoint coordinating this removal that we need to notify when we're done
            auto* value = _gossiper.get_application_state_ptr(endpoint, application_state::REMOVAL_COORDINATOR);
            if (!value) {
                auto err = format("Can not find application_state for endpoint={}", endpoint);
                slogger.warn("{}", err);
                throw std::runtime_error(err);
            }
            std::vector<sstring> coordinator;
            boost::split(coordinator, value->value, boost::is_any_of(sstring(versioned_value::DELIMITER_STR)));
            if (coordinator.size() != 2) {
                auto err = format("Can not split REMOVAL_COORDINATOR for endpoint={}, value={}", endpoint, value->value);
                slogger.warn("{}", err);
                throw std::runtime_error(err);
            }
            UUID host_id(coordinator[1]);
            // grab any data we are now responsible for and notify responsible node
            auto ep = get_token_metadata().get_endpoint_for_host_id(host_id);
            if (!ep) {
                auto err = format("Can not find host_id={}", host_id);
                slogger.warn("{}", err);
                throw std::runtime_error(err);
            }
            // Kick off streaming commands. No need to wait for
            // restore_replica_count to complete which can take a long time,
            // since when it completes, this node will send notification to
            // tell the removal_coordinator with IP address notify_endpoint
            // that the restore process is finished on this node. This node
            // will be removed from _replicating_nodes on the
            // removal_coordinator.
            auto notify_endpoint = ep.value();
            //FIXME: discarded future.
            (void)restore_replica_count(endpoint, notify_endpoint).handle_exception([endpoint, notify_endpoint] (auto ep) {
                slogger.info("Failed to restore_replica_count for node {}, notify_endpoint={} : {}", endpoint, notify_endpoint, ep);
            });
        }
    } else { // now that the gossiper has told us about this nonexistent member, notify the gossiper to remove it
        if (sstring(gms::versioned_value::REMOVED_TOKEN) == pieces[0]) {
            add_expire_time_if_found(endpoint, extract_expire_time(pieces));
        }
        remove_endpoint(endpoint);
    }
}

void storage_service::on_join(gms::inet_address endpoint, gms::endpoint_state ep_state) {
    slogger.debug("endpoint={} on_join", endpoint);
    for (const auto& e : ep_state.get_application_state_map()) {
        on_change(endpoint, e.first, e.second);
    }
    //FIXME: discarded future.
    (void)_migration_manager.local().schedule_schema_pull(endpoint, ep_state).handle_exception([endpoint] (auto ep) {
        slogger.warn("Fail to pull schema from {}: {}", endpoint, ep);
    });
}

void storage_service::on_alive(gms::inet_address endpoint, gms::endpoint_state state) {
    slogger.debug("endpoint={} on_alive", endpoint);
    //FIXME: discarded future.
    (void)_migration_manager.local().schedule_schema_pull(endpoint, state).handle_exception([endpoint] (auto ep) {
        slogger.warn("Fail to pull schema from {}: {}", endpoint, ep);
    });
    if (get_token_metadata().is_member(endpoint)) {
        notify_up(endpoint);
    }
    if (_replacing_nodes_pending_ranges_updater.contains(endpoint)) {
        _replacing_nodes_pending_ranges_updater.erase(endpoint);
        slogger.info("Trigger pending ranges updater for replacing node {}", endpoint);
        auto tmlock = get_token_metadata_lock().get0();
        auto tmptr = get_mutable_token_metadata_ptr().get0();
        handle_state_replacing_update_pending_ranges(tmptr, endpoint);
        replicate_to_all_cores(std::move(tmptr)).get();
    }
}

void storage_service::before_change(gms::inet_address endpoint, gms::endpoint_state current_state, gms::application_state new_state_key, const gms::versioned_value& new_value) {
    slogger.debug("endpoint={} before_change: new app_state={}, new versioned_value={}", endpoint, new_state_key, new_value);
}

void storage_service::on_change(inet_address endpoint, application_state state, const versioned_value& value) {
    slogger.debug("endpoint={} on_change:     app_state={}, versioned_value={}", endpoint, state, value);
    if (state == application_state::STATUS) {
        std::vector<sstring> pieces;
        boost::split(pieces, value.value, boost::is_any_of(sstring(versioned_value::DELIMITER_STR)));
        if (pieces.empty()) {
            slogger.warn("Fail to split status in on_change: endpoint={}, app_state={}, value={}", endpoint, state, value);
            return;
        }
        sstring move_name = pieces[0];
        if (move_name == sstring(versioned_value::STATUS_BOOTSTRAPPING)) {
            handle_state_bootstrap(endpoint);
        } else if (move_name == sstring(versioned_value::STATUS_NORMAL) ||
                   move_name == sstring(versioned_value::SHUTDOWN)) {
            handle_state_normal(endpoint);
        } else if (move_name == sstring(versioned_value::REMOVING_TOKEN) ||
                   move_name == sstring(versioned_value::REMOVED_TOKEN)) {
            handle_state_removing(endpoint, pieces);
        } else if (move_name == sstring(versioned_value::STATUS_LEAVING)) {
            handle_state_leaving(endpoint);
        } else if (move_name == sstring(versioned_value::STATUS_LEFT)) {
            handle_state_left(endpoint, pieces);
        } else if (move_name == sstring(versioned_value::STATUS_MOVING)) {
            handle_state_moving(endpoint, pieces);
        } else if (move_name == sstring(versioned_value::HIBERNATE)) {
            handle_state_replacing(endpoint);
        } else {
            return; // did nothing.
        }
    } else {
        auto* ep_state = _gossiper.get_endpoint_state_for_endpoint_ptr(endpoint);
        if (!ep_state || _gossiper.is_dead_state(*ep_state)) {
            slogger.debug("Ignoring state change for dead or unknown endpoint: {}", endpoint);
            return;
        }
        if (get_token_metadata().is_member(endpoint)) {
            do_update_system_peers_table(endpoint, state, value);
            if (state == application_state::SCHEMA) {
                //FIXME: discarded future.
                (void)_migration_manager.local().schedule_schema_pull(endpoint, *ep_state).handle_exception([endpoint] (auto ep) {
                    slogger.warn("Failed to pull schema from {}: {}", endpoint, ep);
                });
            } else if (state == application_state::RPC_READY) {
                slogger.debug("Got application_state::RPC_READY for node {}, is_cql_ready={}", endpoint, ep_state->is_cql_ready());
                notify_cql_change(endpoint, ep_state->is_cql_ready());
            }
        }
    }
}


void storage_service::on_remove(gms::inet_address endpoint) {
    slogger.debug("endpoint={} on_remove", endpoint);
    auto tmlock = get_token_metadata_lock().get0();
    auto tmptr = get_mutable_token_metadata_ptr().get0();
    tmptr->remove_endpoint(endpoint);
    update_pending_ranges(tmptr, format("on_remove {}", endpoint)).get();
    replicate_to_all_cores(std::move(tmptr)).get();
}

void storage_service::on_dead(gms::inet_address endpoint, gms::endpoint_state state) {
    slogger.debug("endpoint={} on_dead", endpoint);
    notify_down(endpoint);
}

void storage_service::on_restart(gms::inet_address endpoint, gms::endpoint_state state) {
    slogger.debug("endpoint={} on_restart", endpoint);
    // If we have restarted before the node was even marked down, we need to reset the connection pool
    if (state.is_alive()) {
        on_dead(endpoint, state);
    }
}

// Runs inside seastar::async context
template <typename T>
static void update_table(gms::inet_address endpoint, sstring col, T value) {
    db::system_keyspace::update_peer_info(endpoint, col, value).then_wrapped([col, endpoint] (auto&& f) {
        try {
            f.get();
        } catch (...) {
            slogger.error("fail to update {} for {}: {}", col, endpoint, std::current_exception());
        }
        return make_ready_future<>();
    }).get();
}

// Runs inside seastar::async context
void storage_service::do_update_system_peers_table(gms::inet_address endpoint, const application_state& state, const versioned_value& value) {
    slogger.debug("Update system.peers table: endpoint={}, app_state={}, versioned_value={}", endpoint, state, value);
    if (state == application_state::RELEASE_VERSION) {
        update_table(endpoint, "release_version", value.value);
    } else if (state == application_state::DC) {
        update_table(endpoint, "data_center", value.value);
    } else if (state == application_state::RACK) {
        update_table(endpoint, "rack", value.value);
    } else if (state == application_state::RPC_ADDRESS) {
        auto col = sstring("rpc_address");
        inet_address ep;
        try {
            ep = gms::inet_address(value.value);
        } catch (...) {
            slogger.error("fail to update {} for {}: invalid rcpaddr {}", col, endpoint, value.value);
            return;
        }
        update_table(endpoint, col, ep.addr());
    } else if (state == application_state::SCHEMA) {
        update_table(endpoint, "schema_version", utils::UUID(value.value));
    } else if (state == application_state::HOST_ID) {
        update_table(endpoint, "host_id", utils::UUID(value.value));
    } else if (state == application_state::SUPPORTED_FEATURES) {
        update_table(endpoint, "supported_features", value.value);
    }
}

// Runs inside seastar::async context
void storage_service::update_peer_info(gms::inet_address endpoint) {
    using namespace gms;
    auto* ep_state = _gossiper.get_endpoint_state_for_endpoint_ptr(endpoint);
    if (!ep_state) {
        return;
    }
    for (auto& entry : ep_state->get_application_state_map()) {
        auto& app_state = entry.first;
        auto& value = entry.second;
        do_update_system_peers_table(endpoint, app_state, value);
    }
}

std::unordered_set<locator::token> storage_service::get_tokens_for(inet_address endpoint) {
    auto tokens_string = _gossiper.get_application_state_value(endpoint, application_state::TOKENS);
    slogger.trace("endpoint={}, tokens_string={}", endpoint, tokens_string);
    auto ret = versioned_value::tokens_from_string(tokens_string);
    slogger.trace("endpoint={}, tokens={}", endpoint, ret);
    return ret;
}

void storage_service::register_subscriber(endpoint_lifecycle_subscriber* subscriber)
{
    _lifecycle_subscribers.add(subscriber);
}

future<> storage_service::unregister_subscriber(endpoint_lifecycle_subscriber* subscriber) noexcept
{
    return _lifecycle_subscribers.remove(subscriber);
}

future<> storage_service::stop_transport() {
    return seastar::async([this] {
        slogger.info("Stop transport: starts");

        shutdown_client_servers();
        slogger.info("Stop transport: shutdown rpc and cql server done");

        gms::stop_gossiping().get();
        slogger.info("Stop transport: stop_gossiping done");

        do_stop_ms().get();
        slogger.info("Stop transport: shutdown messaging_service done");

        do_stop_stream_manager().get();
        slogger.info("Stop transport: shutdown stream_manager done");

        slogger.info("Stop transport: done");
    });
}

future<> storage_service::drain_on_shutdown() {
    assert(this_shard_id() == 0);
    return (_operation_mode == mode::DRAINING || _operation_mode == mode::DRAINED) ?
        _drain_finished.get_future() : do_drain(true);
}

future<> storage_service::init_messaging_service_part() {
    return container().invoke_on_all(&service::storage_service::init_messaging_service);
}

future<> storage_service::uninit_messaging_service_part() {
    return container().invoke_on_all(&service::storage_service::uninit_messaging_service);
}

future<> storage_service::init_server(bind_messaging_port do_bind) {
    assert(this_shard_id() == 0);

    return seastar::async([this, do_bind] {
        _initialized = true;

        std::unordered_set<inet_address> loaded_endpoints;
        if (_db.local().get_config().load_ring_state()) {
            slogger.info("Loading persisted ring state");
            auto loaded_tokens = db::system_keyspace::load_tokens().get0();
            auto loaded_host_ids = db::system_keyspace::load_host_ids().get0();

            for (auto& x : loaded_tokens) {
                slogger.debug("Loaded tokens: endpoint={}, tokens={}", x.first, x.second);
            }

            for (auto& x : loaded_host_ids) {
                slogger.debug("Loaded host_id: endpoint={}, uuid={}", x.first, x.second);
            }

            auto tmlock = get_token_metadata_lock().get0();
            auto tmptr = get_mutable_token_metadata_ptr().get0();
            for (auto x : loaded_tokens) {
                auto ep = x.first;
                auto tokens = x.second;
                if (ep == get_broadcast_address()) {
                    // entry has been mistakenly added, delete it
                    db::system_keyspace::remove_endpoint(ep).get();
                } else {
                    tmptr->update_normal_tokens(tokens, ep).get();
                    if (loaded_host_ids.contains(ep)) {
                        tmptr->update_host_id(loaded_host_ids.at(ep), ep);
                    }
                    loaded_endpoints.insert(ep);
                    _gossiper.add_saved_endpoint(ep);
                }
            }
            replicate_to_all_cores(std::move(tmptr)).get();
        }

        // Seeds are now only used as the initial contact point nodes. If the
        // loaded_endpoints are empty which means this node is a completely new
        // node, we use the nodes specified in seeds as the initial contact
        // point nodes, otherwise use the peer nodes persisted in system table.
        auto seeds = _gossiper.get_seeds();
        auto initial_contact_nodes = loaded_endpoints.empty() ?
            std::unordered_set<gms::inet_address>(seeds.begin(), seeds.end()) :
            loaded_endpoints;
        auto loaded_peer_features = db::system_keyspace::load_peer_features().get0();
        slogger.info("initial_contact_nodes={}, loaded_endpoints={}, loaded_peer_features={}",
                initial_contact_nodes, loaded_endpoints, loaded_peer_features.size());
        for (auto& x : loaded_peer_features) {
            slogger.info("peer={}, supported_features={}", x.first, x.second);
        }
        prepare_to_join(std::move(initial_contact_nodes), std::move(loaded_endpoints), std::move(loaded_peer_features), do_bind);
    });
}

future<> storage_service::join_cluster() {
    return seastar::async([this] {
        join_token_ring(get_ring_delay().count());
    });
}

future<> storage_service::replicate_to_all_cores(mutable_token_metadata_ptr tmptr) noexcept {
    assert(this_shard_id() == 0);

    slogger.debug("Replicating token_metadata to all cores");
    _pending_token_metadata_ptr = tmptr;
    // clone a local copy of updated token_metadata on all other shards
    return container().invoke_on_others([tmptr] (storage_service& ss) {
      return tmptr->clone_async().then([&ss] (token_metadata tm) {
        ss._pending_token_metadata_ptr = make_token_metadata_ptr(std::move(tm));
      });
    }).then_wrapped([this] (future<> f) {
        if (f.failed()) {
            return container().invoke_on_all([] (storage_service& ss) {
                if (auto tmptr = std::move(ss._pending_token_metadata_ptr)) {
                    return tmptr->clear_gently().then_wrapped([tmptr = std::move(tmptr)] (future<> f) {
                        if (f.failed()) {
                            slogger.warn("Failure to reset pending token_metadata in cleanup path: {}. Ignored.", f.get_exception());
                        }
                    });
                } else {
                    return make_ready_future<>();
                }
            }).finally([ep = f.get_exception()] () mutable {
                return make_exception_future<>(std::move(ep));
            });
        }
        return container().invoke_on_all([] (storage_service& ss) {
            ss._shared_token_metadata.set(std::move(ss._pending_token_metadata_ptr));
        }).handle_exception([] (auto e) noexcept {
            // applying the changes on all shards should never fail
            // it will end up in an inconsistent state that we can't recover from.
            slogger.error("Failed to replicate token_metadata: {}. Aborting.", e);
            abort();
        });
    });
}

future<> storage_service::gossip_sharder() {
    return _gossiper.add_local_application_state({
        { gms::application_state::SHARD_COUNT, versioned_value::shard_count(smp::count) },
        { gms::application_state::IGNORE_MSB_BITS, versioned_value::ignore_msb_bits(_db.local().get_config().murmur3_partitioner_ignore_msb_bits()) },
    });
}

future<> storage_service::stop() {
    // make sure nobody uses the semaphore
    node_ops_singal_abort(std::nullopt);
    _listeners.clear();
    return _schema_version_publisher.join().finally([this] {
        return std::move(_node_ops_abort_thread);
    });
}

future<> storage_service::check_for_endpoint_collision(std::unordered_set<gms::inet_address> initial_contact_nodes, const std::unordered_map<gms::inet_address, sstring>& loaded_peer_features, bind_messaging_port do_bind) {
    slogger.debug("Starting shadow gossip round to check for endpoint collision");

    return seastar::async([this, initial_contact_nodes, loaded_peer_features, do_bind] {
        auto t = gms::gossiper::clk::now();
        bool found_bootstrapping_node = false;
        auto local_features = _feature_service.known_feature_set();
        do {
            slogger.info("Checking remote features with gossip");
            _gossiper.do_shadow_round(initial_contact_nodes, gms::bind_messaging_port(bool(do_bind))).get();
            _gossiper.check_knows_remote_features(local_features, loaded_peer_features);
            _gossiper.check_snitch_name_matches();
            auto addr = get_broadcast_address();
            if (!_gossiper.is_safe_for_bootstrap(addr)) {
                throw std::runtime_error(sprint("A node with address %s already exists, cancelling join. "
                    "Use replace_address if you want to replace this node.", addr));
            }
            if (_db.local().get_config().consistent_rangemovement()) {
                found_bootstrapping_node = false;
                for (auto& x : _gossiper.get_endpoint_states()) {
                    auto state = _gossiper.get_gossip_status(x.second);
                    if (state == sstring(versioned_value::STATUS_UNKNOWN)) {
                        continue;
                    }
                    auto addr = x.first;
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

// Runs inside seastar::async context
void storage_service::remove_endpoint(inet_address endpoint) {
    _gossiper.remove_endpoint(endpoint);
    db::system_keyspace::remove_endpoint(endpoint).then_wrapped([endpoint] (auto&& f) {
        try {
            f.get();
        } catch (...) {
            slogger.error("fail to remove endpoint={}: {}", endpoint, std::current_exception());
        }
        return make_ready_future<>();
    }).get();
}

future<storage_service::replacement_info>
storage_service::prepare_replacement_info(std::unordered_set<gms::inet_address> initial_contact_nodes, const std::unordered_map<gms::inet_address, sstring>& loaded_peer_features, bind_messaging_port do_bind) {
    if (!_db.local().get_replace_address()) {
        throw std::runtime_error(format("replace_address is empty"));
    }
    auto replace_address = _db.local().get_replace_address().value();
    slogger.info("Gathering node replacement information for {}", replace_address);

    // if (!MessagingService.instance().isListening())
    //     MessagingService.instance().listen(FBUtilities.getLocalAddress());
    auto seeds = _gossiper.get_seeds();
    if (seeds.size() == 1 && seeds.contains(replace_address)) {
        throw std::runtime_error(format("Cannot replace_address {} because no seed node is up", replace_address));
    }

    // make magic happen
    slogger.info("Checking remote features with gossip");
    return _gossiper.do_shadow_round(initial_contact_nodes, gms::bind_messaging_port(bool(do_bind))).then([this, loaded_peer_features, replace_address] {
        auto local_features = _feature_service.known_feature_set();
        _gossiper.check_knows_remote_features(local_features, loaded_peer_features);

        // now that we've gossiped at least once, we should be able to find the node we're replacing
        auto* state = _gossiper.get_endpoint_state_for_endpoint_ptr(replace_address);
        if (!state) {
            throw std::runtime_error(format("Cannot replace_address {} because it doesn't exist in gossip", replace_address));
        }

        // Reject to replace a node that has left the ring
        auto status = _gossiper.get_gossip_status(replace_address);
        if (status == gms::versioned_value::STATUS_LEFT || status == gms::versioned_value::REMOVED_TOKEN) {
            throw std::runtime_error(format("Cannot replace_address {} because it has left the ring, status={}", replace_address, status));
        }

        auto tokens = get_tokens_for(replace_address);
        if (tokens.empty()) {
            throw std::runtime_error(format("Could not find tokens for {} to replace", replace_address));
        }

        replacement_info ret {std::move(tokens)};

        // use the replacee's host Id as our own so we receive hints, etc
        auto host_id = _gossiper.get_host_id(replace_address);
        return db::system_keyspace::set_local_host_id(host_id).discard_result().then([this, ret = std::move(ret)] () mutable {
            return _gossiper.reset_endpoint_state_map().then([ret = std::move(ret)] () mutable { // clean up since we have what we need
                return make_ready_future<replacement_info>(std::move(ret));
            });
        });
    });
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
    return run_with_no_api_lock([keyspace_name] (storage_service& ss) mutable {
        if (keyspace_name != "") {
            //find throws no such keyspace if it is missing
            const keyspace& ks = ss._db.local().find_keyspace(keyspace_name);
            // This is ugly, but it follows origin
            auto&& rs = ks.get_replication_strategy();  // clang complains about typeid(ks.get_replication_strategy());
            if (typeid(rs) == typeid(locator::local_strategy)) {
                throw std::runtime_error("Ownership values for keyspaces with LocalStrategy are meaningless");
            }
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
        }

        // The following loops seems computationally heavy, but it's not as bad.
        // The upper two simply iterate over all the endpoints by iterating over all the
        // DC and all the instances in each DC.
        //
        // The call for get_range_for_endpoint is done once per endpoint
        const auto& tm = ss.get_token_metadata();
        return do_with(dht::token::describe_ownership(tm.sorted_tokens()),
                tm.get_topology().get_datacenter_endpoints(),
                std::map<gms::inet_address, float>(),
                std::move(keyspace_name),
                [&ss](const std::map<token, float>& token_ownership, std::unordered_map<sstring,
                        std::unordered_set<gms::inet_address>>& datacenter_endpoints,
                        std::map<gms::inet_address, float>& final_ownership,
                        sstring& keyspace_name) {
            return do_for_each(datacenter_endpoints, [&ss, &keyspace_name, &final_ownership, &token_ownership](std::pair<sstring,std::unordered_set<inet_address>>&& endpoints) mutable {
                return do_with(std::unordered_set<inet_address>(endpoints.second), [&ss, &keyspace_name, &final_ownership, &token_ownership](const std::unordered_set<inet_address>& endpoints_map) mutable {
                    return do_for_each(endpoints_map, [&ss, &keyspace_name, &final_ownership, &token_ownership](const gms::inet_address& endpoint) mutable {
                        // calculate the ownership with replication and add the endpoint to the final ownership map
                        try {
                            return do_with(float(0.0f), dht::token_range_vector(ss.get_ranges_for_endpoint(keyspace_name, endpoint)),
                                    [&final_ownership, &token_ownership, &endpoint](float& ownership, const dht::token_range_vector& ranges) mutable {
                                ownership = 0.0f;
                                return do_for_each(ranges, [&token_ownership,&ownership](const dht::token_range& r) mutable {
                                    // get_ranges_for_endpoint will unwrap the first range.
                                    // With t0 t1 t2 t3, the first range (t3,t0] will be splitted
                                    // as (min,t0] and (t3,max]. Skippping the range (t3,max]
                                    // we will get the correct ownership number as if the first
                                    // range were not splitted.
                                    if (!r.end()) {
                                        return make_ready_future<>();
                                    }
                                    auto end_token = r.end()->value();
                                    auto loc = token_ownership.find(end_token);
                                    if (loc != token_ownership.end()) {
                                        ownership += loc->second;
                                    }
                                    return make_ready_future<>();
                                }).then([&final_ownership, &ownership, &endpoint]() mutable {
                                    final_ownership[endpoint] = ownership;
                                });
                            });
                        }  catch (no_such_keyspace&) {
                            // In case ss.get_ranges_for_endpoint(keyspace_name, endpoint) is not found, just mark it as zero and continue
                            final_ownership[endpoint] = 0;
                            return make_ready_future<>();
                        }

                    });
                });
            }).then([&final_ownership] {
                return final_ownership;
            });
        });
    });
}

static const std::map<storage_service::mode, sstring> mode_names = {
    {storage_service::mode::STARTING,       "STARTING"},
    {storage_service::mode::NORMAL,         "NORMAL"},
    {storage_service::mode::JOINING,        "JOINING"},
    {storage_service::mode::LEAVING,        "LEAVING"},
    {storage_service::mode::DECOMMISSIONED, "DECOMMISSIONED"},
    {storage_service::mode::MOVING,         "MOVING"},
    {storage_service::mode::DRAINING,       "DRAINING"},
    {storage_service::mode::DRAINED,        "DRAINED"},
};

std::ostream& operator<<(std::ostream& os, const storage_service::mode& m) {
    os << mode_names.at(m);
    return os;
}

void storage_service::set_mode(mode m, bool log) {
    set_mode(m, "", log);
}

void storage_service::set_mode(mode m, sstring msg, bool log) {
    _operation_mode = m;
    if (log) {
        slogger.info("{}: {}", m, msg);
    } else {
        slogger.debug("{}: {}", m, msg);
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
                return std::pair<gms::inet_address, std::optional<utils::UUID>>(host, std::nullopt);
            }
            return std::pair<gms::inet_address, std::optional<utils::UUID>>(host, f.get0());
        });
    }, std::move(results), [] (auto results, auto host_and_version) {
        auto version = host_and_version.second ? host_and_version.second->to_sstring() : UNREACHABLE;
        results.try_emplace(version).first->second.emplace_back(host_and_version.first.to_sstring());
        return results;
    }).then([] (auto results) {
        // we're done: the results map is ready to return to the client.  the rest is just debug logging:
        auto it_unreachable = results.find(UNREACHABLE);
        if (it_unreachable != results.end()) {
            slogger.debug("Hosts not in agreement. Didn't get a response from everybody: {}", ::join( ",", it_unreachable->second));
        }
        auto my_version = get_local_storage_service().get_schema_version();
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

future<sstring> storage_service::get_operation_mode() {
    return run_with_no_api_lock([] (storage_service& ss) {
        auto mode = ss._operation_mode;
        return make_ready_future<sstring>(format("{}", mode));
    });
}

future<bool> storage_service::is_starting() {
    return run_with_no_api_lock([] (storage_service& ss) {
        auto mode = ss._operation_mode;
        return mode == storage_service::mode::STARTING;
    });
}

future<bool> storage_service::is_gossip_running() {
    return run_with_no_api_lock([] (storage_service& ss) {
        return ss._gossiper.is_enabled();
    });
}

future<> storage_service::start_gossiping(bind_messaging_port do_bind) {
    return run_with_api_lock(sstring("start_gossiping"), [do_bind] (storage_service& ss) {
        return seastar::async([&ss, do_bind] {
            if (!ss._initialized) {
                slogger.warn("Starting gossip by operator request");
                auto cdc_gen_ts = db::system_keyspace::get_cdc_generation_id().get0();
                if (!cdc_gen_ts) {
                    cdc_log.warn("CDC generation timestamp missing when starting gossip");
                }
                set_gossip_tokens(ss._gossiper,
                        db::system_keyspace::get_local_tokens().get0(),
                        cdc_gen_ts);
                ss._gossiper.force_newer_generation();
                ss._gossiper.start_gossiping(utils::get_generation_number(), gms::bind_messaging_port(bool(do_bind))).then([&ss] {
                    ss._initialized = true;
                }).get();
            }
        });
    });
}

future<> storage_service::stop_gossiping() {
    return run_with_api_lock(sstring("stop_gossiping"), [] (storage_service& ss) {
        if (ss._initialized) {
            slogger.warn("Stopping gossip by operator request");
            return gms::stop_gossiping().then([&ss] {
                ss._initialized = false;
            });
        }
        return make_ready_future<>();
    });
}

future<> storage_service::do_stop_ms() {
    if (_ms_stopped) {
        return make_ready_future<>();
    }
    _ms_stopped = true;
    return _messaging.invoke_on_all([] (auto& ms) {
        return ms.shutdown();
    }).then([] {
        slogger.info("messaging_service stopped");
    });
}

future<> storage_service::do_stop_stream_manager() {
    if (_stream_manager_stopped) {
        return make_ready_future<>();
    }
    _stream_manager_stopped = true;
    return streaming::get_stream_manager().invoke_on_all([] (auto& sm) {
        return sm.stop();
    }).then([] {
        slogger.info("stream_manager stopped");
    });
}

future<> storage_service::decommission() {
    return run_with_api_lock(sstring("decommission"), [] (storage_service& ss) {
        return seastar::async([&ss] {
            auto uuid = utils::make_random_uuid();
            auto tmptr = ss.get_token_metadata_ptr();
            auto& db = ss._db.local();
            auto endpoint = ss.get_broadcast_address();
            if (!tmptr->is_member(endpoint)) {
                throw std::runtime_error("local node is not a member of the token ring yet");
            }

            auto temp = tmptr->clone_after_all_left().get0();
            auto num_tokens_after_all_left = temp.sorted_tokens().size();
            temp.clear_gently().get();
            if (num_tokens_after_all_left < 2) {
                throw std::runtime_error("no other normal nodes in the ring; decommission would be pointless");
            }

            if (ss._operation_mode != mode::NORMAL) {
                throw std::runtime_error(format("Node in {} state; wait for status to become normal or restart", ss._operation_mode));
            }

            ss.update_pending_ranges(format("decommission {}", endpoint)).get();

            auto non_system_keyspaces = db.get_non_system_keyspaces();
            for (const auto& keyspace_name : non_system_keyspaces) {
                if (ss.get_token_metadata().has_pending_ranges(keyspace_name, ss.get_broadcast_address())) {
                    throw std::runtime_error("data is currently moving to this node; unable to leave the ring");
                }
            }

            slogger.info("DECOMMISSIONING: starts");
            auto leaving_nodes = std::list<gms::inet_address>{endpoint};
            // TODO: wire ignore_nodes provided by user
            std::list<gms::inet_address> ignore_nodes;

            future<> heartbeat_updater = make_ready_future<>();
            auto heartbeat_updater_done = make_lw_shared<bool>(false);

            // Step 1: Decide who needs to sync data
            std::vector<gms::inet_address> nodes;
            for (const auto& x : tmptr->get_endpoint_to_host_id_map_for_reading()) {
                seastar::thread::maybe_yield();
                if (std::find(ignore_nodes.begin(), ignore_nodes.end(), x.first) == ignore_nodes.end()) {
                    nodes.push_back(x.first);
                }
            }
            slogger.info("decommission[{}]: Started decommission operation, removing node={}, sync_nodes={}, ignore_nodes={}", uuid, endpoint, nodes, ignore_nodes);

            // Step 2: Prepare to sync data
            std::unordered_set<gms::inet_address> nodes_unknown_verb;
            std::unordered_set<gms::inet_address> nodes_down;
            auto req = node_ops_cmd_request{node_ops_cmd::decommission_prepare, uuid, ignore_nodes, leaving_nodes, {}};
            try {
                parallel_for_each(nodes, [&ss, &req, &nodes_unknown_verb, &nodes_down, uuid] (const gms::inet_address& node) {
                    return ss._messaging.local().send_node_ops_cmd(netw::msg_addr(node), req).then([uuid, node] (node_ops_cmd_response resp) {
                        slogger.debug("decommission[{}]: Got prepare response from node={}", uuid, node);
                    }).handle_exception_type([&nodes_unknown_verb, node, uuid] (seastar::rpc::unknown_verb_error&) {
                        slogger.warn("decommission[{}]: Node {} does not support decommission verb", uuid, node);
                        nodes_unknown_verb.emplace(node);
                    }).handle_exception_type([&nodes_down, node, uuid] (seastar::rpc::closed_error&) {
                        slogger.warn("decommission[{}]: Node {} is down for node_ops_cmd verb", uuid, node);
                        nodes_down.emplace(node);
                    });
                }).get();
                if (!nodes_unknown_verb.empty()) {
                    auto msg = format("decommission[{}]: Nodes={} do not support decommission verb. Please upgrade your cluster and run decommission again.", uuid, nodes_unknown_verb);
                    slogger.warn("{}", msg);
                    throw std::runtime_error(msg);
                }
                if (!nodes_down.empty()) {
                    auto msg = format("decommission[{}]: Nodes={} needed for decommission operation are down. It is highly recommended to fix the down nodes and try again. To proceed with best-effort mode which might cause data inconsistency, run nodetool decommission --ignore-dead-nodes <list_of_dead_nodes>. E.g., nodetool decommission --ignore-dead-nodes 127.0.0.1,127.0.0.2", uuid, nodes_down);
                    slogger.warn("{}", msg);
                    throw std::runtime_error(msg);
                }

                // Step 3: Start heartbeat updater
                heartbeat_updater = seastar::async([&ss, &nodes, uuid, heartbeat_updater_done] {
                    slogger.debug("decommission[{}]: Started heartbeat_updater", uuid);
                    while (!(*heartbeat_updater_done)) {
                        auto req = node_ops_cmd_request{node_ops_cmd::decommission_heartbeat, uuid, {}, {}, {}};
                        parallel_for_each(nodes, [&ss, &req, uuid] (const gms::inet_address& node) {
                            return ss._messaging.local().send_node_ops_cmd(netw::msg_addr(node), req).then([uuid, node] (node_ops_cmd_response resp) {
                                slogger.debug("decommission[{}]: Got heartbeat response from node={}", uuid, node);
                                return make_ready_future<>();
                            });
                        }).handle_exception([uuid] (std::exception_ptr ep) {
                            slogger.warn("decommission[{}]: Failed to send heartbeat: {}", uuid, ep);
                        }).get();
                        int nr_seconds = 10;
                        while (!(*heartbeat_updater_done) && nr_seconds--) {
                            sleep(std::chrono::seconds(1)).get();
                        }
                    }
                    slogger.debug("decommission[{}]: Stopped heartbeat_updater", uuid);
                });
                auto stop_heartbeat_updater = defer([&] {
                    *heartbeat_updater_done = true;
                    heartbeat_updater.get();
                });

                // Step 5: Start to sync data
                slogger.info("DECOMMISSIONING: unbootstrap starts");
                ss.unbootstrap();
                slogger.info("DECOMMISSIONING: unbootstrap done");

                // Step 6: Finish
                req.cmd = node_ops_cmd::decommission_done;
                parallel_for_each(nodes, [&ss, &req, uuid] (const gms::inet_address& node) {
                    return ss._messaging.local().send_node_ops_cmd(netw::msg_addr(node), req).then([uuid, node] (node_ops_cmd_response resp) {
                        slogger.debug("decommission[{}]: Got done response from node={}", uuid, node);
                        return make_ready_future<>();
                    });
                }).get();
                slogger.info("decommission[{}]: Finished decommission operation, removing node={}, sync_nodes={}, ignore_nodes={}", uuid, endpoint, nodes, ignore_nodes);
            } catch (...) {
                slogger.warn("decommission[{}]: Abort decommission operation started, removing node={}, sync_nodes={}, ignore_nodes={}", uuid, endpoint, nodes, ignore_nodes);
                // we need to revert the effect of prepare verb the decommission ops is failed
                req.cmd = node_ops_cmd::decommission_abort;
                parallel_for_each(nodes, [&ss, &req, &nodes_unknown_verb, &nodes_down, uuid] (const gms::inet_address& node) {
                    if (nodes_unknown_verb.contains(node) || nodes_down.contains(node)) {
                        // No need to revert previous prepare cmd for those who do not apply prepare cmd.
                        return make_ready_future<>();
                    }
                    return ss._messaging.local().send_node_ops_cmd(netw::msg_addr(node), req).then([uuid, node] (node_ops_cmd_response resp) {
                        slogger.debug("decommission[{}]: Got abort response from node={}", uuid, node);
                    });
                }).get();
                slogger.warn("decommission[{}]: Abort decommission operation finished, removing node={}, sync_nodes={}, ignore_nodes={}", uuid, endpoint, nodes, ignore_nodes);
                throw;
            }

            ss.shutdown_client_servers();
            slogger.info("DECOMMISSIONING: shutdown rpc and cql server done");

            db::get_batchlog_manager().invoke_on_all([] (auto& bm) {
                return bm.stop();
            }).get();
            slogger.info("DECOMMISSIONING: stop batchlog_manager done");

            gms::stop_gossiping().get();
            slogger.info("DECOMMISSIONING: stop_gossiping done");
            ss.do_stop_ms().get();
            slogger.info("DECOMMISSIONING: stop messaging_service done");
            // StageManager.shutdownNow();
            db::system_keyspace::set_bootstrap_state(db::system_keyspace::bootstrap_state::DECOMMISSIONED).get();
            slogger.info("DECOMMISSIONING: set_bootstrap_state done");
            ss.set_mode(mode::DECOMMISSIONED, true);
            slogger.info("DECOMMISSIONING: done");
            // let op be responsible for killing the process
        });
    });
}

// Runs inside seastar::async context
void storage_service::run_bootstrap_ops() {
    auto uuid = utils::make_random_uuid();
    // TODO: Specify ignore_nodes
    std::list<gms::inet_address> ignore_nodes;
    std::list<gms::inet_address> sync_nodes;

    auto start_time = std::chrono::steady_clock::now();
    for (;;) {
        sync_nodes.clear();
        // Step 1: Decide who needs to sync data for bootstrap operation
        for (const auto& x :_gossiper.endpoint_state_map) {
            seastar::thread::maybe_yield();
            const auto& node = x.first;
            slogger.info("bootstrap[{}]: Check node={}, status={}", uuid, node, _gossiper.get_gossip_status(node));
            if (node != get_broadcast_address() &&
                    _gossiper.is_normal_ring_member(node) &&
                    std::find(ignore_nodes.begin(), ignore_nodes.end(), x.first) == ignore_nodes.end()) {
                sync_nodes.push_back(node);
            }
        }
        sync_nodes.push_front(get_broadcast_address());

        // Step 2: Wait until no pending node operations
        std::unordered_map<gms::inet_address, std::list<utils::UUID>> pending_ops;
        auto req = node_ops_cmd_request(node_ops_cmd::query_pending_ops, uuid);
        parallel_for_each(sync_nodes, [this, req, uuid, &pending_ops] (const gms::inet_address& node) {
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
                throw std::runtime_error(format("bootstrap[{}]: Found pending node ops = {}, reject bootstrap", uuid, pending_ops));
            }
            slogger.warn("bootstrap[{}]: Found pending node ops = {}, sleep 5 seconds and check again", uuid, pending_ops);
            sleep_abortable(std::chrono::seconds(5), _abort_source).get();
        }
    }

    std::unordered_set<gms::inet_address> nodes_unknown_verb;
    std::unordered_set<gms::inet_address> nodes_down;
    std::unordered_set<gms::inet_address> nodes_aborted;
    auto tokens = std::list<dht::token>(_bootstrap_tokens.begin(), _bootstrap_tokens.end());
    std::unordered_map<gms::inet_address, std::list<dht::token>> bootstrap_nodes = {
        {get_broadcast_address(), tokens},
    };
    auto req = node_ops_cmd_request(node_ops_cmd::bootstrap_prepare, uuid, ignore_nodes, {}, {}, bootstrap_nodes);
    slogger.info("bootstrap[{}]: Started bootstrap operation, bootstrap_nodes={}, sync_nodes={}, ignore_nodes={}", uuid, bootstrap_nodes, sync_nodes, ignore_nodes);
    future<> heartbeat_updater = make_ready_future<>();
    auto heartbeat_updater_done = make_lw_shared<bool>(false);
    try {
        // Step 3: Prepare to sync data
        parallel_for_each(sync_nodes, [this, &req, &nodes_unknown_verb, &nodes_down, uuid] (const gms::inet_address& node) {
            return _messaging.local().send_node_ops_cmd(netw::msg_addr(node), req).then([uuid, node] (node_ops_cmd_response resp) {
                slogger.debug("bootstrap[{}]: Got node_ops_cmd::bootstrap_prepare response from node={}", uuid, node);
            }).handle_exception_type([&nodes_unknown_verb, node, uuid] (seastar::rpc::unknown_verb_error&) {
                slogger.warn("bootstrap[{}]: Node {} does not support node_ops_cmd verb", uuid, node);
                nodes_unknown_verb.emplace(node);
            }).handle_exception_type([&nodes_down, node, uuid] (seastar::rpc::closed_error&) {
                slogger.warn("bootstrap[{}]: Node {} is down for node_ops_cmd verb", uuid, node);
                nodes_down.emplace(node);
            });
        }).get();
        if (!nodes_unknown_verb.empty()) {
            auto msg = format("bootstrap[{}]: Nodes={} do not support bootstrap verb. Please upgrade your cluster and run bootstrap again.", uuid, nodes_unknown_verb);
            slogger.warn("{}", msg);
            throw std::runtime_error(msg);
        }
        if (!nodes_down.empty()) {
            auto msg = format("bootstrap[{}]: Nodes={} needed for bootstrap operation are down. It is highly recommended to fix the down nodes and try again. To proceed with best-effort mode which might cause data inconsistency, add --ignore-dead-nodes <list_of_dead_nodes>. E.g., scylla --ignore-dead-nodes 127.0.0.1,127.0.0.2", uuid, nodes_down);
            slogger.warn("{}", msg);
            throw std::runtime_error(msg);
        }

        // Step 4: Start heartbeat updater
        heartbeat_updater = seastar::async([this, &sync_nodes, uuid, heartbeat_updater_done] {
            slogger.debug("bootstrap[{}]: Started heartbeat_updater", uuid);
            while (!(*heartbeat_updater_done)) {
                auto req = node_ops_cmd_request(node_ops_cmd::bootstrap_heartbeat, uuid);
                parallel_for_each(sync_nodes, [this, req, uuid] (const gms::inet_address& node) {
                    return _messaging.local().send_node_ops_cmd(netw::msg_addr(node), req).then([uuid, node] (node_ops_cmd_response resp) {
                        slogger.debug("bootstrap[{}]: Got heartbeat response from node={}", uuid, node);
                        return make_ready_future<>();
                    });
                }).handle_exception([uuid] (std::exception_ptr ep) {
                    slogger.warn("bootstrap[{}]: Failed to send heartbeat: {}", uuid, ep);
                }).get();
                int nr_seconds = 10;
                while (!(*heartbeat_updater_done) && nr_seconds--) {
                    sleep(std::chrono::seconds(1)).get();
                }
            }
            slogger.debug("bootstrap[{}]: Stopped heartbeat_updater", uuid);
        });
        auto stop_heartbeat_updater = defer([&] {
            *heartbeat_updater_done = true;
            heartbeat_updater.get();
        });

        // Step 5: Sync data for bootstrap
        _repair.local().bootstrap_with_repair(get_token_metadata_ptr(), _bootstrap_tokens).get();

        // Step 6: Finish
        req.cmd = node_ops_cmd::bootstrap_done;
        parallel_for_each(sync_nodes, [this, &req, &nodes_aborted, uuid] (const gms::inet_address& node) {
            return _messaging.local().send_node_ops_cmd(netw::msg_addr(node), req).then([&nodes_aborted, uuid, node] (node_ops_cmd_response resp) {
                nodes_aborted.emplace(node);
                slogger.debug("bootstrap[{}]: Got done response from node={}", uuid, node);
                return make_ready_future<>();
            });
        }).get();
    } catch (...) {
        slogger.error("bootstrap[{}]: Abort bootstrap operation started, bootstrap_nodes={}, sync_nodes={}, ignore_nodes={}: {}",
                uuid, bootstrap_nodes, sync_nodes, ignore_nodes, std::current_exception());
        // we need to revert the effect of prepare verb the bootstrap ops is failed
        req.cmd = node_ops_cmd::bootstrap_abort;
        parallel_for_each(sync_nodes, [this, &req, &nodes_unknown_verb, &nodes_down, &nodes_aborted, uuid] (const gms::inet_address& node) {
            if (nodes_unknown_verb.contains(node) || nodes_down.contains(node) || nodes_aborted.contains(node)) {
                // No need to revert previous prepare cmd for those who do not apply prepare cmd.
                return make_ready_future<>();
            }
            return _messaging.local().send_node_ops_cmd(netw::msg_addr(node), req).then([uuid, node] (node_ops_cmd_response resp) {
                slogger.debug("bootstrap[{}]: Got abort response from node={}", uuid, node);
            });
        }).get();
        slogger.error("bootstrap[{}]: Abort bootstrap operation finished, bootstrap_nodes={}, sync_nodes={}, ignore_nodes={}: {}",
                uuid, bootstrap_nodes, sync_nodes, ignore_nodes, std::current_exception());
        throw;
    }
}

// Runs inside seastar::async context
void storage_service::run_replace_ops() {
    if (!_db.local().get_replace_address()) {
        throw std::runtime_error(format("replace_address is empty"));
    }
    auto replace_address = _db.local().get_replace_address().value();
    auto uuid = utils::make_random_uuid();
    // TODO: Specify ignore_nodes
    std::list<gms::inet_address> ignore_nodes;
    // Step 1: Decide who needs to sync data for replace operation
    std::list<gms::inet_address> sync_nodes;
    for (const auto& x :_gossiper.endpoint_state_map) {
        seastar::thread::maybe_yield();
        const auto& node = x.first;
        slogger.debug("replace[{}]: Check node={}, status={}", uuid, node, _gossiper.get_gossip_status(node));
        if (node != get_broadcast_address() &&
                node != replace_address &&
                _gossiper.is_normal_ring_member(node) &&
                std::find(ignore_nodes.begin(), ignore_nodes.end(), x.first) == ignore_nodes.end()) {
            sync_nodes.push_back(node);
        }
    }
    sync_nodes.push_front(get_broadcast_address());
    auto sync_nodes_generations = _gossiper.get_generation_for_nodes(sync_nodes).get();
    // Map existing nodes to replacing nodes
    std::unordered_map<gms::inet_address, gms::inet_address> replace_nodes = {
        {replace_address, get_broadcast_address()},
    };
    std::unordered_set<gms::inet_address> nodes_unknown_verb;
    std::unordered_set<gms::inet_address> nodes_down;
    std::unordered_set<gms::inet_address> nodes_aborted;
    auto req = node_ops_cmd_request{node_ops_cmd::replace_prepare, uuid, ignore_nodes, {}, replace_nodes};
    slogger.info("replace[{}]: Started replace operation, replace_nodes={}, sync_nodes={}, ignore_nodes={}", uuid, replace_nodes, sync_nodes, ignore_nodes);
    future<> heartbeat_updater = make_ready_future<>();
    auto heartbeat_updater_done = make_lw_shared<bool>(false);
    try {
        // Step 2: Prepare to sync data
        parallel_for_each(sync_nodes, [this, &req, &nodes_unknown_verb, &nodes_down, uuid] (const gms::inet_address& node) {
            return _messaging.local().send_node_ops_cmd(netw::msg_addr(node), req).then([uuid, node] (node_ops_cmd_response resp) {
                slogger.debug("replace[{}]: Got node_ops_cmd::replace_prepare response from node={}", uuid, node);
            }).handle_exception_type([&nodes_unknown_verb, node, uuid] (seastar::rpc::unknown_verb_error&) {
                slogger.warn("replace[{}]: Node {} does not support node_ops_cmd verb", uuid, node);
                nodes_unknown_verb.emplace(node);
            }).handle_exception_type([&nodes_down, node, uuid] (seastar::rpc::closed_error&) {
                slogger.warn("replace[{}]: Node {} is down for node_ops_cmd verb", uuid, node);
                nodes_down.emplace(node);
            });
        }).get();
        if (!nodes_unknown_verb.empty()) {
            auto msg = format("replace[{}]: Nodes={} do not support replace verb. Please upgrade your cluster and run replace again.", uuid, nodes_unknown_verb);
            slogger.warn("{}", msg);
            throw std::runtime_error(msg);
        }
        if (!nodes_down.empty()) {
            auto msg = format("replace[{}]: Nodes={} needed for replace operation are down. It is highly recommended to fix the down nodes and try again. To proceed with best-effort mode which might cause data inconsistency, add --ignore-dead-nodes <list_of_dead_nodes>. E.g., scylla --ignore-dead-nodes 127.0.0.1,127.0.0.2", uuid, nodes_down);
            slogger.warn("{}", msg);
            throw std::runtime_error(msg);
        }

        // Step 3: Start heartbeat updater
        heartbeat_updater = seastar::async([this, &sync_nodes, uuid, heartbeat_updater_done] {
            slogger.debug("replace[{}]: Started heartbeat_updater", uuid);
            while (!(*heartbeat_updater_done)) {
                auto req = node_ops_cmd_request{node_ops_cmd::replace_heartbeat, uuid, {}, {}, {}};
                parallel_for_each(sync_nodes, [this, req, uuid] (const gms::inet_address& node) {
                    return _messaging.local().send_node_ops_cmd(netw::msg_addr(node), req).then([uuid, node] (node_ops_cmd_response resp) {
                        slogger.debug("replace[{}]: Got heartbeat response from node={}", uuid, node);
                        return make_ready_future<>();
                    });
                }).handle_exception([uuid] (std::exception_ptr ep) {
                    slogger.warn("replace[{}]: Failed to send heartbeat: {}", uuid, ep);
                }).get();
                int nr_seconds = 10;
                while (!(*heartbeat_updater_done) && nr_seconds--) {
                    sleep_abortable(std::chrono::seconds(1), _abort_source).get();
                }
            }
            slogger.debug("replace[{}]: Stopped heartbeat_updater", uuid);
        });
        auto stop_heartbeat_updater = defer([&] {
            *heartbeat_updater_done = true;
            heartbeat_updater.get();
        });


        // Step 4: Allow nodes in sync_nodes list to mark the replacing node as alive
        _gossiper.advertise_to_nodes(sync_nodes_generations).get();
        slogger.info("replace[{}]: Allow nodes={} to mark replacing node={} as alive", uuid, sync_nodes, get_broadcast_address());

        // Step 5: Wait for nodes to finish marking the replacing node as live
        req.cmd = node_ops_cmd::replace_prepare_mark_alive;
        parallel_for_each(sync_nodes, [this, &req, uuid] (const gms::inet_address& node) {
            return _messaging.local().send_node_ops_cmd(netw::msg_addr(node), req).then([uuid, node] (node_ops_cmd_response resp) {
                slogger.debug("replace[{}]: Got prepare_mark_alive response from node={}", uuid, node);
                return make_ready_future<>();
            });
        }).get();

        // Step 6: Update pending ranges on nodes
        req.cmd = node_ops_cmd::replace_prepare_pending_ranges;
        parallel_for_each(sync_nodes, [this, &req, uuid] (const gms::inet_address& node) {
            return _messaging.local().send_node_ops_cmd(netw::msg_addr(node), req).then([uuid, node] (node_ops_cmd_response resp) {
                slogger.debug("replace[{}]: Got pending_ranges response from node={}", uuid, node);
                return make_ready_future<>();
            });
        }).get();


        // Step 7: Sync data for replace
        if (is_repair_based_node_ops_enabled()) {
            slogger.info("replace[{}]: Using repair based node ops to sync data", uuid);
            _repair.local().replace_with_repair(get_token_metadata_ptr(), _bootstrap_tokens).get();
        } else {
            slogger.info("replace[{}]: Using streaming based node ops to sync data", uuid);
            dht::boot_strapper bs(_db, _abort_source, get_broadcast_address(), _bootstrap_tokens, get_token_metadata_ptr());
            bs.bootstrap(streaming::stream_reason::replace).get();
        }


        // Step 8: Finish
        req.cmd = node_ops_cmd::replace_done;
        parallel_for_each(sync_nodes, [this, &req, &nodes_aborted, uuid] (const gms::inet_address& node) {
            return _messaging.local().send_node_ops_cmd(netw::msg_addr(node), req).then([&nodes_aborted, uuid, node] (node_ops_cmd_response resp) {
                nodes_aborted.emplace(node);
                slogger.debug("replace[{}]: Got done response from node={}", uuid, node);
                return make_ready_future<>();
            });
        }).get();
        // Allow any nodes to mark the replacing node as alive
        _gossiper.advertise_to_nodes({}).get();
        slogger.info("replace[{}]: Allow any nodes to mark replacing node={} as alive", uuid,  get_broadcast_address());
    } catch (...) {
        slogger.error("replace[{}]: Abort replace operation started, replace_nodes={}, sync_nodes={}, ignore_nodes={}: {}",
                uuid, replace_nodes, sync_nodes, ignore_nodes, std::current_exception());
        // we need to revert the effect of prepare verb the replace ops is failed
        req.cmd = node_ops_cmd::replace_abort;
        parallel_for_each(sync_nodes, [this, &req, &nodes_unknown_verb, &nodes_down, &nodes_aborted, uuid] (const gms::inet_address& node) {
            if (nodes_unknown_verb.contains(node) || nodes_down.contains(node) || nodes_aborted.contains(node)) {
                // No need to revert previous prepare cmd for those who do not apply prepare cmd.
                return make_ready_future<>();
            }
            return _messaging.local().send_node_ops_cmd(netw::msg_addr(node), req).then([uuid, node] (node_ops_cmd_response resp) {
                slogger.debug("replace[{}]: Got abort response from node={}", uuid, node);
            });
        }).get();
        slogger.error("replace[{}]: Abort replace operation finished, replace_nodes={}, sync_nodes={}, ignore_nodes={}: {}",
                uuid, replace_nodes, sync_nodes, ignore_nodes, std::current_exception());
        throw;
    }
}

future<> storage_service::removenode(sstring host_id_string, std::list<gms::inet_address> ignore_nodes) {
    return run_with_api_lock(sstring("removenode"), [host_id_string, ignore_nodes = std::move(ignore_nodes)] (storage_service& ss) mutable {
        return seastar::async([&ss, host_id_string, ignore_nodes = std::move(ignore_nodes)] {
            auto uuid = utils::make_random_uuid();
            auto tmptr = ss.get_token_metadata_ptr();
            auto host_id = utils::UUID(host_id_string);
            auto endpoint_opt = tmptr->get_endpoint_for_host_id(host_id);
            if (!endpoint_opt) {
                throw std::runtime_error(format("removenode[{}]: Host ID not found in the cluster", uuid));
            }
            auto endpoint = *endpoint_opt;
            auto tokens = tmptr->get_tokens(endpoint);
            auto leaving_nodes = std::list<gms::inet_address>{endpoint};

            future<> heartbeat_updater = make_ready_future<>();
            auto heartbeat_updater_done = make_lw_shared<bool>(false);

            // Step 1: Decide who needs to sync data
            //
            // By default, we require all nodes in the cluster to participate
            // the removenode operation and sync data if needed. We fail the
            // removenode operation if any of them is down or fails.
            //
            // If the user want the removenode opeartion to succeed even if some of the nodes
            // are not available, the user has to explicitly pass a list of
            // node that can be skipped for the operation.
            std::vector<gms::inet_address> nodes;
            for (const auto& x : tmptr->get_endpoint_to_host_id_map_for_reading()) {
                seastar::thread::maybe_yield();
                if (x.first != endpoint && std::find(ignore_nodes.begin(), ignore_nodes.end(), x.first) == ignore_nodes.end()) {
                    nodes.push_back(x.first);
                }
            }
            slogger.info("removenode[{}]: Started removenode operation, removing node={}, sync_nodes={}, ignore_nodes={}", uuid, endpoint, nodes, ignore_nodes);

            // Step 2: Prepare to sync data
            std::unordered_set<gms::inet_address> nodes_unknown_verb;
            std::unordered_set<gms::inet_address> nodes_down;
            auto req = node_ops_cmd_request{node_ops_cmd::removenode_prepare, uuid, ignore_nodes, leaving_nodes, {}};
            try {
                parallel_for_each(nodes, [&ss, &req, &nodes_unknown_verb, &nodes_down, uuid] (const gms::inet_address& node) {
                    return ss._messaging.local().send_node_ops_cmd(netw::msg_addr(node), req).then([uuid, node] (node_ops_cmd_response resp) {
                        slogger.debug("removenode[{}]: Got prepare response from node={}", uuid, node);
                    }).handle_exception_type([&nodes_unknown_verb, node, uuid] (seastar::rpc::unknown_verb_error&) {
                        slogger.warn("removenode[{}]: Node {} does not support removenode verb", uuid, node);
                        nodes_unknown_verb.emplace(node);
                    }).handle_exception_type([&nodes_down, node, uuid] (seastar::rpc::closed_error&) {
                        slogger.warn("removenode[{}]: Node {} is down for node_ops_cmd verb", uuid, node);
                        nodes_down.emplace(node);
                    });
                }).get();
                if (!nodes_unknown_verb.empty()) {
                    auto msg = format("removenode[{}]: Nodes={} do not support removenode verb. Please upgrade your cluster and run removenode again.", uuid, nodes_unknown_verb);
                    slogger.warn("{}", msg);
                    throw std::runtime_error(msg);
                }
                if (!nodes_down.empty()) {
                    auto msg = format("removenode[{}]: Nodes={} needed for removenode operation are down. It is highly recommended to fix the down nodes and try again. To proceed with best-effort mode which might cause data inconsistency, run nodetool removenode --ignore-dead-nodes <list_of_dead_nodes> <host_id>. E.g., nodetool removenode --ignore-dead-nodes 127.0.0.1,127.0.0.2 817e9515-316f-4fe3-aaab-b00d6f12dddd", uuid, nodes_down);
                    slogger.warn("{}", msg);
                    throw std::runtime_error(msg);
                }

                // Step 3: Start heartbeat updater
                heartbeat_updater = seastar::async([&ss, &nodes, uuid, heartbeat_updater_done] {
                    slogger.debug("removenode[{}]: Started heartbeat_updater", uuid);
                    while (!(*heartbeat_updater_done)) {
                        auto req = node_ops_cmd_request{node_ops_cmd::removenode_heartbeat, uuid, {}, {}, {}};
                        parallel_for_each(nodes, [&ss, &req, uuid] (const gms::inet_address& node) {
                            return ss._messaging.local().send_node_ops_cmd(netw::msg_addr(node), req).then([uuid, node] (node_ops_cmd_response resp) {
                                slogger.debug("removenode[{}]: Got heartbeat response from node={}", uuid, node);
                                return make_ready_future<>();
                            });
                        }).handle_exception([uuid] (std::exception_ptr ep) {
                            slogger.warn("removenode[{}]: Failed to send heartbeat", uuid);
                        }).get();
                        int nr_seconds = 10;
                        while (!(*heartbeat_updater_done) && nr_seconds--) {
                            sleep_abortable(std::chrono::seconds(1), ss._abort_source).get();
                        }
                    }
                    slogger.debug("removenode[{}]: Stopped heartbeat_updater", uuid);
                });
                auto stop_heartbeat_updater = defer([&] {
                    *heartbeat_updater_done = true;
                    heartbeat_updater.get();
                });

                // Step 4: Start to sync data
                req.cmd = node_ops_cmd::removenode_sync_data;
                parallel_for_each(nodes, [&ss, &req, uuid] (const gms::inet_address& node) {
                    return ss._messaging.local().send_node_ops_cmd(netw::msg_addr(node), req).then([uuid, node] (node_ops_cmd_response resp) {
                        slogger.debug("removenode[{}]: Got sync_data response from node={}", uuid, node);
                        return make_ready_future<>();
                    });
                }).get();


                // Step 5: Announce the node has left
                ss._gossiper.advertise_token_removed(endpoint, host_id).get();
                std::unordered_set<token> tmp(tokens.begin(), tokens.end());
                ss.excise(std::move(tmp), endpoint);

                // Step 6: Finish
                req.cmd = node_ops_cmd::removenode_done;
                parallel_for_each(nodes, [&ss, &req, uuid] (const gms::inet_address& node) {
                    return ss._messaging.local().send_node_ops_cmd(netw::msg_addr(node), req).then([uuid, node] (node_ops_cmd_response resp) {
                        slogger.debug("removenode[{}]: Got done response from node={}", uuid, node);
                        return make_ready_future<>();
                    });
                }).get();
                slogger.info("removenode[{}]: Finished removenode operation, removing node={}, sync_nodes={}, ignore_nodes={}", uuid, endpoint, nodes, ignore_nodes);
            } catch (...) {
                // we need to revert the effect of prepare verb the removenode ops is failed
                req.cmd = node_ops_cmd::removenode_abort;
                parallel_for_each(nodes, [&ss, &req, &nodes_unknown_verb, &nodes_down, uuid] (const gms::inet_address& node) {
                    if (nodes_unknown_verb.contains(node) || nodes_down.contains(node)) {
                        // No need to revert previous prepare cmd for those who do not apply prepare cmd.
                        return make_ready_future<>();
                    }
                    return ss._messaging.local().send_node_ops_cmd(netw::msg_addr(node), req).then([uuid, node] (node_ops_cmd_response resp) {
                        slogger.debug("removenode[{}]: Got abort response from node={}", uuid, node);
                    });
                }).get();
                slogger.info("removenode[{}]: Aborted removenode operation, removing node={}, sync_nodes={}, ignore_nodes={}", uuid, endpoint, nodes, ignore_nodes);
                throw;
            }
        });
    });
}

void storage_service::node_ops_cmd_check(gms::inet_address coordinator, const node_ops_cmd_request& req) {
    auto ops_uuids = boost::copy_range<std::vector<utils::UUID>>(_node_ops| boost::adaptors::map_keys);
    std::string msg;
    if (req.cmd == node_ops_cmd::removenode_prepare || req.cmd == node_ops_cmd::replace_prepare ||
            req.cmd == node_ops_cmd::decommission_prepare || req.cmd == node_ops_cmd::bootstrap_prepare) {
        // Peer node wants to start a new node operation. Make sure no pending node operation is in progress.
        if (!_node_ops.empty()) {
            msg = format("node_ops_cmd_check: Node {} rejected node_ops_cmd={} from node={} with ops_uuid={}, pending_node_ops={}, pending node ops is in progress",
                    get_broadcast_address(), uint32_t(req.cmd), coordinator, req.ops_uuid, ops_uuids);
        }
    } else {
        if (ops_uuids.size() == 1 && ops_uuids.front() == req.ops_uuid) {
            // Check is good, since we know this ops_uuid and this is the only ops_uuid we are working on.
        } else if (ops_uuids.size() == 0) {
            // The ops_uuid received is unknown. Fail the request.
            msg = format("node_ops_cmd_check: Node {} rejected node_ops_cmd={} from node={} with ops_uuid={}, pending_node_ops={}, the node ops is unknown",
                    get_broadcast_address(), uint32_t(req.cmd), coordinator, req.ops_uuid, ops_uuids);
        } else {
            // Other node ops is in progress. Fail the request.
            msg = format("node_ops_cmd_check: Node {} rejected node_ops_cmd={} from node={} with ops_uuid={}, pending_node_ops={}, pending node ops is in progress",
                    get_broadcast_address(), uint32_t(req.cmd), coordinator, req.ops_uuid, ops_uuids);
        }
    }
    if (!msg.empty()) {
        slogger.warn("{}", msg);
        throw std::runtime_error(msg);
    }
}

future<node_ops_cmd_response> storage_service::node_ops_cmd_handler(gms::inet_address coordinator, node_ops_cmd_request req) {
    return get_storage_service().invoke_on(0, [coordinator, req = std::move(req)] (auto& ss) mutable {
        return seastar::async([&ss, coordinator, req = std::move(req)] () mutable {
            auto ops_uuid = req.ops_uuid;
            slogger.debug("node_ops_cmd_handler cmd={}, ops_uuid={}", uint32_t(req.cmd), ops_uuid);

            if (req.cmd == node_ops_cmd::query_pending_ops) {
                bool ok = true;
                auto ops_uuids = boost::copy_range<std::list<utils::UUID>>(ss._node_ops| boost::adaptors::map_keys);
                node_ops_cmd_response resp(ok, ops_uuids);
                slogger.debug("node_ops_cmd_handler: Got query_pending_ops request from {}, pending_ops={}", coordinator, ops_uuids);
                return resp;
            } else if (req.cmd == node_ops_cmd::repair_updater) {
                slogger.debug("repair[{}]: Got repair_updater request from {}", ops_uuid, coordinator);
                ss._db.invoke_on_all([coordinator, ops_uuid, tables = req.repair_tables] (database &db) {
                    for (const auto& table_id : tables) {
                        auto& table = db.find_column_family(table_id);
                        table.update_off_strategy_trigger();
                        slogger.debug("repair[{}]: Updated off_strategy_trigger for table {}.{} by node {}",
                                ops_uuid, table.schema()->ks_name(), table.schema()->cf_name(), coordinator);
                    }
                }).get();
                bool ok = true;
                return node_ops_cmd_response(ok);
            }

            ss.node_ops_cmd_check(coordinator, req);

            if (req.cmd == node_ops_cmd::removenode_prepare) {
                if (req.leaving_nodes.size() > 1) {
                    auto msg = format("removenode[{}]: Could not removenode more than one node at a time: leaving_nodes={}", req.ops_uuid, req.leaving_nodes);
                    slogger.warn("{}", msg);
                    throw std::runtime_error(msg);
                }
                ss.mutate_token_metadata([coordinator, &req, &ss] (mutable_token_metadata_ptr tmptr) mutable {
                    for (auto& node : req.leaving_nodes) {
                        slogger.info("removenode[{}]: Added node={} as leaving node, coordinator={}", req.ops_uuid, node, coordinator);
                        tmptr->add_leaving_endpoint(node);
                    }
                    return ss.update_pending_ranges(tmptr, format("removenode {}", req.leaving_nodes));
                }).get();
                auto ops = seastar::make_shared<node_ops_info>(node_ops_info{ops_uuid, false, std::move(req.ignore_nodes)});
                auto meta = node_ops_meta_data(ops_uuid, coordinator, std::move(ops), [&ss, coordinator, req = std::move(req)] () mutable {
                    return ss.mutate_token_metadata([&ss, coordinator, req = std::move(req)] (mutable_token_metadata_ptr tmptr) mutable {
                        for (auto& node : req.leaving_nodes) {
                            slogger.info("removenode[{}]: Removed node={} as leaving node, coordinator={}", req.ops_uuid, node, coordinator);
                            tmptr->del_leaving_endpoint(node);
                        }
                        return ss.update_pending_ranges(tmptr, format("removenode {}", req.leaving_nodes));
                    });
                },
                [&ss, ops_uuid] () mutable { ss.node_ops_singal_abort(ops_uuid); });
                ss._node_ops.emplace(ops_uuid, std::move(meta));
            } else if (req.cmd == node_ops_cmd::removenode_heartbeat) {
                slogger.debug("removenode[{}]: Updated heartbeat from coordinator={}", req.ops_uuid,  coordinator);
                ss.node_ops_update_heartbeat(ops_uuid);
            } else if (req.cmd == node_ops_cmd::removenode_done) {
                slogger.info("removenode[{}]: Marked ops done from coordinator={}", req.ops_uuid, coordinator);
                ss.node_ops_done(ops_uuid);
            } else if (req.cmd == node_ops_cmd::removenode_sync_data) {
                auto it = ss._node_ops.find(ops_uuid);
                if (it == ss._node_ops.end()) {
                    throw std::runtime_error(format("removenode[{}]: Can not find ops_uuid={}", ops_uuid, ops_uuid));
                }
                auto ops = it->second.get_ops_info();
                auto as = it->second.get_abort_source();
                for (auto& node : req.leaving_nodes) {
                    if (ss.is_repair_based_node_ops_enabled()) {
                        slogger.info("removenode[{}]: Started to sync data for removing node={} using repair, coordinator={}", req.ops_uuid, node, coordinator);
                        ss._repair.local().removenode_with_repair(ss.get_token_metadata_ptr(), node, ops).get();
                    } else {
                        slogger.info("removenode[{}]: Started to sync data for removing node={} using stream, coordinator={}", req.ops_uuid, node, coordinator);
                        ss.removenode_with_stream(node, as).get();
                    }
                }
            } else if (req.cmd == node_ops_cmd::removenode_abort) {
                ss.node_ops_abort(ops_uuid);
            } else if (req.cmd == node_ops_cmd::decommission_prepare) {
                if (req.leaving_nodes.size() > 1) {
                    auto msg = format("decommission[{}]: Could not decommission more than one node at a time: leaving_nodes={}", req.ops_uuid, req.leaving_nodes);
                    slogger.warn("{}", msg);
                    throw std::runtime_error(msg);
                }
                ss.mutate_token_metadata([coordinator, &req, &ss] (mutable_token_metadata_ptr tmptr) mutable {
                    for (auto& node : req.leaving_nodes) {
                        slogger.info("decommission[{}]: Added node={} as leaving node, coordinator={}", req.ops_uuid, node, coordinator);
                        tmptr->add_leaving_endpoint(node);
                    }
                    return ss.update_pending_ranges(tmptr, format("decommission {}", req.leaving_nodes));
                }).get();
                auto ops = seastar::make_shared<node_ops_info>(node_ops_info{ops_uuid, false, std::move(req.ignore_nodes)});
                auto meta = node_ops_meta_data(ops_uuid, coordinator, std::move(ops), [&ss, coordinator, req = std::move(req)] () mutable {
                    return ss.mutate_token_metadata([&ss, coordinator, req = std::move(req)] (mutable_token_metadata_ptr tmptr) mutable {
                        for (auto& node : req.leaving_nodes) {
                            slogger.info("decommission[{}]: Removed node={} as leaving node, coordinator={}", req.ops_uuid, node, coordinator);
                            tmptr->del_leaving_endpoint(node);
                        }
                        return ss.update_pending_ranges(tmptr, format("decommission {}", req.leaving_nodes));
                    });
                },
                [&ss, ops_uuid] () mutable { ss.node_ops_singal_abort(ops_uuid); });
                ss._node_ops.emplace(ops_uuid, std::move(meta));
            } else if (req.cmd == node_ops_cmd::decommission_heartbeat) {
                slogger.debug("decommission[{}]: Updated heartbeat from coordinator={}", req.ops_uuid,  coordinator);
                ss.node_ops_update_heartbeat(ops_uuid);
            } else if (req.cmd == node_ops_cmd::decommission_done) {
                slogger.info("decommission[{}]: Marked ops done from coordinator={}", req.ops_uuid, coordinator);
                slogger.debug("Triggering off-strategy compaction for all non-system tables on decommission completion");
                ss._db.invoke_on_all([](database &db) {
                    for (auto& table : db.get_non_system_column_families()) {
                        table->trigger_offstrategy_compaction();
                    }
                }).get();
                ss.node_ops_done(ops_uuid);
            } else if (req.cmd == node_ops_cmd::decommission_abort) {
                ss.node_ops_abort(ops_uuid);
            } else if (req.cmd == node_ops_cmd::replace_prepare) {
                // Mark the replacing node as replacing
                if (req.replace_nodes.size() > 1) {
                    auto msg = format("replace[{}]: Could not replace more than one node at a time: replace_nodes={}", req.ops_uuid, req.replace_nodes);
                    slogger.warn("{}", msg);
                    throw std::runtime_error(msg);
                }
                ss.mutate_token_metadata([coordinator, &req, &ss] (mutable_token_metadata_ptr tmptr) mutable {
                    for (auto& x: req.replace_nodes) {
                        auto existing_node = x.first;
                        auto replacing_node = x.second;
                        slogger.info("replace[{}]: Added replacing_node={} to replace existing_node={}, coordinator={}", req.ops_uuid, replacing_node, existing_node, coordinator);
                        tmptr->add_replacing_endpoint(existing_node, replacing_node);
                    }
                    return make_ready_future<>();
                }).get();
                auto ops = seastar::make_shared<node_ops_info>(node_ops_info{ops_uuid, false, std::move(req.ignore_nodes)});
                auto meta = node_ops_meta_data(ops_uuid, coordinator, std::move(ops), [&ss, coordinator, req = std::move(req)] () mutable {
                    return ss.mutate_token_metadata([&ss, coordinator, req = std::move(req)] (mutable_token_metadata_ptr tmptr) mutable {
                        for (auto& x: req.replace_nodes) {
                            auto existing_node = x.first;
                            auto replacing_node = x.second;
                            slogger.info("replace[{}]: Removed replacing_node={} to replace existing_node={}, coordinator={}", req.ops_uuid, replacing_node, existing_node, coordinator);
                            tmptr->del_replacing_endpoint(existing_node);
                        }
                        return ss.update_pending_ranges(tmptr, format("replace {}", req.replace_nodes));
                    });
                },
                [&ss, ops_uuid ] { ss.node_ops_singal_abort(ops_uuid); });
                ss._node_ops.emplace(ops_uuid, std::move(meta));
            } else if (req.cmd == node_ops_cmd::replace_prepare_mark_alive) {
                // Wait for local node has marked replacing node as alive
                auto nodes = boost::copy_range<std::vector<inet_address>>(req.replace_nodes| boost::adaptors::map_values);
                try {
                    ss._gossiper.wait_alive(nodes, std::chrono::milliseconds(120 * 1000));
                } catch (...) {
                    slogger.warn("replace[{}]: Failed to wait for marking replacing node as up, replace_nodes={}: {}",
                            req.ops_uuid, req.replace_nodes, std::current_exception());
                    throw;
                }
            } else if (req.cmd == node_ops_cmd::replace_prepare_pending_ranges) {
                // Update the pending_ranges for the replacing node
                slogger.debug("replace[{}]: Updated pending_ranges from coordinator={}", req.ops_uuid, coordinator);
                ss.mutate_token_metadata([coordinator, &req, &ss] (mutable_token_metadata_ptr tmptr) mutable {
                    return ss.update_pending_ranges(tmptr, format("replace {}", req.replace_nodes));
                }).get();
            } else if (req.cmd == node_ops_cmd::replace_heartbeat) {
                slogger.debug("replace[{}]: Updated heartbeat from coordinator={}", req.ops_uuid, coordinator);
                ss.node_ops_update_heartbeat(ops_uuid);
            } else if (req.cmd == node_ops_cmd::replace_done) {
                slogger.info("replace[{}]: Marked ops done from coordinator={}", req.ops_uuid, coordinator);
                ss.node_ops_done(ops_uuid);
            } else if (req.cmd == node_ops_cmd::replace_abort) {
                ss.node_ops_abort(ops_uuid);
            } else if (req.cmd == node_ops_cmd::bootstrap_prepare) {
                // Mark the bootstrap node as bootstrapping
                if (req.bootstrap_nodes.size() > 1) {
                    auto msg = format("bootstrap[{}]: Could not bootstrap more than one node at a time: bootstrap_nodes={}", req.ops_uuid, req.bootstrap_nodes);
                    slogger.warn("{}", msg);
                    throw std::runtime_error(msg);
                }
                ss.mutate_token_metadata([coordinator, &req, &ss] (mutable_token_metadata_ptr tmptr) mutable {
                    for (auto& x: req.bootstrap_nodes) {
                        auto& endpoint = x.first;
                        auto tokens = std::unordered_set<dht::token>(x.second.begin(), x.second.end());
                        slogger.info("bootstrap[{}]: Added node={} as bootstrap, coordinator={}", req.ops_uuid, endpoint, coordinator);
                        tmptr->add_bootstrap_tokens(tokens, endpoint);
                    }
                    return ss.update_pending_ranges(tmptr, format("bootstrap {}", req.bootstrap_nodes));
                }).get();
                auto ops = seastar::make_shared<node_ops_info>(node_ops_info{ops_uuid, false, std::move(req.ignore_nodes)});
                auto meta = node_ops_meta_data(ops_uuid, coordinator, std::move(ops), [&ss, coordinator, req = std::move(req)] () mutable {
                    return ss.mutate_token_metadata([&ss, coordinator, req = std::move(req)] (mutable_token_metadata_ptr tmptr) mutable {
                        for (auto& x: req.bootstrap_nodes) {
                            auto& endpoint = x.first;
                            auto tokens = std::unordered_set<dht::token>(x.second.begin(), x.second.end());
                            slogger.info("bootstrap[{}]: Removed node={} as bootstrap, coordinator={}", req.ops_uuid, endpoint, coordinator);
                            tmptr->remove_bootstrap_tokens(tokens);
                        }
                        return ss.update_pending_ranges(tmptr, format("bootstrap {}", req.bootstrap_nodes));
                    });
                },
                [&ss, ops_uuid ] { ss.node_ops_singal_abort(ops_uuid); });
                ss._node_ops.emplace(ops_uuid, std::move(meta));
            } else if (req.cmd == node_ops_cmd::bootstrap_heartbeat) {
                slogger.debug("bootstrap[{}]: Updated heartbeat from coordinator={}", req.ops_uuid, coordinator);
                ss.node_ops_update_heartbeat(ops_uuid);
            } else if (req.cmd == node_ops_cmd::bootstrap_done) {
                slogger.info("bootstrap[{}]: Marked ops done from coordinator={}", req.ops_uuid, coordinator);
                ss.node_ops_done(ops_uuid);
            } else if (req.cmd == node_ops_cmd::bootstrap_abort) {
                ss.node_ops_abort(ops_uuid);
            } else {
                auto msg = format("node_ops_cmd_handler: ops_uuid={}, unknown cmd={}", req.ops_uuid, uint32_t(req.cmd));
                slogger.warn("{}", msg);
                throw std::runtime_error(msg);
            }
            bool ok = true;
            node_ops_cmd_response resp(ok);
            return resp;
        });
    });
}

// Runs inside seastar::async context
void storage_service::flush_column_families() {
    container().invoke_on_all([] (auto& ss) {
        auto& local_db = ss._db.local();
        auto non_system_cfs = local_db.get_column_families() | boost::adaptors::filtered([] (auto& uuid_and_cf) {
            auto cf = uuid_and_cf.second;
            return !is_system_keyspace(cf->schema()->ks_name());
        });
        // count CFs first
        auto total_cfs = boost::distance(non_system_cfs);
        ss._drain_progress.total_cfs = total_cfs;
        ss._drain_progress.remaining_cfs = total_cfs;
        // flush
        return parallel_for_each(non_system_cfs, [&ss] (auto&& uuid_and_cf) {
            auto cf = uuid_and_cf.second;
            return cf->flush().then([&ss] {
                ss._drain_progress.remaining_cfs--;
            });
        });
    }).get();
    // flush the system ones after all the rest are done, just in case flushing modifies any system state
    // like CASSANDRA-5151. don't bother with progress tracking since system data is tiny.
    container().invoke_on_all([] (auto& ss) {
        auto& local_db = ss._db.local();
        auto system_cfs = local_db.get_column_families() | boost::adaptors::filtered([] (auto& uuid_and_cf) {
            auto cf = uuid_and_cf.second;
            return is_system_keyspace(cf->schema()->ks_name());
        });
        return parallel_for_each(system_cfs, [&ss] (auto&& uuid_and_cf) {
            auto cf = uuid_and_cf.second;
            return cf->flush();
        });
    }).get();
}

future<> storage_service::drain() {
    return run_with_api_lock(sstring("drain"), [] (storage_service& ss) {
        if (ss._operation_mode == mode::DRAINED) {
            slogger.warn("Cannot drain node (did it already happen?)");
            return make_ready_future<>();
        }

        ss.set_mode(mode::DRAINING, "starting drain process", true);
        return ss.do_drain(false).then([&ss] {
            ss._drain_finished.set_value();
            ss.set_mode(mode::DRAINED, true);
        });
    });
}

future<> storage_service::do_drain(bool on_shutdown) {
    return seastar::async([this, on_shutdown] {
        stop_transport().get();

        tracing::tracing::tracing_instance().invoke_on_all(&tracing::tracing::shutdown).get();

        if (!on_shutdown) {
            // Interrupt on going compaction and shutdown to prevent further compaction
            _db.invoke_on_all([] (auto& db) {
                return db.get_compaction_manager().drain();
            }).get();
        }

        set_mode(mode::DRAINING, "flushing column families", false);
        flush_column_families();

        db::get_batchlog_manager().invoke_on_all([] (auto& bm) {
            return bm.stop();
        }).get();

        set_mode(mode::DRAINING, "shutting down migration manager", false);
        _migration_manager.invoke_on_all(&service::migration_manager::stop).get();

        _db.invoke_on_all([] (auto& db) {
            return db.commitlog()->shutdown();
        }).get();
    });
}

future<> storage_service::rebuild(sstring source_dc) {
    return run_with_api_lock(sstring("rebuild"), [source_dc] (storage_service& ss) {
        slogger.info("rebuild from dc: {}", source_dc == "" ? "(any dc)" : source_dc);
        auto tmptr = ss.get_token_metadata_ptr();
        if (ss.is_repair_based_node_ops_enabled()) {
            return ss._repair.local().rebuild_with_repair(tmptr, std::move(source_dc));
        } else {
            auto streamer = make_lw_shared<dht::range_streamer>(ss._db, tmptr, ss._abort_source,
                    ss.get_broadcast_address(), "Rebuild", streaming::stream_reason::rebuild);
            streamer->add_source_filter(std::make_unique<dht::range_streamer::failure_detector_source_filter>(ss._gossiper.get_unreachable_members()));
            if (source_dc != "") {
                streamer->add_source_filter(std::make_unique<dht::range_streamer::single_datacenter_filter>(source_dc));
            }
            auto keyspaces = make_lw_shared<std::vector<sstring>>(ss._db.local().get_non_system_keyspaces());
            return do_for_each(*keyspaces, [keyspaces, streamer, &ss] (sstring& keyspace_name) {
                return streamer->add_ranges(keyspace_name, ss.get_ranges_for_endpoint(keyspace_name, utils::fb_utilities::get_broadcast_address()));
            }).then([streamer] {
                return streamer->stream_async().then([streamer] {
                    slogger.info("Streaming for rebuild successful");
                }).handle_exception([] (auto ep) {
                    // This is used exclusively through JMX, so log the full trace but only throw a simple RTE
                    slogger.warn("Error while rebuilding node: {}", ep);
                    return make_exception_future<>(std::move(ep));
                });
            });
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

future<bool> storage_service::is_initialized() {
    return run_with_no_api_lock([] (storage_service& ss) {
        return ss._initialized;
    });
}

// Runs inside seastar::async context
std::unordered_multimap<dht::token_range, inet_address> storage_service::get_changed_ranges_for_leaving(sstring keyspace_name, inet_address endpoint) {
    // First get all ranges the leaving endpoint is responsible for
    auto ranges = get_ranges_for_endpoint(keyspace_name, endpoint);

    slogger.debug("Node {} ranges [{}]", endpoint, ranges);

    std::unordered_map<dht::token_range, inet_address_vector_replica_set> current_replica_endpoints;

    // Find (for each range) all nodes that store replicas for these ranges as well
    auto tmptr = get_token_metadata_ptr();
    for (auto& r : ranges) {
        auto& ks = _db.local().find_keyspace(keyspace_name);
        auto end_token = r.end() ? r.end()->value() : dht::maximum_token();
        auto eps = ks.get_replication_strategy().calculate_natural_endpoints(end_token, *tmptr, utils::can_yield::yes);
        current_replica_endpoints.emplace(r, std::move(eps));
    }

    auto temp = tmptr->clone_after_all_left().get0();

    // release the token_metadata_ptr
    // as it's not needed from this point on
    // after we got clone_after_all_left()
    tmptr = nullptr;

    // endpoint might or might not be 'leaving'. If it was not leaving (that is, removenode
    // command was used), it is still present in temp and must be removed.
    if (temp.is_member(endpoint)) {
        temp.remove_endpoint(endpoint);
    }

    std::unordered_multimap<dht::token_range, inet_address> changed_ranges;

    // Go through the ranges and for each range check who will be
    // storing replicas for these ranges when the leaving endpoint
    // is gone. Whoever is present in newReplicaEndpoints list, but
    // not in the currentReplicaEndpoints list, will be needing the
    // range.
    for (auto& r : ranges) {
        auto& ks = _db.local().find_keyspace(keyspace_name);
        auto end_token = r.end() ? r.end()->value() : dht::maximum_token();
        auto new_replica_endpoints = ks.get_replication_strategy().calculate_natural_endpoints(end_token, temp, utils::can_yield::yes);

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
    }
    temp.clear_gently().get();

    return changed_ranges;
}

// Runs inside seastar::async context
void storage_service::unbootstrap() {
    db::get_local_batchlog_manager().do_batch_log_replay().get();
    if (is_repair_based_node_ops_enabled()) {
        _repair.local().decommission_with_repair(get_token_metadata_ptr()).get();
    } else {
        std::unordered_map<sstring, std::unordered_multimap<dht::token_range, inet_address>> ranges_to_stream;

        auto non_system_keyspaces = _db.local().get_non_system_keyspaces();
        for (const auto& keyspace_name : non_system_keyspaces) {
            auto ranges_mm = get_changed_ranges_for_leaving(keyspace_name, get_broadcast_address());
            if (slogger.is_enabled(logging::log_level::debug)) {
                std::vector<range<token>> ranges;
                for (auto& x : ranges_mm) {
                    ranges.push_back(x.first);
                }
                slogger.debug("Ranges needing transfer for keyspace={} are [{}]", keyspace_name, ranges);
            }
            ranges_to_stream.emplace(keyspace_name, std::move(ranges_mm));
        }

        set_mode(mode::LEAVING, "replaying batch log and streaming data to other nodes", true);

        auto stream_success = stream_ranges(ranges_to_stream);
        // Wait for batch log to complete before streaming hints.
        slogger.debug("waiting for batch log processing.");
        // Start with BatchLog replay, which may create hints but no writes since this is no longer a valid endpoint.
        db::get_local_batchlog_manager().do_batch_log_replay().get();

        set_mode(mode::LEAVING, "streaming hints to other nodes", true);

        // wait for the transfer runnables to signal the latch.
        slogger.debug("waiting for stream acks.");
        try {
            stream_success.get();
        } catch (...) {
            slogger.warn("unbootstrap fails to stream : {}", std::current_exception());
            throw;
        }
        slogger.debug("stream acks all received.");
    }
    leave_ring();
}

future<> storage_service::removenode_add_ranges(lw_shared_ptr<dht::range_streamer> streamer, gms::inet_address leaving_node) {
    auto tmptr = get_token_metadata_ptr();
    auto my_address = get_broadcast_address();
    auto non_system_keyspaces = _db.local().get_non_system_keyspaces();
    for (const auto& keyspace_name : non_system_keyspaces) {
        std::unordered_multimap<dht::token_range, inet_address> changed_ranges = get_changed_ranges_for_leaving(keyspace_name, leaving_node);
        dht::token_range_vector my_new_ranges;
        for (auto& x : changed_ranges) {
            if (x.second == my_address) {
                my_new_ranges.emplace_back(x.first);
            }
        }
        std::unordered_multimap<inet_address, dht::token_range> source_ranges = get_new_source_ranges(keyspace_name, my_new_ranges, *tmptr);
        std::unordered_map<inet_address, dht::token_range_vector> ranges_per_endpoint;
        for (auto& x : source_ranges) {
            ranges_per_endpoint[x.first].emplace_back(x.second);
        }
        streamer->add_rx_ranges(keyspace_name, std::move(ranges_per_endpoint));
    }
    return make_ready_future<>();
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
        auto streamer = make_lw_shared<dht::range_streamer>(_db, tmptr, as, get_broadcast_address(), "Removenode", streaming::stream_reason::removenode);
        removenode_add_ranges(streamer, leaving_node).get();
        try {
            streamer->stream_async().get();
        } catch (...) {
            slogger.warn("removenode_with_stream: stream failed: {}", std::current_exception());
            throw;
        }
    });
}

future<> storage_service::restore_replica_count(inet_address endpoint, inet_address notify_endpoint) {
    if (is_repair_based_node_ops_enabled()) {
        auto ops_uuid = utils::make_random_uuid();
        auto ops = seastar::make_shared<node_ops_info>(node_ops_info{ops_uuid, false, std::list<gms::inet_address>()});
        return _repair.local().removenode_with_repair(get_token_metadata_ptr(), endpoint, ops).finally([this, notify_endpoint] () {
            return send_replication_notification(notify_endpoint);
        });
    }
  return seastar::async([this, endpoint, notify_endpoint] {
    auto tmptr = get_token_metadata_ptr();
    abort_source as;
    auto sub = _abort_source.subscribe([&as] () noexcept {
        if (!as.abort_requested()) {
            as.request_abort();
        }
    });
    auto streamer = make_lw_shared<dht::range_streamer>(_db, tmptr, as, get_broadcast_address(), "Restore_replica_count", streaming::stream_reason::removenode);
    removenode_add_ranges(streamer, endpoint).get();
    auto status_checker = seastar::async([this, endpoint, &as] {
        slogger.info("restore_replica_count: Started status checker for removing node {}", endpoint);
        while (!as.abort_requested()) {
            auto status = _gossiper.get_gossip_status(endpoint);
            // If the node to be removed is already in removed status, it has
            // probably been removed forcely with `nodetool removenode force`.
            // Abort the restore_replica_count in such case to avoid streaming
            // attempt since the user has removed the node forcely.
            if (status == sstring(versioned_value::REMOVED_TOKEN)) {
                slogger.info("restore_replica_count: Detected node {} has left the cluster, status={}, abort restore_replica_count for removing node {}",
                        endpoint, status, endpoint);
                if (!as.abort_requested()) {
                    as.request_abort();
                }
                return;
            }
            slogger.debug("restore_replica_count: Sleep and detect removing node {}, status={}", endpoint, status);
            sleep_abortable(std::chrono::seconds(10), as).get();
        }
    });
    auto stop_status_checker = defer([endpoint, &status_checker, &as] () mutable {
        try {
            slogger.info("restore_replica_count: Started to stop status checker for removing node {}", endpoint);
            if (!as.abort_requested()) {
                as.request_abort();
            }
            status_checker.get();
        } catch (const seastar::sleep_aborted& ignored) {
            slogger.debug("restore_replica_count: Got sleep_abort to stop status checker for removing node {}: {}", endpoint, ignored);
        } catch (...) {
            slogger.warn("restore_replica_count: Found error in status checker for removing node {}: {}",
                    endpoint, std::current_exception());
        }
        slogger.info("restore_replica_count: Finished to stop status checker for removing node {}", endpoint);
    });

    streamer->stream_async().then_wrapped([this, streamer, notify_endpoint] (auto&& f) {
        try {
            f.get();
            return this->send_replication_notification(notify_endpoint);
        } catch (...) {
            slogger.warn("Streaming to restore replica count failed: {}", std::current_exception());
            // We still want to send the notification
            return this->send_replication_notification(notify_endpoint);
        }
        return make_ready_future<>();
    }).get();
  });
}

// Runs inside seastar::async context
void storage_service::excise(std::unordered_set<token> tokens, inet_address endpoint) {
    slogger.info("Removing tokens {} for {}", tokens, endpoint);
    // FIXME: HintedHandOffManager.instance.deleteHintsForEndpoint(endpoint);
    remove_endpoint(endpoint);
    auto tmlock = std::make_optional(get_token_metadata_lock().get0());
    auto tmptr = get_mutable_token_metadata_ptr().get0();
    tmptr->remove_endpoint(endpoint);
    tmptr->remove_bootstrap_tokens(tokens);

    update_pending_ranges(tmptr, format("excise {}", endpoint)).get();
    replicate_to_all_cores(std::move(tmptr)).get();
    tmlock.reset();

    notify_left(endpoint);
}

void storage_service::excise(std::unordered_set<token> tokens, inet_address endpoint, int64_t expire_time) {
    add_expire_time_if_found(endpoint, expire_time);
    excise(tokens, endpoint);
}

future<> storage_service::send_replication_notification(inet_address remote) {
    // notify the remote token
    auto done = make_shared<bool>(false);
    auto local = get_broadcast_address();
    auto sent = make_lw_shared<int>(0);
    slogger.debug("Notifying {} of replication completion", remote);
    return do_until(
        [this, done, sent, remote] {
            // The node can send REPLICATION_FINISHED to itself, in which case
            // is_alive will be true. If the messaging_service is stopped,
            // REPLICATION_FINISHED can be sent infinitely here. To fix, limit
            // the number of retries.
            return *done || !_gossiper.is_alive(remote) || *sent >= 3;
        },
        [this, done, sent, remote, local] {
            netw::msg_addr id{remote, 0};
            (*sent)++;
            return _messaging.local().send_replication_finished(id, local).then_wrapped([id, done] (auto&& f) {
                try {
                    f.get();
                    *done = true;
                } catch (...) {
                    slogger.warn("Fail to send REPLICATION_FINISHED to {}: {}", id, std::current_exception());
                }
            });
        }
    );
}

future<> storage_service::confirm_replication(inet_address node) {
    return run_with_no_api_lock([node] (storage_service& ss) {
        auto removing_node = bool(ss._removing_node) ? format("{}", *ss._removing_node) : "NONE";
        slogger.info("Got confirm_replication from {}, removing_node {}", node, removing_node);
        // replicatingNodes can be empty in the case where this node used to be a removal coordinator,
        // but restarted before all 'replication finished' messages arrived. In that case, we'll
        // still go ahead and acknowledge it.
        if (!ss._replicating_nodes.empty()) {
            ss._replicating_nodes.erase(node);
        } else {
            slogger.info("Received unexpected REPLICATION_FINISHED message from {}. Was this node recently a removal coordinator?", node);
        }
    });
}

// Runs inside seastar::async context
void storage_service::leave_ring() {
    db::system_keyspace::set_bootstrap_state(db::system_keyspace::bootstrap_state::NEEDS_BOOTSTRAP).get();
    mutate_token_metadata([this] (mutable_token_metadata_ptr tmptr) {
        auto endpoint = get_broadcast_address();
        tmptr->remove_endpoint(endpoint);
        return update_pending_ranges(std::move(tmptr), format("leave_ring {}", endpoint));
    }).get();

    auto expire_time = _gossiper.compute_expire_time().time_since_epoch().count();
    _gossiper.add_local_application_state(gms::application_state::STATUS,
            versioned_value::left(db::system_keyspace::get_local_tokens().get0(), expire_time)).get();
    auto delay = std::max(get_ring_delay(), gms::gossiper::INTERVAL);
    slogger.info("Announcing that I have left the ring for {}ms", delay.count());
    sleep_abortable(delay, _abort_source).get();
}

future<>
storage_service::stream_ranges(std::unordered_map<sstring, std::unordered_multimap<dht::token_range, inet_address>> ranges_to_stream_by_keyspace) {
    auto streamer = make_lw_shared<dht::range_streamer>(_db, get_token_metadata_ptr(), _abort_source, get_broadcast_address(), "Unbootstrap", streaming::stream_reason::decommission);
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
        }
        streamer->add_tx_ranges(keyspace, std::move(ranges_per_endpoint));
    }
    return streamer->stream_async().then([streamer] {
        slogger.info("stream_ranges successful");
    }).handle_exception([] (auto ep) {
        slogger.warn("stream_ranges failed: {}", ep);
        return make_exception_future<>(std::move(ep));
    });
}

future<> storage_service::start_leaving() {
    return _gossiper.add_local_application_state(application_state::STATUS, versioned_value::leaving(db::system_keyspace::get_local_tokens().get0())).then([this] {
        return mutate_token_metadata([this] (mutable_token_metadata_ptr tmptr) {
            auto endpoint = get_broadcast_address();
            tmptr->add_leaving_endpoint(endpoint);
            return update_pending_ranges(std::move(tmptr), format("start_leaving {}", endpoint));
        });
    });
}

void storage_service::add_expire_time_if_found(inet_address endpoint, int64_t expire_time) {
    if (expire_time != 0L) {
        using clk = gms::gossiper::clk;
        auto time = clk::time_point(clk::duration(expire_time));
        _gossiper.add_expire_time_for_endpoint(endpoint, time);
    }
}

class send_meta_data {
    gms::inet_address _node;
    rpc::sink<frozen_mutation_fragment, streaming::stream_mutation_fragments_cmd> _sink;
    rpc::source<int32_t> _source;
    bool _error_from_peer = false;
    size_t _num_partitions_sent = 0;
    size_t _num_bytes_sent = 0;
    future<> _receive_done;
private:
    future<> do_receive() {
        int32_t status = 0;
        while (auto status_opt = co_await _source()) {
            status = std::get<0>(*status_opt);
            slogger.debug("send_meta_data: got error code={}, from node={}, status={}", status, _node);
            if (status == -1) {
                _error_from_peer = true;
            }
        }
        slogger.debug("send_meta_data: finished reading source from node={}", _node);
        if (_error_from_peer) {
            throw std::runtime_error(format("send_meta_data: got error code={} from node={}", status, _node));
        }
        co_return;
    }
public:
    send_meta_data(gms::inet_address node,
            rpc::sink<frozen_mutation_fragment, streaming::stream_mutation_fragments_cmd> sink,
            rpc::source<int32_t> source)
        : _node(std::move(node))
        , _sink(std::move(sink))
        , _source(std::move(source))
        , _receive_done(make_ready_future<>()) {
    }
    void receive() {
        _receive_done = do_receive();
    }
    future<> send(const frozen_mutation_fragment& fmf, bool is_partition_start) {
        if (_error_from_peer) {
            throw std::runtime_error(format("send_meta_data: got error from peer node={}", _node));
        }
        auto size = fmf.representation().size();
        if (is_partition_start) {
            ++_num_partitions_sent;
        }
        _num_bytes_sent += size;
        slogger.trace("send_meta_data: send mf to node={}, size={}", _node, size);
        co_return co_await _sink(fmf, streaming::stream_mutation_fragments_cmd::mutation_fragment_data);
    }
    future<> finish(bool failed) {
        std::exception_ptr eptr;
        try {
            if (failed) {
                co_await _sink(frozen_mutation_fragment(bytes_ostream()), streaming::stream_mutation_fragments_cmd::error);
            } else {
                co_await _sink(frozen_mutation_fragment(bytes_ostream()), streaming::stream_mutation_fragments_cmd::end_of_stream);
            }
        } catch (...) {
            eptr = std::current_exception();
            slogger.warn("send_meta_data: failed to send {} to node={}, err={}",
                    failed ? "stream_mutation_fragments_cmd::error" : "stream_mutation_fragments_cmd::end_of_stream", _node, eptr);
        }
        try {
            co_await _sink.close();
        } catch (...)  {
            eptr = std::current_exception();
            slogger.warn("send_meta_data: failed to close sink to node={}, err={}", _node, eptr);
        }
        try {
            co_await std::move(_receive_done);
        } catch (...)  {
            eptr = std::current_exception();
            slogger.warn("send_meta_data: failed to process source from node={}, err={}", _node, eptr);
        }
        if (eptr) {
            std::rethrow_exception(eptr);
        }
        co_return;
    }
    size_t num_partitions_sent() {
        return _num_partitions_sent;
    }
    size_t num_bytes_sent() {
        return _num_bytes_sent;
    }
};

future<> storage_service::load_and_stream(sstring ks_name, sstring cf_name,
        utils::UUID table_id, std::vector<sstables::shared_sstable> sstables, bool primary_replica_only) {
    const auto full_partition_range = dht::partition_range::make_open_ended_both_sides();
    const auto full_token_range = dht::token_range::make_open_ended_both_sides();
    auto& table = _db.local().find_column_family(table_id);
    auto s = table.schema();
    const auto cf_id = s->id();
    const auto reason = streaming::stream_reason::rebuild;
    auto& rs = _db.local().find_keyspace(ks_name).get_replication_strategy();

    size_t nr_sst_total = sstables.size();
    size_t nr_sst_current = 0;
    while (!sstables.empty()) {
        auto ops_uuid = utils::make_random_uuid();
        auto sst_set = make_lw_shared<sstables::sstable_set>(sstables::make_partitioned_sstable_set(s,
                make_lw_shared<sstable_list>(sstable_list{}), false));
        size_t batch_sst_nr = 16;
        std::vector<sstring> sst_names;
        std::vector<sstables::shared_sstable> sst_processed;
        size_t estimated_partitions = 0;
        while (batch_sst_nr-- && !sstables.empty()) {
            auto sst = sstables.back();
            estimated_partitions += sst->estimated_keys_for_range(full_token_range);
            sst_names.push_back(sst->get_filename());
            sst_set->insert(sst);
            sst_processed.push_back(sst);
            sstables.pop_back();
        }

        slogger.info("load_and_stream: started ops_uuid={}, process [{}-{}] out of {} sstables={}",
                ops_uuid, nr_sst_current, nr_sst_current + sst_processed.size(), nr_sst_total, sst_names);

        auto start_time = std::chrono::steady_clock::now();
        inet_address_vector_replica_set current_targets;
        std::unordered_map<gms::inet_address, send_meta_data> metas;
        size_t num_partitions_processed = 0;
        size_t num_bytes_read = 0;
        nr_sst_current += sst_processed.size();
        auto reader = table.make_streaming_reader(s, full_partition_range, sst_set);
        std::exception_ptr eptr;
        bool failed = false;
        try {
            netw::messaging_service& ms = _messaging.local();
            while (auto mf = co_await reader(db::no_timeout)) {
                bool is_partition_start = mf->is_partition_start();
                if (is_partition_start) {
                    ++num_partitions_processed;
                    auto& start = mf->as_partition_start();
                    const auto& current_dk = start.key();

                    current_targets = rs.get_natural_endpoints(current_dk.token());
                    if (primary_replica_only && current_targets.size() > 1) {
                        current_targets.resize(1);
                    }
                    slogger.trace("load_and_stream: ops_uuid={}, current_dk={}, current_targets={}", ops_uuid,
                            current_dk.token(), current_targets);
                    for (auto& node : current_targets) {
                        if (!metas.contains(node)) {
                            auto [sink, source] = co_await ms.make_sink_and_source_for_stream_mutation_fragments(reader.schema()->version(),
                                    ops_uuid, cf_id, estimated_partitions, reason, netw::messaging_service::msg_addr(node));
                            slogger.debug("load_and_stream: ops_uuid={}, make sink and source for node={}", ops_uuid, node);
                            metas.emplace(node, send_meta_data(node, std::move(sink), std::move(source)));
                            metas.at(node).receive();
                        }
                    }
                }
                frozen_mutation_fragment fmf = freeze(*s, *mf);
                num_bytes_read += fmf.representation().size();
                co_await parallel_for_each(current_targets, [&metas, &fmf, is_partition_start] (const gms::inet_address& node) {
                    return metas.at(node).send(fmf, is_partition_start);
                });
            }
        } catch (...) {
            failed = true;
            eptr = std::current_exception();
            slogger.warn("load_and_stream: ops_uuid={}, ks={}, table={}, send_phase, err={}",
                    ops_uuid, ks_name, cf_name, eptr);
        }
        try {
            co_await parallel_for_each(metas.begin(), metas.end(), [failed] (std::pair<const gms::inet_address, send_meta_data>& pair) {
                auto& meta = pair.second;
                return meta.finish(failed);
            });
        } catch (...) {
            failed = true;
            eptr = std::current_exception();
            slogger.warn("load_and_stream: ops_uuid={}, ks={}, table={}, finish_phase, err={}",
                    ops_uuid, ks_name, cf_name, eptr);
        }
        if (!failed) {
            try {
                co_await parallel_for_each(sst_processed, [&] (sstables::shared_sstable& sst) {
                    slogger.debug("load_and_stream: ops_uuid={}, ks={}, table={}, remove sst={}",
                            ops_uuid, ks_name, cf_name, sst->component_filenames());
                    return sst->unlink();
                });
            } catch (...) {
                failed = true;
                eptr = std::current_exception();
                slogger.warn("load_and_stream: ops_uuid={}, ks={}, table={}, del_sst_phase, err={}",
                        ops_uuid, ks_name, cf_name, eptr);
            }
        }
        auto duration = std::chrono::duration_cast<std::chrono::duration<float>>(std::chrono::steady_clock::now() - start_time).count();
        for (auto& [node, meta] : metas) {
            slogger.info("load_and_stream: ops_uuid={}, ks={}, table={}, target_node={}, num_partitions_sent={}, num_bytes_sent={}",
                    ops_uuid, ks_name, cf_name, node, meta.num_partitions_sent(), meta.num_bytes_sent());
        }
        auto partition_rate = std::fabs(duration) > FLT_EPSILON ? num_partitions_processed / duration : 0;
        auto bytes_rate = std::fabs(duration) > FLT_EPSILON ? num_bytes_read / duration / 1024 / 1024 : 0;
        auto status = failed ? "failed" : "succeeded";
        slogger.info("load_and_stream: finished ops_uuid={}, ks={}, table={}, partitions_processed={} partitions, bytes_processed={} bytes, partitions_per_second={} partitions/s, bytes_per_second={} MiB/s, duration={} s, status={}",
                ops_uuid, ks_name, cf_name, num_partitions_processed, num_bytes_read, partition_rate, bytes_rate, duration, status);
        if (failed) {
            std::rethrow_exception(eptr);
        }
    }
    co_return;
}

// For more details, see the commends on column_family::load_new_sstables
// All the global operations are going to happen here, and just the reloading happens
// in there.
future<> storage_service::load_new_sstables(sstring ks_name, sstring cf_name,
    bool load_and_stream, bool primary_replica_only) {
    if (_loading_new_sstables) {
        throw std::runtime_error("Already loading SSTables. Try again later");
    } else {
        _loading_new_sstables = true;
    }
    slogger.info("Loading new SSTables for keyspace={}, table={}, load_and_stream={}, primary_replica_only={}",
            ks_name, cf_name, load_and_stream, primary_replica_only);
    try {
        if (load_and_stream) {
            utils::UUID table_id;
            std::vector<std::vector<sstables::shared_sstable>> sstables_on_shards;
            std::tie(table_id, sstables_on_shards) = co_await distributed_loader::get_sstables_from_upload_dir(_db, ks_name, cf_name);
            co_await container().invoke_on_all([&sstables_on_shards, ks_name, cf_name, table_id, primary_replica_only] (storage_service& ss) mutable -> future<> {
                co_await ss.load_and_stream(ks_name, cf_name, table_id, std::move(sstables_on_shards[this_shard_id()]), primary_replica_only);
            });
        } else {
            co_await distributed_loader::process_upload_dir(_db, _sys_dist_ks, _view_update_generator, ks_name, cf_name);
        }
    } catch (...) {
        slogger.warn("Done loading new SSTables for keyspace={}, table={}, load_and_stream={}, primary_replica_only={}, status=failed: {}",
                ks_name, cf_name, load_and_stream, primary_replica_only, std::current_exception());
        _loading_new_sstables = false;
        throw;
    }
    slogger.info("Done loading new SSTables for keyspace={}, table={}, load_and_stream={}, primary_replica_only={}, status=succeeded",
            ks_name, cf_name, load_and_stream, primary_replica_only);
    _loading_new_sstables = false;
    co_return;
}

void storage_service::shutdown_client_servers() {
    for (auto& [name, hook] : _client_shutdown_hooks) {
        slogger.info("Shutting down {}", name);
        try {
            hook();
        } catch (...) {
            slogger.error("Unexpected error shutting down {}: {}",
                    name, std::current_exception());
            throw;
        }
        slogger.info("Shutting down {} was successful", name);
    }
}

future<>
storage_service::set_tables_autocompaction(const sstring &keyspace, std::vector<sstring> tables, bool enabled) {
    slogger.info("set_tables_autocompaction: enabled={} keyspace={} tables={}", enabled, keyspace, tables);
    return do_with(keyspace, std::move(tables), [this, enabled] (const sstring &keyspace, const std::vector<sstring>& tables) {
        return run_with_api_lock(sstring("set_tables_autocompaction"), [&keyspace, &tables, enabled] (auto&& ss) {
            if (!ss._initialized) {
                return make_exception_future<>(std::runtime_error("Too early: storage service not initialized yet"));
            }

            return ss._db.invoke_on_all([&keyspace, &tables, enabled] (database& db) {
                return parallel_for_each(tables, [&db, &keyspace, enabled] (const sstring& table) {
                    column_family& cf = db.find_column_family(keyspace, table);
                    if (enabled) {
                        cf.enable_auto_compaction();
                    } else {
                        cf.disable_auto_compaction();
                    }
                    return make_ready_future<>();
                });
            });
        });
    });
}

// Must be called from a seastar thread
std::unordered_multimap<inet_address, dht::token_range>
storage_service::get_new_source_ranges(const sstring& keyspace_name, const dht::token_range_vector& ranges, const token_metadata& tm) {
    auto my_address = get_broadcast_address();
    auto& ks = _db.local().find_keyspace(keyspace_name);
    auto& strat = ks.get_replication_strategy();
    std::unordered_map<dht::token_range, inet_address_vector_replica_set> range_addresses = strat.get_range_addresses(tm, utils::can_yield::yes);
    std::unordered_multimap<inet_address, dht::token_range> source_ranges;

    // find alive sources for our new ranges
    for (auto r : ranges) {
        inet_address_vector_replica_set possible_nodes;
        auto it = range_addresses.find(r);
        if (it != range_addresses.end()) {
            possible_nodes = it->second;
        }

        auto& snitch = locator::i_endpoint_snitch::get_local_snitch_ptr();
        inet_address_vector_replica_set sources = snitch->get_sorted_list_by_proximity(my_address, possible_nodes);

        if (std::find(sources.begin(), sources.end(), my_address) != sources.end()) {
            auto err = format("get_new_source_ranges: sources={}, my_address={}", sources, my_address);
            slogger.warn("{}", err);
            throw std::runtime_error(err);
        }


        for (auto& source : sources) {
            if (_gossiper.is_alive(source)) {
                source_ranges.emplace(source, r);
                break;
            }
        }
    }
    return source_ranges;
}

future<> storage_service::move(token new_token) {
    return run_with_api_lock(sstring("move"), [new_token] (storage_service& ss) mutable {
        return make_exception_future<>(std::runtime_error("Move opeartion is not supported only more"));
    });
}

std::vector<storage_service::token_range_endpoints>
storage_service::describe_ring(const sstring& keyspace, bool include_only_local_dc) const {
    std::vector<token_range_endpoints> ranges;
    //Token.TokenFactory tf = getPartitioner().getTokenFactory();

    std::unordered_map<dht::token_range, inet_address_vector_replica_set> range_to_address_map =
            include_only_local_dc
                    ? get_range_to_address_map_in_local_dc(keyspace)
                    : get_range_to_address_map(keyspace);
    for (auto entry : range_to_address_map) {
        auto range = entry.first;
        auto addresses = entry.second;
        token_range_endpoints tr;
        if (range.start()) {
            tr._start_token = range.start()->value().to_sstring();
        }
        if (range.end()) {
            tr._end_token = range.end()->value().to_sstring();
        }
        for (auto endpoint : addresses) {
            endpoint_details details;
            details._host = boost::lexical_cast<std::string>(endpoint);
            details._datacenter = locator::i_endpoint_snitch::get_local_snitch_ptr()->get_datacenter(endpoint);
            details._rack = locator::i_endpoint_snitch::get_local_snitch_ptr()->get_rack(endpoint);
            tr._rpc_endpoints.push_back(get_rpc_address(endpoint));
            tr._endpoints.push_back(details._host);
            tr._endpoint_details.push_back(details);
        }
        ranges.push_back(tr);
    }
    // Convert to wrapping ranges
    auto left_inf = boost::find_if(ranges, [] (const token_range_endpoints& tr) {
        return tr._start_token.empty();
    });
    auto right_inf = boost::find_if(ranges, [] (const token_range_endpoints& tr) {
        return tr._end_token.empty();
    });
    using set = std::unordered_set<sstring>;
    if (left_inf != right_inf
            && left_inf != ranges.end()
            && right_inf != ranges.end()
            && (boost::copy_range<set>(left_inf->_endpoints)
                 == boost::copy_range<set>(right_inf->_endpoints))) {
        left_inf->_start_token = std::move(right_inf->_start_token);
        ranges.erase(right_inf);
    }
    return ranges;
}

std::unordered_map<dht::token_range, inet_address_vector_replica_set>
storage_service::construct_range_to_endpoint_map(
        const sstring& keyspace,
        const dht::token_range_vector& ranges) const {
    std::unordered_map<dht::token_range, inet_address_vector_replica_set> res;
    for (auto r : ranges) {
        res[r] = _db.local().find_keyspace(keyspace).get_replication_strategy().get_natural_endpoints(
                r.end() ? r.end()->value() : dht::maximum_token());
    }
    return res;
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

future<> storage_service::with_token_metadata_lock(std::function<future<> ()> func) noexcept {
    assert(this_shard_id() == 0);
    return get_token_metadata_lock().then([func = std::move(func)] (token_metadata_lock tmlock) {
        return func().finally([tmlock = std::move(tmlock)] {});
    });
}

future<> storage_service::mutate_token_metadata(std::function<future<> (mutable_token_metadata_ptr)> func) noexcept {
    return with_token_metadata_lock([this, func = std::move(func)] () mutable {
        return get_mutable_token_metadata_ptr().then([this, func = std::move(func)] (mutable_token_metadata_ptr tmptr) mutable {
            return func(tmptr).then([this, tmptr = std::move(tmptr)] () mutable {
                return replicate_to_all_cores(std::move(tmptr));
            });
        });
    });
}

future<> storage_service::update_pending_ranges(mutable_token_metadata_ptr tmptr, sstring reason) {
    assert(this_shard_id() == 0);

    // long start = System.currentTimeMillis();
    return do_with(_db.local().get_non_system_keyspaces(), [this, tmptr = std::move(tmptr)] (auto& keyspaces) mutable {
        return do_for_each(keyspaces, [this, tmptr = std::move(tmptr)] (auto& keyspace_name) mutable {
            auto& ks = this->_db.local().find_keyspace(keyspace_name);
            auto& strategy = ks.get_replication_strategy();
            slogger.debug("Updating pending ranges for keyspace={} starts", keyspace_name);
            return tmptr->update_pending_ranges(strategy, keyspace_name).finally([&keyspace_name] {
                slogger.debug("Updating pending ranges for keyspace={} ends", keyspace_name);
            });
        });
    }).handle_exception([this, reason = std::move(reason)] (std::exception_ptr ep) mutable {
        slogger.error("Failed to update pending ranges for {}: {}", reason, ep);
        return make_exception_future<>(std::move(ep));
    });
    // slogger.debug("finished calculation for {} keyspaces in {}ms", keyspaces.size(), System.currentTimeMillis() - start);
}

future<> storage_service::update_pending_ranges(sstring reason) {
    return mutate_token_metadata([this, reason = std::move(reason)] (mutable_token_metadata_ptr tmptr) mutable {
        return update_pending_ranges(std::move(tmptr), std::move(reason));
    });
}

future<> storage_service::keyspace_changed(const sstring& ks_name) {
    // Update pending ranges since keyspace can be changed after we calculate pending ranges.
    sstring reason = format("keyspace {}", ks_name);
    return container().invoke_on(0, [reason = std::move(reason)] (auto& ss) mutable {
        return ss.update_pending_ranges(reason).handle_exception([reason = std::move(reason)] (auto ep) {
            slogger.warn("Failure to update pending ranges for {} ignored", reason);
        });
    });
}

future<> storage_service::update_topology(inet_address endpoint) {
    return service::get_storage_service().invoke_on(0, [endpoint] (auto& ss) {
        return ss.mutate_token_metadata([&ss, endpoint] (mutable_token_metadata_ptr tmptr) mutable {
            // initiate the token metadata endpoints cache reset
            tmptr->invalidate_cached_rings();
            // re-read local rack and DC info
            tmptr->update_topology(endpoint);
            return make_ready_future<>();
        });
    });
}

void storage_service::init_messaging_service() {
    _messaging.local().register_replication_finished([] (gms::inet_address from) {
        return get_local_storage_service().confirm_replication(from);
    });
}

future<> storage_service::uninit_messaging_service() {
    return _messaging.local().unregister_replication_finished();
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
        if (!ss._removing_node) {
            return make_ready_future<sstring>(sstring("No token removals in process."));
        }
        auto tokens = ss.get_token_metadata().get_tokens(*ss._removing_node);
        if (tokens.empty()) {
            return make_ready_future<sstring>(sstring("Node has no token"));
        }
        auto status = format("Removing token ({}). Waiting for replication confirmation from [{}].",
                tokens.front(), join(",", ss._replicating_nodes));
        return make_ready_future<sstring>(status);
    });
}

future<> storage_service::force_remove_completion() {
    return run_with_no_api_lock([] (storage_service& ss) {
        return seastar::async([&ss] {
            while (!ss._operation_in_progress.empty()) {
                if (ss._operation_in_progress != sstring("removenode")) {
                    throw std::runtime_error(format("Operation {} is in progress, try again", ss._operation_in_progress));
                }

                // This flag will make removenode stop waiting for the confirmation,
                // wait it to complete
                slogger.info("Operation removenode is in progress, wait for it to complete");

                ss._force_remove_completion = true;
                sleep_abortable(std::chrono::seconds(1), ss._abort_source).get();
                ss._force_remove_completion = false;
            }
            ss._operation_in_progress = sstring("removenode_force");

            try {
                const auto& tm = ss.get_token_metadata();
                if (!ss._replicating_nodes.empty() || !tm.get_leaving_endpoints().empty()) {
                    auto leaving = tm.get_leaving_endpoints();
                    slogger.warn("Removal not confirmed for {}, Leaving={}", join(",", ss._replicating_nodes), leaving);
                    for (auto endpoint : leaving) {
                        utils::UUID host_id;
                        auto tokens = tm.get_tokens(endpoint);
                        try {
                            host_id = tm.get_host_id(endpoint);
                        } catch (...) {
                            slogger.warn("No host_id is found for endpoint {}", endpoint);
                            continue;
                        }
                        ss._gossiper.advertise_token_removed(endpoint, host_id).get();
                        std::unordered_set<token> tokens_set(tokens.begin(), tokens.end());
                        ss.excise(tokens_set, endpoint);
                    }
                    ss._replicating_nodes.clear();
                    ss._removing_node = std::nullopt;
                } else {
                    slogger.warn("No tokens to force removal on, call 'removenode' first");
                }
                ss._operation_in_progress = {};
            } catch (...) {
                ss._operation_in_progress = {};
                throw;
            }
        });
    });
}

/**
 * Takes an ordered list of adjacent tokens and divides them in the specified number of ranges.
 */
static std::vector<std::pair<dht::token_range, uint64_t>>
calculate_splits(std::vector<dht::token> tokens, uint64_t split_count, column_family& cf) {
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
storage_service::get_ranges_for_endpoint(const sstring& name, const gms::inet_address& ep) const {
    return _db.local().find_keyspace(name).get_replication_strategy().get_ranges(ep);
}

dht::token_range_vector
storage_service::get_all_ranges(const std::vector<token>& sorted_tokens) const {
    if (sorted_tokens.empty())
        return dht::token_range_vector();
    int size = sorted_tokens.size();
    dht::token_range_vector ranges;
    ranges.push_back(dht::token_range::make_ending_with(range_bound<token>(sorted_tokens[0], true)));
    for (int i = 1; i < size; ++i) {
        dht::token_range r(range<token>::bound(sorted_tokens[i - 1], false), range<token>::bound(sorted_tokens[i], true));
        ranges.push_back(r);
    }
    ranges.push_back(dht::token_range::make_starting_with(range_bound<token>(sorted_tokens[size-1], false)));

    return ranges;
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
    return _db.local().find_keyspace(keyspace).get_replication_strategy().get_natural_endpoints(pos);
}

future<std::unordered_map<sstring, sstring>>
storage_service::view_build_statuses(sstring keyspace, sstring view_name) const {
    return _sys_dist_ks.local().view_status(std::move(keyspace), std::move(view_name)).then([this] (std::unordered_map<utils::UUID, sstring> status) {
        auto& endpoint_to_host_id = get_token_metadata().get_endpoint_to_host_id_map_for_reading();
        return boost::copy_range<std::unordered_map<sstring, sstring>>(endpoint_to_host_id
                | boost::adaptors::transformed([&status] (const std::pair<inet_address, utils::UUID>& p) {
                    auto it = status.find(p.second);
                    auto s = it != status.end() ? std::move(it->second) : "UNKNOWN";
                    return std::pair(p.first.to_sstring(), std::move(s));
                }));
    });
}

future<> init_storage_service(sharded<abort_source>& abort_source, distributed<database>& db, sharded<gms::gossiper>& gossiper,
        sharded<db::system_distributed_keyspace>& sys_dist_ks,
        sharded<db::view::view_update_generator>& view_update_generator, sharded<gms::feature_service>& feature_service,
        storage_service_config config,
        sharded<service::migration_manager>& mm, sharded<locator::shared_token_metadata>& stm,
        sharded<netw::messaging_service>& ms, sharded<cdc::generation_service>& cdc_gen_service,
        sharded<repair_service>& repair) {
    return service::get_storage_service().start(std::ref(abort_source), std::ref(db), std::ref(gossiper), std::ref(sys_dist_ks), std::ref(view_update_generator), std::ref(feature_service), config, std::ref(mm), std::ref(stm), std::ref(ms), std::ref(cdc_gen_service), std::ref(repair));
}

future<> deinit_storage_service() {
    return service::get_storage_service().stop();
}

void storage_service::notify_down(inet_address endpoint) {
    container().invoke_on_all([endpoint] (auto&& ss) {
        ss._messaging.local().remove_rpc_client(netw::msg_addr{endpoint, 0});
        return seastar::async([&ss, endpoint] {
            ss._lifecycle_subscribers.for_each([endpoint] (endpoint_lifecycle_subscriber* subscriber) {
                try {
                    subscriber->on_down(endpoint);
                } catch (...) {
                    slogger.warn("Down notification failed {}: {}", endpoint, std::current_exception());
                }
            });
        });
    }).get();
    slogger.debug("Notify node {} has been down", endpoint);
}

void storage_service::notify_left(inet_address endpoint) {
    container().invoke_on_all([endpoint] (auto&& ss) {
        return seastar::async([&ss, endpoint] {
            ss._lifecycle_subscribers.for_each([endpoint] (endpoint_lifecycle_subscriber* subscriber) {
                try {
                    subscriber->on_leave_cluster(endpoint);
                } catch (...) {
                    slogger.warn("Leave cluster notification failed {}: {}", endpoint, std::current_exception());
                }
            });
        });
    }).get();
    slogger.debug("Notify node {} has left the cluster", endpoint);
}

void storage_service::notify_up(inet_address endpoint)
{
    if (!_gossiper.is_cql_ready(endpoint) || !_gossiper.is_alive(endpoint)) {
        return;
    }
    container().invoke_on_all([endpoint] (auto&& ss) {
        return seastar::async([&ss, endpoint] {
            ss._lifecycle_subscribers.for_each([endpoint] (endpoint_lifecycle_subscriber* subscriber) {
                try {
                    subscriber->on_up(endpoint);
                } catch (...) {
                    slogger.warn("Up notification failed {}: {}", endpoint, std::current_exception());
                }
            });
        });
    }).get();
    slogger.debug("Notify node {} has been up", endpoint);
}

void storage_service::notify_joined(inet_address endpoint)
{
    if (!_gossiper.is_normal(endpoint)) {
        return;
    }

    container().invoke_on_all([endpoint] (auto&& ss) {
        return seastar::async([&ss, endpoint] {
            ss._lifecycle_subscribers.for_each([endpoint] (endpoint_lifecycle_subscriber* subscriber) {
                try {
                    subscriber->on_join_cluster(endpoint);
                } catch (...) {
                    slogger.warn("Join cluster notification failed {}: {}", endpoint, std::current_exception());
                }
            });
        });
    }).get();
    slogger.debug("Notify node {} has joined the cluster", endpoint);
}

void storage_service::notify_cql_change(inet_address endpoint, bool ready)
{
    if (ready) {
        notify_up(endpoint);
    } else {
        notify_down(endpoint);
    }
}

future<bool> storage_service::is_cleanup_allowed(sstring keyspace) {
    return container().invoke_on(0, [keyspace = std::move(keyspace)] (storage_service& ss) {
        auto my_address = ss.get_broadcast_address();
        auto pending_ranges = ss.get_token_metadata().has_pending_ranges(keyspace, my_address);
        bool is_bootstrap_mode = ss._is_bootstrap_mode;
        slogger.debug("is_cleanup_allowed: keyspace={}, is_bootstrap_mode={}, pending_ranges={}",
                keyspace, is_bootstrap_mode, pending_ranges);
        return !is_bootstrap_mode && !pending_ranges;
    });
}

bool storage_service::is_repair_based_node_ops_enabled() {
    return _db.local().get_config().enable_repair_based_node_ops();
}

node_ops_meta_data::node_ops_meta_data(
        utils::UUID ops_uuid,
        gms::inet_address coordinator,
        shared_ptr<node_ops_info> ops,
        std::function<future<> ()> abort_func,
        std::function<void ()> signal_func)
    : _ops_uuid(std::move(ops_uuid))
    , _coordinator(std::move(coordinator))
    , _abort(std::move(abort_func))
    , _abort_source(seastar::make_shared<abort_source>())
    , _signal(std::move(signal_func))
    , _ops(std::move(ops))
    , _watchdog([sig = _signal] { sig(); }) {
    _watchdog.arm(_watchdog_interval);
}

future<> node_ops_meta_data::abort() {
    slogger.debug("node_ops_meta_data: ops_uuid={} abort", _ops_uuid);
    _aborted = true;
    if (_ops) {
        _ops->abort = true;
    }
    _watchdog.cancel();
    return _abort();
}

void node_ops_meta_data::update_watchdog() {
    slogger.debug("node_ops_meta_data: ops_uuid={} update_watchdog", _ops_uuid);
    if (_aborted) {
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

void storage_service::node_ops_update_heartbeat(utils::UUID ops_uuid) {
    slogger.debug("node_ops_update_heartbeat: ops_uuid={}", ops_uuid);
    auto permit = seastar::get_units(_node_ops_abort_sem, 1);
    auto it = _node_ops.find(ops_uuid);
    if (it != _node_ops.end()) {
        node_ops_meta_data& meta = it->second;
        meta.update_watchdog();
    }
}

void storage_service::node_ops_done(utils::UUID ops_uuid) {
    slogger.debug("node_ops_done: ops_uuid={}", ops_uuid);
    auto permit = seastar::get_units(_node_ops_abort_sem, 1);
    auto it = _node_ops.find(ops_uuid);
    if (it != _node_ops.end()) {
        node_ops_meta_data& meta = it->second;
        meta.cancel_watchdog();
        _node_ops.erase(it);
    }
}

void storage_service::node_ops_abort(utils::UUID ops_uuid) {
    slogger.debug("node_ops_abort: ops_uuid={}", ops_uuid);
    auto permit = seastar::get_units(_node_ops_abort_sem, 1);
    auto it = _node_ops.find(ops_uuid);
    if (it != _node_ops.end()) {
        node_ops_meta_data& meta = it->second;
        meta.abort().get();
        auto as = meta.get_abort_source();
        if (as && !as->abort_requested()) {
            as->request_abort();
        }
        abort_repair_node_ops(ops_uuid).get();
        _node_ops.erase(it);
    }
}

void storage_service::node_ops_singal_abort(std::optional<utils::UUID> ops_uuid) {
    slogger.debug("node_ops_singal_abort: ops_uuid={}", ops_uuid);
    _node_ops_abort_queue.push_back(ops_uuid);
    _node_ops_abort_cond.signal();
}

future<> storage_service::node_ops_abort_thread() {
    return seastar::async([this] {
        slogger.info("Started node_ops_abort_thread");
        for (;;) {
            _node_ops_abort_cond.wait([this] { return !_node_ops_abort_queue.empty(); }).get();
            slogger.debug("Awoke node_ops_abort_thread: node_ops_abort_queue={}", _node_ops_abort_queue);
            while (!_node_ops_abort_queue.empty()) {
                auto uuid_opt = _node_ops_abort_queue.front();
                _node_ops_abort_queue.pop_front();
                if (!uuid_opt) {
                    return;
                }
                try {
                    storage_service::node_ops_abort(*uuid_opt);
                } catch (...) {
                    slogger.warn("Failed to abort node operation ops_uuid={}: {}", *uuid_opt, std::current_exception());
                }
            }
        }
        slogger.info("Stopped node_ops_abort_thread");
    });
}


} // namespace service

