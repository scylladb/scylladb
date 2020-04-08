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
 * Copyright (C) 2015 ScyllaDB
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
#include "utils/UUID.hh"
#include "gms/inet_address.hh"
#include "log.hh"
#include "service/migration_manager.hh"
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
#include "thrift/server.hh"
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
#include "auth/common.hh"
#include "distributed_loader.hh"
#include "database.hh"
#include <seastar/core/metrics.hh>
#include "cdc/generation.hh"
#include "repair/repair.hh"
#include "service/priority_manager.hh"

using token = dht::token;
using UUID = utils::UUID;
using inet_address = gms::inet_address;

extern logging::logger cdc_log;

namespace service {

static logging::logger slogger("storage_service");

static const sstring SSTABLE_FORMAT_PARAM_NAME = "sstable_format";

distributed<storage_service> _the_storage_service;


int get_generation_number() {
    using namespace std::chrono;
    auto now = high_resolution_clock::now().time_since_epoch();
    int generation_number = duration_cast<seconds>(now).count();
    return generation_number;
}

storage_service::storage_service(abort_source& abort_source, distributed<database>& db, gms::gossiper& gossiper, sharded<auth::service>& auth_service, sharded<db::system_distributed_keyspace>& sys_dist_ks,
        sharded<db::view::view_update_generator>& view_update_generator, gms::feature_service& feature_service, storage_service_config config, sharded<service::migration_notifier>& mn, locator::token_metadata& tm, bool for_testing)
        : _abort_source(abort_source)
        , _feature_service(feature_service)
        , _db(db)
        , _gossiper(gossiper)
        , _auth_service(auth_service)
        , _mnotifier(mn)
        , _service_memory_total(config.available_memory / 10)
        , _service_memory_limiter(_service_memory_total)
        , _for_testing(for_testing)
        , _token_metadata(tm)
        , _mc_feature_listener(*this, _feature_listeners_sem, sstables::sstable_version_types::mc)
        , _replicate_action([this] { return do_replicate_to_all_cores(); })
        , _update_pending_ranges_action([this] { return do_update_pending_ranges(); })
        , _sys_dist_ks(sys_dist_ks)
        , _view_update_generator(view_update_generator) {
    register_metrics();
    sstable_read_error.connect([this] { isolate_on_error(); });
    sstable_write_error.connect([this] { isolate_on_error(); });
    general_disk_error.connect([this] { isolate_on_error(); });
    commit_error.connect([this] { isolate_on_commit_error(); });

    if (!for_testing) {
        if (this_shard_id() == 0) {
            _feature_service.cluster_supports_mc_sstable().when_enabled(_mc_feature_listener);
        }
    } else {
        _sstables_format = sstables::sstable_version_types::mc;
    }
}

void storage_service::enable_all_features() {
    auto features = get_config_supported_features_set();
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

void
storage_service::isolate_on_error() {
    do_isolate_on_error(disk_error::regular);
}

void
storage_service::isolate_on_commit_error() {
    do_isolate_on_error(disk_error::commit);
}

bool storage_service::is_auto_bootstrap() const {
    return _db.local().get_config().auto_bootstrap();
}

// The features this node supports
std::set<std::string_view> storage_service::get_known_features_set() {
    return _feature_service.known_feature_set();
}

sstring storage_service::get_config_supported_features() {
    return join(",", get_config_supported_features_set());
}

// The features this node supports and is allowed to advertise to other nodes
std::set<std::string_view> storage_service::get_config_supported_features_set() {
    auto features = _feature_service.known_feature_set();

    if (sstables::is_later(sstables::sstable_version_types::mc, _sstables_format)) {
        features.erase(gms::features::UNBOUNDED_RANGE_TOMBSTONES);
    }

    return features;
}

std::unordered_set<token> get_replace_tokens() {
    std::unordered_set<token> ret;
    std::unordered_set<sstring> tokens;
    auto tokens_string = get_local_storage_service().db().local().get_config().replace_token();
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

std::optional<UUID> get_replace_node() {
    auto replace_node = get_local_storage_service().db().local().get_config().replace_node();
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

bool get_property_rangemovement() {
    return get_local_storage_service().db().local().get_config().consistent_rangemovement();
}

bool get_property_load_ring_state() {
    return get_local_storage_service().db().local().get_config().load_ring_state();
}

bool storage_service::should_bootstrap() const {
    return is_auto_bootstrap() && !db::system_keyspace::bootstrap_complete() && !_gossiper.get_seeds().count(get_broadcast_address());
}

// Runs inside seastar::async context
void storage_service::prepare_to_join(std::vector<inet_address> loaded_endpoints, const std::unordered_map<gms::inet_address, sstring>& loaded_peer_features, bind_messaging_port do_bind) {
    std::map<gms::application_state, gms::versioned_value> app_states;
    if (db::system_keyspace::was_decommissioned()) {
        if (db().local().get_config().override_decommission()) {
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
    if (db().local().is_replacing()) {
        if (db::system_keyspace::bootstrap_complete()) {
            throw std::runtime_error("Cannot replace address with a node that is already bootstrapped");
        }
        if (!is_auto_bootstrap()) {
            throw std::runtime_error("Trying to replace_address with auto_bootstrap disabled will not work, check your configuration");
        }

        std::tie(_bootstrap_tokens, _cdc_streams_ts) = prepare_replacement_info(loaded_peer_features).get0();
        auto replace_address = db().local().get_replace_address();
        replacing_a_node_with_same_ip = replace_address && *replace_address == get_broadcast_address();
        replacing_a_node_with_diff_ip = replace_address && *replace_address != get_broadcast_address();
    } else if (should_bootstrap()) {
        check_for_endpoint_collision(loaded_peer_features).get();
    } else {
        auto seeds = _gossiper.get_seeds();
        auto my_ep = get_broadcast_address();
        auto local_features = get_known_features_set();

        if (seeds.count(my_ep)) {
            // This node is a seed node
            if (loaded_peer_features.empty()) {
                // This is a competely new seed node, skip the check
                slogger.info("Checking remote features skipped, since this node is a new seed node which knows nothing about the cluster");
            } else {
                // This is a existing seed node
                if (seeds.size() == 1) {
                    // This node is the only seed node, check features with system table
                    slogger.info("Checking remote features with system table, since this node is the only seed node");
                    _gossiper.check_knows_remote_features(local_features, loaded_peer_features);
                } else {
                    // More than one seed node in the seed list, do shadow round with other seed nodes
                    try {
                        slogger.info("Checking remote features with gossip and system tables");
                        _gossiper.do_shadow_round().get();
                    } catch (...) {
                        slogger.info("Shadow round failed with {}, checking remote features with system tables only",
                                std::current_exception());
                        _gossiper.finish_shadow_round();
                    }

                    _gossiper.check_knows_remote_features(local_features, loaded_peer_features);
                    _gossiper.reset_endpoint_state_map().get();
                    for (auto ep : loaded_endpoints) {
                        _gossiper.add_saved_endpoint(ep);
                    }
                }
            }
        } else {
            // This node is a non-seed node
            // Do shadow round to check if this node knows all the features
            // advertised by all other nodes, otherwise this node is too old
            // (missing features) to join the cluser.
            slogger.info("Checking remote features with gossip");
            _gossiper.do_shadow_round().get();
            _gossiper.check_knows_remote_features(local_features, loaded_peer_features);
            _gossiper.reset_endpoint_state_map().get();
            for (auto ep : loaded_endpoints) {
                _gossiper.add_saved_endpoint(ep);
            }
        }
    }

    // If this is a restarting node, we should update tokens before gossip starts
    auto my_tokens = db::system_keyspace::get_saved_tokens().get0();
    bool restarting_normal_node = db::system_keyspace::bootstrap_complete() && !db().local().is_replacing() && !my_tokens.empty();
    if (restarting_normal_node) {
        slogger.info("Restarting a node in NORMAL status");
        // This node must know about its chosen tokens before other nodes do
        // since they may start sending writes to this node after it gossips status = NORMAL.
        // Therefore we update _token_metadata now, before gossip starts.
        _token_metadata.update_normal_tokens(my_tokens, get_broadcast_address());

        _cdc_streams_ts = db::system_keyspace::get_saved_cdc_streams_timestamp().get0();
        if (!_cdc_streams_ts && db().local().get_config().check_experimental(db::experimental_features_t::CDC)) {
            // We could not have completed joining if we didn't generate and persist a CDC streams timestamp,
            // unless we are restarting after upgrading from non-CDC supported version.
            // In that case we won't begin a CDC generation: it should be done by one of the nodes
            // after it learns that it everyone supports the CDC feature.
            cdc_log.warn(
                    "Restarting node in NORMAL status with CDC enabled, but no streams timestamp was proposed"
                    " by this node according to its local tables. Are we upgrading from a non-CDC supported version?");
        }
    }

    if (replacing_a_node_with_same_ip) {
        slogger.info("Replacing a node with same IP address, my address={}, node being replaced={}",
                    get_broadcast_address(), get_broadcast_address());
        slogger.info("Update tokens for replacing node early, replacing node has the same IP address of the node being replaced");
        _token_metadata.update_normal_tokens(_bootstrap_tokens, get_broadcast_address());
    }

    if (replacing_a_node_with_diff_ip) {
        // replacing_a_node_with_diff_ip guarantees replace_address contains a value
        auto replace_address = db().local().get_replace_address();
        slogger.info("Replacing a node with different IP address, my address={}, node to being replaced={}",
                get_broadcast_address(), *replace_address);
    }

    // have to start the gossip service before we can see any info on other nodes.  this is necessary
    // for bootstrap to get the load info it needs.
    // (we won't be part of the storage ring though until we add a counterId to our state, below.)
    // Seed the host ID-to-endpoint map with our own ID.
    auto local_host_id = db::system_keyspace::get_local_host_id().get0();
    get_storage_service().invoke_on_all([local_host_id] (auto& ss) {
        ss._local_host_id = local_host_id;
    }).get();
    auto features = get_config_supported_features();
    if (!replacing_a_node_with_diff_ip) {
        // Replacing node with a different ip should own the host_id only after
        // the replacing node becomes NORMAL status. It is updated in
        // handle_state_normal().
        _token_metadata.update_host_id(local_host_id, get_broadcast_address());
    }

    // Replicate the tokens early because once gossip runs other nodes
    // might send reads/writes to this node. Replicate it early to make
    // sure the tokens are valid on all the shards.
    replicate_to_all_cores().get();

    auto broadcast_rpc_address = utils::fb_utilities::get_broadcast_rpc_address();
    auto& proxy = service::get_storage_proxy();
    // Ensure we know our own actual Schema UUID in preparation for updates
    auto schema_version = update_schema_version(proxy, _feature_service.cluster_schema_features()).get0();
    app_states.emplace(gms::application_state::NET_VERSION, versioned_value::network_version());
    app_states.emplace(gms::application_state::HOST_ID, versioned_value::host_id(local_host_id));
    app_states.emplace(gms::application_state::RPC_ADDRESS, versioned_value::rpcaddress(broadcast_rpc_address));
    app_states.emplace(gms::application_state::RELEASE_VERSION, versioned_value::release_version());
    app_states.emplace(gms::application_state::SUPPORTED_FEATURES, versioned_value::supported_features(features));
    app_states.emplace(gms::application_state::CACHE_HITRATES, versioned_value::cache_hitrates(""));
    app_states.emplace(gms::application_state::SCHEMA_TABLES_VERSION, versioned_value(db::schema_tables::version));
    app_states.emplace(gms::application_state::RPC_READY, versioned_value::cql_ready(false));
    app_states.emplace(gms::application_state::VIEW_BACKLOG, versioned_value(""));
    app_states.emplace(gms::application_state::SCHEMA, versioned_value::schema(schema_version));
    if (restarting_normal_node) {
        // Order is important: both the CDC streams timestamp and tokens must be known when a node handles our status.
        // Exception: there might be no CDC streams timestamp proposed by us if we're upgrading from a non-CDC version.
        app_states.emplace(gms::application_state::TOKENS, versioned_value::tokens(my_tokens));
        app_states.emplace(gms::application_state::CDC_STREAMS_TIMESTAMP, versioned_value::cdc_streams_timestamp(_cdc_streams_ts));
        app_states.emplace(gms::application_state::STATUS, versioned_value::normal(my_tokens));
    }
    slogger.info("Starting up server gossip");

    auto generation_number = db::system_keyspace::increment_and_get_generation().get0();
    _gossiper.start_gossiping(generation_number, app_states, gms::bind_messaging_port(bool(do_bind))).get();

    // gossip snitch infos (local DC and rack)
    gossip_snitch_info().get();

    // gossip local partitioner information (shard count and ignore_msb_bits)
    gossip_sharder().get();

    // gossip Schema.emptyVersion forcing immediate check for schema updates (see MigrationManager#maybeScheduleSchemaPull)

    // Wait for gossip to settle so that the fetures will be enabled
    if (do_bind) {
        gms::get_local_gossiper().wait_for_gossip_to_settle().get();
    }
    wait_for_feature_listeners_to_finish();
}

void storage_service::wait_for_feature_listeners_to_finish() {
    // This makes sure that every feature listener that was started
    // finishes before we move forward.
    get_units(_feature_listeners_sem, 1).get0();
}

static auth::permissions_cache_config permissions_cache_config_from_db_config(const db::config& dc) {
    auth::permissions_cache_config c;
    c.max_entries = dc.permissions_cache_max_entries();
    c.validity_period = std::chrono::milliseconds(dc.permissions_validity_in_ms());
    c.update_period = std::chrono::milliseconds(dc.permissions_update_interval_in_ms());

    return c;
}

static auth::service_config auth_service_config_from_db_config(const db::config& dc) {
    const qualified_name qualified_authorizer_name(auth::meta::AUTH_PACKAGE_NAME, dc.authorizer());
    const qualified_name qualified_authenticator_name(auth::meta::AUTH_PACKAGE_NAME, dc.authenticator());
    const qualified_name qualified_role_manager_name(auth::meta::AUTH_PACKAGE_NAME, dc.role_manager());

    auth::service_config c;
    c.authorizer_java_name = qualified_authorizer_name;
    c.authenticator_java_name = qualified_authenticator_name;
    c.role_manager_java_name = qualified_role_manager_name;

    return c;
}

void storage_service::maybe_start_sys_dist_ks() {
    supervisor::notify("starting system distributed keyspace");
    _sys_dist_ks.start(std::ref(cql3::get_query_processor()), std::ref(service::get_migration_manager())).get();
    _sys_dist_ks.invoke_on_all(&db::system_distributed_keyspace::start).get();
}

/* If we're not a seed node or there are seeds other than us,
 * wait until we contact any of them and learn what tokens are used by other nodes in the cluster,
 * with a timeout.
 *
 * Returns true if we're the only seed or we've managed to learn the other nodes' tokens before timeouting.
 */
// Run in seastar::async context.
static bool wait_for_other_tokens(const locator::token_metadata& tm,
        const std::set<inet_address>& seeds, inet_address my_ep, abort_source& as) {
    if (seeds.size() == 1 && seeds.count(my_ep)) {
        return true;
    }

    int retries = 30;
    while (retries--) {
        auto& tok_to_ep = tm.get_token_to_endpoint();
        if (std::any_of(tok_to_ep.begin(), tok_to_ep.end(),
                [my_ep] (const std::pair<token, inet_address>& p) { return p.second != my_ep; })) {
            return true;
        }

        slogger.warn("Couldn't retrieve other nodes' tokens. Retrying again in one second...");
        sleep_abortable(std::chrono::seconds(1), as).get();
    }

    return false;
}

// Runs inside seastar::async context
void storage_service::join_token_ring(int delay) {
    // This function only gets called on shard 0, but we want to set _joined
    // on all shards, so this variable can be later read locally.
    get_storage_service().invoke_on_all([] (auto&& ss) {
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
    slogger.debug("Bootstrap variables: {} {} {} {}",
                 is_auto_bootstrap(),
                 db::system_keyspace::bootstrap_in_progress(),
                 db::system_keyspace::bootstrap_complete(),
                 _gossiper.get_seeds().count(get_broadcast_address()));
    if (is_auto_bootstrap() && !db::system_keyspace::bootstrap_complete() && _gossiper.get_seeds().count(get_broadcast_address())) {
        slogger.info("This node will not auto bootstrap because it is configured to be a seed node.");
    }
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
        while (!get_local_migration_manager().have_schema_agreement()) {
            set_mode(mode::JOINING, "waiting for schema information to complete", true);
            sleep_abortable(std::chrono::seconds(1), _abort_source).get();
        }
        set_mode(mode::JOINING, "schema complete, ready to bootstrap", true);
        set_mode(mode::JOINING, "waiting for pending range calculation", true);
        update_pending_ranges().get();
        set_mode(mode::JOINING, "calculation complete, ready to bootstrap", true);
        slogger.debug("... got ring + schema info");

        auto t = gms::gossiper::clk::now();
        while (get_property_rangemovement() &&
            (!_token_metadata.get_bootstrap_tokens().empty() ||
             !_token_metadata.get_leaving_endpoints().empty())) {
            auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(gms::gossiper::clk::now() - t).count();
            slogger.info("Checking bootstrapping/leaving nodes: tokens {}, leaving {}, sleep 1 second and check again ({} seconds elapsed)",
                _token_metadata.get_bootstrap_tokens().size(),
                _token_metadata.get_leaving_endpoints().size(),
                elapsed);

            sleep_abortable(std::chrono::seconds(1), _abort_source).get();

            if (gms::gossiper::clk::now() > t + std::chrono::seconds(60)) {
                throw std::runtime_error("Other bootstrapping/leaving nodes detected, cannot bootstrap while consistent_rangemovement is true");
            }

            // Check the schema and pending range again
            while (!get_local_migration_manager().have_schema_agreement()) {
                set_mode(mode::JOINING, "waiting for schema information to complete", true);
                sleep_abortable(std::chrono::seconds(1), _abort_source).get();
            }
            update_pending_ranges().get();
        }
        slogger.info("Checking bootstrapping/leaving nodes: ok");

        if (!db().local().is_replacing()) {
            if (_token_metadata.is_member(get_broadcast_address())) {
                throw std::runtime_error("This node is already a member of the token ring; bootstrap aborted. (If replacing a dead node, remove the old one from the ring first.)");
            }
            set_mode(mode::JOINING, "getting bootstrap token", true);
            if (resume_bootstrap) {
                _bootstrap_tokens = db::system_keyspace::get_saved_tokens().get0();
                if (!_bootstrap_tokens.empty()) {
                    slogger.info("Using previously saved tokens = {}", _bootstrap_tokens);
                } else {
                    _bootstrap_tokens = boot_strapper::get_bootstrap_tokens(_token_metadata, _db.local());
                    slogger.info("Using newly generated tokens = {}", _bootstrap_tokens);
                }
            } else {
                _bootstrap_tokens = boot_strapper::get_bootstrap_tokens(_token_metadata, _db.local());
                slogger.info("Using newly generated tokens = {}", _bootstrap_tokens);
            }
        } else {
            auto replace_addr = db().local().get_replace_address();
            if (replace_addr && *replace_addr != get_broadcast_address()) {
                // Sleep additionally to make sure that the server actually is not alive
                // and giving it more time to gossip if alive.
                sleep_abortable(service::load_broadcaster::BROADCAST_INTERVAL, _abort_source).get();

                // check for operator errors...
                for (auto token : _bootstrap_tokens) {
                    auto existing = _token_metadata.get_endpoint(token);
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
                _bootstrap_tokens = boot_strapper::get_random_tokens(_token_metadata, num_tokens);
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
    // This node must know about its chosen tokens before other nodes do
    // since they may start sending writes to this node after it gossips status = NORMAL.
    // Therefore, in case we haven't updated _token_metadata with our tokens yet, do it now.
    _token_metadata.update_normal_tokens(_bootstrap_tokens, get_broadcast_address());

    if (!db::system_keyspace::bootstrap_complete()) {
        // If we're not bootstrapping nor replacing, then we shouldn't have chosen a CDC streams timestamp yet.
        assert(should_bootstrap() || db().local().is_replacing() || !_cdc_streams_ts);
    }

    if (!_cdc_streams_ts && db().local().get_config().check_experimental(db::experimental_features_t::CDC)) {
        // If we didn't choose a CDC streams timestamp at this point, then either
        // 1. we're replacing a node which didn't gossip a CDC streams timestamp for whatever reason,
        // 2. we've already bootstrapped, but are upgrading from a non-CDC version,
        // 3. we're starting for the first time, but we're skipping the streaming phase (seed node/auto_bootstrap=off)
        //    and directly joining the token ring.

        // In the replacing case we won't propose any CDC generation: we're not introducing any new tokens,
        // so the current generation used by the cluster is fine.
        // In the case of an upgrading cluster, one of the nodes is responsible for proposing
        // the first CDC generation. We'll check if it's us.
        // Finally, if we're simply a new node joining the ring but skipping bootstrapping,
        // we'll propose a new generation just as normally bootstrapping nodes do.

        if (!db::system_keyspace::bootstrap_complete()) {
            // Check if we managed to learn about other nodes' tokens (if there are other nodes).
            // We might have not due to misconfiguration, e.g. skipping the "wait for gossip to settle" phase,
            // or due to a network partition which didn't allow us to contact other seeds.
            // But CDC requires that we know other nodes' tokens - they are needed to make a new generation of streams.
            if (!wait_for_other_tokens(_token_metadata, _gossiper.get_seeds(), get_broadcast_address(), _abort_source)) {
                throw std::runtime_error(format(
                    "Can't boot this node because we weren't able to retrieve other nodes' tokens,"
                    " which is required for CDC to work. Make sure that:\n"
                    " - you don't use the \"--skip-wait-for-gossip-to-settle\" flag,\n"
                    " - it is possible to contact other nodes (fix any network issues).\n"
                    "Configured seeds: {}", _gossiper.get_seeds()));
            }
        }

        if (!db().local().is_replacing()
                && (!db::system_keyspace::bootstrap_complete()
                    || cdc::should_propose_first_generation(get_broadcast_address(), _gossiper))) {

            _cdc_streams_ts = cdc::make_new_cdc_generation(db().local().get_config(),
                    _bootstrap_tokens, _token_metadata, _gossiper,
                    _sys_dist_ks.local(), get_ring_delay(), _for_testing);
        }
    }

    // Persist the CDC streams timestamp before we persist bootstrap_state = COMPLETED.
    if (_cdc_streams_ts) {
        db::system_keyspace::update_cdc_streams_timestamp(*_cdc_streams_ts).get();
    }
    // If we crash now, we will choose a new CDC streams timestamp anyway (because we will also choose a new set of tokens).
    // But if we crash after setting bootstrap_state = COMPLETED, we will keep using the persisted CDC streams timestamp after restarting.

    db::system_keyspace::set_bootstrap_state(db::system_keyspace::bootstrap_state::COMPLETED).get();
    // At this point our local tokens and CDC streams timestamp are chosen (_bootstrap_tokens, _cdc_streams_ts) and will not be changed.

    replicate_to_all_cores().get();
    // start participating in the ring.
    set_gossip_tokens(_bootstrap_tokens, _cdc_streams_ts);
    set_mode(mode::NORMAL, "node is now in normal status", true);

    if (_token_metadata.sorted_tokens().empty()) {
        auto err = format("join_token_ring: Sorted token in token_metadata is empty");
        slogger.error("{}", err);
        throw std::runtime_error(err);
    }

    // Retrieve the latest CDC generation seen in gossip (if any).
    scan_cdc_generations();

    _auth_service.start(
            permissions_cache_config_from_db_config(_db.local().get_config()),
            std::ref(cql3::get_query_processor()),
            std::ref(_mnotifier),
            std::ref(service::get_migration_manager()),
            auth_service_config_from_db_config(_db.local().get_config())).get();

    _auth_service.invoke_on_all([] (auth::service& auth) {
        return auth.start(service::get_local_migration_manager());
    }).get();

    supervisor::notify("starting tracing");
    tracing::tracing::start_tracing().get();
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

// Run inside seastar::async context.
bool storage_service::do_handle_cdc_generation(db_clock::time_point ts) {

    auto gen = _sys_dist_ks.local().read_cdc_topology_description(
            ts, { _token_metadata.count_normal_token_owners() }).get0();
    if (!gen) {
        throw std::runtime_error(format(
            "Could not find CDC generation with timestamp {} in distributed system tables (current time: {}),"
            " even though some node gossiped about it.",
            ts, db_clock::now()));
    }

    // If we're not gossiping our own generation timestamp (because we've upgraded from a non-CDC/old version,
    // or we somehow lost it due to a byzantine failure), start gossiping someone else's timestamp.
    // This is to avoid the upgrade check on every restart (see `should_propose_first_cdc_generation`).
    // And if we notice that `ts` is higher than our timestamp, we will start gossiping it instead,
    // so if the node that initially gossiped `ts` leaves the cluster while `ts` is still the latest generation,
    // the cluster will remember.
    if (!_cdc_streams_ts || *_cdc_streams_ts < ts) {
        _cdc_streams_ts = ts;
        db::system_keyspace::update_cdc_streams_timestamp(ts).get();
        _gossiper.add_local_application_state(
                gms::application_state::CDC_STREAMS_TIMESTAMP, versioned_value::cdc_streams_timestamp(ts)).get();
    }

    class orer {
    private:
        bool _result = false;
    public:
        future<> operator()(bool value) {
            _result = value || _result;
            return make_ready_future<>();
        }
        bool get() {
            return _result;
        }
    };

    // Return `true` iff the generation was inserted on any of our shards.
    return get_storage_service().map_reduce(orer(), [ts, &gen] (storage_service& ss) {
        auto gen_ = *gen;
        return ss._cdc_metadata.insert(ts, std::move(gen_));
    }).get0();
}

class ander {
private:
    bool _result = true;
public:
    future<> operator()(bool value) {
        _result = value && _result;
        return make_ready_future<>();
    }
    bool get() {
        return _result;
    }
};

void storage_service::async_handle_cdc_generation(db_clock::time_point ts) {

    // It is safe to discard this future: we keep the storage_service, gossiper,
    // and system distributed keyspace alive for the whole duration of this operation.
    (void)seastar::async([ts,
        g = _gossiper.shared_from_this(), ss = this->shared_from_this(), sys_dist_ks = _sys_dist_ks.local_shared()
    ] {
        while (true) {
            sleep_abortable(std::chrono::seconds(5), ss->_abort_source).get();
            try {
                bool using_this_gen = ss->do_handle_cdc_generation(ts);
                if (using_this_gen) {
                    cdc::update_streams_description(ts, sys_dist_ks,
                            [ss] { return ss->get_token_metadata().count_normal_token_owners(); }, ss->_abort_source);
                }
                return;
            } catch (...) {
                if (get_storage_service().map_reduce(ander(), [ts] (storage_service& ss) {
                    return ss._cdc_metadata.known_or_obsolete(ts);
                }).get0()) {
                    return;
                }

                cdc_log.warn("Could not retrieve CDC streams with timestamp {}: {}. Will retry again.",
                        ts, std::current_exception());
            }
        }
    });
}

// Run inside async
void storage_service::handle_cdc_generation(std::optional<db_clock::time_point> ts) {
    if (!ts) {
        return;
    }

    if (!db::system_keyspace::bootstrap_complete() || !_sys_dist_ks.local_is_initialized()) {
        // We still haven't finished the startup process.
        // We will handle this generation in `scan_cdc_generations` (unless there's a newer one).
        return;
    }

    if (get_storage_service().map_reduce(ander(), [ts = *ts] (storage_service& ss) {
        return !ss._cdc_metadata.prepare(ts);
    }).get0()) {
        return;
    }

    bool using_this_gen = false;
    try {
        using_this_gen = do_handle_cdc_generation(*ts);
    } catch(...) {
        cdc_log.warn("Could not retrieve CDC streams with timestamp {} on gossip event: {}."
                " Will retry in the background.", *ts, std::current_exception());
        async_handle_cdc_generation(*ts);
        return;
    }

    if (using_this_gen) {
        cdc::update_streams_description(*ts, _sys_dist_ks.local_shared(),
               [ss = this->shared_from_this()] { return ss->get_token_metadata().count_normal_token_owners(); }, _abort_source);
    }
}

// Runs inside seastar::async context.
void storage_service::scan_cdc_generations() {
    std::optional<db_clock::time_point> latest;
    for (const auto& ep: _gossiper.get_endpoint_states()) {
        auto ts = cdc::get_streams_timestamp_for(ep.first, _gossiper);
        if (!latest || (ts && *ts > *latest)) {
            latest = ts;
        }
    }

    if (latest) {
        cdc_log.info("Latest generation seen during startup: {}", *latest);
        handle_cdc_generation(latest);
    } else {
        cdc_log.info("No generation seen during startup.");
    }
}

// Runs inside seastar::async context
void storage_service::bootstrap() {
    _is_bootstrap_mode = true;
    auto x = seastar::defer([this] { _is_bootstrap_mode = false; });

    if (!db().local().is_replacing()) {
        // Wait until we know tokens of existing node before announcing join status.
        _gossiper.wait_for_range_setup().get();

        // Even if we reached this point before but crashed, we will make a new CDC generation.
        // It doesn't hurt: other nodes will (potentially) just do more generation switches.
        // We do this because with this new attempt at bootstrapping we picked a different set of tokens.

        if (db().local().get_config().check_experimental(db::experimental_features_t::CDC)) {
            // Update pending ranges now, so we correctly count ourselves as a pending replica
            // when inserting the new CDC generation.
            _token_metadata.add_bootstrap_tokens(_bootstrap_tokens, get_broadcast_address());
            update_pending_ranges().get();

            // After we pick a generation timestamp, we start gossiping it, and we stick with it.
            // We don't do any other generation switches (unless we crash before complecting bootstrap).
            assert(!_cdc_streams_ts);

            _cdc_streams_ts = cdc::make_new_cdc_generation(db().local().get_config(),
                    _bootstrap_tokens, _token_metadata, _gossiper,
                    _sys_dist_ks.local(), get_ring_delay(), _for_testing);
        } else {
            // We should not be able to join the cluster if other nodes support CDC but we don't.
            // The check should have been made somewhere in prepare_to_join (`check_knows_remote_features`).
            assert(!_feature_service.cluster_supports_cdc());
        }

        _gossiper.add_local_application_state({
            // Order is important: both the CDC streams timestamp and tokens must be known when a node handles our status.
            { gms::application_state::TOKENS, versioned_value::tokens(_bootstrap_tokens) },
            { gms::application_state::CDC_STREAMS_TIMESTAMP, versioned_value::cdc_streams_timestamp(_cdc_streams_ts) },
            { gms::application_state::STATUS, versioned_value::bootstrapping(_bootstrap_tokens) },
        }).get();

        set_mode(mode::JOINING, format("sleeping {} ms for pending range setup", get_ring_delay().count()), true);
        _gossiper.wait_for_range_setup().get();
    } else {
        // Wait until we know tokens of existing node before announcing replacing status.
        set_mode(mode::JOINING, sprint("Wait until local node knows tokens of peer nodes"), true);
        _gossiper.wait_for_range_setup().get();
        set_mode(mode::JOINING, sprint("Announce tokens and status of the replacing node"), true);
        _gossiper.add_local_application_state({
            { gms::application_state::TOKENS, versioned_value::tokens(_bootstrap_tokens) },
            { gms::application_state::CDC_STREAMS_TIMESTAMP, versioned_value::cdc_streams_timestamp(_cdc_streams_ts) },
            { gms::application_state::STATUS, versioned_value::hibernate(true) },
        }).get();
        set_mode(mode::JOINING, format("Wait until peer nodes know the bootstrap tokens of local node"), true);
        _gossiper.wait_for_range_setup().get();
        auto replace_addr = db().local().get_replace_address();
        if (replace_addr) {
            slogger.debug("Removing replaced endpoint {} from system.peers", *replace_addr);
            db::system_keyspace::remove_endpoint(*replace_addr).get();
        }
    }

    _gossiper.check_seen_seeds();

    _db.invoke_on_all([this] (database& db) {
        for (auto& cf : db.get_non_system_column_families()) {
            cf->notify_bootstrap_or_replace_start();
        }
    }).get();

    set_mode(mode::JOINING, "Starting to bootstrap...", true);
    if (is_repair_based_node_ops_enabled()) {
        if (db().local().is_replacing()) {
            replace_with_repair(_db, _token_metadata, _bootstrap_tokens).get();
        } else {
            bootstrap_with_repair(_db, _token_metadata, _bootstrap_tokens).get();
        }
    } else {
        dht::boot_strapper bs(_db, _abort_source, get_broadcast_address(), _bootstrap_tokens, _token_metadata);
        // Does the actual streaming of newly replicated token ranges.
        bs.bootstrap().get();
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

std::unordered_map<dht::token_range, std::vector<inet_address>>
storage_service::get_range_to_address_map(const sstring& keyspace) const {
    return get_range_to_address_map(keyspace, _token_metadata.sorted_tokens());
}

std::unordered_map<dht::token_range, std::vector<inet_address>>
storage_service::get_range_to_address_map_in_local_dc(
        const sstring& keyspace) const {
    std::function<bool(const inet_address&)> filter =  [this](const inet_address& address) {
        return is_local_dc(address);
    };

    auto orig_map = get_range_to_address_map(keyspace, get_tokens_in_local_dc());
    std::unordered_map<dht::token_range, std::vector<inet_address>> filtered_map;
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
    for (auto token : _token_metadata.sorted_tokens()) {
        auto endpoint = _token_metadata.get_endpoint(token);
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

std::unordered_map<dht::token_range, std::vector<inet_address>>
storage_service::get_range_to_address_map(const sstring& keyspace,
        const std::vector<token>& sorted_tokens) const {
    // some people just want to get a visual representation of things. Allow null and set it to the first
    // non-system keyspace.
    if (keyspace == "" && _db.local().get_non_system_keyspaces().empty()) {
        throw std::runtime_error("No keyspace provided and no non system kespace exist");
    }
    const sstring& ks = (keyspace == "") ? _db.local().get_non_system_keyspaces()[0] : keyspace;
    return construct_range_to_endpoint_map(ks, get_all_ranges(sorted_tokens));
}

void storage_service::handle_state_replacing(inet_address replacing_node) {
    slogger.debug("endpoint={} handle_state_replacing", replacing_node);
    auto host_id = _gossiper.get_host_id(replacing_node);
    auto existing_node_opt = _token_metadata.get_endpoint_for_host_id(host_id);
    auto replace_addr = db().local().get_replace_address();
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
    _token_metadata.add_replacing_endpoint(existing_node, replacing_node);
    update_pending_ranges().get();
}

void storage_service::handle_state_bootstrap(inet_address endpoint) {
    slogger.debug("endpoint={} handle_state_bootstrap", endpoint);
    // explicitly check for TOKENS, because a bootstrapping node might be bootstrapping in legacy mode; that is, not using vnodes and no token specified
    auto tokens = get_tokens_for(endpoint);
    auto cdc_streams_ts = cdc::get_streams_timestamp_for(endpoint, _gossiper);

    slogger.debug("Node {} state bootstrapping, token {}", endpoint, tokens);

    // if this node is present in token metadata, either we have missed intermediate states
    // or the node had crashed. Print warning if needed, clear obsolete stuff and
    // continue.
    if (_token_metadata.is_member(endpoint)) {
        // If isLeaving is false, we have missed both LEAVING and LEFT. However, if
        // isLeaving is true, we have only missed LEFT. Waiting time between completing
        // leave operation and rebootstrapping is relatively short, so the latter is quite
        // common (not enough time for gossip to spread). Therefore we report only the
        // former in the log.
        if (!_token_metadata.is_leaving(endpoint)) {
            slogger.info("Node {} state jump to bootstrap", endpoint);
        }
        _token_metadata.remove_endpoint(endpoint);
    }

    handle_cdc_generation(cdc_streams_ts);

    _token_metadata.add_bootstrap_tokens(tokens, endpoint);
    update_pending_ranges().get();

    if (_gossiper.uses_host_id(endpoint)) {
        _token_metadata.update_host_id(_gossiper.get_host_id(endpoint), endpoint);
    }
}

void storage_service::handle_state_normal(inet_address endpoint) {
    slogger.debug("endpoint={} handle_state_normal", endpoint);
    auto tokens = get_tokens_for(endpoint);
    auto cdc_streams_ts = cdc::get_streams_timestamp_for(endpoint, _gossiper);

    slogger.debug("Node {} state normal, token {}", endpoint, tokens);
    cdc_log.debug("Node {} state normal, streams timestamp: {}", endpoint, cdc_streams_ts);

    if (_token_metadata.is_member(endpoint)) {
        slogger.info("Node {} state jump to normal", endpoint);
    }
    update_peer_info(endpoint);

    std::unordered_set<inet_address> endpoints_to_remove;

    auto do_remove_node = [&] (gms::inet_address node) {
        _token_metadata.remove_endpoint(node);
        endpoints_to_remove.insert(node);
    };
    // Order Matters, TM.updateHostID() should be called before TM.updateNormalToken(), (see CASSANDRA-4300).
    if (_gossiper.uses_host_id(endpoint)) {
        auto host_id = _gossiper.get_host_id(endpoint);
        auto existing = _token_metadata.get_endpoint_for_host_id(host_id);
        if (existing && *existing != endpoint) {
            if (*existing == get_broadcast_address()) {
                slogger.warn("Not updating host ID {} for {} because it's mine", host_id, endpoint);
                do_remove_node(endpoint);
            } else if (_gossiper.compare_endpoint_startup(endpoint, *existing) > 0) {
                slogger.warn("Host ID collision for {} between {} and {}; {} is the new owner", host_id, *existing, endpoint, endpoint);
                do_remove_node(*existing);
                slogger.info("Set host_id={} to be owned by node={}, existing={}", host_id, endpoint, *existing);
                _token_metadata.update_host_id(host_id, endpoint);
                slogger.info("Remove node {} from pending replacing endpoint", endpoint);
                _token_metadata.del_replacing_endpoint(endpoint);
            } else {
                slogger.warn("Host ID collision for {} between {} and {}; ignored {}", host_id, *existing, endpoint, endpoint);
                do_remove_node(endpoint);
            }
        } else if (existing && *existing == endpoint) {
            slogger.info("Remove node {} from pending replacing endpoint", endpoint);
            _token_metadata.del_replacing_endpoint(endpoint);
        } else {
            slogger.info("Set host_id={} to be owned by node={}", host_id, endpoint);
            _token_metadata.update_host_id(host_id, endpoint);
        }
    }

    // Tokens owned by the handled endpoint.
    // The endpoint broadcasts its set of chosen tokens. If a token was also chosen by another endpoint,
    // the collision is resolved by assigning the token to the endpoint which started later.
    std::unordered_set<token> owned_tokens;

    for (auto t : tokens) {
        // we don't want to update if this node is responsible for the token and it has a later startup time than endpoint.
        auto current_owner = _token_metadata.get_endpoint(t);
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
            if (ep_to_token_copy.count(*current_owner) < 1) {
                slogger.info("handle_state_normal: endpoints_to_remove endpoint={}", *current_owner);
                endpoints_to_remove.insert(*current_owner);
            }
            slogger.info("handle_state_normal: Nodes {} and {} have the same token {}. {} is the new owner", endpoint, *current_owner, t, endpoint);
        } else {
            slogger.info("handle_state_normal: Nodes {} and {} have the same token {}. Ignoring {}", endpoint, *current_owner, t, endpoint);
        }
    }

    handle_cdc_generation(cdc_streams_ts);

    bool is_member = _token_metadata.is_member(endpoint);
    // Update pending ranges after update of normal tokens immediately to avoid
    // a race where natural endpoint was updated to contain node A, but A was
    // not yet removed from pending endpoints
    _token_metadata.update_normal_tokens(owned_tokens, endpoint);
    _update_pending_ranges_action.trigger_later().get();

    for (auto ep : endpoints_to_remove) {
        remove_endpoint(ep);
    }
    slogger.debug("handle_state_normal: endpoint={} owned_tokens = {}", endpoint, owned_tokens);
    if (!owned_tokens.empty()) {
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

    update_pending_ranges().get();
    if (slogger.is_enabled(logging::log_level::debug)) {
        auto ver = _token_metadata.get_ring_version();
        for (auto& x : _token_metadata.get_token_to_endpoint()) {
            slogger.debug("handle_state_normal: token_metadata.ring_version={}, token={} -> endpoint={}", ver, x.first, x.second);
        }
    }
}

void storage_service::handle_state_leaving(inet_address endpoint) {
    slogger.debug("endpoint={} handle_state_leaving", endpoint);

    auto tokens = get_tokens_for(endpoint);
    auto cdc_streams_ts = cdc::get_streams_timestamp_for(endpoint, _gossiper);

    slogger.debug("Node {} state leaving, tokens {}", endpoint, tokens);
    cdc_log.debug("Node {} state leaving, streams timestamp: {}", endpoint, cdc_streams_ts);

    // If the node is previously unknown or tokens do not match, update tokenmetadata to
    // have this node as 'normal' (it must have been using this token before the
    // leave). This way we'll get pending ranges right.
    if (!_token_metadata.is_member(endpoint)) {
        // FIXME: this code should probably resolve token collisions too, like handle_state_normal
        slogger.info("Node {} state jump to leaving", endpoint);

        handle_cdc_generation(cdc_streams_ts);
        _token_metadata.update_normal_tokens(tokens, endpoint);
    } else {
        auto tokens_ = _token_metadata.get_tokens(endpoint);
        std::set<token> tmp(tokens.begin(), tokens.end());
        if (!std::includes(tokens_.begin(), tokens_.end(), tmp.begin(), tmp.end())) {
            slogger.warn("Node {} 'leaving' token mismatch. Long network partition?", endpoint);
            slogger.debug("tokens_={}, tokens={}", tokens_, tmp);

            handle_cdc_generation(cdc_streams_ts);
            _token_metadata.update_normal_tokens(tokens, endpoint);
        }
    }

    // at this point the endpoint is certainly a member with this token, so let's proceed
    // normally
    _token_metadata.add_leaving_endpoint(endpoint);
    update_pending_ranges_nowait(endpoint);
}

void storage_service::update_pending_ranges_nowait(inet_address endpoint) {
    //FIXME: discarded future.
    (void)update_pending_ranges().handle_exception([endpoint] (std::exception_ptr ep) {
        slogger.info("Failed to update_pending_ranges for node {}: {}", endpoint, ep);
    });
}

void storage_service::handle_state_left(inet_address endpoint, std::vector<sstring> pieces) {
    slogger.debug("endpoint={} handle_state_left", endpoint);
    if (pieces.size() < 2) {
        slogger.warn("Fail to handle_state_left endpoint={} pieces={}", endpoint, pieces);
        return;
    }
    auto tokens = get_tokens_for(endpoint);
    slogger.debug("Node {} state left, tokens {}", endpoint, tokens);
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
    if (_token_metadata.is_member(endpoint)) {
        auto state = pieces[0];
        auto remove_tokens = _token_metadata.get_tokens(endpoint);
        if (sstring(gms::versioned_value::REMOVED_TOKEN) == state) {
            std::unordered_set<token> tmp(remove_tokens.begin(), remove_tokens.end());
            excise(std::move(tmp), endpoint, extract_expire_time(pieces));
        } else if (sstring(gms::versioned_value::REMOVING_TOKEN) == state) {
            slogger.debug("Tokens {} removed manually (endpoint was {})", remove_tokens, endpoint);
            // Note that the endpoint is being removed
            _token_metadata.add_leaving_endpoint(endpoint);
            update_pending_ranges().get();
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
            auto ep = _token_metadata.get_endpoint_for_host_id(host_id);
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
    (void)get_local_migration_manager().schedule_schema_pull(endpoint, ep_state).handle_exception([endpoint] (auto ep) {
        slogger.warn("Fail to pull schema from {}: {}", endpoint, ep);
    });
}

void storage_service::on_alive(gms::inet_address endpoint, gms::endpoint_state state) {
    slogger.debug("endpoint={} on_alive", endpoint);
    //FIXME: discarded future.
    (void)get_local_migration_manager().schedule_schema_pull(endpoint, state).handle_exception([endpoint] (auto ep) {
        slogger.warn("Fail to pull schema from {}: {}", endpoint, ep);
    });
    if (_token_metadata.is_member(endpoint)) {
        notify_up(endpoint);
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
        // we have (most likely) modified token metadata
        replicate_to_all_cores().get();
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
                (void)get_local_migration_manager().schedule_schema_pull(endpoint, *ep_state).handle_exception([endpoint] (auto ep) {
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
    _token_metadata.remove_endpoint(endpoint);
    update_pending_ranges().get();
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

// Runs inside seastar::async context
// Assumes that no other functions modify CDC_STREAMS_TIMESTAMP, TOKENS or STATUS
// in the gossiper's local application state while this function runs.
void storage_service::set_gossip_tokens(
        const std::unordered_set<dht::token>& tokens, std::optional<db_clock::time_point> cdc_streams_ts) {
    assert(!tokens.empty());

    // Order is important: both the CDC streams timestamp and tokens must be known when a node handles our status.
    _gossiper.add_local_application_state({
        { gms::application_state::TOKENS, versioned_value::tokens(tokens) },
        { gms::application_state::CDC_STREAMS_TIMESTAMP, versioned_value::cdc_streams_timestamp(cdc_streams_ts) },
        { gms::application_state::STATUS, versioned_value::normal(tokens) }
    }).get();
}

void storage_service::register_subscriber(endpoint_lifecycle_subscriber* subscriber)
{
    _lifecycle_subscribers.emplace_back(subscriber);
}

void storage_service::unregister_subscriber(endpoint_lifecycle_subscriber* subscriber)
{
    _lifecycle_subscribers.erase(std::remove(_lifecycle_subscribers.begin(), _lifecycle_subscribers.end(), subscriber), _lifecycle_subscribers.end());
}

static std::optional<future<>> drain_in_progress;

future<> storage_service::stop_transport() {
    return run_with_no_api_lock([] (storage_service& ss) {
        return seastar::async([&ss] {
            slogger.info("Stop transport: starts");

            ss.shutdown_client_servers();
            slogger.info("Stop transport: shutdown rpc and cql server done");

            gms::stop_gossiping().get();
            slogger.info("Stop transport: stop_gossiping done");

            ss.do_stop_ms().get();
            slogger.info("Stop transport: shutdown messaging_service done");

            ss.do_stop_stream_manager().get();
            slogger.info("Stop transport: shutdown stream_manager done");

            ss._auth_service.stop().get();
            slogger.info("Stop transport: auth shutdown");

            slogger.info("Stop transport: done");
        });
    });
}

future<> storage_service::drain_on_shutdown() {
    return run_with_no_api_lock([] (storage_service& ss) {
        if (drain_in_progress) {
            return std::move(*drain_in_progress);
        }
        return seastar::async([&ss] {
            slogger.info("Drain on shutdown: starts");

            ss.stop_transport().get();
            slogger.info("Drain on shutdown: stop_transport done");

            tracing::tracing::tracing_instance().invoke_on_all([] (auto& tr) {
                return tr.shutdown();
            }).get();

            tracing::tracing::tracing_instance().stop().get();
            slogger.info("Drain on shutdown: tracing is stopped");

            ss._sys_dist_ks.invoke_on_all(&db::system_distributed_keyspace::stop).get();
            slogger.info("Drain on shutdown: system distributed keyspace stopped");

            get_storage_proxy().invoke_on_all([] (storage_proxy& local_proxy) mutable {
                auto& ss = service::get_local_storage_service();
                ss.unregister_subscriber(&local_proxy);
                return local_proxy.drain_on_shutdown();
            }).get();
            slogger.info("Drain on shutdown: hints manager is stopped");

            ss.flush_column_families();
            slogger.info("Drain on shutdown: flush column_families done");

            ss.db().invoke_on_all([] (auto& db) {
                return db.commitlog()->shutdown();
            }).get();
            slogger.info("Drain on shutdown: shutdown commitlog done");

            ss._mnotifier.local().unregister_listener(&ss).get();

            slogger.info("Drain on shutdown: done");
        });
    });
}

future<> storage_service::init_messaging_service_part() {
    return get_storage_service().invoke_on_all(&service::storage_service::init_messaging_service);
}

future<> storage_service::init_server(int delay, bind_messaging_port do_bind) {
    return seastar::async([this, delay, do_bind] {
        _initialized = true;

        // Register storage_service to migration_notifier so we can update
        // pending ranges when keyspace is chagned
        _mnotifier.local().register_listener(this);

        std::vector<inet_address> loaded_endpoints;
        if (get_property_load_ring_state()) {
            slogger.info("Loading persisted ring state");
            auto loaded_tokens = db::system_keyspace::load_tokens().get0();
            auto loaded_host_ids = db::system_keyspace::load_host_ids().get0();

            for (auto& x : loaded_tokens) {
                slogger.debug("Loaded tokens: endpoint={}, tokens={}", x.first, x.second);
            }

            for (auto& x : loaded_host_ids) {
                slogger.debug("Loaded host_id: endpoint={}, uuid={}", x.first, x.second);
            }

            for (auto x : loaded_tokens) {
                auto ep = x.first;
                auto tokens = x.second;
                if (ep == get_broadcast_address()) {
                    // entry has been mistakenly added, delete it
                    db::system_keyspace::remove_endpoint(ep).get();
                } else {
                    _token_metadata.update_normal_tokens(tokens, ep);
                    if (loaded_host_ids.count(ep)) {
                        _token_metadata.update_host_id(loaded_host_ids.at(ep), ep);
                    }
                    loaded_endpoints.push_back(ep);
                    _gossiper.add_saved_endpoint(ep);
                }
            }
        }

        auto loaded_peer_features = db::system_keyspace::load_peer_features().get0();
        slogger.info("loaded_peer_features: peer_features size={}", loaded_peer_features.size());
        for (auto& x : loaded_peer_features) {
            slogger.info("loaded_peer_features: peer={}, supported_features={}", x.first, x.second);
        }

        prepare_to_join(std::move(loaded_endpoints), loaded_peer_features, do_bind);

        join_token_ring(delay);

        if (_db.local().get_config().enable_sstable_data_integrity_check()) {
            slogger.info0("SSTable data integrity checker is enabled.");
        } else {
            slogger.info0("SSTable data integrity checker is disabled.");
        }
    });
}

// Serialized
future<> storage_service::replicate_tm_only() {
    auto tm = _token_metadata;

    return do_with(std::move(tm), [] (token_metadata& tm) {
        return get_storage_service().invoke_on_all([&tm] (storage_service& local_ss){
            if (this_shard_id() != 0) {
                local_ss._token_metadata = tm;
            }
        });
    });
}

future<> storage_service::replicate_to_all_cores() {
    // sanity checks: this function is supposed to be run on shard 0 only and
    // when gossiper has already been initialized.
    if (this_shard_id() != 0) {
        auto err = format("replicate_to_all_cores is not ran on cpu zero");
        slogger.warn("{}", err);
        throw std::runtime_error(err);
    }

    return _replicate_action.trigger_later().then([self = shared_from_this()] {});
}

future<> storage_service::do_replicate_to_all_cores() {
    return replicate_tm_only().handle_exception([] (auto e) {
        slogger.error("Fail to replicate _token_metadata: {}", e);
    });
}

future<> storage_service::gossip_snitch_info() {
    auto& snitch = locator::i_endpoint_snitch::get_local_snitch_ptr();
    auto addr = get_broadcast_address();
    auto dc = snitch->get_datacenter(addr);
    auto rack = snitch->get_rack(addr);
    return _gossiper.add_local_application_state({
        { gms::application_state::DC, versioned_value::datacenter(dc) },
        { gms::application_state::RACK, versioned_value::rack(rack) },
    });
}

future<> storage_service::gossip_sharder() {
    return _gossiper.add_local_application_state({
        { gms::application_state::SHARD_COUNT, versioned_value::shard_count(smp::count) },
        { gms::application_state::IGNORE_MSB_BITS, versioned_value::ignore_msb_bits(_db.local().get_config().murmur3_partitioner_ignore_msb_bits()) },
    });
}

future<> storage_service::stop() {
    return with_semaphore(_feature_listeners_sem, 1, [this] {
        return uninit_messaging_service();
    }).then([this] {
        return _service_memory_limiter.wait(_service_memory_total); // make sure nobody uses the semaphore
    });
}

future<> storage_service::check_for_endpoint_collision(const std::unordered_map<gms::inet_address, sstring>& loaded_peer_features) {
    slogger.debug("Starting shadow gossip round to check for endpoint collision");

    return seastar::async([this, loaded_peer_features] {
        auto t = gms::gossiper::clk::now();
        bool found_bootstrapping_node = false;
        auto local_features = get_known_features_set();
        do {
            slogger.info("Checking remote features with gossip");
            _gossiper.do_shadow_round().get();
            _gossiper.check_knows_remote_features(local_features, loaded_peer_features);
            auto addr = get_broadcast_address();
            if (!_gossiper.is_safe_for_bootstrap(addr)) {
                throw std::runtime_error(sprint("A node with address %s already exists, cancelling join. "
                    "Use replace_address if you want to replace this node.", addr));
            }
            if (dht::range_streamer::use_strict_consistency()) {
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
                            _gossiper.goto_shadow_round();
                            _gossiper.reset_endpoint_state_map().get();
                            found_bootstrapping_node = true;
                            auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(gms::gossiper::clk::now() - t).count();
                            slogger.info("Checking bootstrapping/leaving/moving nodes: node={}, status={}, sleep 1 second and check again ({} seconds elapsed) (check_for_endpoint_collision)", addr, state, elapsed);
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
storage_service::prepare_replacement_info(const std::unordered_map<gms::inet_address, sstring>& loaded_peer_features) {
    if (!db().local().get_replace_address()) {
        throw std::runtime_error(format("replace_address is empty"));
    }
    auto replace_address = db().local().get_replace_address().value();
    slogger.info("Gathering node replacement information for {}", replace_address);

    // if (!MessagingService.instance().isListening())
    //     MessagingService.instance().listen(FBUtilities.getLocalAddress());
    auto seeds = _gossiper.get_seeds();
    if (seeds.size() == 1 && seeds.count(replace_address)) {
        throw std::runtime_error(format("Cannot replace_address {} because no seed node is up", replace_address));
    }

    // make magic happen
    slogger.info("Checking remote features with gossip");
    return _gossiper.do_shadow_round().then([this, loaded_peer_features, replace_address] {
        auto local_features = get_known_features_set();
        _gossiper.check_knows_remote_features(local_features, loaded_peer_features);

        // now that we've gossiped at least once, we should be able to find the node we're replacing
        auto* state = _gossiper.get_endpoint_state_for_endpoint_ptr(replace_address);
        if (!state) {
            throw std::runtime_error(format("Cannot replace_address {} because it doesn't exist in gossip", replace_address));
        }

        auto tokens = get_tokens_for(replace_address);
        if (tokens.empty()) {
            throw std::runtime_error(format("Could not find tokens for {} to replace", replace_address));
        }

        auto cdc_streams_ts = cdc::get_streams_timestamp_for(replace_address, _gossiper);
        replacement_info ret {tokens, cdc_streams_ts};

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
        auto token_map = dht::token::describe_ownership(ss._token_metadata.sorted_tokens());
        // describeOwnership returns tokens in an unspecified order, let's re-order them
        std::map<gms::inet_address, float> ownership;
        for (auto entry : token_map) {
            gms::inet_address endpoint = ss._token_metadata.get_endpoint(entry.first).value();
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
        auto token_ownership = dht::token::describe_ownership(ss._token_metadata.sorted_tokens());

        std::map<gms::inet_address, float> final_ownership;

        // calculate ownership per dc
        for (auto endpoints : ss._token_metadata.get_topology().get_datacenter_endpoints()) {
            // calculate the ownership with replication and add the endpoint to the final ownership map
            for (const gms::inet_address& endpoint : endpoints.second) {
                float ownership = 0.0f;
                for (range<token> r : ss.get_ranges_for_endpoint(keyspace_name, endpoint)) {
                    // get_ranges_for_endpoint will unwrap the first range.
                    // With t0 t1 t2 t3, the first range (t3,t0] will be splitted
                    // as (min,t0] and (t3,max]. Skippping the range (t3,max]
                    // we will get the correct ownership number as if the first
                    // range were not splitted.
                    if (!r.end()) {
                        continue;
                    }
                    auto end_token = r.end()->value();
                    if (token_ownership.find(end_token) != token_ownership.end()) {
                        ownership += token_ownership[end_token];
                    }
                }
                final_ownership[endpoint] = ownership;
            }
        }
        return final_ownership;
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
    return map_reduce(std::move(live_hosts), [] (auto host) {
        auto f0 = netw::get_messaging_service().local().send_schema_check(netw::msg_addr{ host, 0 });
        return std::move(f0).then_wrapped([host] (auto f) {
            if (f.failed()) {
                f.ignore_ready_future();
                return std::pair<gms::inet_address, std::optional<utils::UUID>>(host, std::nullopt);
            }
            return std::pair<gms::inet_address, std::optional<utils::UUID>>(host, f.get0());
        });
    }, std::move(results), [] (auto results, auto host_and_version) {
        auto version = host_and_version.second ? host_and_version.second->to_sstring() : UNREACHABLE;
        auto it = results.find(version);
        if (it == results.end()) {
            results.emplace(std::move(version), std::vector<sstring> { host_and_version.first.to_sstring() });
        } else {
            it->second.emplace_back(host_and_version.first.to_sstring());
        }
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
                bool cdc_enabled = ss.db().local().get_config().check_experimental(db::experimental_features_t::CDC);
                ss.set_gossip_tokens(db::system_keyspace::get_local_tokens().get0(),
                        cdc_enabled ? std::make_optional(cdc::get_local_streams_timestamp().get0()) : std::nullopt);
                ss._gossiper.force_newer_generation();
                ss._gossiper.start_gossiping(get_generation_number(), gms::bind_messaging_port(bool(do_bind))).then([&ss] {
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
    return netw::get_messaging_service().invoke_on_all([] (auto& ms) {
        return ms.stop();
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

future<> check_snapshot_not_exist(database& db, sstring ks_name, sstring name) {
    auto& ks = db.find_keyspace(ks_name);
    return parallel_for_each(ks.metadata()->cf_meta_data(), [&db, ks_name = std::move(ks_name), name = std::move(name)] (auto& pair) {
        auto& cf = db.find_column_family(pair.second);
        return cf.snapshot_exists(name).then([ks_name = std::move(ks_name), name] (bool exists) {
            if (exists) {
                throw std::runtime_error(format("Keyspace {}: snapshot {} already exists.", ks_name, name));
            }
        });
    });
}

template <typename Func>
std::result_of_t<Func()> storage_service::run_snapshot_modify_operation(Func&& f) {
    auto& ss = get_storage_service();
    return with_gate(ss.local()._snapshot_ops, [f = std::move(f), &ss] () {
        return ss.invoke_on(0, [f = std::move(f)] (auto& ss) mutable {
            return with_lock(ss._snapshot_lock.for_write(), std::move(f));
        });
    });
}

template <typename Func>
std::result_of_t<Func()> storage_service::run_snapshot_list_operation(Func&& f) {
    auto& ss = get_storage_service();
    return with_gate(ss.local()._snapshot_ops, [f = std::move(f), &ss] () {
        return ss.invoke_on(0, [f = std::move(f)] (auto& ss) mutable {
            return with_lock(ss._snapshot_lock.for_read(), std::move(f));
        });
    });
}

future<> storage_service::take_snapshot(sstring tag, std::vector<sstring> keyspace_names) {
    if (tag.empty()) {
        throw std::runtime_error("You must supply a snapshot name.");
    }

    if (keyspace_names.size() == 0) {
        boost::copy(_db.local().get_keyspaces() | boost::adaptors::map_keys, std::back_inserter(keyspace_names));
    };

    return run_snapshot_modify_operation([tag = std::move(tag), keyspace_names = std::move(keyspace_names), this] {
        return parallel_for_each(keyspace_names, [tag, this] (auto& ks_name) {
            return check_snapshot_not_exist(_db.local(), ks_name, tag);
        }).then([this, tag, keyspace_names] {
            return _db.invoke_on_all([tag = std::move(tag), keyspace_names] (database& db) {
                return parallel_for_each(keyspace_names, [&db, tag = std::move(tag)] (auto& ks_name) {
                    auto& ks = db.find_keyspace(ks_name);
                    return parallel_for_each(ks.metadata()->cf_meta_data(), [&db, tag = std::move(tag)] (auto& pair) {
                        auto& cf = db.find_column_family(pair.second);
                        return cf.snapshot(tag);
                    });
                });
            });
        });
    });

}

future<> storage_service::take_column_family_snapshot(sstring ks_name, sstring cf_name, sstring tag) {
    if (ks_name.empty()) {
        throw std::runtime_error("You must supply a keyspace name");
    }
    if (cf_name.empty()) {
        throw std::runtime_error("You must supply a table name");
    }
    if (cf_name.find(".") != sstring::npos) {
        throw std::invalid_argument("Cannot take a snapshot of a secondary index by itself. Run snapshot on the table that owns the index.");
    }

    if (tag.empty()) {
        throw std::runtime_error("You must supply a snapshot name.");
    }

    return run_snapshot_modify_operation([this, ks_name = std::move(ks_name), cf_name = std::move(cf_name), tag = std::move(tag)] {
        return check_snapshot_not_exist(_db.local(), ks_name, tag).then([this, ks_name, cf_name, tag] {
            return _db.invoke_on_all([ks_name, cf_name, tag] (database &db) {
                auto& cf = db.find_column_family(ks_name, cf_name);
                return cf.snapshot(tag);
            });
        });
    });
}

future<> storage_service::clear_snapshot(sstring tag, std::vector<sstring> keyspace_names, sstring cf_name) {
    return run_snapshot_modify_operation([this, tag = std::move(tag), keyspace_names = std::move(keyspace_names), cf_name = std::move(cf_name)] {
        return _db.local().clear_snapshot(tag, keyspace_names, cf_name);
    });
}

future<std::unordered_map<sstring, std::vector<service::storage_service::snapshot_details>>>
storage_service::get_snapshot_details() {
    using cf_snapshot_map = std::unordered_map<utils::UUID, column_family::snapshot_details>;
    using snapshot_map = std::unordered_map<sstring, cf_snapshot_map>;

    class snapshot_reducer {
    private:
        snapshot_map _result;
    public:
        future<> operator()(const snapshot_map& value) {
            for (auto&& vp: value) {
                if (_result.count(vp.first) == 0) {
                    _result.emplace(vp.first, std::move(vp.second));
                    continue;
                }

                auto& rp = _result.at(vp.first);
                for (auto&& cf: vp.second) {
                    if (rp.count(cf.first) == 0) {
                        rp.emplace(cf.first, std::move(cf.second));
                        continue;
                    }
                    auto& rcf = rp.at(cf.first);
                    rcf.live = cf.second.live;
                    rcf.total = cf.second.total;
                }
            }
            return make_ready_future<>();
        }
        snapshot_map get() && {
            return std::move(_result);
        }
    };
  return run_snapshot_list_operation([] {
    return get_local_storage_service()._db.map_reduce(snapshot_reducer(), [] (database& db) {
        auto local_snapshots = make_lw_shared<snapshot_map>();
        return parallel_for_each(db.get_column_families(), [local_snapshots] (auto& cf_pair) {
            return cf_pair.second->get_snapshot_details().then([uuid = cf_pair.first, local_snapshots] (auto map) {
                for (auto&& snap_map: map) {
                    if (local_snapshots->count(snap_map.first) == 0) {
                        local_snapshots->emplace(snap_map.first, cf_snapshot_map());
                    }
                    local_snapshots->at(snap_map.first).emplace(uuid, snap_map.second);
                }
                return make_ready_future<>();
            });
        }).then([local_snapshots] {
            return make_ready_future<snapshot_map>(std::move(*local_snapshots));
        });
    }).then([] (snapshot_map&& map) {
        std::unordered_map<sstring, std::vector<service::storage_service::snapshot_details>> result;
        for (auto&& pair: map) {
            std::vector<service::storage_service::snapshot_details> details;

            for (auto&& snap_map: pair.second) {
                auto& cf = get_local_storage_service()._db.local().find_column_family(snap_map.first);
                details.push_back({ snap_map.second.live, snap_map.second.total, cf.schema()->cf_name(), cf.schema()->ks_name() });
            }
            result.emplace(pair.first, std::move(details));
        }

        return make_ready_future<std::unordered_map<sstring, std::vector<service::storage_service::snapshot_details>>>(std::move(result));
    });
  });
}

future<int64_t> storage_service::true_snapshots_size() {
    return _db.map_reduce(adder<int64_t>(), [] (database& db) {
        return do_with(int64_t(0), [&db] (auto& local_total) {
            return parallel_for_each(db.get_column_families(), [&local_total] (auto& cf_pair) {
                return cf_pair.second->get_snapshot_details().then([&local_total] (auto map) {
                    for (auto&& snap_map: map) {
                        local_total += snap_map.second.live;
                    }
                    return make_ready_future<>();
                 });
            }).then([&local_total] {
                return make_ready_future<int64_t>(local_total);
            });
        });
    });
}

static std::atomic<bool> isolated = { false };

future<> storage_service::start_rpc_server() {
    return run_with_api_lock(sstring("start_rpc_server"), [] (storage_service& ss) {
        if (ss._thrift_server || isolated.load()) {
            return make_ready_future<>();
        }

        ss._thrift_server = std::make_unique<distributed<thrift_server>>();
        auto tserver = &*ss._thrift_server;

        auto& cfg = ss._db.local().get_config();
        auto port = cfg.rpc_port();
        auto addr = cfg.rpc_address();
        auto preferred = cfg.rpc_interface_prefer_ipv6() ? std::make_optional(net::inet_address::family::INET6) : std::nullopt;
        auto family = cfg.enable_ipv6_dns_lookup() || preferred ? std::nullopt : std::make_optional(net::inet_address::family::INET);
        auto keepalive = cfg.rpc_keepalive();
        thrift_server_config tsc;
        tsc.timeout_config = make_timeout_config(cfg);
        tsc.max_request_size = cfg.thrift_max_message_length_in_mb() * (uint64_t(1) << 20);
        return gms::inet_address::lookup(addr, family, preferred).then([&ss, tserver, addr, port, keepalive, tsc] (gms::inet_address ip) {
            return tserver->start(std::ref(ss._db), std::ref(cql3::get_query_processor()), std::ref(ss._auth_service), tsc).then([tserver, port, addr, ip, keepalive] {
                // #293 - do not stop anything
                //engine().at_exit([tserver] {
                //    return tserver->stop();
                //});
                return tserver->invoke_on_all(&thrift_server::listen, socket_address{ip, port}, keepalive);
            });
        }).then([addr, port] {
            slogger.info("Thrift server listening on {}:{} ...", addr, port);
        });
    });
}

future<> storage_service::do_stop_rpc_server() {
    return do_with(std::move(_thrift_server), [this] (std::unique_ptr<distributed<thrift_server>>& tserver) {
        if (tserver) {
            return tserver->stop().then([] {
                slogger.info("Thrift server stopped");
            });
        }
        return make_ready_future<>();
    });
}

future<> storage_service::stop_rpc_server() {
    return run_with_api_lock(sstring("stop_rpc_server"), [] (storage_service& ss) {
        return ss.do_stop_rpc_server();
    });
}

future<bool> storage_service::is_rpc_server_running() {
    return run_with_no_api_lock([] (storage_service& ss) {
        return bool(ss._thrift_server);
    });
}

future<> storage_service::start_native_transport() {
    return run_with_api_lock(sstring("start_native_transport"), [] (storage_service& ss) {
        if (ss._cql_server || isolated.load()) {
            return make_ready_future<>();
        }
        return seastar::async([&ss] {
            ss._cql_server = std::make_unique<distributed<cql_transport::cql_server>>();
            auto cserver = &*ss._cql_server;

            auto& cfg = ss._db.local().get_config();
            auto addr = cfg.rpc_address();
            auto preferred = cfg.rpc_interface_prefer_ipv6() ? std::make_optional(net::inet_address::family::INET6) : std::nullopt;
            auto family = cfg.enable_ipv6_dns_lookup() || preferred ? std::nullopt : std::make_optional(net::inet_address::family::INET);
            auto ceo = cfg.client_encryption_options();
            auto keepalive = cfg.rpc_keepalive();
            cql_transport::cql_server_config cql_server_config;
            cql_server_config.timeout_config = make_timeout_config(cfg);
            cql_server_config.max_request_size = ss._service_memory_total;
            cql_server_config.get_service_memory_limiter_semaphore = [ss = std::ref(get_storage_service())] () -> semaphore& { return ss.get().local()._service_memory_limiter; };
            cql_server_config.allow_shard_aware_drivers = cfg.enable_shard_aware_drivers();
            cql_server_config.sharding_ignore_msb = cfg.murmur3_partitioner_ignore_msb_bits();
            cql_server_config.partitioner_name = cfg.partitioner();
            smp_service_group_config cql_server_smp_service_group_config;
            cql_server_smp_service_group_config.max_nonlocal_requests = 5000;
            cql_server_config.bounce_request_smp_service_group = create_smp_service_group(cql_server_smp_service_group_config).get0();
            seastar::net::inet_address ip = gms::inet_address::lookup(addr, family, preferred).get0();
            cserver->start(std::ref(cql3::get_query_processor()), std::ref(ss._auth_service), std::ref(ss._mnotifier), cql_server_config).get();
            struct listen_cfg {
                socket_address addr;
                std::shared_ptr<seastar::tls::credentials_builder> cred;
            };

            std::vector<listen_cfg> configs({ { socket_address{ip, cfg.native_transport_port()} }});

            // main should have made sure values are clean and neatish
            if (ceo.at("enabled") == "true") {
                auto cred = std::make_shared<seastar::tls::credentials_builder>();

                cred->set_dh_level(seastar::tls::dh_params::level::MEDIUM);
                cred->set_priority_string(db::config::default_tls_priority);

                if (ceo.count("priority_string")) {
                    cred->set_priority_string(ceo.at("priority_string"));
                }
                if (ceo.count("require_client_auth") && ceo.at("require_client_auth") == "true") {
                    cred->set_client_auth(seastar::tls::client_auth::REQUIRE);
                }

                cred->set_x509_key_file(ceo.at("certificate"), ceo.at("keyfile"), seastar::tls::x509_crt_format::PEM).get();

                if (ceo.count("truststore")) {
                    cred->set_x509_trust_file(ceo.at("truststore"), seastar::tls::x509_crt_format::PEM).get();
                }

                slogger.info("Enabling encrypted CQL connections between client and server");

                if (cfg.native_transport_port_ssl.is_set() && cfg.native_transport_port_ssl() != cfg.native_transport_port()) {
                    configs.emplace_back(listen_cfg{{ip, cfg.native_transport_port_ssl()}, std::move(cred)});
                } else {
                    configs.back().cred = std::move(cred);
                }
            }

            parallel_for_each(configs, [cserver, keepalive](const listen_cfg & cfg) {
                return cserver->invoke_on_all(&cql_transport::cql_server::listen, cfg.addr, cfg.cred, keepalive).then([cfg] {
                    slogger.info("Starting listening for CQL clients on {} ({})"
                            , cfg.addr, cfg.cred ? "encrypted" : "unencrypted"
                    );
                });
            }).get();

            ss.set_cql_ready(true).get();
        });
    });
}

future<> storage_service::do_stop_native_transport() {
    return do_with(std::move(_cql_server), [this] (std::unique_ptr<distributed<cql_transport::cql_server>>& cserver) {
        if (cserver) {
            // FIXME: cql_server::stop() doesn't kill existing connections and wait for them
            return set_cql_ready(false).then([&cserver] {
                return cserver->stop().then([] {
                    slogger.info("CQL server stopped");
                });
            });
        }
        return make_ready_future<>();
    });
}

future<> storage_service::stop_native_transport() {
    return run_with_api_lock(sstring("stop_native_transport"), [] (storage_service& ss) {
        return ss.do_stop_native_transport();
    });
}

future<bool> storage_service::is_native_transport_running() {
    return run_with_no_api_lock([] (storage_service& ss) {
        return bool(ss._cql_server);
    });
}

future<> storage_service::decommission() {
    return run_with_api_lock(sstring("decommission"), [] (storage_service& ss) {
        return seastar::async([&ss] {
            auto& tm = ss.get_token_metadata();
            auto& db = ss.db().local();
            if (!tm.is_member(ss.get_broadcast_address())) {
                throw std::runtime_error("local node is not a member of the token ring yet");
            }

            if (tm.clone_after_all_left().sorted_tokens().size() < 2) {
                throw std::runtime_error("no other normal nodes in the ring; decommission would be pointless");
            }

            if (ss._operation_mode != mode::NORMAL) {
                throw std::runtime_error(format("Node in {} state; wait for status to become normal or restart", ss._operation_mode));
            }

            ss.update_pending_ranges().get();

            auto non_system_keyspaces = db.get_non_system_keyspaces();
            for (const auto& keyspace_name : non_system_keyspaces) {
                if (tm.get_pending_ranges(keyspace_name, ss.get_broadcast_address()).size() > 0) {
                    throw std::runtime_error("data is currently moving to this node; unable to leave the ring");
                }
            }

            slogger.info("DECOMMISSIONING: starts");
            ss.start_leaving().get();
            // FIXME: long timeout = Math.max(RING_DELAY, BatchlogManager.instance.getBatchlogTimeout());
            auto timeout = ss.get_ring_delay();
            ss.set_mode(mode::LEAVING, format("sleeping {} ms for batch processing and pending range setup", timeout.count()), true);
            sleep_abortable(timeout, ss._abort_source).get();

            slogger.info("DECOMMISSIONING: unbootstrap starts");
            ss.unbootstrap();
            slogger.info("DECOMMISSIONING: unbootstrap done");

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

future<> storage_service::removenode(sstring host_id_string) {
    return run_with_api_lock(sstring("removenode"), [host_id_string] (storage_service& ss) mutable {
        return seastar::async([&ss, host_id_string] {
            slogger.debug("removenode: host_id = {}", host_id_string);
            auto my_address = ss.get_broadcast_address();
            auto& tm = ss._token_metadata;
            auto local_host_id = tm.get_host_id(my_address);
            auto host_id = utils::UUID(host_id_string);
            auto endpoint_opt = tm.get_endpoint_for_host_id(host_id);
            if (!endpoint_opt) {
                throw std::runtime_error("Host ID not found.");
            }
            auto endpoint = *endpoint_opt;

            auto tokens = tm.get_tokens(endpoint);

            slogger.debug("removenode: endpoint = {}", endpoint);

            if (endpoint == my_address) {
                throw std::runtime_error("Cannot remove self");
            }

            if (ss._gossiper.get_live_members().count(endpoint)) {
                throw std::runtime_error(format("Node {} is alive and owns this ID. Use decommission command to remove it from the ring", endpoint));
            }

            // A leaving endpoint that is dead is already being removed.
            if (tm.is_leaving(endpoint)) {
                slogger.warn("Node {} is already being removed, continuing removal anyway", endpoint);
            }

            if (!ss._replicating_nodes.empty()) {
                throw std::runtime_error("This node is already processing a removal. Wait for it to complete, or use 'removenode force' if this has failed.");
            }

            auto non_system_keyspaces = ss.db().local().get_non_system_keyspaces();
            // Find the endpoints that are going to become responsible for data
            for (const auto& keyspace_name : non_system_keyspaces) {
                auto& ks = ss.db().local().find_keyspace(keyspace_name);
                // if the replication factor is 1 the data is lost so we shouldn't wait for confirmation
                if (ks.get_replication_strategy().get_replication_factor() == 1) {
                    slogger.warn("keyspace={} has replication factor 1, the data is probably lost", keyspace_name);
                    continue;
                }

                // get all ranges that change ownership (that is, a node needs
                // to take responsibility for new range)
                std::unordered_multimap<dht::token_range, inet_address> changed_ranges =
                    ss.get_changed_ranges_for_leaving(keyspace_name, endpoint);
                for (auto& x: changed_ranges) {
                    auto ep = x.second;
                    if (ss._gossiper.is_alive(ep)) {
                        ss._replicating_nodes.emplace(ep);
                    } else {
                        slogger.warn("Endpoint {} is down and will not receive data for re-replication of {}", ep, endpoint);
                    }
                }
            }
            slogger.info("removenode: endpoint = {}, replicating_nodes = {}", endpoint, ss._replicating_nodes);
            ss._removing_node = endpoint;
            tm.add_leaving_endpoint(endpoint);
            ss.update_pending_ranges().get();

            // the gossiper will handle spoofing this node's state to REMOVING_TOKEN for us
            // we add our own token so other nodes to let us know when they're done
            ss._gossiper.advertise_removing(endpoint, host_id, local_host_id).get();

            // kick off streaming commands
            // No need to wait for restore_replica_count to complete, since
            // when it completes, the node will be removed from _replicating_nodes,
            // and we wait for _replicating_nodes to become empty below
            //FIXME: discarded future.
            (void)ss.restore_replica_count(endpoint, my_address).handle_exception([endpoint, my_address] (auto ep) {
                slogger.info("Failed to restore_replica_count for node {} on node {}", endpoint, my_address);
            });

            // wait for ReplicationFinishedVerbHandler to signal we're done
            while (!(ss._replicating_nodes.empty() || ss._force_remove_completion)) {
                sleep_abortable(std::chrono::milliseconds(100), ss._abort_source).get();
            }

            if (ss._force_remove_completion) {
                throw std::runtime_error("nodetool removenode force is called by user");
            }

            std::unordered_set<token> tmp(tokens.begin(), tokens.end());
            ss.excise(std::move(tmp), endpoint);

            // gossiper will indicate the token has left
            ss._gossiper.advertise_token_removed(endpoint, host_id).get();

            ss._replicating_nodes.clear();
            ss._removing_node = std::nullopt;
        });
    });
}

// Runs inside seastar::async context
void storage_service::flush_column_families() {
    service::get_storage_service().invoke_on_all([] (auto& ss) {
        auto& local_db = ss.db().local();
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
    service::get_storage_service().invoke_on_all([] (auto& ss) {
        auto& local_db = ss.db().local();
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
        return seastar::async([&ss] {
            if (ss._operation_mode == mode::DRAINED) {
                slogger.warn("Cannot drain node (did it already happen?)");
                return;
            }

            promise<> p;
            drain_in_progress = p.get_future();

            ss.set_mode(mode::DRAINING, "starting drain process", true);
            ss.shutdown_client_servers();
            gms::stop_gossiping().get();

            ss.set_mode(mode::DRAINING, "shutting down messaging_service", false);
            ss.do_stop_ms().get();

            // Interrupt on going compaction and shutdown to prevent further compaction
            ss.db().invoke_on_all([] (auto& db) {
                return db.get_compaction_manager().stop();
            }).get();

            ss.set_mode(mode::DRAINING, "flushing column families", false);
            ss.flush_column_families();

            db::get_batchlog_manager().invoke_on_all([] (auto& bm) {
                return bm.stop();
            }).get();

            ss.set_mode(mode::DRAINING, "shutting down migration manager", false);
            service::get_migration_manager().stop().get();

            ss.db().invoke_on_all([] (auto& db) {
                return db.commitlog()->shutdown();
            }).get();

            ss.set_mode(mode::DRAINED, true);
            p.set_value();
        });
    });
}

future<> storage_service::rebuild(sstring source_dc) {
    return run_with_api_lock(sstring("rebuild"), [source_dc] (storage_service& ss) {
        slogger.info("rebuild from dc: {}", source_dc == "" ? "(any dc)" : source_dc);
        if (ss.is_repair_based_node_ops_enabled()) {
            return rebuild_with_repair(ss._db, ss._token_metadata, std::move(source_dc));
        } else {
            auto streamer = make_lw_shared<dht::range_streamer>(ss._db, ss._token_metadata, ss._abort_source,
                    ss.get_broadcast_address(), "Rebuild", streaming::stream_reason::rebuild);
            streamer->add_source_filter(std::make_unique<dht::range_streamer::failure_detector_source_filter>(ss._gossiper.get_unreachable_members()));
            if (source_dc != "") {
                streamer->add_source_filter(std::make_unique<dht::range_streamer::single_datacenter_filter>(source_dc));
            }
            auto keyspaces = make_lw_shared<std::vector<sstring>>(ss._db.local().get_non_system_keyspaces());
            return do_for_each(*keyspaces, [keyspaces, streamer, &ss] (sstring& keyspace_name) {
                return streamer->add_ranges(keyspace_name, ss.get_local_ranges(keyspace_name));
            }).then([streamer] {
                return streamer->stream_async().then([streamer] {
                    slogger.info("Streaming for rebuild successful");
                }).handle_exception([] (auto ep) {
                    // This is used exclusively through JMX, so log the full trace but only throw a simple RTE
                    slogger.warn("Error while rebuilding node: {}", std::current_exception());
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

    std::unordered_map<dht::token_range, std::vector<inet_address>> current_replica_endpoints;

    // Find (for each range) all nodes that store replicas for these ranges as well
    auto metadata = _token_metadata.clone_only_token_map(); // don't do this in the loop! #7758
    for (auto& r : ranges) {
        seastar::thread::maybe_yield();
        auto& ks = _db.local().find_keyspace(keyspace_name);
        auto end_token = r.end() ? r.end()->value() : dht::maximum_token();
        auto eps = ks.get_replication_strategy().calculate_natural_endpoints(end_token, metadata);
        current_replica_endpoints.emplace(r, std::move(eps));
    }

    auto temp = _token_metadata.clone_after_all_left();

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
        seastar::thread::maybe_yield();
        auto& ks = _db.local().find_keyspace(keyspace_name);
        auto end_token = r.end() ? r.end()->value() : dht::maximum_token();
        auto new_replica_endpoints = ks.get_replication_strategy().calculate_natural_endpoints(end_token, temp);

        auto rg = current_replica_endpoints.equal_range(r);
        for (auto it = rg.first; it != rg.second; it++) {
            const dht::token_range& range_ = it->first;
            std::vector<inet_address>& current_eps = it->second;
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

    return changed_ranges;
}

// Runs inside seastar::async context
void storage_service::unbootstrap() {
    db::get_local_batchlog_manager().do_batch_log_replay().get();
    if (is_repair_based_node_ops_enabled()) {
        decommission_with_repair(_db, _token_metadata).get();
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

future<> storage_service::restore_replica_count(inet_address endpoint, inet_address notify_endpoint) {
    if (is_repair_based_node_ops_enabled()) {
        return removenode_with_repair(_db, _token_metadata, endpoint).finally([this, notify_endpoint] () {
            return send_replication_notification(notify_endpoint);
        });
    }
  return seastar::async([this, endpoint, notify_endpoint] {
    auto streamer = make_lw_shared<dht::range_streamer>(_db, get_token_metadata(), _abort_source, get_broadcast_address(), "Restore_replica_count", streaming::stream_reason::removenode);
    auto my_address = get_broadcast_address();
    auto non_system_keyspaces = _db.local().get_non_system_keyspaces();
    for (const auto& keyspace_name : non_system_keyspaces) {
        std::unordered_multimap<dht::token_range, inet_address> changed_ranges = get_changed_ranges_for_leaving(keyspace_name, endpoint);
        dht::token_range_vector my_new_ranges;
        for (auto& x : changed_ranges) {
            if (x.second == my_address) {
                my_new_ranges.emplace_back(x.first);
            }
        }
        std::unordered_multimap<inet_address, dht::token_range> source_ranges = get_new_source_ranges(keyspace_name, my_new_ranges);
        std::unordered_map<inet_address, dht::token_range_vector> ranges_per_endpoint;
        for (auto& x : source_ranges) {
            ranges_per_endpoint[x.first].emplace_back(x.second);
        }
        streamer->add_rx_ranges(keyspace_name, std::move(ranges_per_endpoint));
    }
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
    _token_metadata.remove_endpoint(endpoint);
    _token_metadata.remove_bootstrap_tokens(tokens);

    notify_left(endpoint);

    update_pending_ranges().get();
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
        [done, sent, remote, local] {
            auto& ms = netw::get_local_messaging_service();
            netw::msg_addr id{remote, 0};
            (*sent)++;
            return ms.send_replication_finished(id, local).then_wrapped([id, done] (auto&& f) {
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
    _token_metadata.remove_endpoint(get_broadcast_address());
    update_pending_ranges().get();

    auto expire_time = _gossiper.compute_expire_time().time_since_epoch().count();
    _gossiper.add_local_application_state(gms::application_state::STATUS,
            versioned_value::left(db::system_keyspace::get_local_tokens().get0(), expire_time)).get();
    auto delay = std::max(get_ring_delay(), gms::gossiper::INTERVAL);
    slogger.info("Announcing that I have left the ring for {}ms", delay.count());
    sleep_abortable(delay, _abort_source).get();
}

future<>
storage_service::stream_ranges(std::unordered_map<sstring, std::unordered_multimap<dht::token_range, inet_address>> ranges_to_stream_by_keyspace) {
    auto streamer = make_lw_shared<dht::range_streamer>(_db, get_token_metadata(), _abort_source, get_broadcast_address(), "Unbootstrap", streaming::stream_reason::decommission);
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
        _token_metadata.add_leaving_endpoint(get_broadcast_address());
        return update_pending_ranges();
    });
}

void storage_service::add_expire_time_if_found(inet_address endpoint, int64_t expire_time) {
    if (expire_time != 0L) {
        using clk = gms::gossiper::clk;
        auto time = clk::time_point(clk::duration(expire_time));
        _gossiper.add_expire_time_for_endpoint(endpoint, time);
    }
}

// For more details, see the commends on column_family::load_new_sstables
// All the global operations are going to happen here, and just the reloading happens
// in there.
future<> storage_service::load_new_sstables(sstring ks_name, sstring cf_name) {
    class max_element {
        int64_t _result = 0;
    public:
        future<> operator()(int64_t value) {
            _result = std::max(value, _result);
            return make_ready_future<>();
        }
        int64_t get() && {
            return _result;
        }
    };

    if (_loading_new_sstables) {
        throw std::runtime_error("Already loading SSTables. Try again later");
    } else {
        _loading_new_sstables = true;
    }

    slogger.info("Loading new SSTables for {}.{}...", ks_name, cf_name);

    // First, we need to stop SSTable creation for that CF in all shards. This is a really horrible
    // thing to do, because under normal circumnstances this can make dirty memory go up to the point
    // of explosion.
    //
    // Remember, however, that we are assuming this is going to be ran on an empty CF. In that scenario,
    // stopping the SSTables should have no effect, while guaranteeing we will see no data corruption
    // * in case * this is ran on a live CF.
    //
    // The statement above is valid at least from the Scylla side of things: it is still totally possible
    // that someones just copies the table over existing ones. There isn't much we can do about it.
    return _db.map_reduce(max_element(), [ks_name, cf_name] (database& db) {
        auto& cf = db.find_column_family(ks_name, cf_name);
        return cf.disable_sstable_write();
    }).then([this, cf_name, ks_name] (int64_t max_seen_sstable) {
        // Then, we will reshuffle the tables to make sure that the generation numbers don't go too high.
        // We will do all of it the same CPU, to make sure that we won't have two parallel shufflers stepping
        // onto each other.

        class all_generations {
            std::set<int64_t> _result;
        public:
            future<> operator()(std::set<int64_t> value) {
                _result.insert(value.begin(), value.end());
                return make_ready_future<>();
            }
            std::set<int64_t> get() && {
                return _result;
            }
        };

        // We provide to reshuffle_sstables() the generation of all existing sstables, such that it will
        // easily know which sstables are new.
        return _db.map_reduce(all_generations(), [ks_name, cf_name] (database& db) {
            auto& cf = db.find_column_family(ks_name, cf_name);
            std::set<int64_t> generations;
            for (auto& p : *(cf.get_sstables())) {
                generations.insert(p->generation());
            }
            return make_ready_future<std::set<int64_t>>(std::move(generations));
        }).then([this, max_seen_sstable, ks_name, cf_name] (std::set<int64_t> all_generations) {
            auto shard = std::hash<sstring>()(cf_name) % smp::count;
            return _db.invoke_on(shard, [ks_name, cf_name, max_seen_sstable, all_generations = std::move(all_generations)] (database& db) {
                auto& cf = db.find_column_family(ks_name, cf_name);
                return cf.reshuffle_sstables(std::move(all_generations), max_seen_sstable + 1);
            });
        });
    }).then_wrapped([this, ks_name, cf_name] (future<std::vector<sstables::entry_descriptor>> f) {
        std::vector<sstables::entry_descriptor> new_tables;
        std::exception_ptr eptr;
        int64_t new_gen = -1;

        try {
            new_tables = f.get0();
        } catch(std::exception& e) {
            slogger.error("Loading of new tables failed to {}.{} due to {}", ks_name, cf_name, e.what());
            eptr = std::current_exception();
        } catch(...) {
            slogger.error("Loading of new tables failed to {}.{} due to unexpected reason", ks_name, cf_name);
            eptr = std::current_exception();
        }

        if (new_tables.size() > 0) {
            new_gen = new_tables.back().generation;
        }

        slogger.debug("Now accepting writes for sstables with generation larger or equal than {}", new_gen);
        return _db.invoke_on_all([ks_name, cf_name, new_gen] (database& db) {
            auto& cf = db.find_column_family(ks_name, cf_name);
            auto disabled = std::chrono::duration_cast<std::chrono::microseconds>(cf.enable_sstable_write(new_gen)).count();
            slogger.info("CF {}.{} at shard {} had SSTables writes disabled for {} usec", ks_name, cf_name, this_shard_id(), disabled);
            return make_ready_future<>();
        }).then([new_tables = std::move(new_tables), eptr = std::move(eptr)] {
            if (eptr) {
                return make_exception_future<std::vector<sstables::entry_descriptor>>(eptr);
            }
            return make_ready_future<std::vector<sstables::entry_descriptor>>(std::move(new_tables));
        });
    }).then([this, ks_name, cf_name] (std::vector<sstables::entry_descriptor> new_tables) {
        auto f = distributed_loader::flush_upload_dir(_db, _sys_dist_ks, ks_name, cf_name);
        return f.then([new_tables = std::move(new_tables), ks_name, cf_name] (std::vector<sstables::entry_descriptor> new_tables_from_upload) mutable {
            if (new_tables.empty() && new_tables_from_upload.empty()) {
                slogger.info("No new SSTables were found for {}.{}", ks_name, cf_name);
            }
            // merge new sstables found in both column family and upload directories, if any.
            new_tables.insert(new_tables.end(), new_tables_from_upload.begin(), new_tables_from_upload.end());
            return make_ready_future<std::vector<sstables::entry_descriptor>>(std::move(new_tables));
        });
    }).then([this, ks_name, cf_name] (std::vector<sstables::entry_descriptor> new_tables) {
        return distributed_loader::load_new_sstables(_db, _view_update_generator, ks_name, cf_name, std::move(new_tables)).then([ks_name, cf_name] {
            slogger.info("Done loading new SSTables for {}.{} for all shards", ks_name, cf_name);
        });
    }).finally([this] {
        _loading_new_sstables = false;
    });
}

void storage_service::shutdown_client_servers() {
    do_stop_rpc_server().get();
    do_stop_native_transport().get();
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

std::unordered_multimap<inet_address, dht::token_range>
storage_service::get_new_source_ranges(const sstring& keyspace_name, const dht::token_range_vector& ranges) {
    auto my_address = get_broadcast_address();
    auto& ks = _db.local().find_keyspace(keyspace_name);
    auto& strat = ks.get_replication_strategy();
    auto tm = _token_metadata.clone_only_token_map();
    std::unordered_map<dht::token_range, std::vector<inet_address>> range_addresses = strat.get_range_addresses(tm);
    std::unordered_multimap<inet_address, dht::token_range> source_ranges;

    // find alive sources for our new ranges
    for (auto r : ranges) {
        std::vector<inet_address> possible_nodes;
        auto it = range_addresses.find(r);
        if (it != range_addresses.end()) {
            possible_nodes = it->second;
        }

        auto& snitch = locator::i_endpoint_snitch::get_local_snitch_ptr();
        std::vector<inet_address> sources = snitch->get_sorted_list_by_proximity(my_address, possible_nodes);

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

    std::unordered_map<dht::token_range, std::vector<inet_address>> range_to_address_map =
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

std::unordered_map<dht::token_range, std::vector<inet_address>>
storage_service::construct_range_to_endpoint_map(
        const sstring& keyspace,
        const dht::token_range_vector& ranges) const {
    std::unordered_map<dht::token_range, std::vector<inet_address>> res;
    for (auto r : ranges) {
        res[r] = _db.local().find_keyspace(keyspace).get_replication_strategy().get_natural_endpoints(
                r.end() ? r.end()->value() : dht::maximum_token());
    }
    return res;
}


std::map<token, inet_address> storage_service::get_token_to_endpoint_map() {
    return _token_metadata.get_normal_and_bootstrapping_token_to_endpoint_map();
}

std::chrono::milliseconds storage_service::get_ring_delay() {
    auto ring_delay = _db.local().get_config().ring_delay_ms();
    slogger.trace("Get RING_DELAY: {}ms", ring_delay);
    return std::chrono::milliseconds(ring_delay);
}

future<> storage_service::do_update_pending_ranges() {
    if (this_shard_id() != 0) {
        return make_exception_future<>(std::runtime_error("do_update_pending_ranges should be called on cpu zero"));
    }
    // long start = System.currentTimeMillis();
    return do_with(_db.local().get_non_system_keyspaces(), [this] (auto& keyspaces){
        return do_for_each(keyspaces, [this] (auto& keyspace_name) {
            auto& ks = this->_db.local().find_keyspace(keyspace_name);
            auto& strategy = ks.get_replication_strategy();
            slogger.debug("Calculating pending ranges for keyspace={} starts", keyspace_name);
            return get_token_metadata().calculate_pending_ranges(strategy, keyspace_name).finally([&keyspace_name] {
                slogger.debug("Calculating pending ranges for keyspace={} ends", keyspace_name);
            });
        });
    });
    // slogger.debug("finished calculation for {} keyspaces in {}ms", keyspaces.size(), System.currentTimeMillis() - start);
}

future<> storage_service::update_pending_ranges() {
    return get_storage_service().invoke_on(0, [] (auto& ss){
        return ss._update_pending_ranges_action.trigger_later().then([&ss] {
            // calculate_pending_ranges will modify token_metadata, we need to repliate to other cores
            return ss.replicate_to_all_cores().then([s = ss.shared_from_this()] { });
        });
    });
}

future<> storage_service::keyspace_changed(const sstring& ks_name) {
    // Update pending ranges since keyspace can be changed after we calculate pending ranges.
    return update_pending_ranges().handle_exception([ks_name] (auto ep) {
        slogger.warn("Failed to update pending ranges for ks = {}: {}", ks_name, ep);
    });
}

void storage_service::init_messaging_service() {
    auto& ms = netw::get_local_messaging_service();
    ms.register_replication_finished([] (gms::inet_address from) {
        return get_local_storage_service().confirm_replication(from);
    });
}

future<> storage_service::uninit_messaging_service() {
    auto& ms = netw::get_local_messaging_service();
    return ms.unregister_replication_finished();
}

void storage_service::do_isolate_on_error(disk_error type)
{
    if (!isolated.exchange(true)) {
        slogger.warn("Shutting down communications due to I/O errors until operator intervention");
        slogger.warn("{} error: {}", type == disk_error::commit ? "Commitlog" : "Disk", std::current_exception());
        // isolated protect us against multiple stops
        //FIXME: discarded future.
        (void)service::get_local_storage_service().stop_transport();
    }
}

future<sstring> storage_service::get_removal_status() {
    return run_with_no_api_lock([] (storage_service& ss) {
        if (!ss._removing_node) {
            return make_ready_future<sstring>(sstring("No token removals in process."));
        }
        auto tokens = ss._token_metadata.get_tokens(*ss._removing_node);
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
                if (!ss._replicating_nodes.empty() || !ss._token_metadata.get_leaving_endpoints().empty()) {
                    auto leaving = ss._token_metadata.get_leaving_endpoints();
                    slogger.warn("Removal not confirmed for {}, Leaving={}", join(",", ss._replicating_nodes), leaving);
                    for (auto endpoint : leaving) {
                        utils::UUID host_id;
                        auto tokens = ss._token_metadata.get_tokens(endpoint);
                        try {
                            host_id = ss._token_metadata.get_host_id(endpoint);
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
calculate_splits(std::vector<dht::token> tokens, uint32_t split_count, column_family& cf) {
    auto sstables = cf.get_sstables();
    const double step = static_cast<double>(tokens.size() - 1) / split_count;
    auto prev_token_idx = 0;
    std::vector<std::pair<dht::token_range, uint64_t>> splits;
    splits.reserve(split_count);
    for (uint32_t i = 1; i <= split_count; ++i) {
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
    uint32_t split_count = std::max(uint32_t(1), static_cast<uint32_t>(std::min(max_split_count, total_row_count_estimate / keys_per_split)));

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

std::vector<gms::inet_address>
storage_service::get_natural_endpoints(const sstring& keyspace,
        const sstring& cf, const sstring& key) const {
    sstables::key_view key_view = sstables::key_view(bytes_view(reinterpret_cast<const signed char*>(key.c_str()), key.size()));
    dht::token token = _db.local().find_schema(keyspace, cf)->get_partitioner().get_token(key_view);
    return get_natural_endpoints(keyspace, token);
}

std::vector<gms::inet_address>
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

future<> init_storage_service(sharded<abort_source>& abort_source, distributed<database>& db, sharded<gms::gossiper>& gossiper, sharded<auth::service>& auth_service,
        sharded<db::system_distributed_keyspace>& sys_dist_ks,
        sharded<db::view::view_update_generator>& view_update_generator, sharded<gms::feature_service>& feature_service,
        storage_service_config config, sharded<service::migration_notifier>& mn, sharded<locator::token_metadata>& tm) {
    return service::get_storage_service().start(std::ref(abort_source), std::ref(db), std::ref(gossiper), std::ref(auth_service), std::ref(sys_dist_ks), std::ref(view_update_generator), std::ref(feature_service), config, std::ref(mn), std::ref(tm));
}

future<> deinit_storage_service() {
    return service::get_storage_service().stop();
}

void feature_enabled_listener::on_enabled() {
    if (_started) {
        return;
    }
    _started = true;
    //FIXME: discarded future.
    (void)with_semaphore(_sem, 1, [this] {
        if (!sstables::is_later(_format, _s._sstables_format)) {
            return make_ready_future<bool>(false);
        }
        return db::system_keyspace::set_scylla_local_param(SSTABLE_FORMAT_PARAM_NAME, to_string(_format)).then([this] {
            return get_storage_service().invoke_on_all([this] (storage_service& s) {
                s._sstables_format = _format;
            });
        }).then([] { return true; });
    }).then([this] (bool update_features) {
        if (!update_features) {
            return make_ready_future<>();
        }
        return gms::get_local_gossiper().add_local_application_state(gms::application_state::SUPPORTED_FEATURES,
                                                                     gms::versioned_value::supported_features(_s.get_config_supported_features()));
    });
}

future<> read_sstables_format(distributed<storage_service>& ss) {
    return db::system_keyspace::get_scylla_local_param(SSTABLE_FORMAT_PARAM_NAME).then([&ss] (std::optional<sstring> format_opt) {
        if (format_opt) {
            sstables::sstable_version_types format = sstables::from_string(*format_opt);
            return ss.invoke_on_all([format] (storage_service& s) {
                s._sstables_format = format;
            });
        }
        return make_ready_future<>();
    });
}

future<> storage_service::set_cql_ready(bool ready) {
    return _gossiper.add_local_application_state(application_state::RPC_READY, versioned_value::cql_ready(ready));
}

void storage_service::notify_down(inet_address endpoint) {
    get_storage_service().invoke_on_all([endpoint] (auto&& ss) {
        netw::get_local_messaging_service().remove_rpc_client(netw::msg_addr{endpoint, 0});
        return seastar::async([&ss, endpoint] {
            for (auto&& subscriber : ss._lifecycle_subscribers) {
                try {
                    subscriber->on_down(endpoint);
                } catch (...) {
                    slogger.warn("Down notification failed {}: {}", endpoint, std::current_exception());
                }
            }
        });
    }).get();
    slogger.debug("Notify node {} has been down", endpoint);
}

void storage_service::notify_left(inet_address endpoint) {
    get_storage_service().invoke_on_all([endpoint] (auto&& ss) {
        return seastar::async([&ss, endpoint] {
            for (auto&& subscriber : ss._lifecycle_subscribers) {
                try {
                    subscriber->on_leave_cluster(endpoint);
                } catch (...) {
                    slogger.warn("Leave cluster notification failed {}: {}", endpoint, std::current_exception());
                }
            }
        });
    }).get();
    slogger.debug("Notify node {} has left the cluster", endpoint);
}

void storage_service::notify_up(inet_address endpoint)
{
    if (!_gossiper.is_cql_ready(endpoint) || !_gossiper.is_alive(endpoint)) {
        return;
    }
    get_storage_service().invoke_on_all([endpoint] (auto&& ss) {
        return seastar::async([&ss, endpoint] {
            for (auto&& subscriber : ss._lifecycle_subscribers) {
                try {
                    subscriber->on_up(endpoint);
                } catch (...) {
                    slogger.warn("Up notification failed {}: {}", endpoint, std::current_exception());
                }
            }
        });
    }).get();
    slogger.debug("Notify node {} has been up", endpoint);
}

void storage_service::notify_joined(inet_address endpoint)
{
    if (!_gossiper.is_normal(endpoint)) {
        return;
    }

    get_storage_service().invoke_on_all([endpoint] (auto&& ss) {
        return seastar::async([&ss, endpoint] {
            for (auto&& subscriber : ss._lifecycle_subscribers) {
                try {
                    subscriber->on_join_cluster(endpoint);
                } catch (...) {
                    slogger.warn("Join cluster notification failed {}: {}", endpoint, std::current_exception());
                }
            }
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
    return get_storage_service().invoke_on(0, [keyspace = std::move(keyspace)] (storage_service& ss) {
        auto my_address = ss.get_broadcast_address();
        auto pending_ranges = ss.get_token_metadata().get_pending_ranges(keyspace, my_address).size();
        bool is_bootstrap_mode = ss._is_bootstrap_mode;
        slogger.debug("is_cleanup_allowed: keyspace={}, is_bootstrap_mode={}, pending_ranges={}",
                keyspace, is_bootstrap_mode, pending_ranges);
        return !is_bootstrap_mode && pending_ranges == 0;
    });
}

bool storage_service::is_repair_based_node_ops_enabled() {
    return _db.local().get_config().enable_repair_based_node_ops();
}

} // namespace service

