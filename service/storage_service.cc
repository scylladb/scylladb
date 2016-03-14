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
 * Modified by Cloudius Systems.
 * Copyright 2015 Cloudius Systems.
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
#include "core/distributed.hh"
#include "locator/snitch_base.hh"
#include "db/system_keyspace.hh"
#include "utils/UUID.hh"
#include "gms/inet_address.hh"
#include "log.hh"
#include "service/migration_manager.hh"
#include "to_string.hh"
#include "gms/gossiper.hh"
#include "gms/failure_detector.hh"
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
#include "dns.hh"
#include <seastar/core/rwlock.hh>
#include "db/batchlog_manager.hh"
#include "db/commitlog/commitlog.hh"
#include "auth/auth.hh"
#include <seastar/net/tls.hh>

using token = dht::token;
using UUID = utils::UUID;
using inet_address = gms::inet_address;

namespace service {

static logging::logger logger("storage_service");

distributed<storage_service> _the_storage_service;

int get_generation_number() {
    using namespace std::chrono;
    auto now = high_resolution_clock::now().time_since_epoch();
    int generation_number = duration_cast<seconds>(now).count();
    return generation_number;
}

bool storage_service::is_auto_bootstrap() {
    return _db.local().get_config().auto_bootstrap();
}

std::set<inet_address> get_seeds() {
    // FIXME: DatabaseDescriptor.getSeeds()
    auto& gossiper = gms::get_local_gossiper();
    return gossiper.get_seeds();
}

std::unordered_set<token> get_replace_tokens() {
    std::unordered_set<token> ret;
    std::unordered_set<sstring> tokens;
    auto tokens_string = get_local_storage_service().db().local().get_config().replace_token();
    try {
        boost::split(tokens, tokens_string, boost::is_any_of(sstring(",")));
    } catch (...) {
        throw std::runtime_error(sprint("Unable to parse replace_token=%s", tokens_string));
    }
    tokens.erase("");
    for (auto token_string : tokens) {
        auto token = dht::global_partitioner().from_sstring(token_string);
        ret.insert(token);
    }
    return ret;
}

std::experimental::optional<UUID> get_replace_node() {
    auto replace_node = get_local_storage_service().db().local().get_config().replace_node();
    if (replace_node.empty()) {
        return std::experimental::nullopt;
    }
    try {
        return utils::UUID(replace_node);
    } catch (...) {
        auto msg = sprint("Unable to parse %s as host-id", replace_node);
        logger.error("{}", msg);
        throw std::runtime_error(msg);
    }
}

bool get_property_join_ring() {
    return get_local_storage_service().db().local().get_config().join_ring();
}

bool get_property_rangemovement() {
    return get_local_storage_service().db().local().get_config().consistent_rangemovement();
}

bool get_property_load_ring_state() {
    return get_local_storage_service().db().local().get_config().load_ring_state();
}

bool storage_service::should_bootstrap() {
    return is_auto_bootstrap() && !db::system_keyspace::bootstrap_complete() && !get_seeds().count(get_broadcast_address());
}

// Runs inside seastar::async context
void storage_service::prepare_to_join() {
    if (_joined) {
        return;
    }

    std::map<gms::application_state, gms::versioned_value> app_states;
    if (db::system_keyspace::was_decommissioned()) {
        if (db().local().get_config().override_decommission()) {
            logger.warn("This node was decommissioned, but overriding by operator request.");
            db::system_keyspace::set_bootstrap_state(db::system_keyspace::bootstrap_state::COMPLETED).get();
        } else {
            auto msg = sstring("This node was decommissioned and will not rejoin the ring unless cassandra.override_decommission=true has been set,"
                               "or all existing data is removed and the node is bootstrapped again");
            logger.error(msg.c_str());
            throw std::runtime_error(msg.c_str());
        }
    }
    if (db().local().is_replacing() && !get_property_join_ring()) {
        throw std::runtime_error("Cannot set both join_ring=false and attempt to replace a node");
    }
    if (get_replace_tokens().size() > 0 || get_replace_node()) {
         throw std::runtime_error("Replace method removed; use cassandra.replace_address instead");
    }
    if (db().local().is_replacing()) {
        if (db::system_keyspace::bootstrap_complete()) {
            throw std::runtime_error("Cannot replace address with a node that is already bootstrapped");
        }
        if (!is_auto_bootstrap()) {
            throw std::runtime_error("Trying to replace_address with auto_bootstrap disabled will not work, check your configuration");
        }
        _bootstrap_tokens = prepare_replacement_info().get0();
        app_states.emplace(gms::application_state::TOKENS, value_factory.tokens(_bootstrap_tokens));
        app_states.emplace(gms::application_state::STATUS, value_factory.hibernate(true));
    } else if (should_bootstrap()) {
        check_for_endpoint_collision().get();
    }

    // have to start the gossip service before we can see any info on other nodes.  this is necessary
    // for bootstrap to get the load info it needs.
    // (we won't be part of the storage ring though until we add a counterId to our state, below.)
    // Seed the host ID-to-endpoint map with our own ID.
    auto local_host_id = db::system_keyspace::get_local_host_id().get0();
    _token_metadata.update_host_id(local_host_id, get_broadcast_address());
    auto broadcast_rpc_address = utils::fb_utilities::get_broadcast_rpc_address();
    app_states.emplace(gms::application_state::NET_VERSION, value_factory.network_version());
    app_states.emplace(gms::application_state::HOST_ID, value_factory.host_id(local_host_id));
    app_states.emplace(gms::application_state::RPC_ADDRESS, value_factory.rpcaddress(broadcast_rpc_address));
    app_states.emplace(gms::application_state::RELEASE_VERSION, value_factory.release_version());
    logger.info("Starting up server gossip");

    auto& gossiper = gms::get_local_gossiper();
    gossiper.register_(this->shared_from_this());
    auto generation_number = db::system_keyspace::increment_and_get_generation().get0();
    gossiper.start_gossiping(generation_number, app_states).then([this] {
#if SS_DEBUG
        gms::get_local_gossiper().debug_show();
        _token_metadata.debug_show();
#endif
    }).get();

    // gossip snitch infos (local DC and rack)
    gossip_snitch_info().get();

    auto& proxy = service::get_storage_proxy();
    // gossip Schema.emptyVersion forcing immediate check for schema updates (see MigrationManager#maybeScheduleSchemaPull)
    update_schema_version_and_announce(proxy).get();// Ensure we know our own actual Schema UUID in preparation for updates
#if 0
    if (!MessagingService.instance().isListening())
        MessagingService.instance().listen(FBUtilities.getLocalAddress());
    LoadBroadcaster.instance.startBroadcasting();

    HintedHandOffManager.instance.start();
    BatchlogManager.instance.start();
#endif
}

// Runs inside seastar::async context
void storage_service::join_token_ring(int delay) {
    _joined = true;
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
    logger.debug("Bootstrap variables: {} {} {} {}",
                 is_auto_bootstrap(),
                 db::system_keyspace::bootstrap_in_progress(),
                 db::system_keyspace::bootstrap_complete(),
                 get_seeds().count(get_broadcast_address()));
    if (is_auto_bootstrap() && !db::system_keyspace::bootstrap_complete() && get_seeds().count(get_broadcast_address())) {
        logger.info("This node will not auto bootstrap because it is configured to be a seed node.");
    }
    if (should_bootstrap()) {
        if (db::system_keyspace::bootstrap_in_progress()) {
            logger.warn("Detected previous bootstrap failure; retrying");
        } else {
            db::system_keyspace::set_bootstrap_state(db::system_keyspace::bootstrap_state::IN_PROGRESS).get();
        }
        set_mode(mode::JOINING, "waiting for ring information", true);
        // first sleep the delay to make sure we see all our peers
        for (int i = 0; i < delay; i += 1000) {
            // if we see schema, we can proceed to the next check directly
            if (_db.local().get_version() != database::empty_version) {
                logger.debug("got schema: {}", _db.local().get_version());
                break;
            }
            sleep(std::chrono::seconds(1)).get();
        }
        // if our schema hasn't matched yet, keep sleeping until it does
        // (post CASSANDRA-1391 we don't expect this to be necessary very often, but it doesn't hurt to be careful)
        while (!get_local_migration_manager().is_ready_for_bootstrap()) {
            set_mode(mode::JOINING, "waiting for schema information to complete", true);
            sleep(std::chrono::seconds(1)).get();
        }
        set_mode(mode::JOINING, "schema complete, ready to bootstrap", true);
        set_mode(mode::JOINING, "waiting for pending range calculation", true);
        block_until_update_pending_ranges_finished().get();
        set_mode(mode::JOINING, "calculation complete, ready to bootstrap", true);
        logger.debug("... got ring + schema info");

        auto t = gms::gossiper::clk::now();
        while (get_property_rangemovement() &&
            (!_token_metadata.get_bootstrap_tokens().empty() ||
             !_token_metadata.get_leaving_endpoints().empty() ||
             !_token_metadata.get_moving_endpoints().empty())) {
            auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(gms::gossiper::clk::now() - t).count();
            logger.info("Checking bootstrapping/leaving/moving nodes: tokens {}, leaving {}, moving {}, sleep 1 second and check again ({} seconds elapsed)",
                _token_metadata.get_bootstrap_tokens().size(),
                _token_metadata.get_leaving_endpoints().size(),
                _token_metadata.get_moving_endpoints().size(),
                elapsed);

            sleep(std::chrono::seconds(1)).get();

            if (gms::gossiper::clk::now() > t + std::chrono::seconds(60)) {
                throw std::runtime_error("Other bootstrapping/leaving/moving nodes detected, cannot bootstrap while cassandra.consistent.rangemovement is true");
            }

            // Check the schema and pending range again
            while (!get_local_migration_manager().is_ready_for_bootstrap()) {
                set_mode(mode::JOINING, "waiting for schema information to complete", true);
                sleep(std::chrono::seconds(1)).get();
            }
            block_until_update_pending_ranges_finished().get();
        }
        logger.info("Checking bootstrapping/leaving/moving nodes: ok");

        if (!db().local().is_replacing()) {
            if (_token_metadata.is_member(get_broadcast_address())) {
                throw std::runtime_error("This node is already a member of the token ring; bootstrap aborted. (If replacing a dead node, remove the old one from the ring first.)");
            }
            set_mode(mode::JOINING, "getting bootstrap token", true);
            _bootstrap_tokens = boot_strapper::get_bootstrap_tokens(_token_metadata, _db.local());
        } else {
            auto replace_addr = db().local().get_replace_address();
            if (replace_addr && *replace_addr != get_broadcast_address()) {
                // Sleep additionally to make sure that the server actually is not alive
                // and giving it more time to gossip if alive.
                sleep(service::load_broadcaster::BROADCAST_INTERVAL).get();

                // check for operator errors...
                for (auto token : _bootstrap_tokens) {
                    auto existing = _token_metadata.get_endpoint(token);
                    if (existing) {
                        auto& gossiper = gms::get_local_gossiper();
                        auto eps = gossiper.get_endpoint_state_for_endpoint(*existing);
                        if (eps && eps->get_update_timestamp() > gms::gossiper::clk::now() - std::chrono::milliseconds(delay)) {
                            throw std::runtime_error("Cannot replace a live node...");
                        }
                        current.insert(*existing);
                    } else {
                        throw std::runtime_error(sprint("Cannot replace token %s which does not exist!", token));
                    }
                }
            } else {
                sleep(get_ring_delay()).get();
            }
            std::stringstream ss;
            ss << _bootstrap_tokens;
            set_mode(mode::JOINING, sprint("Replacing a node with token(s): %s", ss.str()), true);
        }
        bootstrap(_bootstrap_tokens);
        // bootstrap will block until finished
        if (_is_bootstrap_mode) {
            auto err = sprint("We are not supposed in bootstrap mode any more");
            logger.warn(err.c_str());
            throw std::runtime_error(err);
        }
    } else {
        size_t num_tokens = _db.local().get_config().num_tokens();
        _bootstrap_tokens = db::system_keyspace::get_saved_tokens().get0();
        if (_bootstrap_tokens.empty()) {
            auto initial_tokens = _db.local().get_initial_tokens();
            if (initial_tokens.size() < 1) {
                _bootstrap_tokens = boot_strapper::get_random_tokens(_token_metadata, num_tokens);
                if (num_tokens == 1) {
                    logger.warn("Generated random token {}. Random tokens will result in an unbalanced ring; see http://wiki.apache.org/cassandra/Operations", _bootstrap_tokens);
                } else {
                    logger.info("Generated random tokens. tokens are {}", _bootstrap_tokens);
                }
            } else {
                for (auto token_string : initial_tokens) {
                    auto token = dht::global_partitioner().from_sstring(token_string);
                    _bootstrap_tokens.insert(token);
                }
                logger.info("Saved tokens not found. Using configuration value: {}", _bootstrap_tokens);
            }
        } else {
            if (_bootstrap_tokens.size() != num_tokens) {
                throw std::runtime_error(sprint("Cannot change the number of tokens from %ld to %ld", _bootstrap_tokens.size(), num_tokens));
            } else {
                logger.info("Using saved tokens {}", _bootstrap_tokens);
            }
        }
    }
#if 0
    // if we don't have system_traces keyspace at this point, then create it manually
    if (Schema.instance.getKSMetaData(TraceKeyspace.NAME) == null)
        MigrationManager.announceNewKeyspace(TraceKeyspace.definition(), 0, false);
#endif

    if (!_is_survey_mode) {
        // start participating in the ring.
        db::system_keyspace::set_bootstrap_state(db::system_keyspace::bootstrap_state::COMPLETED).get();
        set_tokens(_bootstrap_tokens);
        // remove the existing info about the replaced node.
        if (!current.empty()) {
            auto& gossiper = gms::get_local_gossiper();
            for (auto existing : current) {
                gossiper.replaced_endpoint(existing);
            }
        }
        if (_token_metadata.sorted_tokens().empty()) {
            auto err = sprint("join_token_ring: Sorted token in token_metadata is empty");
            logger.error(err.c_str());
            throw std::runtime_error(err);
        }

        auth::auth::setup().get();
    } else {
        logger.info("Startup complete, but write survey mode is active, not becoming an active ring member. Use JMX (StorageService->joinRing()) to finalize ring joining.");
    }
}

future<> storage_service::join_ring() {
    return run_with_api_lock(sstring("join_ring"), [] (storage_service& ss) {
        return seastar::async([&ss] {
            if (!ss._joined) {
                logger.info("Joining ring by operator request");
                ss.join_token_ring(0);
            } else if (ss._is_survey_mode) {
                auto tokens = db::system_keyspace::get_saved_tokens().get0();
                ss.set_tokens(std::move(tokens));
                db::system_keyspace::set_bootstrap_state(db::system_keyspace::bootstrap_state::COMPLETED).get();
                ss._is_survey_mode = false;
                logger.info("Leaving write survey mode and joining ring at operator request");
                if (ss._token_metadata.sorted_tokens().empty()) {
                    auto err = sprint("join_ring: Sorted token in token_metadata is empty");
                    logger.error(err.c_str());
                    throw std::runtime_error(err);
                }
                auth::auth::setup().get();
            }
        });
    });
}

future<bool> storage_service::is_joined() {
    return run_with_no_api_lock([] (storage_service& ss) {
        return ss._joined && !ss._is_survey_mode;
    });
}


// Runs inside seastar::async context
void storage_service::bootstrap(std::unordered_set<token> tokens) {
    _is_bootstrap_mode = true;
    // DON'T use set_token, that makes us part of the ring locally which is incorrect until we are done bootstrapping
    db::system_keyspace::update_tokens(tokens).get();
    auto& gossiper = gms::get_local_gossiper();
    if (!db().local().is_replacing()) {
        // if not an existing token then bootstrap
        gossiper.add_local_application_state(gms::application_state::TOKENS, value_factory.tokens(tokens)).get();
        gossiper.add_local_application_state(gms::application_state::STATUS, value_factory.bootstrapping(tokens)).get();
        set_mode(mode::JOINING, sprint("sleeping %s ms for pending range setup", get_ring_delay().count()), true);
        sleep(get_ring_delay()).get();
    } else {
        // Dont set any state for the node which is bootstrapping the existing token...
        _token_metadata.update_normal_tokens(tokens, get_broadcast_address());
        auto replace_addr = db().local().get_replace_address();
        if (replace_addr) {
            logger.debug("Removing replaced endpoint {} from system.peers", *replace_addr);
            db::system_keyspace::remove_endpoint(*replace_addr).get();
        }
    }
    if (!gossiper.seen_any_seed()) {
         throw std::runtime_error("Unable to contact any seeds!");
    }
    set_mode(mode::JOINING, "Starting to bootstrap...", true);
    dht::boot_strapper bs(_db, get_broadcast_address(), tokens, _token_metadata);
    bs.bootstrap().get(); // handles token update
    logger.info("Bootstrap completed! for the tokens {}", tokens);
}

void storage_service::handle_state_bootstrap(inet_address endpoint) {
    logger.debug("endpoint={} handle_state_bootstrap", endpoint);
    // explicitly check for TOKENS, because a bootstrapping node might be bootstrapping in legacy mode; that is, not using vnodes and no token specified
    auto tokens = get_tokens_for(endpoint);

    logger.debug("Node {} state bootstrapping, token {}", endpoint, tokens);

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
            logger.info("Node {} state jump to bootstrap", endpoint);
        }
        _token_metadata.remove_endpoint(endpoint);
    }

    _token_metadata.add_bootstrap_tokens(tokens, endpoint);
    update_pending_ranges().get();

    auto& gossiper = gms::get_local_gossiper();
    if (gossiper.uses_host_id(endpoint)) {
        _token_metadata.update_host_id(gossiper.get_host_id(endpoint), endpoint);
    }
}

void storage_service::handle_state_normal(inet_address endpoint) {
    logger.debug("endpoint={} handle_state_normal", endpoint);
    auto tokens = get_tokens_for(endpoint);
    auto& gossiper = gms::get_local_gossiper();

    std::unordered_set<token> tokens_to_update_in_metadata;
    std::unordered_set<token> tokens_to_update_in_system_keyspace;
    std::unordered_set<token> local_tokens_to_remove;
    std::unordered_set<inet_address> endpoints_to_remove;

    logger.debug("Node {} state normal, token {}", endpoint, tokens);

    if (_token_metadata.is_member(endpoint)) {
        logger.info("Node {} state jump to normal", endpoint);
    }
    update_peer_info(endpoint);

    // Order Matters, TM.updateHostID() should be called before TM.updateNormalToken(), (see CASSANDRA-4300).
    if (gossiper.uses_host_id(endpoint)) {
        auto host_id = gossiper.get_host_id(endpoint);
        auto existing = _token_metadata.get_endpoint_for_host_id(host_id);
        if (db().local().is_replacing() &&
            db().local().get_replace_address() &&
            gossiper.get_endpoint_state_for_endpoint(db().local().get_replace_address().value())  &&
            (host_id == gossiper.get_host_id(db().local().get_replace_address().value()))) {
            logger.warn("Not updating token metadata for {} because I am replacing it", endpoint);
        } else {
            if (existing && *existing != endpoint) {
                if (*existing == get_broadcast_address()) {
                    logger.warn("Not updating host ID {} for {} because it's mine", host_id, endpoint);
                    _token_metadata.remove_endpoint(endpoint);
                    endpoints_to_remove.insert(endpoint);
                } else if (gossiper.compare_endpoint_startup(endpoint, *existing) > 0) {
                    logger.warn("Host ID collision for {} between {} and {}; {} is the new owner", host_id, *existing, endpoint, endpoint);
                    _token_metadata.remove_endpoint(*existing);
                    endpoints_to_remove.insert(*existing);
                    _token_metadata.update_host_id(host_id, endpoint);
                } else {
                    logger.warn("Host ID collision for {} between {} and {}; ignored {}", host_id, *existing, endpoint, endpoint);
                    _token_metadata.remove_endpoint(endpoint);
                    endpoints_to_remove.insert(endpoint);
                }
            } else {
                _token_metadata.update_host_id(host_id, endpoint);
            }
        }
    }

    for (auto t : tokens) {
        // we don't want to update if this node is responsible for the token and it has a later startup time than endpoint.
        auto current_owner = _token_metadata.get_endpoint(t);
        if (!current_owner) {
            logger.debug("handle_state_normal: New node {} at token {}", endpoint, t);
            tokens_to_update_in_metadata.insert(t);
            tokens_to_update_in_system_keyspace.insert(t);
        } else if (endpoint == *current_owner) {
            logger.debug("handle_state_normal: endpoint={} == current_owner={} token {}", endpoint, *current_owner, t);
            // set state back to normal, since the node may have tried to leave, but failed and is now back up
            tokens_to_update_in_metadata.insert(t);
            tokens_to_update_in_system_keyspace.insert(t);
        } else if (gossiper.compare_endpoint_startup(endpoint, *current_owner) > 0) {
            logger.debug("handle_state_normal: endpoint={} > current_owner={}, token {}", endpoint, *current_owner, t);
            tokens_to_update_in_metadata.insert(t);
            tokens_to_update_in_system_keyspace.insert(t);
            // currentOwner is no longer current, endpoint is.  Keep track of these moves, because when
            // a host no longer has any tokens, we'll want to remove it.
            std::multimap<inet_address, token> ep_to_token_copy = get_token_metadata().get_endpoint_to_token_map_for_reading();
            auto rg = ep_to_token_copy.equal_range(*current_owner);
            for (auto it = rg.first; it != rg.second; it++) {
                if (it->second == t) {
                    logger.info("handle_state_normal: remove endpoint={} token={}", *current_owner, t);
                    ep_to_token_copy.erase(it);
                }
            }
            if (ep_to_token_copy.count(*current_owner) < 1) {
                logger.info("handle_state_normal: endpoints_to_remove endpoint={}", *current_owner);
                endpoints_to_remove.insert(*current_owner);
            }
            logger.info("handle_state_normal: Nodes {} and {} have the same token {}. {} is the new owner", endpoint, *current_owner, t, endpoint);
        } else {
            logger.info("handle_state_normal: Nodes {} and {} have the same token {}. Ignoring {}", endpoint, *current_owner, t, endpoint);
        }
    }

    bool is_moving = _token_metadata.is_moving(endpoint); // capture because updateNormalTokens clears moving status

    // Update pending ranges after update of normal tokens immediately to avoid
    // a race where natural endpoint was updated to contain node A, but A was
    // not yet removed from pending endpoints
    _token_metadata.update_normal_tokens(tokens_to_update_in_metadata, endpoint);
    do_update_pending_ranges();

    for (auto ep : endpoints_to_remove) {
        remove_endpoint(ep);
        auto replace_addr = db().local().get_replace_address();
        if (db().local().is_replacing() && replace_addr && *replace_addr == ep) {
            gossiper.replacement_quarantine(ep); // quarantine locally longer than normally; see CASSANDRA-8260
        }
    }
    logger.debug("handle_state_normal: endpoint={} tokens_to_update_in_system_keyspace = {}", endpoint, tokens_to_update_in_system_keyspace);
    if (!tokens_to_update_in_system_keyspace.empty()) {
        db::system_keyspace::update_tokens(endpoint, tokens_to_update_in_system_keyspace).then_wrapped([endpoint] (auto&& f) {
            try {
                f.get();
            } catch (...) {
                logger.error("handle_state_normal: fail to update tokens for {}: {}", endpoint, std::current_exception());
            }
            return make_ready_future<>();
        }).get();
    }
    if (!local_tokens_to_remove.empty()) {
        db::system_keyspace::update_local_tokens(std::unordered_set<dht::token>(), local_tokens_to_remove).discard_result().get();
    }

    if (is_moving || _operation_mode == mode::MOVING) {
        _token_metadata.remove_from_moving(endpoint);
        get_storage_service().invoke_on_all([endpoint] (auto&& ss) {
            for (auto&& subscriber : ss._lifecycle_subscribers) {
                try {
                    subscriber->on_move(endpoint);
                } catch (...) {
                    logger.warn("Move notification failed {}: {}", endpoint, std::current_exception());
                }
            }
        }).get();
    } else {
        get_storage_service().invoke_on_all([endpoint] (auto&& ss) {
            for (auto&& subscriber : ss._lifecycle_subscribers) {
                try {
                    subscriber->on_join_cluster(endpoint);
                } catch (...) {
                    logger.warn("Join cluster notification failed {}: {}", endpoint, std::current_exception());
                }
            }
        }).get();
    }

    update_pending_ranges().get();
    if (logger.is_enabled(logging::log_level::debug)) {
        auto ver = _token_metadata.get_ring_version();
        for (auto& x : _token_metadata.get_token_to_endpoint()) {
            logger.debug("handle_state_normal: token_metadata.ring_version={}, token={} -> endpoint={}", ver, x.first, x.second);
        }
    }
}

void storage_service::handle_state_leaving(inet_address endpoint) {
    logger.debug("endpoint={} handle_state_leaving", endpoint);

    auto tokens = get_tokens_for(endpoint);

    logger.debug("Node {} state leaving, tokens {}", endpoint, tokens);

    // If the node is previously unknown or tokens do not match, update tokenmetadata to
    // have this node as 'normal' (it must have been using this token before the
    // leave). This way we'll get pending ranges right.
    if (!_token_metadata.is_member(endpoint)) {
        logger.info("Node {} state jump to leaving", endpoint);
        _token_metadata.update_normal_tokens(tokens, endpoint);
    } else {
        auto tokens_ = _token_metadata.get_tokens(endpoint);
        std::set<token> tmp(tokens.begin(), tokens.end());
        if (!std::includes(tokens_.begin(), tokens_.end(), tmp.begin(), tmp.end())) {
            logger.warn("Node {} 'leaving' token mismatch. Long network partition?", endpoint);
            logger.debug("tokens_={}, tokens={}", tokens_, tmp);
            _token_metadata.update_normal_tokens(tokens, endpoint);
        }
    }

    // at this point the endpoint is certainly a member with this token, so let's proceed
    // normally
    _token_metadata.add_leaving_endpoint(endpoint);
    update_pending_ranges().get();
}

void storage_service::handle_state_left(inet_address endpoint, std::vector<sstring> pieces) {
    logger.debug("endpoint={} handle_state_left", endpoint);
    if (pieces.size() < 2) {
        logger.warn("Fail to handle_state_left endpoint={} pieces={}", endpoint, pieces);
        return;
    }
    auto tokens = get_tokens_for(endpoint);
    logger.debug("Node {} state left, tokens {}", endpoint, tokens);
    excise(tokens, endpoint, extract_expire_time(pieces));
}

void storage_service::handle_state_moving(inet_address endpoint, std::vector<sstring> pieces) {
    logger.debug("endpoint={} handle_state_moving", endpoint);
    if (pieces.size() < 2) {
        logger.warn("Fail to handle_state_moving endpoint={} pieces={}", endpoint, pieces);
        return;
    }
    auto token = dht::global_partitioner().from_sstring(pieces[1]);
    logger.debug("Node {} state moving, new token {}", endpoint, token);
    _token_metadata.add_moving_endpoint(token, endpoint);
    update_pending_ranges().get();
}

void storage_service::handle_state_removing(inet_address endpoint, std::vector<sstring> pieces) {
    logger.debug("endpoint={} handle_state_removing", endpoint);
    if (pieces.empty()) {
        logger.warn("Fail to handle_state_removing endpoint={} pieces={}", endpoint, pieces);
        return;
    }
    if (endpoint == get_broadcast_address()) {
        logger.info("Received removenode gossip about myself. Is this node rejoining after an explicit removenode?");
        try {
            drain().get();
        } catch (...) {
            logger.error("Fail to drain: {}", std::current_exception());
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
            auto& gossiper = gms::get_local_gossiper();
            logger.debug("Tokens {} removed manually (endpoint was {})", remove_tokens, endpoint);
            // Note that the endpoint is being removed
            _token_metadata.add_leaving_endpoint(endpoint);
            update_pending_ranges().get();
            // find the endpoint coordinating this removal that we need to notify when we're done
            auto state = gossiper.get_endpoint_state_for_endpoint(endpoint);
            if (!state) {
                auto err = sprint("Can not find endpoint_state for endpoint=%s", endpoint);
                logger.warn(err.c_str());
                throw std::runtime_error(err);
            }
            auto value = state->get_application_state(application_state::REMOVAL_COORDINATOR);
            if (!value) {
                auto err = sprint("Can not find application_state for endpoint=%s", endpoint);
                logger.warn(err.c_str());
                throw std::runtime_error(err);
            }
            std::vector<sstring> coordinator;
            boost::split(coordinator, value->value, boost::is_any_of(sstring(versioned_value::DELIMITER_STR)));
            if (coordinator.size() != 2) {
                auto err = sprint("Can not split REMOVAL_COORDINATOR for endpoint=%s, value=%s", endpoint, value->value);
                logger.warn(err.c_str());
                throw std::runtime_error(err);
            }
            UUID host_id(coordinator[1]);
            // grab any data we are now responsible for and notify responsible node
            auto ep = _token_metadata.get_endpoint_for_host_id(host_id);
            if (!ep) {
                auto err = sprint("Can not find host_id=%s", host_id);
                logger.warn(err.c_str());
                throw std::runtime_error(err);
            }
            restore_replica_count(endpoint, ep.value()).get();
        }
    } else { // now that the gossiper has told us about this nonexistent member, notify the gossiper to remove it
        if (sstring(gms::versioned_value::REMOVED_TOKEN) == pieces[0]) {
            add_expire_time_if_found(endpoint, extract_expire_time(pieces));
        }
        remove_endpoint(endpoint);
    }
}

void storage_service::on_join(gms::inet_address endpoint, gms::endpoint_state ep_state) {
    logger.debug("endpoint={} on_join", endpoint);
    for (const auto& e : ep_state.get_application_state_map()) {
        on_change(endpoint, e.first, e.second);
    }
    get_local_migration_manager().schedule_schema_pull(endpoint, ep_state).handle_exception([endpoint] (auto ep) {
        logger.warn("Fail to pull schmea from {}: {}", endpoint, ep);
    });
}

void storage_service::on_alive(gms::inet_address endpoint, gms::endpoint_state state) {
    logger.debug("endpoint={} on_alive", endpoint);
    get_local_migration_manager().schedule_schema_pull(endpoint, state).handle_exception([endpoint] (auto ep) {
        logger.warn("Fail to pull schmea from {}: {}", endpoint, ep);
    });
    if (_token_metadata.is_member(endpoint)) {
#if 0
        HintedHandOffManager.instance.scheduleHintDelivery(endpoint, true);
#endif
        get_storage_service().invoke_on_all([endpoint] (auto&& ss) {
            for (auto&& subscriber : ss._lifecycle_subscribers) {
                try {
                    subscriber->on_up(endpoint);
                } catch (...) {
                    logger.warn("Up notification failed {}: {}", endpoint, std::current_exception());
                }
            }
        });
    }
}

void storage_service::before_change(gms::inet_address endpoint, gms::endpoint_state current_state, gms::application_state new_state_key, const gms::versioned_value& new_value) {
    logger.debug("endpoint={} before_change: new app_state={}, new versioned_value={}", endpoint, new_state_key, new_value);
}

void storage_service::on_change(inet_address endpoint, application_state state, const versioned_value& value) {
    logger.debug("endpoint={} on_change:     app_state={}, versioned_value={}", endpoint, state, value);
    if (state == application_state::STATUS) {
        std::vector<sstring> pieces;
        boost::split(pieces, value.value, boost::is_any_of(sstring(versioned_value::DELIMITER_STR)));
        if (pieces.empty()) {
            logger.warn("Fail to split status in on_change: endpoint={}, app_state={}, value={}", endpoint, state, value);
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
        }
    } else {
        auto& gossiper = gms::get_local_gossiper();
        auto ep_state = gossiper.get_endpoint_state_for_endpoint(endpoint);
        if (!ep_state || gossiper.is_dead_state(*ep_state)) {
            logger.debug("Ignoring state change for dead or unknown endpoint: {}", endpoint);
            return;
        }
        if (get_token_metadata().is_member(endpoint)) {
            do_update_system_peers_table(endpoint, state, value);
            if (state == application_state::SCHEMA) {
                get_local_migration_manager().schedule_schema_pull(endpoint, *ep_state).handle_exception([endpoint] (auto ep) {
                    logger.warn("Fail to pull schmea from {}: {}", endpoint, ep);
                });
            }
        }
    }
    replicate_to_all_cores().get();
}


void storage_service::on_remove(gms::inet_address endpoint) {
    logger.debug("endpoint={} on_remove", endpoint);
    _token_metadata.remove_endpoint(endpoint);
    update_pending_ranges().get();
}

void storage_service::on_dead(gms::inet_address endpoint, gms::endpoint_state state) {
    logger.debug("endpoint={} on_dead", endpoint);
    net::get_local_messaging_service().remove_rpc_client(net::msg_addr{endpoint, 0});
    get_storage_service().invoke_on_all([endpoint] (auto&& ss) {
        for (auto&& subscriber : ss._lifecycle_subscribers) {
            try {
                subscriber->on_down(endpoint);
            } catch (...) {
                logger.warn("Down notification failed {}: {}", endpoint, std::current_exception());
            }
        }
    }).get();
}

void storage_service::on_restart(gms::inet_address endpoint, gms::endpoint_state state) {
    logger.debug("endpoint={} on_restart", endpoint);
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
            logger.error("fail to update {} for {}: {}", col, endpoint, std::current_exception());
        }
        return make_ready_future<>();
    }).get();
}

// Runs inside seastar::async context
void storage_service::do_update_system_peers_table(gms::inet_address endpoint, const application_state& state, const versioned_value& value) {
    logger.debug("Update system.peers table: endpoint={}, app_state={}, versioned_value={}", endpoint, state, value);
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
            logger.error("fail to update {} for {}: invalid rcpaddr {}", col, endpoint, value.value);
            return;
        }
        update_table(endpoint, col, ep.addr());
    } else if (state == application_state::SCHEMA) {
        update_table(endpoint, "schema_version", utils::UUID(value.value));
    } else if (state == application_state::HOST_ID) {
        update_table(endpoint, "host_id", utils::UUID(value.value));
    }
}

// Runs inside seastar::async context
void storage_service::update_peer_info(gms::inet_address endpoint) {
    using namespace gms;
    auto& gossiper = gms::get_local_gossiper();
    auto ep_state = gossiper.get_endpoint_state_for_endpoint(endpoint);
    if (!ep_state) {
        return;
    }
    for (auto& entry : ep_state->get_application_state_map()) {
        auto& app_state = entry.first;
        auto& value = entry.second;
        do_update_system_peers_table(endpoint, app_state, value);
    }
}

sstring storage_service::get_application_state_value(inet_address endpoint, application_state appstate) {
    auto& gossiper = gms::get_local_gossiper();
    auto eps = gossiper.get_endpoint_state_for_endpoint(endpoint);
    if (!eps) {
        return {};
    }
    auto v = eps->get_application_state(appstate);
    if (!v) {
        return {};
    }
    return v->value;
}

std::unordered_set<locator::token> storage_service::get_tokens_for(inet_address endpoint) {
    auto tokens_string = get_application_state_value(endpoint, application_state::TOKENS);
    logger.trace("endpoint={}, tokens_string={}", endpoint, tokens_string);
    std::vector<sstring> tokens;
    std::unordered_set<token> ret;
    boost::split(tokens, tokens_string, boost::is_any_of(";"));
    for (auto str : tokens) {
        auto t = dht::global_partitioner().from_sstring(str);
        logger.trace("endpoint={}, token_str={} token={}", endpoint, str, t);
        ret.emplace(std::move(t));
    }
    return ret;
}

// Runs inside seastar::async context
void storage_service::set_tokens(std::unordered_set<token> tokens) {
    logger.debug("Setting tokens to {}", tokens);
    db::system_keyspace::update_tokens(tokens).get();
    _token_metadata.update_normal_tokens(tokens, get_broadcast_address());
    auto local_tokens = get_local_tokens();
    set_gossip_tokens(local_tokens);
    set_mode(mode::NORMAL, "node is now in normal status", true);
    replicate_to_all_cores().get();
}

void storage_service::set_gossip_tokens(const std::unordered_set<dht::token>& local_tokens) {
    auto& gossiper = gms::get_local_gossiper();
    gossiper.add_local_application_state(gms::application_state::TOKENS, value_factory.tokens(local_tokens)).get();
    gossiper.add_local_application_state(gms::application_state::STATUS, value_factory.normal(local_tokens)).get();
}

void storage_service::register_subscriber(endpoint_lifecycle_subscriber* subscriber)
{
    _lifecycle_subscribers.emplace_back(subscriber);
}

void storage_service::unregister_subscriber(endpoint_lifecycle_subscriber* subscriber)
{
    _lifecycle_subscribers.erase(std::remove(_lifecycle_subscribers.begin(), _lifecycle_subscribers.end(), subscriber), _lifecycle_subscribers.end());
}

static stdx::optional<future<>> drain_in_progress;

future<> storage_service::drain_on_shutdown() {
    return run_with_no_api_lock([] (storage_service& ss) {
        if (drain_in_progress) {
            return std::move(*drain_in_progress);
        }
        return seastar::async([&ss] {
            logger.info("Drain on shutdown: starts");

            gms::get_local_gossiper().stop_gossiping().get();
            logger.info("Drain on shutdown: stop_gossiping done");

            ss.shutdown_client_servers().get();
            logger.info("Drain on shutdown: shutdown rpc and cql server done");

            ss.do_stop_ms().get();
            logger.info("Drain on shutdown: shutdown messaging_service done");

            auth::auth::shutdown().get();
            logger.info("Drain on shutdown: auth shutdown");

            ss.flush_column_families();
            logger.info("Drain on shutdown: flush column_families done");

            ss.db().invoke_on_all([] (auto& db) {
                return db.commitlog()->shutdown();
            }).get();
            logger.info("Drain on shutdown: shutdown commitlog done");
            logger.info("Drain on shutdown: done");
        });
    });
#if 0
        // daemon threads, like our executors', continue to run while shutdown hooks are invoked
        drainOnShutdown = new Thread(new WrappedRunnable()
        {
            @Override
            public void runMayThrow() throws InterruptedException
            {
                ExecutorService counterMutationStage = StageManager.getStage(Stage.COUNTER_MUTATION);
                ExecutorService mutationStage = StageManager.getStage(Stage.MUTATION);
                if (mutationStage.isShutdown() && counterMutationStage.isShutdown())
                    return; // drained already

                if (daemon != null)
                    shutdownClientServers();
                ScheduledExecutors.optionalTasks.shutdown();
                Gossiper.instance.stop();

                // In-progress writes originating here could generate hints to be written, so shut down MessagingService
                // before mutation stage, so we can get all the hints saved before shutting down
                MessagingService.instance().shutdown();
                counterMutationStage.shutdown();
                mutationStage.shutdown();
                counterMutationStage.awaitTermination(3600, TimeUnit.SECONDS);
                mutationStage.awaitTermination(3600, TimeUnit.SECONDS);
                StorageProxy.instance.verifyNoHintsInProgress();

                List<Future<?>> flushes = new ArrayList<>();
                for (Keyspace keyspace : Keyspace.all())
                {
                    KSMetaData ksm = Schema.instance.getKSMetaData(keyspace.getName());
                    if (!ksm.durableWrites)
                    {
                        for (ColumnFamilyStore cfs : keyspace.getColumnFamilyStores())
                            flushes.add(cfs.forceFlush());
                    }
                }
                try
                {
                    FBUtilities.waitOnFutures(flushes);
                }
                catch (Throwable t)
                {
                    JVMStabilityInspector.inspectThrowable(t);
                    // don't let this stop us from shutting down the commitlog and other thread pools
                    logger.warn("Caught exception while waiting for memtable flushes during shutdown hook", t);
                }

                CommitLog.instance.shutdownBlocking();

                // wait for miscellaneous tasks like sstable and commitlog segment deletion
                ScheduledExecutors.nonPeriodicTasks.shutdown();
                if (!ScheduledExecutors.nonPeriodicTasks.awaitTermination(1, TimeUnit.MINUTES))
                    logger.warn("Miscellaneous task executor still busy after one minute; proceeding with shutdown");
            }
        }, "StorageServiceShutdownHook");
        Runtime.getRuntime().addShutdownHook(drainOnShutdown);
#endif
}

future<> storage_service::init_server(int delay) {
    return seastar::async([this, delay] {
        auto& gossiper = gms::get_local_gossiper();
#if 0
        logger.info("Cassandra version: {}", FBUtilities.getReleaseVersionString());
        logger.info("Thrift API version: {}", cassandraConstants.VERSION);
        logger.info("CQL supported versions: {} (default: {})", StringUtils.join(ClientState.getCQLSupportedVersion(), ","), ClientState.DEFAULT_CQL_VERSION);
#endif
        _initialized = true;
#if 0
        try
        {
            // Ensure StorageProxy is initialized on start-up; see CASSANDRA-3797.
            Class.forName("org.apache.cassandra.service.StorageProxy");
            // also IndexSummaryManager, which is otherwise unreferenced
            Class.forName("org.apache.cassandra.io.sstable.IndexSummaryManager");
        }
        catch (ClassNotFoundException e)
        {
            throw new AssertionError(e);
        }
#endif

        if (get_property_load_ring_state()) {
            logger.info("Loading persisted ring state");
            auto loaded_tokens = db::system_keyspace::load_tokens().get0();
            auto loaded_host_ids = db::system_keyspace::load_host_ids().get0();

            for (auto& x : loaded_tokens) {
                logger.debug("Loaded tokens: endpoint={}, tokens={}", x.first, x.second);
            }

            for (auto& x : loaded_host_ids) {
                logger.debug("Loaded host_id: endpoint={}, uuid={}", x.first, x.second);
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
                    gossiper.add_saved_endpoint(ep);
                }
            }
        }

        prepare_to_join();
#if 0
        // Has to be called after the host id has potentially changed in prepareToJoin().
        for (ColumnFamilyStore cfs : ColumnFamilyStore.all())
            if (cfs.metadata.isCounter())
                cfs.initCounterCache();
#endif

        if (get_property_join_ring()) {
            join_token_ring(delay);
        } else {
            auto tokens = db::system_keyspace::get_saved_tokens().get0();
            if (!tokens.empty()) {
                _token_metadata.update_normal_tokens(tokens, get_broadcast_address());
                // order is important here, the gossiper can fire in between adding these two states.  It's ok to send TOKENS without STATUS, but *not* vice versa.
                gossiper.add_local_application_state(gms::application_state::TOKENS, value_factory.tokens(tokens)).get();
                gossiper.add_local_application_state(gms::application_state::STATUS, value_factory.hibernate(true)).get();
            }
            logger.info("Not joining ring as requested. Use JMX (StorageService->joinRing()) to initiate ring joining");
        }
    });
}

future<> storage_service::replicate_to_all_cores() {
    if (engine().cpu_id() != 0) {
        auto err = sprint("replicate_to_all_cores is not ran on cpu zero");
        logger.warn(err.c_str());
        throw std::runtime_error(err);
    }
    // FIXME: There is no back pressure. If the remote cores are slow, and
    // replication is called often, it will queue tasks to the semaphore
    // without end.
    return _replicate_task.wait().then([this] {
        return _the_storage_service.invoke_on_all([tm = _token_metadata] (storage_service& local_ss) {
            if (engine().cpu_id() != 0) {
                local_ss._token_metadata = tm;
            }
        });
    }).then_wrapped([this] (auto&& f) {
        try {
            _replicate_task.signal();
            f.get();
        } catch (...) {
            logger.error("Fail to replicate _token_metadata");
        }
        return make_ready_future<>();
    });
}

future<> storage_service::gossip_snitch_info() {
    auto& snitch = locator::i_endpoint_snitch::get_local_snitch_ptr();
    auto addr = get_broadcast_address();
    auto dc = snitch->get_datacenter(addr);
    auto rack = snitch->get_rack(addr);
    auto& gossiper = gms::get_local_gossiper();
    return gossiper.add_local_application_state(gms::application_state::DC, value_factory.datacenter(dc)).then([this, &gossiper, rack] {
        return gossiper.add_local_application_state(gms::application_state::RACK, value_factory.rack(rack));
    });
}

future<> storage_service::stop() {
    return make_ready_future<>();
}

future<> storage_service::check_for_endpoint_collision() {
    logger.debug("Starting shadow gossip round to check for endpoint collision");
#if 0
    if (!MessagingService.instance().isListening())
        MessagingService.instance().listen(FBUtilities.getLocalAddress());
#endif
    return seastar::async([this] {
        auto& gossiper = gms::get_local_gossiper();
        auto t = gms::gossiper::clk::now();
        bool found_bootstrapping_node = false;
        do {
            gossiper.do_shadow_round().get();
            auto addr = get_broadcast_address();
            if (!gossiper.is_safe_for_bootstrap(addr)) {
                throw std::runtime_error(sprint("A node with address %s already exists, cancelling join. "
                    "Use cassandra.replace_address if you want to replace this node.", addr));
            }
            if (dht::range_streamer::use_strict_consistency()) {
                found_bootstrapping_node = false;
                for (auto& x : gossiper.get_endpoint_states()) {
                    auto state = gossiper.get_gossip_status(x.second);
                    if (state.empty()) {
                        continue;
                    }
                    logger.debug("Checking bootstrapping/leaving/moving nodes: node={}, status={} (check_for_endpoint_collision)", x.first, state);
                    if (state == sstring(versioned_value::STATUS_BOOTSTRAPPING) ||
                        state == sstring(versioned_value::STATUS_LEAVING) ||
                        state == sstring(versioned_value::STATUS_MOVING)) {
                        if (gms::gossiper::clk::now() > t + std::chrono::seconds(60)) {
                            throw std::runtime_error("Other bootstrapping/leaving/moving nodes detected, cannot bootstrap while cassandra.consistent.rangemovement is true (check_for_endpoint_collision)");
                        } else {
                            gossiper.goto_shadow_round();
                            gossiper.reset_endpoint_state_map();
                            found_bootstrapping_node = true;
                            auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(gms::gossiper::clk::now() - t).count();
                            logger.info("Checking bootstrapping/leaving/moving nodes: node={}, status={}, sleep 1 second and check again ({} seconds elapsed) (check_for_endpoint_collision)", x.first, state, elapsed);
                            sleep(std::chrono::seconds(1)).get();
                            break;
                        }
                    }
                }
            }
        } while (found_bootstrapping_node);
        logger.info("Checking bootstrapping/leaving/moving nodes: ok (check_for_endpoint_collision)");
        gossiper.reset_endpoint_state_map();
    });
}

// Runs inside seastar::async context
void storage_service::remove_endpoint(inet_address endpoint) {
    auto& gossiper = gms::get_local_gossiper();
    gossiper.remove_endpoint(endpoint);
    db::system_keyspace::remove_endpoint(endpoint).then_wrapped([endpoint] (auto&& f) {
        try {
            f.get();
        } catch (...) {
            logger.error("fail to remove endpoint={}: {}", endpoint, std::current_exception());
        }
        return make_ready_future<>();
    }).get();
}

future<std::unordered_set<token>> storage_service::prepare_replacement_info() {
    if (!db().local().get_replace_address()) {
        throw std::runtime_error(sprint("replace_address is empty"));
    }
    auto replace_address = db().local().get_replace_address().value();
    logger.info("Gathering node replacement information for {}", replace_address);

    // if (!MessagingService.instance().isListening())
    //     MessagingService.instance().listen(FBUtilities.getLocalAddress());

    // make magic happen
    return gms::get_local_gossiper().do_shadow_round().then([this, replace_address] {
        auto& gossiper = gms::get_local_gossiper();
        // now that we've gossiped at least once, we should be able to find the node we're replacing
        auto state = gossiper.get_endpoint_state_for_endpoint(replace_address);
        if (!state) {
            throw std::runtime_error(sprint("Cannot replace_address %s because it doesn't exist in gossip", replace_address));
        }
        auto host_id = gossiper.get_host_id(replace_address);
        auto eps = gossiper.get_endpoint_state_for_endpoint(replace_address);
        if (!eps) {
            throw std::runtime_error(sprint("Cannot replace_address %s because can not find gossip endpoint state", replace_address));
        }
        auto value = eps->get_application_state(application_state::TOKENS);
        if (!value) {
            throw std::runtime_error(sprint("Could not find tokens for %s to replace", replace_address));
        }
        auto tokens = get_tokens_for(replace_address);
        // use the replacee's host Id as our own so we receive hints, etc
        return db::system_keyspace::set_local_host_id(host_id).discard_result().then([replace_address, tokens = std::move(tokens)] {
            gms::get_local_gossiper().reset_endpoint_state_map(); // clean up since we have what we need
            return make_ready_future<std::unordered_set<token>>(std::move(tokens));
        });
    });
}

future<std::map<gms::inet_address, float>> storage_service::get_ownership() {
    return run_with_no_api_lock([] (storage_service& ss) {
        auto token_map = dht::global_partitioner().describe_ownership(ss._token_metadata.sorted_tokens());
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
            if (typeid(ks.get_replication_strategy()) == typeid(locator::local_strategy)) {
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
        auto token_ownership = dht::global_partitioner().describe_ownership(ss._token_metadata.sorted_tokens());

        std::map<gms::inet_address, float> final_ownership;

        // calculate ownership per dc
        for (auto endpoints : ss._token_metadata.get_topology().get_datacenter_endpoints()) {
            // calculate the ownership with replication and add the endpoint to the final ownership map
            for (const gms::inet_address& endpoint : endpoints.second) {
                float ownership = 0.0f;
                for (range<token> r : ss.get_ranges_for_endpoint(keyspace_name, endpoint)) {
                    if (token_ownership.find(r.end().value().value()) != token_ownership.end()) {
                        ownership += token_ownership[r.end().value().value()];
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
        logger.info("{}: {}", m, msg);
    } else {
        logger.debug("{}: {}", m, msg);
    }
}

// Runs inside seastar::async context
std::unordered_set<dht::token> storage_service::get_local_tokens() {
    auto tokens = db::system_keyspace::get_saved_tokens().get0();
    // should not be called before initServer sets this
    if (tokens.empty()) {
        auto err = sprint("get_local_tokens: tokens is empty");
        logger.error(err.c_str());
        throw std::runtime_error(err);
    }
    return tokens;
}

sstring storage_service::get_release_version() {
    return version::release();
}

sstring storage_service::get_schema_version() {
    return _db.local().get_version().to_sstring();
}

static constexpr auto UNREACHABLE = "UNREACHABLE";

future<std::unordered_map<sstring, std::vector<sstring>>> storage_service::describe_schema_versions() {
    auto live_hosts = gms::get_local_gossiper().get_live_members();
    std::unordered_map<sstring, std::vector<sstring>> results;
    return map_reduce(std::move(live_hosts), [] (auto host) {
        auto f0 = net::get_messaging_service().local().send_schema_check(net::msg_addr{ host, 0 });
        return std::move(f0).then_wrapped([host] (auto f) {
            if (f.failed()) {
                return std::pair<gms::inet_address, stdx::optional<utils::UUID>>(host, stdx::nullopt);
            }
            return std::pair<gms::inet_address, stdx::optional<utils::UUID>>(host, f.get0());
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
            logger.debug("Hosts not in agreement. Didn't get a response from everybody: {}", ::join( ",", it_unreachable->second));
        }
        auto my_version = get_local_storage_service().get_schema_version();
        for (auto&& entry : results) {
            // check for version disagreement. log the hosts that don't agree.
            if (entry.first == UNREACHABLE || entry.first == my_version) {
                continue;
            }
            for (auto&& host : entry.second) {
                logger.debug("{} disagrees ({})", host, entry.first);
            }
        }
        if (results.size() == 1) {
            logger.debug("Schemas are in agreement.");
        }
        return results;
    });
};

future<sstring> storage_service::get_operation_mode() {
    return run_with_no_api_lock([] (storage_service& ss) {
        auto mode = ss._operation_mode;
        return make_ready_future<sstring>(sprint("%s", mode));
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
        return gms::get_local_gossiper().is_enabled();
    });
}

future<> storage_service::start_gossiping() {
    return run_with_api_lock(sstring("start_gossiping"), [] (storage_service& ss) {
        return seastar::async([&ss] {
            if (!ss._initialized) {
                logger.warn("Starting gossip by operator request");
                ss.set_gossip_tokens(ss.get_local_tokens());
                gms::get_local_gossiper().force_newer_generation();
                gms::get_local_gossiper().start_gossiping(get_generation_number()).then([&ss] {
                    ss._initialized = true;
                }).get();
            }
        });
    });
}

future<> storage_service::stop_gossiping() {
    return run_with_api_lock(sstring("stop_gossiping"), [] (storage_service& ss) {
        if (ss._initialized) {
            logger.warn("Stopping gossip by operator request");
            return gms::get_local_gossiper().stop_gossiping().then([&ss] {
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
    return net::get_messaging_service().invoke_on_all([] (auto& ms) {
        return ms.stop();
    }).then([] {
        logger.info("messaging_service stopped");
    });
}

future<> check_snapshot_not_exist(database& db, sstring ks_name, sstring name) {
    auto& ks = db.find_keyspace(ks_name);
    return parallel_for_each(ks.metadata()->cf_meta_data(), [&db, ks_name = std::move(ks_name), name = std::move(name)] (auto& pair) {
        auto& cf = db.find_column_family(pair.second);
        return cf.snapshot_exists(name).then([ks_name = std::move(ks_name), name] (bool exists) {
            if (exists) {
                throw std::runtime_error(sprint("Keyspace %s: snapshot %s already exists.", ks_name, name));
            }
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

    return smp::submit_to(0, [] {
        auto mode = get_local_storage_service()._operation_mode;
        if (mode == storage_service::mode::JOINING) {
            throw std::runtime_error("Cannot snapshot until bootstrap completes");
        }
    }).then([tag = std::move(tag), keyspace_names = std::move(keyspace_names), this] {
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

    return smp::submit_to(0, [] {
        auto mode = get_local_storage_service()._operation_mode;
        if (mode == storage_service::mode::JOINING) {
            throw std::runtime_error("Cannot snapshot until bootstrap completes");
        }
    }).then([this, ks_name = std::move(ks_name), cf_name = std::move(cf_name), tag = std::move(tag)] {
        return check_snapshot_not_exist(_db.local(), ks_name, tag).then([this, ks_name, cf_name, tag] {
            return _db.invoke_on_all([ks_name, cf_name, tag] (database &db) {
                auto& cf = db.find_column_family(ks_name, cf_name);
                return cf.snapshot(tag);
            });
        });
    });
}

future<> storage_service::clear_snapshot(sstring tag, std::vector<sstring> keyspace_names) {
    return _db.local().clear_snapshot(tag, keyspace_names);
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

    return _db.map_reduce(snapshot_reducer(), [] (database& db) {
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
    }).then([this] (snapshot_map&& map) {
        std::unordered_map<sstring, std::vector<service::storage_service::snapshot_details>> result;
        for (auto&& pair: map) {
            std::vector<service::storage_service::snapshot_details> details;

            for (auto&& snap_map: pair.second) {
                auto& cf = _db.local().find_column_family(snap_map.first);
                details.push_back({ snap_map.second.live, snap_map.second.total, cf.schema()->cf_name(), cf.schema()->ks_name() });
            }
            result.emplace(pair.first, std::move(details));
        }

        return make_ready_future<std::unordered_map<sstring, std::vector<service::storage_service::snapshot_details>>>(std::move(result));
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

future<> storage_service::start_rpc_server() {
    return run_with_api_lock(sstring("start_rpc_server"), [] (storage_service& ss) {
        if (ss._thrift_server) {
            return make_ready_future<>();
        }

        auto tserver = make_shared<distributed<thrift_server>>();
        ss._thrift_server = tserver;

        auto& cfg = ss._db.local().get_config();
        auto port = cfg.rpc_port();
        auto addr = cfg.rpc_address();
        return dns::gethostbyname(addr).then([&ss, tserver, addr, port] (dns::hostent e) {
            auto ip = e.addresses[0].in.s_addr;
            return tserver->start(std::ref(ss._db)).then([tserver, port, addr, ip] {
                // #293 - do not stop anything
                //engine().at_exit([tserver] {
                //    return tserver->stop();
                //});
                return tserver->invoke_on_all(&thrift_server::listen, ipv4_addr{ip, port});
            });
        }).then([addr, port] {
            print("Thrift server listening on %s:%s ...\n", addr, port);
        });
    });
}

future<> storage_service::do_stop_rpc_server() {
    auto tserver = _thrift_server;
    _thrift_server = {};
    if (tserver) {
        // FIXME: thrift_server::stop() doesn't kill existing connections and wait for them
        // Note: We must capture tserver so that it will not be freed before tserver->stop
        return tserver->stop().then([tserver] {
            logger.info("Thrift server stopped");
        });
    }
    return make_ready_future<>();
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
        if (ss._cql_server) {
            return make_ready_future<>();
        }
        auto cserver = make_shared<distributed<transport::cql_server>>();
        ss._cql_server = cserver;

        auto& cfg = ss._db.local().get_config();
        auto port = cfg.native_transport_port();
        auto addr = cfg.rpc_address();
        auto ceo = cfg.client_encryption_options();
        transport::cql_load_balance lb = transport::parse_load_balance(cfg.load_balance());
        return dns::gethostbyname(addr).then([cserver, addr, port, lb, ceo = std::move(ceo)] (dns::hostent e) {
            auto ip = e.addresses[0].in.s_addr;
            return cserver->start(std::ref(service::get_storage_proxy()), std::ref(cql3::get_query_processor()), lb).then([cserver, port, addr, ip, ceo]() {
                // #293 - do not stop anything
                //engine().at_exit([cserver] {
                //    return cserver->stop();
                //});

                ::shared_ptr<seastar::tls::server_credentials> cred;
                auto addr = ipv4_addr{ip, port};
                auto f = make_ready_future();

                // main should have made sure values are clean and neatish
                if (ceo.at("enabled") == "true") {
                    cred = ::make_shared<seastar::tls::server_credentials>(::make_shared<seastar::tls::dh_params>(seastar::tls::dh_params::level::MEDIUM));
                    f = cred->set_x509_key_file(ceo.at("certificate"), ceo.at("keyfile"), seastar::tls::x509_crt_format::PEM);
                }
                return f.then([cserver, addr, cred] {
                    return cserver->invoke_on_all(&transport::cql_server::listen, addr, cred);
                });
            });
        }).then([addr, port] {
            logger.info("Starting listening for CQL clients on {}:{}...", addr, port);
        });
    });
}

future<> storage_service::do_stop_native_transport() {
    auto cserver = _cql_server;
    _cql_server = {};
    if (cserver) {
        // FIXME: cql_server::stop() doesn't kill existing connections and wait for them
        // Note: We must capture cserver so that it will not be freed before cserver->stop
        return cserver->stop().then([cserver] {
            logger.info("CQL server stopped");
        });
    }
    return make_ready_future<>();
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
                throw std::runtime_error(sprint("Node in %s state; wait for status to become normal or restart", ss._operation_mode));
            }

            ss.update_pending_ranges().get();

            auto non_system_keyspaces = db.get_non_system_keyspaces();
            for (const auto& keyspace_name : non_system_keyspaces) {
                if (tm.get_pending_ranges(keyspace_name, ss.get_broadcast_address()).size() > 0) {
                    throw std::runtime_error("data is currently moving to this node; unable to leave the ring");
                }
            }

            logger.debug("DECOMMISSIONING");
            ss.start_leaving().get();
            // FIXME: long timeout = Math.max(RING_DELAY, BatchlogManager.instance.getBatchlogTimeout());
            auto timeout = ss.get_ring_delay();
            ss.set_mode(mode::LEAVING, sprint("sleeping %s ms for batch processing and pending range setup", timeout.count()), true);
            sleep(timeout).get();

            logger.debug("DECOMMISSIONING: unbootstrap starts");
            ss.unbootstrap();
            logger.debug("DECOMMISSIONING: unbootstrap done");

            // FIXME: proper shutdown
            ss.shutdown_client_servers().get();
            logger.debug("DECOMMISSIONING: shutdown rpc and cql server done");
            gms::get_local_gossiper().stop_gossiping().get();
            logger.debug("DECOMMISSIONING: stop_gossiping done");
            ss.do_stop_ms().get();
            // StageManager.shutdownNow();
            db::system_keyspace::set_bootstrap_state(db::system_keyspace::bootstrap_state::DECOMMISSIONED).get();
            logger.debug("DECOMMISSIONING: set_bootstrap_state done");
            ss.set_mode(mode::DECOMMISSIONED, true);
            // let op be responsible for killing the process
        });
    });
}

future<> storage_service::remove_node(sstring host_id_string) {
    return run_with_api_lock(sstring("remove_node"), [host_id_string] (storage_service& ss) mutable {
        return seastar::async([&ss, host_id_string] {
            logger.debug("remove_node: host_id = {}", host_id_string);
            auto my_address = ss.get_broadcast_address();
            auto& tm = ss._token_metadata;
            auto local_host_id = tm.get_host_id(my_address);
            auto host_id = utils::UUID(host_id_string);
            auto endpoint_opt = tm.get_endpoint_for_host_id(host_id);
            auto& gossiper = gms::get_local_gossiper();
            if (!endpoint_opt) {
                throw std::runtime_error("Host ID not found.");
            }
            auto endpoint = *endpoint_opt;

            auto tokens = tm.get_tokens(endpoint);

            logger.debug("remove_node: endpoint = {}", endpoint);

            if (endpoint == my_address) {
                throw std::runtime_error("Cannot remove self");
            }

            if (gossiper.get_live_members().count(endpoint)) {
                throw std::runtime_error(sprint("Node %s is alive and owns this ID. Use decommission command to remove it from the ring", endpoint));
            }

            // A leaving endpoint that is dead is already being removed.
            if (tm.is_leaving(endpoint)) {
                logger.warn("Node {} is already being removed, continuing removal anyway", endpoint);
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
                    logger.warn("keyspace={} has replication factor 1, the data is probably lost", keyspace_name);
                    continue;
                }

                // get all ranges that change ownership (that is, a node needs
                // to take responsibility for new range)
                std::unordered_multimap<range<token>, inet_address> changed_ranges =
                    ss.get_changed_ranges_for_leaving(keyspace_name, endpoint);
                auto& fd = gms::get_local_failure_detector();
                for (auto& x: changed_ranges) {
                    auto ep = x.second;
                    if (fd.is_alive(ep)) {
                        ss._replicating_nodes.emplace(ep);
                    } else {
                        logger.warn("Endpoint {} is down and will not receive data for re-replication of {}", ep, endpoint);
                    }
                }
            }
            ss._removing_node = endpoint;
            tm.add_leaving_endpoint(endpoint);
            ss.update_pending_ranges().get();

            // the gossiper will handle spoofing this node's state to REMOVING_TOKEN for us
            // we add our own token so other nodes to let us know when they're done
            gossiper.advertise_removing(endpoint, host_id, local_host_id).get();

            // kick off streaming commands
            ss.restore_replica_count(endpoint, my_address).get();

            // wait for ReplicationFinishedVerbHandler to signal we're done
            while (!ss._replicating_nodes.empty()) {
                sleep(std::chrono::milliseconds(100)).get();
            }

            std::unordered_set<token> tmp(tokens.begin(), tokens.end());
            ss.excise(std::move(tmp), endpoint);

            // gossiper will indicate the token has left
            gossiper.advertise_token_removed(endpoint, host_id).get();

            ss._replicating_nodes.clear();
            ss._removing_node = {};
        });
    });
}

// Runs inside seastar::async context
void storage_service::flush_column_families() {
    service::get_storage_service().invoke_on_all([] (auto& ss) {
        auto& local_db = ss.db().local();
        auto non_system_cfs = local_db.get_column_families() | boost::adaptors::filtered([] (auto& uuid_and_cf) {
            auto cf = uuid_and_cf.second;
            return cf->schema()->ks_name() != db::system_keyspace::NAME;
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
            return cf->schema()->ks_name() == db::system_keyspace::NAME;
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
                logger.warn("Cannot drain node (did it already happen?)");
                return;
            }
            if (drain_in_progress) {
                drain_in_progress->get();
                ss.set_mode(mode::DRAINED, true);
                return;
            }
            promise<> p;
            drain_in_progress = p.get_future();

            ss.set_mode(mode::DRAINING, "starting drain process", true);
            ss.shutdown_client_servers().get();
            gms::get_local_gossiper().stop_gossiping().get();

            ss.set_mode(mode::DRAINING, "shutting down messaging_service", false);
            ss.do_stop_ms().get();

#if 0
    StorageProxy.instance.verifyNoHintsInProgress();
#endif

            ss.set_mode(mode::DRAINING, "flushing column families", false);
            ss.flush_column_families();

            db::get_batchlog_manager().invoke_on_all([] (auto& bm) {
                return bm.stop();
            }).get();

            // Interrupt on going compaction and shutdown to prevent further compaction
            ss.db().invoke_on_all([] (auto& db) {
                // FIXME: ongoing compaction tasks should be interrupted, not
                // waited for which is what compaction_manager::stop() does now.
                return db.get_compaction_manager().stop();
            }).get();

#if 0
    // whilst we've flushed all the CFs, which will have recycled all completed segments, we want to ensure
    // there are no segments to replay, so we force the recycling of any remaining (should be at most one)
    CommitLog.instance.forceRecycleAllSegments();
#endif

            ss.db().invoke_on_all([] (auto& db) {
                return db.commitlog()->shutdown();
            }).get();

            ss.set_mode(mode::DRAINED, true);
            p.set_value();
        });
    });
}

double storage_service::get_load() {
    double bytes = 0;
#if 0
    for (String keyspaceName : Schema.instance.getKeyspaces())
    {
        Keyspace keyspace = Schema.instance.getKeyspaceInstance(keyspaceName);
        if (keyspace == null)
            continue;
        for (ColumnFamilyStore cfs : keyspace.getColumnFamilyStores())
            bytes += cfs.getLiveDiskSpaceUsed();
    }
#endif
    return bytes;
}

sstring storage_service::get_load_string() {
    return sprint("%f", get_load());
}

future<std::map<sstring, double>> storage_service::get_load_map() {
    return run_with_no_api_lock([] (storage_service& ss) {
        std::map<sstring, double> load_map;
        auto& lb = ss.get_load_broadcaster();
        if (lb) {
            for (auto& x : lb->get_load_info()) {
                load_map.emplace(sprint("%s", x.first), x.second);
                logger.debug("get_load_map endpoint={}, load={}", x.first, x.second);
            }
        } else {
            logger.debug("load_broadcaster is not set yet!");
        }
        load_map.emplace(sprint("%s", ss.get_broadcast_address()), ss.get_load());
        return load_map;
    });
}


future<> storage_service::rebuild(sstring source_dc) {
    return run_with_api_lock(sstring("rebuild"), [source_dc] (storage_service& ss) {
        logger.info("rebuild from dc: {}", source_dc == "" ? "(any dc)" : source_dc);
        auto streamer = make_lw_shared<dht::range_streamer>(ss._db, ss._token_metadata, ss.get_broadcast_address(), "Rebuild");
        streamer->add_source_filter(std::make_unique<dht::range_streamer::failure_detector_source_filter>(gms::get_local_failure_detector()));
        if (source_dc != "") {
            streamer->add_source_filter(std::make_unique<dht::range_streamer::single_datacenter_filter>(source_dc));
        }
        for (const auto& keyspace_name : ss._db.local().get_non_system_keyspaces()) {
            streamer->add_ranges(keyspace_name, ss.get_local_ranges(keyspace_name));
        }
        return streamer->fetch_async().then_wrapped([streamer] (auto&& f) {
            try {
                auto state = f.get0();
            } catch (...) {
                // This is used exclusively through JMX, so log the full trace but only throw a simple RTE
                logger.error("Error while rebuilding node: {}", std::current_exception());
                throw std::runtime_error(sprint("Error while rebuilding node: %s", std::current_exception()));
            }
            return make_ready_future<>();
        });
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

std::unordered_multimap<range<token>, inet_address> storage_service::get_changed_ranges_for_leaving(sstring keyspace_name, inet_address endpoint) {
    // First get all ranges the leaving endpoint is responsible for
    auto ranges = get_ranges_for_endpoint(keyspace_name, endpoint);

    logger.debug("Node {} ranges [{}]", endpoint, ranges);

    std::unordered_map<range<token>, std::vector<inet_address>> current_replica_endpoints;

    // Find (for each range) all nodes that store replicas for these ranges as well
    auto metadata = _token_metadata.clone_only_token_map(); // don't do this in the loop! #7758
    for (auto& r : ranges) {
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

    std::unordered_multimap<range<token>, inet_address> changed_ranges;

    // Go through the ranges and for each range check who will be
    // storing replicas for these ranges when the leaving endpoint
    // is gone. Whoever is present in newReplicaEndpoints list, but
    // not in the currentReplicaEndpoints list, will be needing the
    // range.
    for (auto& r : ranges) {
        auto& ks = _db.local().find_keyspace(keyspace_name);
        auto end_token = r.end() ? r.end()->value() : dht::maximum_token();
        auto new_replica_endpoints = ks.get_replication_strategy().calculate_natural_endpoints(end_token, temp);

        auto rg = current_replica_endpoints.equal_range(r);
        for (auto it = rg.first; it != rg.second; it++) {
            const range<token>& range_ = it->first;
            std::vector<inet_address>& current_eps = it->second;
            logger.debug("range={}, current_replica_endpoints={}, new_replica_endpoints={}", range_, current_eps, new_replica_endpoints);
            for (auto ep : it->second) {
                auto beg = new_replica_endpoints.begin();
                auto end = new_replica_endpoints.end();
                new_replica_endpoints.erase(std::remove(beg, end, ep), end);
            }
        }

        if (logger.is_enabled(logging::log_level::debug)) {
            if (new_replica_endpoints.empty()) {
                logger.debug("Range {} already in all replicas", r);
            } else {
                logger.debug("Range {} will be responsibility of {}", r, new_replica_endpoints);
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
    std::unordered_map<sstring, std::unordered_multimap<range<token>, inet_address>> ranges_to_stream;

    auto non_system_keyspaces = _db.local().get_non_system_keyspaces();
    for (const auto& keyspace_name : non_system_keyspaces) {
        auto ranges_mm = get_changed_ranges_for_leaving(keyspace_name, get_broadcast_address());
        if (logger.is_enabled(logging::log_level::debug)) {
            std::vector<range<token>> ranges;
            for (auto& x : ranges_mm) {
                ranges.push_back(x.first);
            }
            logger.debug("Ranges needing transfer for keyspace={} are [{}]", keyspace_name, ranges);
        }
        ranges_to_stream.emplace(keyspace_name, std::move(ranges_mm));
    }

    set_mode(mode::LEAVING, "replaying batch log and streaming data to other nodes", true);

    // Start with BatchLog replay, which may create hints but no writes since this is no longer a valid endpoint.
    // FIXME: Future<?> batchlogReplay = BatchlogManager.instance.startBatchlogReplay();
    auto stream_success = stream_ranges(ranges_to_stream);
#if 0
    // Wait for batch log to complete before streaming hints.
    logger.debug("waiting for batch log processing.");
    try
    {
        batchlogReplay.get();
    }
    catch (ExecutionException | InterruptedException e)
    {
        throw new RuntimeException(e);
    }
#endif

    set_mode(mode::LEAVING, "streaming hints to other nodes", true);

    auto hints_success = stream_hints();

    // wait for the transfer runnables to signal the latch.
    logger.debug("waiting for stream acks.");
    try {
        stream_success.get();
        hints_success.get();
    } catch (...) {
        logger.warn("unbootstrap fails to stream : {}", std::current_exception());
        throw;
    }
    logger.debug("stream acks all received.");
    leave_ring();
}

future<> storage_service::restore_replica_count(inet_address endpoint, inet_address notify_endpoint) {
    std::unordered_multimap<sstring, std::unordered_map<inet_address, std::vector<range<token>>>> ranges_to_fetch;

    auto my_address = get_broadcast_address();

    auto non_system_keyspaces = _db.local().get_non_system_keyspaces();
    for (const auto& keyspace_name : non_system_keyspaces) {
        std::unordered_multimap<range<token>, inet_address> changed_ranges = get_changed_ranges_for_leaving(keyspace_name, endpoint);
        std::vector<range<token>> my_new_ranges;
        for (auto& x : changed_ranges) {
            if (x.second == my_address) {
                my_new_ranges.emplace_back(x.first);
            }
        }
        std::unordered_multimap<inet_address, range<token>> source_ranges = get_new_source_ranges(keyspace_name, my_new_ranges);
        std::unordered_map<inet_address, std::vector<range<token>>> tmp;
        for (auto& x : source_ranges) {
            tmp[x.first].emplace_back(x.second);
        }
        ranges_to_fetch.emplace(keyspace_name, std::move(tmp));
    }
    auto sp = make_lw_shared<streaming::stream_plan>("Restore replica count");
    for (auto& x: ranges_to_fetch) {
        const sstring& keyspace_name = x.first;
        std::unordered_map<inet_address, std::vector<range<token>>>& maps = x.second;
        for (auto& m : maps) {
            auto source = m.first;
            auto ranges = m.second;
            logger.debug("Requesting from {} ranges {}", source, ranges);
            sp->request_ranges(source, keyspace_name, ranges);
        }
    }
    return sp->execute().then_wrapped([this, sp, notify_endpoint] (auto&& f) {
        try {
            auto state = f.get0();
            return this->send_replication_notification(notify_endpoint);
        } catch (...) {
            logger.warn("Streaming to restore replica count failed: {}", std::current_exception());
            // We still want to send the notification
            return this->send_replication_notification(notify_endpoint);
        }
        return make_ready_future<>();
    });
}

// Runs inside seastar::async context
void storage_service::excise(std::unordered_set<token> tokens, inet_address endpoint) {
    logger.info("Removing tokens {} for {}", tokens, endpoint);
    // FIXME: HintedHandOffManager.instance.deleteHintsForEndpoint(endpoint);
    remove_endpoint(endpoint);
    _token_metadata.remove_endpoint(endpoint);
    _token_metadata.remove_bootstrap_tokens(tokens);

    get_storage_service().invoke_on_all([endpoint] (auto&& ss) {
        for (auto&& subscriber : ss._lifecycle_subscribers) {
            try {
                subscriber->on_leave_cluster(endpoint);
            } catch (...) {
                logger.warn("Leave cluster notification failed {}: {}", endpoint, std::current_exception());
            }
        }
    }).get();

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
    logger.debug("Notifying {} of replication completion", remote);
    return do_until(
        [done, remote] {
            return *done || !gms::get_local_failure_detector().is_alive(remote);
        },
        [done, remote, local] {
            auto& ms = net::get_local_messaging_service();
            net::msg_addr id{remote, 0};
            return ms.send_replication_finished(id, local).then_wrapped([id, done] (auto&& f) {
                try {
                    f.get();
                    *done = true;
                } catch (...) {
                    logger.warn("Fail to send REPLICATION_FINISHED to {}: {}", id, std::current_exception());
                }
            });
        }
    );
}

future<> storage_service::confirm_replication(inet_address node) {
    return run_with_no_api_lock([node] (storage_service& ss) {
        // replicatingNodes can be empty in the case where this node used to be a removal coordinator,
        // but restarted before all 'replication finished' messages arrived. In that case, we'll
        // still go ahead and acknowledge it.
        if (!ss._replicating_nodes.empty()) {
            ss._replicating_nodes.erase(node);
        } else {
            logger.info("Received unexpected REPLICATION_FINISHED message from {}. Was this node recently a removal coordinator?", node);
        }
    });
}

// Runs inside seastar::async context
void storage_service::leave_ring() {
    db::system_keyspace::set_bootstrap_state(db::system_keyspace::bootstrap_state::NEEDS_BOOTSTRAP).get();
    _token_metadata.remove_endpoint(get_broadcast_address());
    update_pending_ranges().get();

    auto& gossiper = gms::get_local_gossiper();
    auto expire_time = gossiper.compute_expire_time().time_since_epoch().count();
    gossiper.add_local_application_state(gms::application_state::STATUS, value_factory.left(get_local_tokens(), expire_time)).get();
    auto delay = std::max(get_ring_delay(), gms::gossiper::INTERVAL);
    logger.info("Announcing that I have left the ring for {}ms", delay.count());
    sleep(delay).get();
}

future<>
storage_service::stream_ranges(std::unordered_map<sstring, std::unordered_multimap<range<token>, inet_address>> ranges_to_stream_by_keyspace) {
    // First, we build a list of ranges to stream to each host, per table
    std::unordered_map<sstring, std::unordered_map<inet_address, std::vector<range<token>>>> sessions_to_stream_by_keyspace;
    for (auto& entry : ranges_to_stream_by_keyspace) {
        const auto& keyspace = entry.first;
        auto& ranges_with_endpoints = entry.second;

        if (ranges_with_endpoints.empty()) {
            continue;
        }

        std::unordered_map<inet_address, std::vector<range<token>>> ranges_per_endpoint;
        for (auto& end_point_entry : ranges_with_endpoints) {
            range<token> r = end_point_entry.first;
            inet_address endpoint = end_point_entry.second;
            ranges_per_endpoint[endpoint].emplace_back(r);
        }
        sessions_to_stream_by_keyspace.emplace(keyspace, std::move(ranges_per_endpoint));
    }
    auto sp = make_lw_shared<streaming::stream_plan>("Unbootstrap");
    for (auto& entry : sessions_to_stream_by_keyspace) {
        const auto& keyspace_name = entry.first;
        // TODO: we can move to avoid copy of std::vector
        auto& ranges_per_endpoint = entry.second;

        for (auto& ranges_entry : ranges_per_endpoint) {
            auto& ranges = ranges_entry.second;
            auto new_endpoint = ranges_entry.first;
            // TODO each call to transferRanges re-flushes, this is potentially a lot of waste
            sp->transfer_ranges(new_endpoint, keyspace_name, ranges);
        }
    }
    return sp->execute().discard_result().then([sp] {
        logger.info("stream_ranges successful");
    }).handle_exception([] (auto ep) {
        logger.info("stream_ranges failed: {}", ep);
        return make_exception_future(std::runtime_error("stream_ranges failed"));
    });
}

future<> storage_service::stream_hints() {
    // FIXME: flush hits column family
#if 0
    // StreamPlan will not fail if there are zero files to transfer, so flush anyway (need to get any in-memory hints, as well)
    ColumnFamilyStore hintsCF = Keyspace.open(SystemKeyspace.NAME).getColumnFamilyStore(SystemKeyspace.HINTS);
    FBUtilities.waitOnFuture(hintsCF.forceFlush());
#endif

    // gather all live nodes in the cluster that aren't also leaving
    auto candidates = get_local_storage_service().get_token_metadata().clone_after_all_left().get_all_endpoints();
    auto beg = candidates.begin();
    auto end = candidates.end();
    auto remove_fn = [br = get_broadcast_address()] (const inet_address& ep) {
        return ep == br || !gms::get_local_failure_detector().is_alive(ep);
    };
    candidates.erase(std::remove_if(beg, end, remove_fn), end);

    if (candidates.empty()) {
        logger.warn("Unable to stream hints since no live endpoints seen");
        throw std::runtime_error("Unable to stream hints since no live endpoints seen");
    } else {
        // stream to the closest peer as chosen by the snitch
        auto& snitch = locator::i_endpoint_snitch::get_local_snitch_ptr();

        snitch->sort_by_proximity(get_broadcast_address(), candidates);
        auto hints_destination_host = candidates.front();

        // stream all hints -- range list will be a singleton of "the entire ring"
        std::vector<range<token>> ranges = {range<token>::make_open_ended_both_sides()};
        logger.debug("stream_hints: ranges={}", ranges);

        auto sp = make_lw_shared<streaming::stream_plan>("Hints");
        std::vector<sstring> column_families = { db::system_keyspace::HINTS };
        auto keyspace = db::system_keyspace::NAME;
        sp->transfer_ranges(hints_destination_host, keyspace, ranges, column_families);
        return sp->execute().discard_result().then([sp] {
            logger.info("stream_hints successful");
        }).handle_exception([] (auto ep) {
            logger.info("stream_hints failed: {}", ep);
            return make_exception_future(std::runtime_error("stream_hints failed"));
        });
    }
}

future<> storage_service::start_leaving() {
    auto& gossiper = gms::get_local_gossiper();
    return gossiper.add_local_application_state(application_state::STATUS, value_factory.leaving(get_local_tokens())).then([this] {
        _token_metadata.add_leaving_endpoint(get_broadcast_address());
        return update_pending_ranges();
    });
}

void storage_service::add_expire_time_if_found(inet_address endpoint, int64_t expire_time) {
    if (expire_time != 0L) {
        using clk = gms::gossiper::clk;
        auto time = clk::time_point(clk::duration(expire_time));
        gms::get_local_gossiper().add_expire_time_for_endpoint(endpoint, time);
    }
}

// For more details, see the commends on column_family::load_new_sstables
// All the global operations are going to happen here, and just the reloading happens
// in there.
future<> storage_service::load_new_sstables(sstring ks_name, sstring cf_name) {
    class max_element {
        int64_t _result = 1;
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
        logger.debug("Loading new sstables with generation numbers larger or equal than {}", max_seen_sstable);
        // Then, we will reshuffle the tables to make sure that the generation numbers don't go too high.
        // We will do all of it the same CPU, to make sure that we won't have two parallel shufflers stepping
        // onto each other.
        //
        // Note that this will reshuffle all tables, including existing ones. Figuring out which of the tables
        // are new would require coordination between all shards, so it is simpler this way. Renaming an existing
        // SSTable shouldn't be that bad, and we are assuming empty directory for normal operation anyway.
        auto shard = std::hash<sstring>()(cf_name) % smp::count;
        return _db.invoke_on(shard, [ks_name, cf_name, max_seen_sstable] (database& db) {
            auto& cf = db.find_column_family(ks_name, cf_name);
            return cf.reshuffle_sstables(max_seen_sstable);
        });
    }).then([this, ks_name, cf_name] (std::vector<sstables::entry_descriptor> new_tables) {
        int64_t new_gen = 1;
        if (new_tables.size() > 0) {
            new_gen = new_tables.back().generation;
        }

        logger.debug("Now accepting writes for sstables with generation larger or equal than {}", new_gen);
        return _db.invoke_on_all([ks_name, cf_name, new_gen] (database& db) {
            auto& cf = db.find_column_family(ks_name, cf_name);
            auto disabled = std::chrono::duration_cast<std::chrono::microseconds>(cf.enable_sstable_write(new_gen)).count();
            logger.info("CF {} at shard {} had SSTables writes disabled for {} usec", cf_name, engine().cpu_id(), disabled);
            return make_ready_future<>();
        }).then([new_tables = std::move(new_tables)] {
            return std::move(new_tables);
        });
    }).then([this, ks_name, cf_name] (std::vector<sstables::entry_descriptor> new_tables) {
        return _db.invoke_on_all([ks_name = std::move(ks_name), cf_name = std::move(cf_name), new_tables = std::move(new_tables)] (database& db) {
            auto& cf = db.find_column_family(ks_name, cf_name);
            return cf.load_new_sstables(new_tables);
        });
    }).finally([this] {
        _loading_new_sstables = false;
    });
}

void storage_service::set_load_broadcaster(shared_ptr<load_broadcaster> lb) {
    _lb = lb;
}

shared_ptr<load_broadcaster>& storage_service::get_load_broadcaster() {
    return _lb;
}

future<> storage_service::shutdown_client_servers() {
    return do_stop_rpc_server().then([this] { return do_stop_native_transport(); });
}

std::unordered_multimap<inet_address, range<token>>
storage_service::get_new_source_ranges(const sstring& keyspace_name, const std::vector<range<token>>& ranges) {
    auto my_address = get_broadcast_address();
    auto& fd = gms::get_local_failure_detector();
    auto& ks = _db.local().find_keyspace(keyspace_name);
    auto& strat = ks.get_replication_strategy();
    auto tm = _token_metadata.clone_only_token_map();
    std::unordered_multimap<range<token>, inet_address> range_addresses = strat.get_range_addresses(tm);
    std::unordered_multimap<inet_address, range<token>> source_ranges;

    // find alive sources for our new ranges
    for (auto r : ranges) {
        std::unordered_set<inet_address> possible_ranges;
        auto rg = range_addresses.equal_range(r);
        for (auto it = rg.first; it != rg.second; it++) {
            possible_ranges.emplace(it->second);
        }
        auto& snitch = locator::i_endpoint_snitch::get_local_snitch_ptr();
        std::vector<inet_address> sources = snitch->get_sorted_list_by_proximity(my_address, possible_ranges);

        if (std::find(sources.begin(), sources.end(), my_address) != sources.end()) {
            auto err = sprint("get_new_source_ranges: sources=%s, my_address=%s", sources, my_address);
            logger.warn(err.c_str());
            throw std::runtime_error(err);
        }


        for (auto& source : sources) {
            if (fd.is_alive(source)) {
                source_ranges.emplace(source, r);
                break;
            }
        }
    }
    return source_ranges;
}

std::pair<std::unordered_set<range<token>>, std::unordered_set<range<token>>>
storage_service::calculate_stream_and_fetch_ranges(const std::vector<range<token>>& current, const std::vector<range<token>>& updated) {
    std::unordered_set<range<token>> to_stream;
    std::unordered_set<range<token>> to_fetch;

    for (auto r1 : current) {
        bool intersect = false;
        for (auto r2 : updated) {
            if (r1.overlaps(r2, dht::token_comparator())) {
                // adding difference ranges to fetch from a ring
                for (auto r : r1.subtract(r2, dht::token_comparator())) {
                    to_stream.emplace(r);
                }
                intersect = true;
            }
        }
        if (!intersect) {
            to_stream.emplace(r1); // should seed whole old range
        }
    }

    for (auto r2 : updated) {
        bool intersect = false;
        for (auto r1 : current) {
            if (r2.overlaps(r1, dht::token_comparator())) {
                // adding difference ranges to fetch from a ring
                for (auto r : r2.subtract(r1, dht::token_comparator())) {
                    to_fetch.emplace(r);
                }
                intersect = true;
            }
        }
        if (!intersect) {
            to_fetch.emplace(r2); // should fetch whole old range
        }
    }

    if (logger.is_enabled(logging::log_level::debug)) {
        logger.debug("current   = {}", current);
        logger.debug("updated   = {}", updated);
        logger.debug("to_stream = {}", to_stream);
        logger.debug("to_fetch  = {}", to_fetch);
    }

    return std::pair<std::unordered_set<range<token>>, std::unordered_set<range<token>>>(to_stream, to_fetch);
}

void storage_service::range_relocator::calculate_to_from_streams(std::unordered_set<token> new_tokens, std::vector<sstring> keyspace_names) {
    auto& ss = get_local_storage_service();

    auto local_address = ss.get_broadcast_address();
    auto& snitch = locator::i_endpoint_snitch::get_local_snitch_ptr();

    auto token_meta_clone_all_settled = ss._token_metadata.clone_after_all_settled();
    // clone to avoid concurrent modification in calculateNaturalEndpoints
    auto token_meta_clone = ss._token_metadata.clone_only_token_map();

    for (auto keyspace : keyspace_names) {
        logger.debug("Calculating ranges to stream and request for keyspace {}", keyspace);
        for (auto new_token : new_tokens) {
            // replication strategy of the current keyspace (aka table)
            auto& ks = ss._db.local().find_keyspace(keyspace);
            auto& strategy = ks.get_replication_strategy();
            // getting collection of the currently used ranges by this keyspace
            std::vector<range<token>> current_ranges = ss.get_ranges_for_endpoint(keyspace, local_address);
            // collection of ranges which this node will serve after move to the new token
            std::vector<range<token>> updated_ranges = strategy.get_pending_address_ranges(token_meta_clone, new_token, local_address);

            // ring ranges and endpoints associated with them
            // this used to determine what nodes should we ping about range data
            std::unordered_multimap<range<token>, inet_address> range_addresses = strategy.get_range_addresses(token_meta_clone);
            std::unordered_map<range<token>, std::vector<inet_address>> range_addresses_map;
            for (auto& x : range_addresses) {
                range_addresses_map[x.first].emplace_back(x.second);
            }

            // calculated parts of the ranges to request/stream from/to nodes in the ring
            // std::pair(to_stream, to_fetch)
            std::pair<std::unordered_set<range<token>>, std::unordered_set<range<token>>> ranges_per_keyspace =
                ss.calculate_stream_and_fetch_ranges(current_ranges, updated_ranges);
            /**
             * In this loop we are going through all ranges "to fetch" and determining
             * nodes in the ring responsible for data we are interested in
             */
            std::unordered_multimap<range<token>, inet_address> ranges_to_fetch_with_preferred_endpoints;
            for (range<token> to_fetch : ranges_per_keyspace.second) {
                for (auto& x : range_addresses_map) {
                    const range<token>& r = x.first;
                    std::vector<inet_address>& eps = x.second;
                    if (r.contains(to_fetch, dht::token_comparator())) {
                        std::vector<inet_address> endpoints;
                        if (dht::range_streamer::use_strict_consistency()) {
                            auto end_token = to_fetch.end() ? to_fetch.end()->value() : dht::maximum_token();
                            std::vector<inet_address> old_endpoints = eps;
                            std::vector<inet_address> new_endpoints = strategy.calculate_natural_endpoints(end_token, token_meta_clone_all_settled);

                            //Due to CASSANDRA-5953 we can have a higher RF then we have endpoints.
                            //So we need to be careful to only be strict when endpoints == RF
                            if (old_endpoints.size() == strategy.get_replication_factor()) {
                                for (auto n : new_endpoints) {
                                    auto beg = old_endpoints.begin();
                                    auto end = old_endpoints.end();
                                    old_endpoints.erase(std::remove(beg, end, n), end);
                                }
                                //No relocation required
                                if (old_endpoints.empty()) {
                                    continue;
                                }

                                if (old_endpoints.size() != 1) {
                                    throw std::runtime_error(sprint("Expected 1 endpoint but found %d", old_endpoints.size()));
                                }
                            }
                            endpoints.emplace_back(old_endpoints.front());
                        } else {
                            std::unordered_set<inet_address> eps_set(eps.begin(), eps.end());
                            endpoints = snitch->get_sorted_list_by_proximity(local_address, eps_set);
                        }

                        // storing range and preferred endpoint set
                        for (auto ep : endpoints) {
                            ranges_to_fetch_with_preferred_endpoints.emplace(to_fetch, ep);
                        }
                    }
                }

                std::vector<inet_address> address_list;
                auto rg = ranges_to_fetch_with_preferred_endpoints.equal_range(to_fetch);
                for (auto it = rg.first; it != rg.second; it++) {
                    address_list.push_back(it->second);
                }

                if (address_list.empty()) {
                    continue;
                }

                if (dht::range_streamer::use_strict_consistency()) {
                    if (address_list.size() > 1) {
                        throw std::runtime_error(sprint("Multiple strict sources found for %s", to_fetch));
                    }

                    auto source_ip = address_list.front();
                    auto& gossiper = gms::get_local_gossiper();
                    auto state = gossiper.get_endpoint_state_for_endpoint(source_ip);
                    if (gossiper.is_enabled() && state && !state->is_alive())
                        throw std::runtime_error(sprint("A node required to move the data consistently is down (%s).  If you wish to move the data from a potentially inconsistent replica, restart the node with -Dcassandra.consistent.rangemovement=false", source_ip));
                }
            }
            // calculating endpoints to stream current ranges to if needed
            // in some situations node will handle current ranges as part of the new ranges
            std::unordered_multimap<inet_address, range<token>> endpoint_ranges;
            std::unordered_map<inet_address, std::vector<range<token>>> endpoint_ranges_map;
            for (range<token> to_stream : ranges_per_keyspace.first) {
                auto end_token = to_stream.end() ? to_stream.end()->value() : dht::maximum_token();
                std::vector<inet_address> current_endpoints = strategy.calculate_natural_endpoints(end_token, token_meta_clone);
                std::vector<inet_address> new_endpoints = strategy.calculate_natural_endpoints(end_token, token_meta_clone_all_settled);
                logger.debug("Range: {} Current endpoints: {} New endpoints: {}", to_stream, current_endpoints, new_endpoints);
                std::sort(current_endpoints.begin(), current_endpoints.end());
                std::sort(new_endpoints.begin(), new_endpoints.end());

                std::vector<inet_address> diff;
                std::set_difference(new_endpoints.begin(), new_endpoints.end(),
                        current_endpoints.begin(), current_endpoints.end(), std::back_inserter(diff));
                for (auto address : diff) {
                    logger.debug("Range {} has new owner {}", to_stream, address);
                    endpoint_ranges.emplace(address, to_stream);
                }
            }
            for (auto& x : endpoint_ranges) {
                endpoint_ranges_map[x.first].emplace_back(x.second);
            }

            // stream ranges
            for (auto& x : endpoint_ranges_map) {
                auto& address = x.first;
                auto& ranges = x.second;
                logger.debug("Will stream range {} of keyspace {} to endpoint {}", ranges , keyspace, address);
                _stream_plan.transfer_ranges(address, keyspace, ranges);
            }

            // stream requests
            std::unordered_multimap<inet_address, range<token>> work =
                dht::range_streamer::get_work_map(ranges_to_fetch_with_preferred_endpoints, keyspace);
            std::unordered_map<inet_address, std::vector<range<token>>> work_map;
            for (auto& x : work) {
                work_map[x.first].emplace_back(x.second);
            }

            for (auto& x : work_map) {
                auto& address = x.first;
                auto& ranges = x.second;
                logger.debug("Will request range {} of keyspace {} from endpoint {}", ranges, keyspace, address);
                _stream_plan.request_ranges(address, keyspace, ranges);
            }
            if (logger.is_enabled(logging::log_level::debug)) {
                for (auto& x : work) {
                    logger.debug("Keyspace {}: work map ep = {} --> range = {}", keyspace, x.first, x.second);
                }
            }
        }
    }
}

future<> storage_service::move(token new_token) {
    return run_with_api_lock(sstring("move"), [new_token] (storage_service& ss) mutable {
        return seastar::async([new_token, &ss] {
            auto tokens = ss._token_metadata.sorted_tokens();
            if (std::find(tokens.begin(), tokens.end(), new_token) != tokens.end()) {
                throw std::runtime_error(sprint("target token %s is already owned by another node.", new_token));
            }

            // address of the current node
            auto local_address = ss.get_broadcast_address();

            // This doesn't make any sense in a vnodes environment.
            if (ss.get_token_metadata().get_tokens(local_address).size() > 1) {
                logger.error("Invalid request to move(Token); This node has more than one token and cannot be moved thusly.");
                throw std::runtime_error("This node has more than one token and cannot be moved thusly.");
            }

            auto keyspaces_to_process = ss._db.local().get_non_system_keyspaces();

            ss.block_until_update_pending_ranges_finished().get();

            // checking if data is moving to this node
            for (auto keyspace_name : keyspaces_to_process) {
                if (ss._token_metadata.get_pending_ranges(keyspace_name, local_address).size() > 0) {
                    throw std::runtime_error("data is currently moving to this node; unable to leave the ring");
                }
            }

            gms::get_local_gossiper().add_local_application_state(application_state::STATUS, ss.value_factory.moving(new_token)).get();
            ss.set_mode(mode::MOVING, sprint("Moving %s from %s to %s.", local_address, *(ss.get_local_tokens().begin()), new_token), true);

            ss.set_mode(mode::MOVING, sprint("Sleeping %d ms before start streaming/fetching ranges", ss.get_ring_delay().count()), true);
            sleep(ss.get_ring_delay()).get();

            storage_service::range_relocator relocator(std::unordered_set<token>{new_token}, keyspaces_to_process);

            if (relocator.streams_needed()) {
                ss.set_mode(mode::MOVING, "fetching new ranges and streaming old ranges", true);
                try {
                    relocator.stream().get();
                } catch (...) {
                    throw std::runtime_error(sprint("Interrupted while waiting for stream/fetch ranges to finish: %s", std::current_exception()));
                }
            } else {
                ss.set_mode(mode::MOVING, "No ranges to fetch/stream", true);
            }

            ss.set_tokens(std::unordered_set<token>{new_token}); // setting new token as we have everything settled

            logger.debug("Successfully moved to new token {}", *(ss.get_local_tokens().begin()));
        });
    });
}

std::map<token, inet_address> storage_service::get_token_to_endpoint_map() {
    return _token_metadata.get_normal_and_bootstrapping_token_to_endpoint_map();
}

std::chrono::milliseconds storage_service::get_ring_delay() {
    auto ring_delay = _db.local().get_config().ring_delay_ms();
    logger.trace("Set RING_DELAY to {}ms", ring_delay);
    return std::chrono::milliseconds(ring_delay);
}

void storage_service::do_update_pending_ranges() {
    if (engine().cpu_id() != 0) {
        throw std::runtime_error("do_update_pending_ranges should be called on cpu zero");
    }
    // long start = System.currentTimeMillis();
    auto keyspaces = _db.local().get_non_system_keyspaces();
    for (auto& keyspace_name : keyspaces) {
        auto& ks = _db.local().find_keyspace(keyspace_name);
        auto& strategy = ks.get_replication_strategy();
        get_local_storage_service().get_token_metadata().calculate_pending_ranges(strategy, keyspace_name);
    }
    // logger.debug("finished calculation for {} keyspaces in {}ms", keyspaces.size(), System.currentTimeMillis() - start);
}

future<> storage_service::update_pending_ranges() {
    return get_storage_service().invoke_on(0, [] (auto& ss){
        ss._update_jobs++;
        ss.do_update_pending_ranges();
        // calculate_pending_ranges will modify token_metadata, we need to repliate to other cores
        return ss.replicate_to_all_cores().finally([&ss, ss0 = ss.shared_from_this()] {
            ss._update_jobs--;
        });
    });
}

future<> storage_service::block_until_update_pending_ranges_finished() {
    // We want to be sure the job we're blocking for is actually finished and we can't trust the TPE's active job count
    return smp::submit_to(0, [] {
        return do_until(
            [] { return !(get_local_storage_service()._update_jobs > 0); },
            [] { return sleep(std::chrono::milliseconds(100)); });
    });
}

} // namespace service

