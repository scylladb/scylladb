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
#include "service/pending_range_calculator_service.hh"
#include "streaming/stream_plan.hh"
#include "streaming/stream_state.hh"
#include "dht/range_streamer.hh"
#include <boost/range/adaptors.hpp>
#include <boost/range/algorithm.hpp>

using token = dht::token;
using UUID = utils::UUID;
using inet_address = gms::inet_address;

namespace service {

static logging::logger logger("storage_service");

int storage_service::RING_DELAY = storage_service::get_ring_delay();

distributed<storage_service> _the_storage_service;

static int get_generation_number() {
    using namespace std::chrono;
    auto now = high_resolution_clock::now().time_since_epoch();
    int generation_number = duration_cast<seconds>(now).count();
    return generation_number;
}

bool is_replacing() {
    // FIXME: DatabaseDescriptor.isReplacing()
    return false;
}

bool storage_service::is_auto_bootstrap() {
    return _db.local().get_config().auto_bootstrap();
}

std::set<inet_address> get_seeds() {
    // FIXME: DatabaseDescriptor.getSeeds()
    auto& gossiper = gms::get_local_gossiper();
    return gossiper.get_seeds();
}

std::set<inet_address> get_replace_tokens() {
    // FIXME: DatabaseDescriptor.getReplaceTokens()
    return {};
}

std::experimental::optional<UUID> get_replace_node() {
    // FIXME: DatabaseDescriptor.getReplaceNode()
    return {};
}

std::experimental::optional<inet_address> get_replace_address() {
    // FIXME: DatabaseDescriptor.getReplaceAddress()
    return {};
}

std::unordered_set<sstring> get_initial_tokens() {
    // FIXME: DatabaseDescriptor.getInitialTokens();
    return std::unordered_set<sstring>();
}

bool get_property_join_ring() {
    // FIXME: Boolean.parseBoolean(System.getProperty("cassandra.join_ring", "true")))
    return true;
}

bool get_property_rangemovement() {
    // FIXME: Boolean.parseBoolean(System.getProperty("cassandra.consistent.rangemovement", "true")
    return true;
}

bool get_property_load_ring_state() {
    // FIXME: Boolean.parseBoolean(System.getProperty("cassandra.load_ring_state", "true"))
    return true;
}

bool storage_service::should_bootstrap() {
    return is_auto_bootstrap() && !db::system_keyspace::bootstrap_complete() && !get_seeds().count(get_broadcast_address());
}

future<> storage_service::prepare_to_join() {
    if (_joined) {
        return make_ready_future<>();
    }

    std::map<gms::application_state, gms::versioned_value> app_states;
    auto f = make_ready_future<>();
    if (is_replacing() && !get_property_join_ring()) {
        throw std::runtime_error("Cannot set both join_ring=false and attempt to replace a node");
    }
    if (get_replace_tokens().size() > 0 || get_replace_node()) {
         throw std::runtime_error("Replace method removed; use cassandra.replace_address instead");
    }
    if (is_replacing()) {
        if (db::system_keyspace::bootstrap_complete()) {
            throw std::runtime_error("Cannot replace address with a node that is already bootstrapped");
        }
        if (!is_auto_bootstrap()) {
            throw std::runtime_error("Trying to replace_address with auto_bootstrap disabled will not work, check your configuration");
        }
        _bootstrap_tokens = prepare_replacement_info();
        app_states.emplace(gms::application_state::TOKENS, value_factory.tokens(_bootstrap_tokens));
        app_states.emplace(gms::application_state::STATUS, value_factory.hibernate(true));
    } else if (should_bootstrap()) {
        f = check_for_endpoint_collision();
    }

    // have to start the gossip service before we can see any info on other nodes.  this is necessary
    // for bootstrap to get the load info it needs.
    // (we won't be part of the storage ring though until we add a counterId to our state, below.)
    // Seed the host ID-to-endpoint map with our own ID.
    return f.then([app_states = std::move(app_states)] {
        return db::system_keyspace::get_local_host_id();
    }).then([this, app_states = std::move(app_states)] (auto local_host_id) mutable {
        _token_metadata.update_host_id(local_host_id, this->get_broadcast_address());
        // FIXME: DatabaseDescriptor.getBroadcastRpcAddress()
        auto broadcast_rpc_address = this->get_broadcast_address();
        app_states.emplace(gms::application_state::NET_VERSION, value_factory.network_version());
        app_states.emplace(gms::application_state::HOST_ID, value_factory.host_id(local_host_id));
        app_states.emplace(gms::application_state::RPC_ADDRESS, value_factory.rpcaddress(broadcast_rpc_address));
        app_states.emplace(gms::application_state::RELEASE_VERSION, value_factory.release_version());
        logger.info("Starting up server gossip");

        auto& gossiper = gms::get_local_gossiper();
        gossiper.register_(this);
        // FIXME: SystemKeyspace.incrementAndGetGeneration()
        print("Start gossiper service ...\n");
        return gossiper.start(get_generation_number(), app_states).then([this] {
#if SS_DEBUG
            gms::get_local_gossiper().debug_show();
            _token_metadata.debug_show();
#endif
        });
    }).then([this] {
        // gossip snitch infos (local DC and rack)
        gossip_snitch_info();
        auto& proxy = service::get_storage_proxy();
        // gossip Schema.emptyVersion forcing immediate check for schema updates (see MigrationManager#maybeScheduleSchemaPull)
        return update_schema_version_and_announce(proxy); // Ensure we know our own actual Schema UUID in preparation for updates

#if 0
        if (!MessagingService.instance().isListening())
            MessagingService.instance().listen(FBUtilities.getLocalAddress());
        LoadBroadcaster.instance.startBroadcasting();

        HintedHandOffManager.instance.start();
        BatchlogManager.instance.start();
#endif
    });
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
#if 0
        // if our schema hasn't matched yet, keep sleeping until it does
        // (post CASSANDRA-1391 we don't expect this to be necessary very often, but it doesn't hurt to be careful)
        while (!MigrationManager.isReadyForBootstrap())
        {
            set_mode(mode::JOINING, "waiting for schema information to complete", true);
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        }
#endif
        set_mode(mode::JOINING, "schema complete, ready to bootstrap", true);
        set_mode(mode::JOINING, "waiting for pending range calculation", true);
        get_local_pending_range_calculator_service().block_until_finished().get();
        set_mode(mode::JOINING, "calculation complete, ready to bootstrap", true);
        logger.debug("... got ring + schema info");
#if 0
        if (Boolean.parseBoolean(System.getProperty("cassandra.consistent.rangemovement", "true")) &&
                (
                    _token_metadata.getBootstrapTokens().valueSet().size() > 0 ||
                    _token_metadata.getLeavingEndpoints().size() > 0 ||
                    _token_metadata.getMovingEndpoints().size() > 0
                ))
            throw new UnsupportedOperationException("Other bootstrapping/leaving/moving nodes detected, cannot bootstrap while cassandra.consistent.rangemovement is true");
#endif

        if (!is_replacing()) {
            if (_token_metadata.is_member(get_broadcast_address())) {
                throw std::runtime_error("This node is already a member of the token ring; bootstrap aborted. (If replacing a dead node, remove the old one from the ring first.)");
            }
            set_mode(mode::JOINING, "getting bootstrap token", true);
            _bootstrap_tokens = boot_strapper::get_bootstrap_tokens(_token_metadata, _db.local());
        } else {
            auto replace_addr = get_replace_address();
            if (replace_addr && *replace_addr != get_broadcast_address()) {
                // Sleep additionally to make sure that the server actually is not alive
                // and giving it more time to gossip if alive.
                // FIXME: LoadBroadcaster.BROADCAST_INTERVAL
                std::chrono::milliseconds broadcast_interval(60 * 1000);
                sleep(broadcast_interval).get();

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
                sleep(std::chrono::milliseconds(RING_DELAY)).get();
            }
            std::stringstream ss;
            ss << _bootstrap_tokens;
            set_mode(mode::JOINING, sprint("Replacing a node with token(s): %s", ss.str()), true);
        }
        bootstrap(_bootstrap_tokens);
        assert(!_is_bootstrap_mode); // bootstrap will block until finished
    } else {
        size_t num_tokens = _db.local().get_config().num_tokens();
        _bootstrap_tokens = db::system_keyspace::get_saved_tokens().get0();
        if (_bootstrap_tokens.empty()) {
            auto initial_tokens = get_initial_tokens();
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
        assert(_token_metadata.sorted_tokens().size() > 0);
        //Auth.setup();
    } else {
        logger.info("Startup complete, but write survey mode is active, not becoming an active ring member. Use JMX (StorageService->joinRing()) to finalize ring joining.");
    }
}

future<> storage_service::join_ring() {
    return seastar::async([this] {
        if (!_joined) {
            logger.info("Joining ring by operator request");
            join_token_ring(0);
        } else if (_is_survey_mode) {
            auto tokens = db::system_keyspace::get_saved_tokens().get0();
            set_tokens(std::move(tokens));
            db::system_keyspace::set_bootstrap_state(db::system_keyspace::bootstrap_state::COMPLETED).get();
            _is_survey_mode = false;
            logger.info("Leaving write survey mode and joining ring at operator request");
            assert(_token_metadata.sorted_tokens().size() > 0);
            //Auth.setup();
        }
    });
}

// Runs inside seastar::async context
void storage_service::bootstrap(std::unordered_set<token> tokens) {
    _is_bootstrap_mode = true;
    // DON'T use set_token, that makes us part of the ring locally which is incorrect until we are done bootstrapping
    db::system_keyspace::update_tokens(tokens).get();
    auto& gossiper = gms::get_local_gossiper();
    if (!is_replacing()) {
        // if not an existing token then bootstrap
        gossiper.add_local_application_state(gms::application_state::TOKENS, value_factory.tokens(tokens));
        gossiper.add_local_application_state(gms::application_state::STATUS, value_factory.bootstrapping(tokens));
        set_mode(mode::JOINING, sprint("sleeping %s ms for pending range setup", RING_DELAY), true);
        sleep(std::chrono::milliseconds(RING_DELAY)).get();
    } else {
        // Dont set any state for the node which is bootstrapping the existing token...
        _token_metadata.update_normal_tokens(tokens, get_broadcast_address());
        auto replace_addr = get_replace_address();
        if (replace_addr) {
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
    logger.debug("handle_state_bootstrap endpoint={}", endpoint);
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
    get_local_pending_range_calculator_service().update().get();

    auto& gossiper = gms::get_local_gossiper();
    if (gossiper.uses_host_id(endpoint)) {
        _token_metadata.update_host_id(gossiper.get_host_id(endpoint), endpoint);
    }
}

void storage_service::handle_state_normal(inet_address endpoint) {
    logger.debug("handle_state_normal endpoint={}", endpoint);
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
        if (is_replacing() &&
            get_replace_address() &&
            gossiper.get_endpoint_state_for_endpoint(get_replace_address().value())  &&
            (host_id == gossiper.get_host_id(get_replace_address().value()))) {
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
            logger.debug("New node {} at token {}", endpoint, t);
            tokens_to_update_in_metadata.insert(t);
            tokens_to_update_in_system_keyspace.insert(t);
        } else if (endpoint == *current_owner) {
            logger.debug("endpoint={} == current_owner={}", endpoint, *current_owner);
            // set state back to normal, since the node may have tried to leave, but failed and is now back up
            tokens_to_update_in_metadata.insert(t);
            tokens_to_update_in_system_keyspace.insert(t);
        } else if (gossiper.compare_endpoint_startup(endpoint, *current_owner) > 0) {
            logger.debug("compare_endpoint_startup: endpoint={} > current_owner={}", endpoint, *current_owner);
            tokens_to_update_in_metadata.insert(t);
            tokens_to_update_in_system_keyspace.insert(t);
#if 0

            // currentOwner is no longer current, endpoint is.  Keep track of these moves, because when
            // a host no longer has any tokens, we'll want to remove it.
            Multimap<InetAddress, Token> epToTokenCopy = getTokenMetadata().getEndpointToTokenMapForReading();
            epToTokenCopy.get(currentOwner).remove(token);
            if (epToTokenCopy.get(currentOwner).size() < 1)
                endpointsToRemove.add(currentOwner);

#endif
            logger.info("Nodes {} and {} have the same token {}. {} is the new owner", endpoint, *current_owner, t, endpoint);
        } else {
            logger.info("Nodes {} and {} have the same token {}. Ignoring {}", endpoint, *current_owner, t, endpoint);
        }
    }

    bool is_moving = _token_metadata.is_moving(endpoint); // capture because updateNormalTokens clears moving status
    _token_metadata.update_normal_tokens(tokens_to_update_in_metadata, endpoint);
    for (auto ep : endpoints_to_remove) {
        remove_endpoint(ep);
        auto replace_addr = get_replace_address();
        if (is_replacing() && replace_addr && *replace_addr == ep) {
            gossiper.replacement_quarantine(ep); // quarantine locally longer than normally; see CASSANDRA-8260
        }
    }
    logger.debug("ep={} tokens_to_update_in_system_keyspace = {}", endpoint, tokens_to_update_in_system_keyspace);
    if (!tokens_to_update_in_system_keyspace.empty()) {
        db::system_keyspace::update_tokens(endpoint, tokens_to_update_in_system_keyspace).then_wrapped([endpoint] (auto&& f) {
            try {
                f.get();
            } catch (...) {
                logger.error("fail to update tokens for {}: {}", endpoint, std::current_exception());
            }
            return make_ready_future<>();
        }).get();
    }
    if (!local_tokens_to_remove.empty()) {
        db::system_keyspace::update_local_tokens(std::unordered_set<dht::token>(), local_tokens_to_remove).discard_result().get();
    }

    if (is_moving) {
        _token_metadata.remove_from_moving(endpoint);
        get_storage_service().invoke_on_all([endpoint] (auto&& ss) {
            for (auto&& subscriber : ss._lifecycle_subscribers) {
                subscriber->on_move(endpoint);
            }
        }).get();
    } else {
        get_storage_service().invoke_on_all([endpoint] (auto&& ss) {
            for (auto&& subscriber : ss._lifecycle_subscribers) {
                subscriber->on_join_cluster(endpoint);
            }
        }).get();
    }

    get_local_pending_range_calculator_service().update().get();
    if (logger.is_enabled(logging::log_level::debug)) {
        auto ver = _token_metadata.get_ring_version();
        for (auto& x : _token_metadata.get_token_to_endpoint()) {
            logger.debug("token_metadata.ring_version={}, token={} -> endpoint={}", ver, x.first, x.second);
        }
    }
}

void storage_service::handle_state_leaving(inet_address endpoint) {
    logger.debug("handle_state_leaving endpoint={}", endpoint);

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
        if (!std::includes(tokens_.begin(), tokens_.end(), tokens.begin(), tokens.end())) {
            logger.warn("Node {} 'leaving' token mismatch. Long network partition?", endpoint);
            _token_metadata.update_normal_tokens(tokens, endpoint);
        }
    }

    // at this point the endpoint is certainly a member with this token, so let's proceed
    // normally
    _token_metadata.add_leaving_endpoint(endpoint);
    get_local_pending_range_calculator_service().update().get();
}

void storage_service::handle_state_left(inet_address endpoint, std::vector<sstring> pieces) {
    logger.debug("handle_state_left endpoint={}", endpoint);
    assert(pieces.size() >= 2);
    auto tokens = get_tokens_for(endpoint);
    logger.debug("Node {} state left, tokens {}", endpoint, tokens);
#if 0
     excise(tokens, endpoint, extractExpireTime(pieces));
#endif
}

void storage_service::handle_state_moving(inet_address endpoint, std::vector<sstring> pieces) {
    logger.debug("handle_state_moving endpoint={}", endpoint);
    assert(pieces.size() >= 2);
    auto token = dht::global_partitioner().from_sstring(pieces[1]);
    logger.debug("Node {} state moving, new token {}", endpoint, token);
#if 0
    _token_metadata.addMovingEndpoint(token, endpoint);
#endif
    get_local_pending_range_calculator_service().update().get();
}

void storage_service::handle_state_removing(inet_address endpoint, std::vector<sstring> pieces) {
    logger.debug("handle_state_removing endpoint={}", endpoint);
    assert(pieces.size() > 0);
    if (endpoint == get_broadcast_address()) {
        logger.info("Received removenode gossip about myself. Is this node rejoining after an explicit removenode?");
        try {
            // drain();
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
            // excise(removeTokens, endpoint, extractExpireTime(pieces));
        } else if (sstring(gms::versioned_value::REMOVING_TOKEN) == state) {
            auto& gossiper = gms::get_local_gossiper();
            logger.debug("Tokens {} removed manually (endpoint was {})", remove_tokens, endpoint);
            // Note that the endpoint is being removed
            _token_metadata.add_leaving_endpoint(endpoint);
            get_local_pending_range_calculator_service().update().get();
            // find the endpoint coordinating this removal that we need to notify when we're done
            auto state = gossiper.get_endpoint_state_for_endpoint(endpoint);
            assert(state);
            auto value = state->get_application_state(application_state::REMOVAL_COORDINATOR);
            assert(value);
            std::vector<sstring> coordinator;
            boost::split(coordinator, value->value, boost::is_any_of(sstring(versioned_value::DELIMITER_STR)));
            assert(coordinator.size() == 2);
            UUID host_id(coordinator[1]);
            // grab any data we are now responsible for and notify responsible node
            auto ep = _token_metadata.get_endpoint_for_host_id(host_id);
            assert(ep);
            restore_replica_count(endpoint, ep.value()).get();
        }
    } else { // now that the gossiper has told us about this nonexistent member, notify the gossiper to remove it
        if (sstring(gms::versioned_value::REMOVED_TOKEN) == pieces[0]) {
            // addExpireTimeIfFound(endpoint, extractExpireTime(pieces));
        }
        remove_endpoint(endpoint);
    }
}

void storage_service::on_join(gms::inet_address endpoint, gms::endpoint_state ep_state) {
    logger.debug("on_join endpoint={}", endpoint);
    for (auto e : ep_state.get_application_state_map()) {
        on_change(endpoint, e.first, e.second);
    }
    get_local_migration_manager().schedule_schema_pull(endpoint, ep_state).handle_exception([endpoint] (auto ep) {
        logger.warn("Fail to pull schmea from {}: {}", endpoint, ep);
    });
}

void storage_service::on_alive(gms::inet_address endpoint, gms::endpoint_state state) {
    logger.debug("on_alive endpoint={}", endpoint);
    get_local_migration_manager().schedule_schema_pull(endpoint, state).handle_exception([endpoint] (auto ep) {
        logger.warn("Fail to pull schmea from {}: {}", endpoint, ep);
    });
    if (_token_metadata.is_member(endpoint)) {
#if 0
        HintedHandOffManager.instance.scheduleHintDelivery(endpoint, true);
#endif
        get_storage_service().invoke_on_all([endpoint] (auto&& ss) {
            for (auto&& subscriber : ss._lifecycle_subscribers) {
                subscriber->on_up(endpoint);
            }
        });
    }
}

void storage_service::before_change(gms::inet_address endpoint, gms::endpoint_state current_state, gms::application_state new_state_key, gms::versioned_value new_value) {
    logger.debug("before_change endpoint={}", endpoint);
}

void storage_service::on_change(inet_address endpoint, application_state state, versioned_value value) {
    logger.debug("on_change endpoint={}", endpoint);
    if (state == application_state::STATUS) {
        std::vector<sstring> pieces;
        boost::split(pieces, value.value, boost::is_any_of(sstring(versioned_value::DELIMITER_STR)));
        assert(pieces.size() > 0);
        sstring move_name = pieces[0];
        if (move_name == sstring(versioned_value::STATUS_BOOTSTRAPPING)) {
            handle_state_bootstrap(endpoint);
        } else if (move_name == sstring(versioned_value::STATUS_NORMAL)) {
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
        do_update_system_peers_table(endpoint, state, value);
        if (state == application_state::SCHEMA) {
            get_local_migration_manager().schedule_schema_pull(endpoint, *ep_state).handle_exception([endpoint] (auto ep) {
                logger.warn("Fail to pull schmea from {}: {}", endpoint, ep);
            });
        }
    }
    replicate_to_all_cores().get();
}


void storage_service::on_remove(gms::inet_address endpoint) {
    logger.debug("on_remove endpoint={}", endpoint);
    _token_metadata.remove_endpoint(endpoint);
    get_local_pending_range_calculator_service().update().get();
}

void storage_service::on_dead(gms::inet_address endpoint, gms::endpoint_state state) {
    logger.debug("on_restart endpoint={}", endpoint);
#if 0
    MessagingService.instance().convict(endpoint);
#endif
    get_storage_service().invoke_on_all([endpoint] (auto&& ss) {
        for (auto&& subscriber : ss._lifecycle_subscribers) {
            subscriber->on_down(endpoint);
        }
    }).get();
}

void storage_service::on_restart(gms::inet_address endpoint, gms::endpoint_state state) {
    logger.debug("on_restart endpoint={}", endpoint);
#if 0
    // If we have restarted before the node was even marked down, we need to reset the connection pool
    if (state.isAlive())
        onDead(endpoint, state);
#endif
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
    logger.debug("Update ep={}, state={}, value={}", endpoint, int(state), value.value);
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
    logger.debug("endpoint={}, tokens_string={}", endpoint, tokens_string);
    std::vector<sstring> tokens;
    std::unordered_set<token> ret;
    boost::split(tokens, tokens_string, boost::is_any_of(";"));
    for (auto str : tokens) {
        logger.debug("token={}", str);
        sstring_view sv(str);
        bytes b = from_hex(sv);
        ret.emplace(token::kind::key, b);
    }
    return ret;
}

// Runs inside seastar::async context
void storage_service::set_tokens(std::unordered_set<token> tokens) {
    logger.debug("Setting tokens to {}", tokens);
    db::system_keyspace::update_tokens(tokens).get();
    _token_metadata.update_normal_tokens(tokens, get_broadcast_address());
    // Collection<Token> localTokens = getLocalTokens();
    auto local_tokens = _bootstrap_tokens;
    auto& gossiper = gms::get_local_gossiper();
    gossiper.add_local_application_state(gms::application_state::TOKENS, value_factory.tokens(local_tokens));
    gossiper.add_local_application_state(gms::application_state::STATUS, value_factory.normal(local_tokens));
    set_mode(mode::NORMAL, false);
    replicate_to_all_cores().get();
}

void storage_service::register_subscriber(endpoint_lifecycle_subscriber* subscriber)
{
    _lifecycle_subscribers.emplace_back(subscriber);
}

void storage_service::unregister_subscriber(endpoint_lifecycle_subscriber* subscriber)
{
    _lifecycle_subscribers.erase(std::remove(_lifecycle_subscribers.begin(), _lifecycle_subscribers.end(), subscriber), _lifecycle_subscribers.end());
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
                logger.debug("Loaded tokens: ep={}, tokens={}", x.first, x.second);
            }

            for (auto& x : loaded_host_ids) {
                logger.debug("Loaded host_id: ep={}, uuid={}", x.first, x.second);
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
        prepare_to_join().get();
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
                gossiper.add_local_application_state(gms::application_state::TOKENS, value_factory.tokens(tokens));
                gossiper.add_local_application_state(gms::application_state::STATUS, value_factory.hibernate(true));
            }
            logger.info("Not joining ring as requested. Use JMX (StorageService->joinRing()) to initiate ring joining");
        }
    });
}

future<> storage_service::replicate_to_all_cores() {
    assert(engine().cpu_id() == 0);
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

void storage_service::gossip_snitch_info() {
    auto& snitch = locator::i_endpoint_snitch::get_local_snitch_ptr();
    auto addr = get_broadcast_address();
    auto dc = snitch->get_datacenter(addr);
    auto rack = snitch->get_rack(addr);
    auto& gossiper = gms::get_local_gossiper();
    gossiper.add_local_application_state(gms::application_state::DC, value_factory.datacenter(dc));
    gossiper.add_local_application_state(gms::application_state::RACK, value_factory.rack(rack));
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
    auto& gossiper = gms::get_local_gossiper();
    return gossiper.do_shadow_round().then([this, &gossiper] {
        auto addr = get_broadcast_address();
        auto eps = gossiper.get_endpoint_state_for_endpoint(addr);
        if (eps && !gossiper.is_dead_state(*eps) && !gossiper.is_gossip_only_member(addr)) {
            throw std::runtime_error(sprint("A node with address %s already exists, cancelling join. "
                "Use cassandra.replace_address if you want to replace this node.", addr));
        }
#if 0
        if (RangeStreamer.useStrictConsistency)
        {
            for (Map.Entry<InetAddress, EndpointState> entry : Gossiper.instance.getEndpointStates())
            {

                if (entry.getValue().getApplicationState(ApplicationState.STATUS) == null)
                        continue;
                String[] pieces = entry.getValue().getApplicationState(ApplicationState.STATUS).value.split(VersionedValue.DELIMITER_STR, -1);
                assert (pieces.length > 0);
                String state = pieces[0];
                if (state.equals(VersionedValue.STATUS_BOOTSTRAPPING) || state.equals(VersionedValue.STATUS_LEAVING) || state.equals(VersionedValue.STATUS_MOVING))
                    throw new UnsupportedOperationException("Other bootstrapping/leaving/moving nodes detected, cannot bootstrap while cassandra.consistent.rangemovement is true");
            }
        }
#endif
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

std::unordered_set<token> storage_service::prepare_replacement_info() {
    return std::unordered_set<token>();
#if 0
    logger.info("Gathering node replacement information for {}", DatabaseDescriptor.getReplaceAddress());
    if (!MessagingService.instance().isListening())
        MessagingService.instance().listen(FBUtilities.getLocalAddress());

    // make magic happen
    Gossiper.instance.doShadowRound();

    UUID hostId = null;
    // now that we've gossiped at least once, we should be able to find the node we're replacing
    if (Gossiper.instance.getEndpointStateForEndpoint(DatabaseDescriptor.getReplaceAddress())== null)
        throw new RuntimeException("Cannot replace_address " + DatabaseDescriptor.getReplaceAddress() + " because it doesn't exist in gossip");
    hostId = Gossiper.instance.getHostId(DatabaseDescriptor.getReplaceAddress());
    try
    {
        if (Gossiper.instance.getEndpointStateForEndpoint(DatabaseDescriptor.getReplaceAddress()).getApplicationState(ApplicationState.TOKENS) == null)
            throw new RuntimeException("Could not find tokens for " + DatabaseDescriptor.getReplaceAddress() + " to replace");
        Collection<Token> tokens = TokenSerializer.deserialize(getPartitioner(), new DataInputStream(new ByteArrayInputStream(get_application_state_value(DatabaseDescriptor.getReplaceAddress(), ApplicationState.TOKENS))));

        SystemKeyspace.setLocalHostId(hostId); // use the replacee's host Id as our own so we receive hints, etc
        Gossiper.instance.resetEndpointStateMap(); // clean up since we have what we need
        return tokens;
    }
    catch (IOException e)
    {
        throw new RuntimeException(e);
    }
#endif
}

std::map<gms::inet_address, float> storage_service::get_ownership() const {
    auto token_map = dht::global_partitioner().describe_ownership(_token_metadata.sorted_tokens());

    // describeOwnership returns tokens in an unspecified order, let's re-order them
    std::map<gms::inet_address, float> node_map;
    for (auto entry : token_map) {
        gms::inet_address endpoint = _token_metadata.get_endpoint(entry.first).value();
        auto token_ownership = entry.second;
        node_map[endpoint] += token_ownership;
    }
    return node_map;
}

std::map<gms::inet_address, float> storage_service::effective_ownership(sstring name) const {
    if (name != "") {
        //find throws no such keyspace if it is missing
        const keyspace& ks = _db.local().find_keyspace(name);
        // This is ugly, but it follows origin
        if (typeid(ks.get_replication_strategy()) == typeid(locator::local_strategy)) {
            throw std::runtime_error("Ownership values for keyspaces with LocalStrategy are meaningless");
        }
    } else {
        auto non_system_keyspaces = _db.local().get_non_system_keyspaces();

        //system_traces is a non-system keyspace however it needs to be counted as one for this process
        size_t special_table_count = 0;
        if (std::find(non_system_keyspaces.begin(), non_system_keyspaces.end(), "system_traces") !=
                non_system_keyspaces.end()) {
            special_table_count += 1;
        }
        if (non_system_keyspaces.size() > special_table_count) {
            throw std::runtime_error("Non-system keyspaces don't have the same replication settings, effective ownership information is meaningless");
        }
        name = "system_traces";
    }
    auto token_ownership = dht::global_partitioner().describe_ownership(_token_metadata.sorted_tokens());

    std::map<gms::inet_address, float> final_ownership;

    // calculate ownership per dc
    for (auto endpoints : _token_metadata.get_topology().get_datacenter_endpoints()) {
        // calculate the ownership with replication and add the endpoint to the final ownership map
        for (const gms::inet_address& endpoint : endpoints.second) {
            float ownership = 0.0f;
            for (range<token> r : get_ranges_for_endpoint(name, endpoint)) {
                if (token_ownership.find(r.end().value().value()) != token_ownership.end()) {
                    ownership += token_ownership[r.end().value().value()];
                }
            }
            final_ownership[endpoint] = ownership;
        }
    }
    return final_ownership;
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
    assert(!tokens.empty()); // should not be called before initServer sets this
    return tokens;
}

sstring storage_service::get_release_version() {
    return version::release();
}

sstring storage_service::get_schema_version() {
    return _db.local().get_version().to_sstring();
}

future<sstring> storage_service::get_operation_mode() {
    return smp::submit_to(0, [] {
        auto mode = get_local_storage_service()._operation_mode;
        return make_ready_future<sstring>(sprint("%s", mode));
    });
}

future<bool> storage_service::is_starting() {
    return smp::submit_to(0, [] {
        auto mode = get_local_storage_service()._operation_mode;
        return mode == storage_service::mode::STARTING;
    });
}

future<bool> storage_service::is_gossip_running() {
    return smp::submit_to(0, [] {
        return gms::get_local_gossiper().is_enabled();
    });
}

future<> storage_service::start_gossiping() {
    return smp::submit_to(0, [] {
        auto ss = get_local_storage_service().shared_from_this();
        if (!ss->_initialized) {
            logger.warn("Starting gossip by operator request");
            return gms::get_local_gossiper().start(get_generation_number()).then([ss] () mutable {
                ss->_initialized = true;
            });
        }
        return make_ready_future<>();
    });
}

future<> storage_service::stop_gossiping() {
    return smp::submit_to(0, [this] {
        auto ss = get_local_storage_service().shared_from_this();
        if (ss->_initialized) {
            logger.warn("Stopping gossip by operator request");
            return gms::get_local_gossiper().stop().then([ss] {
                ss->_initialized = false;
            });
        }
        return make_ready_future<>();
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

// For the filesystem operations, this code will assume that all keyspaces are visible in all shards
// (as we have been doing for a lot of the other operations, like the snapshot itself).
future<> storage_service::clear_snapshot(sstring tag, std::vector<sstring> keyspace_names) {
    std::vector<std::reference_wrapper<keyspace>> keyspaces;
    for (auto& ksname: keyspace_names) {
        try {
            keyspaces.push_back(std::reference_wrapper<keyspace>(_db.local().find_keyspace(ksname)));
        } catch (no_such_keyspace& e) {
            return make_exception_future(std::current_exception());
        }
    }

    auto deleted_keyspaces = make_lw_shared<std::vector<sstring>>();
    return parallel_for_each(keyspaces, [this, tag, deleted_keyspaces] (auto& ks) {
        return parallel_for_each(ks.get().metadata()->cf_meta_data(), [this, tag] (auto& pair) {
            auto& cf = _db.local().find_column_family(pair.second);
            return cf.clear_snapshot(tag);
         });
    });
    logger.debug("Cleared out snapshot directories");
}


future<> storage_service::start_rpc_server() {
    fail(unimplemented::cause::STORAGE_SERVICE);
#if 0
    if (daemon == null)
    {
        throw new IllegalStateException("No configured daemon");
    }
    daemon.thriftServer.start();
#endif
    return make_ready_future<>();
}

future<> storage_service::stop_rpc_server() {
    fail(unimplemented::cause::STORAGE_SERVICE);
#if 0
    if (daemon == null)
    {
        throw new IllegalStateException("No configured daemon");
    }
    if (daemon.thriftServer != null)
        daemon.thriftServer.stop();
#endif
    return make_ready_future<>();
}

bool storage_service::is_rpc_server_running() {
#if 0
    if ((daemon == null) || (daemon.thriftServer == null))
    {
        return false;
    }
    return daemon.thriftServer.isRunning();
#endif
    //FIXME
    // We assude the rpc server is running.
    // it will be changed when the API to start and stop
    // it will be added.
    return true;
}

future<> storage_service::start_native_transport() {
    fail(unimplemented::cause::STORAGE_SERVICE);
#if 0
    if (daemon == null)
    {
        throw new IllegalStateException("No configured daemon");
    }

    try
    {
        daemon.nativeServer.start();
    }
    catch (Exception e)
    {
        throw new RuntimeException("Error starting native transport: " + e.getMessage());
    }
#endif
    return make_ready_future<>();
}

future<> storage_service::stop_native_transport() {
    fail(unimplemented::cause::STORAGE_SERVICE);
#if 0
    if (daemon == null)
    {
        throw new IllegalStateException("No configured daemon");
    }
    if (daemon.nativeServer != null)
        daemon.nativeServer.stop();
#endif
    return make_ready_future<>();
}

bool storage_service::is_native_transport_running() {
#if 0
    if ((daemon == null) || (daemon.nativeServer == null))
    {
        return false;
    }
    return daemon.nativeServer.isRunning();
#endif
    // FIXME
    // We assume the native transport is running
    // It will be change when the API to start and stop
    // will be added.
    return true;
}

future<> storage_service::decommission() {
    fail(unimplemented::cause::STORAGE_SERVICE);
    return seastar::async([this] {
        if (!_token_metadata.is_member(get_broadcast_address())) {
            throw std::runtime_error("local node is not a member of the token ring yet");
        }

        if (_token_metadata.clone_after_all_left().sorted_tokens().size() < 2) {
            throw std::runtime_error("no other normal nodes in the ring; decommission would be pointless");
        }

        get_local_pending_range_calculator_service().block_until_finished().get();

        auto non_system_keyspaces = _db.local().get_non_system_keyspaces();
        for (const auto& keyspace_name : non_system_keyspaces) {
            if (_token_metadata.get_pending_ranges(keyspace_name, get_broadcast_address()).size() > 0) {
                throw std::runtime_error("data is currently moving to this node; unable to leave the ring");
            }
        }

        logger.debug("DECOMMISSIONING");
        // FIXME: startLeaving();
        // FIXME: long timeout = Math.max(RING_DELAY, BatchlogManager.instance.getBatchlogTimeout());
        long timeout = get_ring_delay();
        set_mode(mode::LEAVING, sprint("sleeping %s ms for batch processing and pending range setup", timeout), true);
        sleep(std::chrono::milliseconds(timeout)).get();

        unbootstrap();

        // FIXME: proper shutdown
        // shutdownClientServers();
        gms::get_local_gossiper().stop();
        // MessagingService.instance().shutdown();
        // StageManager.shutdownNow();
        set_mode(mode::DECOMMISSIONED, true);
        // let op be responsible for killing the process
    });
}

future<> storage_service::remove_node(sstring host_id_string) {
    fail(unimplemented::cause::STORAGE_SERVICE);
    return seastar::async([this, host_id_string] {
        auto my_address = get_broadcast_address();
        auto local_host_id = _token_metadata.get_host_id(my_address);
        auto host_id = utils::UUID(host_id_string);
        auto endpoint_opt = _token_metadata.get_endpoint_for_host_id(host_id);
        auto& gossiper = gms::get_local_gossiper();
        if (!endpoint_opt) {
            throw std::runtime_error("Host ID not found.");
        }
        auto endpoint = *endpoint_opt;

        auto tokens = _token_metadata.get_tokens(endpoint);

        if (endpoint == my_address) {
            throw std::runtime_error("Cannot remove self");
        }

        if (gossiper.get_live_members().count(endpoint)) {
            throw std::runtime_error(sprint("Node %s is alive and owns this ID. Use decommission command to remove it from the ring", endpoint));
        }

        // A leaving endpoint that is dead is already being removed.
        if (_token_metadata.is_leaving(endpoint)) {
            logger.warn("Node {} is already being removed, continuing removal anyway", endpoint);
        }

        if (!_replicating_nodes.empty()) {
            throw std::runtime_error("This node is already processing a removal. Wait for it to complete, or use 'removenode force' if this has failed.");
        }

        auto non_system_keyspaces = _db.local().get_non_system_keyspaces();
        // Find the endpoints that are going to become responsible for data
        for (const auto& keyspace_name : non_system_keyspaces) {
            auto& ks = _db.local().find_keyspace(keyspace_name);
            // if the replication factor is 1 the data is lost so we shouldn't wait for confirmation
            if (ks.get_replication_strategy().get_replication_factor() == 1) {
                continue;
            }

            // get all ranges that change ownership (that is, a node needs
            // to take responsibility for new range)
            std::unordered_multimap<range<token>, inet_address> changed_ranges = get_changed_ranges_for_leaving(keyspace_name, endpoint);
            auto fd = gms::get_local_failure_detector();
            for (auto& x: changed_ranges) {
                auto ep = x.second;
                if (fd.is_alive(ep)) {
                    _replicating_nodes.emplace(ep);
                } else {
                    logger.warn("Endpoint {} is down and will not receive data for re-replication of {}", ep, endpoint);
                }
            }
        }
        _removing_node = endpoint;
        _token_metadata.add_leaving_endpoint(endpoint);
        get_local_pending_range_calculator_service().update().get();

        // the gossiper will handle spoofing this node's state to REMOVING_TOKEN for us
        // we add our own token so other nodes to let us know when they're done
        gossiper.advertise_removing(endpoint, host_id, local_host_id);

        // kick off streaming commands
        restore_replica_count(endpoint, my_address).get();

        // wait for ReplicationFinishedVerbHandler to signal we're done
        while (!_replicating_nodes.empty()) {
            sleep(std::chrono::milliseconds(100)).get();
        }

        // excise(tokens, endpoint);

        // gossiper will indicate the token has left
        gossiper.advertise_token_removed(endpoint, host_id);

        _replicating_nodes.clear();
        _removing_node = {};
    });
}

future<> storage_service::drain() {
    fail(unimplemented::cause::STORAGE_SERVICE);
#if 0
    ExecutorService counterMutationStage = StageManager.getStage(Stage.COUNTER_MUTATION);
    ExecutorService mutationStage = StageManager.getStage(Stage.MUTATION);
    if (mutationStage.isTerminated() && counterMutationStage.isTerminated())
    {
        logger.warn("Cannot drain node (did it already happen?)");
        return;
    }
    setMode(Mode.DRAINING, "starting drain process", true);
    shutdownClientServers();
    ScheduledExecutors.optionalTasks.shutdown();
    Gossiper.instance.stop();

    setMode(Mode.DRAINING, "shutting down MessageService", false);
    MessagingService.instance().shutdown();

    setMode(Mode.DRAINING, "clearing mutation stage", false);
    counterMutationStage.shutdown();
    mutationStage.shutdown();
    counterMutationStage.awaitTermination(3600, TimeUnit.SECONDS);
    mutationStage.awaitTermination(3600, TimeUnit.SECONDS);

    StorageProxy.instance.verifyNoHintsInProgress();

    setMode(Mode.DRAINING, "flushing column families", false);
    // count CFs first, since forceFlush could block for the flushWriter to get a queue slot empty
    totalCFs = 0;
    for (Keyspace keyspace : Keyspace.nonSystem())
        totalCFs += keyspace.getColumnFamilyStores().size();
    remainingCFs = totalCFs;
    // flush
    List<Future<?>> flushes = new ArrayList<>();
    for (Keyspace keyspace : Keyspace.nonSystem())
    {
        for (ColumnFamilyStore cfs : keyspace.getColumnFamilyStores())
            flushes.add(cfs.forceFlush());
    }
    // wait for the flushes.
    // TODO this is a godawful way to track progress, since they flush in parallel.  a long one could
    // thus make several short ones "instant" if we wait for them later.
    for (Future f : flushes)
    {
        FBUtilities.waitOnFuture(f);
        remainingCFs--;
    }
    // flush the system ones after all the rest are done, just in case flushing modifies any system state
    // like CASSANDRA-5151. don't bother with progress tracking since system data is tiny.
    flushes.clear();
    for (Keyspace keyspace : Keyspace.system())
    {
        for (ColumnFamilyStore cfs : keyspace.getColumnFamilyStores())
            flushes.add(cfs.forceFlush());
    }
    FBUtilities.waitOnFutures(flushes);

    BatchlogManager.shutdown();

    // whilst we've flushed all the CFs, which will have recycled all completed segments, we want to ensure
    // there are no segments to replay, so we force the recycling of any remaining (should be at most one)
    CommitLog.instance.forceRecycleAllSegments();

    ColumnFamilyStore.shutdownPostFlushExecutor();

    CommitLog.instance.shutdownBlocking();

    // wait for miscellaneous tasks like sstable and commitlog segment deletion
    ScheduledExecutors.nonPeriodicTasks.shutdown();
    if (!ScheduledExecutors.nonPeriodicTasks.awaitTermination(1, TimeUnit.MINUTES))
        logger.warn("Miscellaneous task executor still busy after one minute; proceeding with shutdown");

    setMode(Mode.DRAINED, true);
#endif
    return make_ready_future<>();
}

double storage_service::get_load() {
    fail(unimplemented::cause::STORAGE_SERVICE);
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

std::map<sstring, sstring> storage_service::get_load_map() {
    fail(unimplemented::cause::STORAGE_SERVICE);
#if 0
    Map<String, String> map = new HashMap<>();
    for (Map.Entry<InetAddress,Double> entry : LoadBroadcaster.instance.getLoadInfo().entrySet())
    {
        map.put(entry.getKey().getHostAddress(), FileUtils.stringifyFileSize(entry.getValue()));
    }
    // gossiper doesn't see its own updates, so we need to special-case the local node
    map.put(FBUtilities.getBroadcastAddress().getHostAddress(), getLoadString());
    return map;
#endif
    return std::map<sstring, sstring>();
}


future<> storage_service::rebuild(sstring source_dc) {
    using range_streamer = dht::range_streamer;
    logger.info("rebuild from dc: {}", source_dc == "" ? "(any dc)" : source_dc);
    range_streamer streamer(_db, _token_metadata, get_broadcast_address(), "Rebuild");
    streamer.add_source_filter(std::make_unique<range_streamer::failure_detector_source_filter>(gms::get_local_failure_detector()));
    // FIXME: SingleDatacenterFilter
#if 0
    if (source_dc != "")
        streamer.addSourceFilter(new RangeStreamer.SingleDatacenterFilter(DatabaseDescriptor.getEndpointSnitch(), sourceDc));
#endif

    for (const auto& keyspace_name : _db.local().get_non_system_keyspaces()) {
        streamer.add_ranges(keyspace_name, get_local_ranges(keyspace_name));
    }

    return streamer.fetch_async().then_wrapped([this] (auto&& f) {
        try {
            auto state = f.get0();
        } catch (...) {
            // This is used exclusively through JMX, so log the full trace but only throw a simple RTE
            logger.error("Error while rebuilding node: {}", std::current_exception());
            throw std::runtime_error(sprint("Error while rebuilding node: %s", std::current_exception()));
        }
        return make_ready_future<>();
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
    return smp::submit_to(0, [] {
        return get_local_storage_service()._initialized;
    });
}

std::unordered_multimap<range<token>, inet_address> storage_service::get_changed_ranges_for_leaving(sstring keyspace_name, inet_address endpoint) {
    return std::unordered_multimap<range<token>, inet_address>();
    // First get all ranges the leaving endpoint is responsible for
    auto ranges = get_ranges_for_endpoint(keyspace_name, endpoint);

    logger.debug("Node {} ranges [{}]", endpoint, ranges);

    std::unordered_map<range<token>, std::vector<inet_address>> current_replica_endpoints;

    // Find (for each range) all nodes that store replicas for these ranges as well
    auto metadata = _token_metadata.clone_only_token_map(); // don't do this in the loop! #7758
    for (auto& r : ranges) {
        auto& ks = _db.local().find_keyspace(keyspace_name);
        auto eps = ks.get_replication_strategy().calculate_natural_endpoints(r.end()->value(), metadata);
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
        auto new_replica_endpoints = ks.get_replication_strategy().calculate_natural_endpoints(r.end()->value(), temp);

        auto rg = current_replica_endpoints.equal_range(r);
        for (auto it = rg.first; it != rg.second; it++) {
            logger.debug("Remove range={}, eps={} from new_replica_endpoints={}", it->first, it->second, new_replica_endpoints);
            auto beg = new_replica_endpoints.begin();
            auto end = new_replica_endpoints.end();
            for (auto ep : it->second) {
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
            logger.debug("Ranges needing transfer are [{}]", ranges);
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
        auto stream_state = stream_success.get0();
        auto hints_state = hints_success.get0();
    } catch (...) {
        logger.warn("unbootstrap fails to stream : {}", std::current_exception());
        throw;
    }
    logger.debug("stream acks all received.");
    leave_ring();
}

future<> storage_service::restore_replica_count(inet_address endpoint, inet_address notify_endpoint) {
    using stream_plan = streaming::stream_plan;
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
    stream_plan sp("Restore replica count", true);
    for (auto& x: ranges_to_fetch) {
        const sstring& keyspace_name = x.first;
        std::unordered_map<inet_address, std::vector<range<token>>>& maps = x.second;
        for (auto& m : maps) {
            auto source = m.first;
            auto ranges = m.second;
            // FIXME: InetAddress preferred = SystemKeyspace.getPreferredIP(source);
            auto preferred = source;
            logger.debug("Requesting from {} ranges {}", source, ranges);
            sp.request_ranges(source, preferred, keyspace_name, ranges);
        }
    }
    return sp.execute().then_wrapped([this, notify_endpoint] (auto&& f) {
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
            subscriber->on_leave_cluster(endpoint);
        }
    }).get();

    get_local_pending_range_calculator_service().update().get();
}

void storage_service::excise(std::unordered_set<token> tokens, inet_address endpoint, long expire_time) {
    // FIXME: addExpireTimeIfFound(endpoint, expireTime);
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
            net::shard_id id{remote, 0};
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

void storage_service::confirm_replication(inet_address node) {
    // replicatingNodes can be empty in the case where this node used to be a removal coordinator,
    // but restarted before all 'replication finished' messages arrived. In that case, we'll
    // still go ahead and acknowledge it.
    if (!_replicating_nodes.empty()) {
        _replicating_nodes.erase(node);
    } else {
        logger.info("Received unexpected REPLICATION_FINISHED message from {}. Was this node recently a removal coordinator?", node);
    }
}

// Runs inside seastar::async context
void storage_service::leave_ring() {
    db::system_keyspace::set_bootstrap_state(db::system_keyspace::bootstrap_state::NEEDS_BOOTSTRAP).get();
    _token_metadata.remove_endpoint(get_broadcast_address());
    get_local_pending_range_calculator_service().update().get();

    auto& gossiper = gms::get_local_gossiper();
    auto expire_time = gossiper.compute_expire_time().time_since_epoch().count();
    gossiper.add_local_application_state(gms::application_state::STATUS, value_factory.left(get_local_tokens(), expire_time));
    auto delay = std::max(std::chrono::milliseconds(RING_DELAY), gms::gossiper::INTERVAL);
    logger.info("Announcing that I have left the ring for {}ms", delay.count());
    sleep(delay).get();
}

future<streaming::stream_state>
storage_service::stream_ranges(std::unordered_map<sstring, std::unordered_multimap<range<token>, inet_address>> ranges_to_stream_by_keyspace) {
    using stream_plan = streaming::stream_plan;
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
    stream_plan sp("Unbootstrap", true);
    for (auto& entry : sessions_to_stream_by_keyspace) {
        const auto& keyspace_name = entry.first;
        // TODO: we can move to avoid copy of std::vector
        auto& ranges_per_endpoint = entry.second;

        for (auto& ranges_entry : ranges_per_endpoint) {
            auto& ranges = ranges_entry.second;
            auto new_endpoint = ranges_entry.first;
            auto preferred = new_endpoint; // FIXME: SystemKeyspace.getPreferredIP(newEndpoint);

            // TODO each call to transferRanges re-flushes, this is potentially a lot of waste
            sp.transfer_ranges(new_endpoint, preferred, keyspace_name, ranges);
        }
    }
    return sp.execute();
}

future<streaming::stream_state> storage_service::stream_hints() {
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
        auto preferred = hints_destination_host; // FIXME: SystemKeyspace.getPreferredIP(hints_destination_host);

        // stream all hints -- range list will be a singleton of "the entire ring"
        auto t = dht::global_partitioner().get_minimum_token();
        std::vector<range<token>> ranges = {range<token>(t)};

        streaming::stream_plan sp("Hints", true);
        std::vector<sstring> column_families = { db::system_keyspace::HINTS };
        auto keyspace = db::system_keyspace::NAME;
        sp.transfer_ranges(hints_destination_host, preferred, keyspace, ranges, column_families);
        return sp.execute();
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
} // namespace service
