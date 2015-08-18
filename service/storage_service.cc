/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
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
#include <seastar/core/thread.hh>
#include <sstream>

using token = dht::token;
using UUID = utils::UUID;
using inet_address = gms::inet_address;

namespace service {

static logging::logger logger("storage_service");

int storage_service::RING_DELAY = storage_service::get_ring_delay();

distributed<storage_service> _the_storage_service;

bool is_replacing() {
    // FIXME: DatabaseDescriptor.isReplacing()
    return false;
}

bool is_auto_bootstrap() {
    // FIXME: DatabaseDescriptor.isAutoBootstrap()
    return true;
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
        using namespace std::chrono;
        auto now = high_resolution_clock::now().time_since_epoch();
        int generation_number = duration_cast<seconds>(now).count();
        // FIXME: SystemKeyspace.incrementAndGetGeneration()
        print("Start gossiper service ...\n");
        return gossiper.start(generation_number, app_states).then([this] {
#if SS_DEBUG
            gms::get_local_gossiper().debug_show();
            _token_metadata.debug_show();
#endif
        });
    }).then([this] {
        // gossip snitch infos (local DC and rack)
        gossip_snitch_info();
        auto& proxy = service::get_local_storage_proxy();
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

future<> storage_service::join_token_ring(int delay) {
    return seastar::async([this, delay] {
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
            db::system_keyspace::set_bootstrap_state(db::system_keyspace::bootstrap_state::IN_PROGRESS);
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
        //PendingRangeCalculatorService.instance.blockUntilFinished();
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
        // FIXME: _is_bootstrap_mode is set to fasle in BootStrapper::bootstrap
        // assert(!_is_bootstrap_mode); // bootstrap will block until finished
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
                for (auto token : initial_tokens) {
                    // FIXME: token from string
                    // _bootstrap_tokens.insert(getPartitioner().getTokenFactory().fromString(token));
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
        db::system_keyspace::set_bootstrap_state(db::system_keyspace::bootstrap_state::COMPLETED);
        set_tokens(_bootstrap_tokens).get();
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
    });
}

future<> storage_service::join_ring() {
    if (!_joined) {
        logger.info("Joining ring by operator request");
        return join_token_ring(0);
    } else if (_is_survey_mode) {
        return db::system_keyspace::get_saved_tokens().then([this] (auto tokens) {
            return this->set_tokens(std::move(tokens)).then([this] {
                db::system_keyspace::set_bootstrap_state(db::system_keyspace::bootstrap_state::COMPLETED);
                _is_survey_mode = false;
                logger.info("Leaving write survey mode and joining ring at operator request");
                assert(_token_metadata.sorted_tokens().size() > 0);
                //Auth.setup();
                return make_ready_future<>();
            });
        });
    }
    return make_ready_future<>();
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
    // new BootStrapper(FBUtilities.getBroadcastAddress(), tokens, _token_metadata).bootstrap(); // handles token update
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
    // FIXME
    // PendingRangeCalculatorService.instance.update();

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
        // if (DatabaseDescriptor.isReplacing() &&
        //     Gossiper.instance.getEndpointStateForEndpoint(DatabaseDescriptor.getReplaceAddress()) != null &&
        //     (hostId.equals(Gossiper.instance.getHostId(DatabaseDescriptor.getReplaceAddress())))) {
        if (false) {
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

    // PendingRangeCalculatorService.instance.update();
    if (logger.is_enabled(logging::log_level::debug)) {
        auto ver = _token_metadata.get_ring_version();
        for (auto& x : _token_metadata.get_token_to_endpoint()) {
            logger.debug("token_metadata.ring_version={}, token={} -> endpoint={}", ver, x.first, x.second);
        }
    }
}

void storage_service::handle_state_leaving(inet_address endpoint) {
    logger.debug("handle_state_leaving endpoint={}", endpoint);
#if 0
    Collection<Token> tokens;
    tokens = get_tokens_for(endpoint);

    if (logger.isDebugEnabled())
        logger.debug("Node {} state leaving, tokens {}", endpoint, tokens);

    // If the node is previously unknown or tokens do not match, update tokenmetadata to
    // have this node as 'normal' (it must have been using this token before the
    // leave). This way we'll get pending ranges right.
    if (!_token_metadata.isMember(endpoint))
    {
        logger.info("Node {} state jump to leaving", endpoint);
        _token_metadata.updateNormalTokens(tokens, endpoint);
    }
    else if (!_token_metadata.getTokens(endpoint).containsAll(tokens))
    {
        logger.warn("Node {} 'leaving' token mismatch. Long network partition?", endpoint);
        _token_metadata.updateNormalTokens(tokens, endpoint);
    }

    // at this point the endpoint is certainly a member with this token, so let's proceed
    // normally
    _token_metadata.addLeavingEndpoint(endpoint);
    PendingRangeCalculatorService.instance.update();
#endif
}

void storage_service::handle_state_left(inet_address endpoint, std::vector<sstring> pieces) {
    logger.debug("handle_state_left endpoint={}", endpoint);
#if 0
    assert pieces.length >= 2;
    Collection<Token> tokens;
    tokens = get_tokens_for(endpoint);

    if (logger.isDebugEnabled())
        logger.debug("Node {} state left, tokens {}", endpoint, tokens);

    excise(tokens, endpoint, extractExpireTime(pieces));
#endif
}

void storage_service::handle_state_moving(inet_address endpoint, std::vector<sstring> pieces) {
    logger.debug("handle_state_moving endpoint={}", endpoint);
#if 0
    assert pieces.length >= 2;
    Token token = getPartitioner().getTokenFactory().fromString(pieces[1]);

    if (logger.isDebugEnabled())
        logger.debug("Node {} state moving, new token {}", endpoint, token);

    _token_metadata.addMovingEndpoint(token, endpoint);

    PendingRangeCalculatorService.instance.update();
#endif
}

void storage_service::handle_state_removing(inet_address endpoint, std::vector<sstring> pieces) {
    logger.debug("handle_state_removing endpoint={}", endpoint);
#if 0
    assert (pieces.length > 0);

    if (endpoint.equals(FBUtilities.getBroadcastAddress()))
    {
        logger.info("Received removenode gossip about myself. Is this node rejoining after an explicit removenode?");
        try
        {
            drain();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        return;
    }
    if (_token_metadata.isMember(endpoint))
    {
        String state = pieces[0];
        Collection<Token> removeTokens = _token_metadata.getTokens(endpoint);

        if (VersionedValue.REMOVED_TOKEN.equals(state))
        {
            excise(removeTokens, endpoint, extractExpireTime(pieces));
        }
        else if (VersionedValue.REMOVING_TOKEN.equals(state))
        {
            if (logger.isDebugEnabled())
                logger.debug("Tokens {} removed manually (endpoint was {})", removeTokens, endpoint);

            // Note that the endpoint is being removed
            _token_metadata.addLeavingEndpoint(endpoint);
            PendingRangeCalculatorService.instance.update();

            // find the endpoint coordinating this removal that we need to notify when we're done
            String[] coordinator = Gossiper.instance.getEndpointStateForEndpoint(endpoint).getApplicationState(ApplicationState.REMOVAL_COORDINATOR).value.split(VersionedValue.DELIMITER_STR, -1);
            UUID hostId = UUID.fromString(coordinator[1]);
            // grab any data we are now responsible for and notify responsible node
            restoreReplicaCount(endpoint, _token_metadata.getEndpointForHostId(hostId));
        }
    }
    else // now that the gossiper has told us about this nonexistent member, notify the gossiper to remove it
    {
        if (VersionedValue.REMOVED_TOKEN.equals(pieces[0]))
            addExpireTimeIfFound(endpoint, extractExpireTime(pieces));
        removeEndpoint(endpoint);
    }
#endif
}

void storage_service::on_join(gms::inet_address endpoint, gms::endpoint_state ep_state) {
    logger.debug("on_join endpoint={}", endpoint);
    for (auto e : ep_state.get_application_state_map()) {
        on_change(endpoint, e.first, e.second);
    }
    get_local_migration_manager().schedule_schema_pull(endpoint, ep_state).get();
}

void storage_service::on_alive(gms::inet_address endpoint, gms::endpoint_state state) {
    logger.debug("on_alive endpoint={}", endpoint);
    get_local_migration_manager().schedule_schema_pull(endpoint, state).get();
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
            get_local_migration_manager().schedule_schema_pull(endpoint, *ep_state).get();
        }
    }
    replicate_to_all_cores().get();
}


void storage_service::on_remove(gms::inet_address endpoint) {
    logger.debug("on_remove endpoint={}", endpoint);
    _token_metadata.remove_endpoint(endpoint);
#if 0
    PendingRangeCalculatorService.instance.update();
#endif
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
    });
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

future<> storage_service::set_tokens(std::unordered_set<token> tokens) {
    logger.debug("Setting tokens to {}", tokens);
    auto f = db::system_keyspace::update_tokens(tokens);
    return f.then([this, tokens = std::move(tokens)] {
        _token_metadata.update_normal_tokens(tokens, get_broadcast_address());
        // Collection<Token> localTokens = getLocalTokens();
        auto local_tokens = _bootstrap_tokens;
        auto& gossiper = gms::get_local_gossiper();
        gossiper.add_local_application_state(gms::application_state::TOKENS, value_factory.tokens(local_tokens));
        gossiper.add_local_application_state(gms::application_state::STATUS, value_factory.normal(local_tokens));
        set_mode(mode::NORMAL, false);
        return replicate_to_all_cores();
    });
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
        join_token_ring(delay).get();
    } else {
        auto tokens = std::get<0>(db::system_keyspace::get_saved_tokens().get());
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

} // namespace service
