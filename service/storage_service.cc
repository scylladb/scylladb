/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#include "storage_service.hh"
#include "core/distributed.hh"

namespace service {

int storage_service::RING_DELAY = storage_service::get_ring_delay();

distributed<storage_service> _the_storage_service;

bool storage_service::should_bootstrap() {
    // FIXME: Currently, we do boostrap if we are not a seed node.
    // return DatabaseDescriptor.isAutoBootstrap() && !SystemKeyspace.bootstrapComplete() && !DatabaseDescriptor.getSeeds().contains(FBUtilities.getBroadcastAddress());
    auto& gossiper = gms::get_local_gossiper();
    auto seeds = gossiper.get_seeds();
    return !seeds.count(get_broadcast_address());
}

future<> storage_service::prepare_to_join() {
    if (!_joined) {
        std::map<gms::application_state, gms::versioned_value> app_states;
#if 0
        if (DatabaseDescriptor.isReplacing() && !(Boolean.parseBoolean(System.getProperty("cassandra.join_ring", "true"))))
            throw new ConfigurationException("Cannot set both join_ring=false and attempt to replace a node");
        if (DatabaseDescriptor.getReplaceTokens().size() > 0 || DatabaseDescriptor.getReplaceNode() != null)
            throw new RuntimeException("Replace method removed; use cassandra.replace_address instead");
        if (DatabaseDescriptor.isReplacing())
        {
            if (SystemKeyspace.bootstrapComplete())
                throw new RuntimeException("Cannot replace address with a node that is already bootstrapped");
            if (!DatabaseDescriptor.isAutoBootstrap())
                throw new RuntimeException("Trying to replace_address with auto_bootstrap disabled will not work, check your configuration");
            _bootstrap_tokens = prepareReplacementInfo();
            appStates.put(ApplicationState.TOKENS, valueFactory.tokens(_bootstrap_tokens));
            appStates.put(ApplicationState.STATUS, valueFactory.hibernate(true));
        }
        else if (should_bootstrap())
        {
            checkForEndpointCollision();
        }
#endif

        // have to start the gossip service before we can see any info on other nodes.  this is necessary
        // for bootstrap to get the load info it needs.
        // (we won't be part of the storage ring though until we add a counterId to our state, below.)
        // Seed the host ID-to-endpoint map with our own ID.
        auto local_host_id = db::system_keyspace::get_local_host_id();
        _token_metadata.update_host_id(local_host_id, get_broadcast_address());
        // FIXME: DatabaseDescriptor.getBroadcastRpcAddress()
        gms::inet_address broadcast_rpc_address;
        app_states.emplace(gms::application_state::NET_VERSION, value_factory.network_version());
        app_states.emplace(gms::application_state::HOST_ID, value_factory.host_id(local_host_id));
        app_states.emplace(gms::application_state::RPC_ADDRESS, value_factory.rpcaddress(broadcast_rpc_address));
        app_states.emplace(gms::application_state::RELEASE_VERSION, value_factory.release_version());
        //logger.info("Starting up server gossip");

        auto& gossiper = gms::get_local_gossiper();
        gossiper.register_(this);
        using namespace std::chrono;
        auto now = high_resolution_clock::now().time_since_epoch();
        int generation_number = duration_cast<seconds>(now).count();
        // FIXME: SystemKeyspace.incrementAndGetGeneration()
        return gossiper.start(generation_number, app_states).then([this] {
            print("Start gossiper service ...\n");
#if SS_DEBUG
            gms::get_local_gossiper().debug_show();
            _token_metadata.debug_show();
#endif
        });
#if 0
        // gossip snitch infos (local DC and rack)
        gossipSnitchInfo();
        // gossip Schema.emptyVersion forcing immediate check for schema updates (see MigrationManager#maybeScheduleSchemaPull)
        Schema.instance.updateVersionAndAnnounce(); // Ensure we know our own actual Schema UUID in preparation for updates

        if (!MessagingService.instance().isListening())
            MessagingService.instance().listen(FBUtilities.getLocalAddress());
        LoadBroadcaster.instance.startBroadcasting();

        HintedHandOffManager.instance.start();
        BatchlogManager.instance.start();
#endif
    }
    return make_ready_future<>();
}

future<> storage_service::join_token_ring(int delay) {
    auto f = make_ready_future<>();
    _joined = true;
#if 0
    // We bootstrap if we haven't successfully bootstrapped before, as long as we are not a seed.
    // If we are a seed, or if the user manually sets auto_bootstrap to false,
    // we'll skip streaming data from other nodes and jump directly into the ring.
    //
    // The seed check allows us to skip the RING_DELAY sleep for the single-node cluster case,
    // which is useful for both new users and testing.
    //
    // We attempted to replace this with a schema-presence check, but you need a meaningful sleep
    // to get schema info from gossip which defeats the purpose.  See CASSANDRA-4427 for the gory details.
    Set<InetAddress> current = new HashSet<>();
    logger.debug("Bootstrap variables: {} {} {} {}",
                 DatabaseDescriptor.isAutoBootstrap(),
                 SystemKeyspace.bootstrapInProgress(),
                 SystemKeyspace.bootstrapComplete(),
                 DatabaseDescriptor.getSeeds().contains(FBUtilities.getBroadcastAddress()));
    if (DatabaseDescriptor.isAutoBootstrap() && !SystemKeyspace.bootstrapComplete() && DatabaseDescriptor.getSeeds().contains(FBUtilities.getBroadcastAddress()))
        logger.info("This node will not auto bootstrap because it is configured to be a seed node.");
#endif
    if (should_bootstrap()) {
        _bootstrap_tokens = boot_strapper::get_bootstrap_tokens(_token_metadata);
        f = bootstrap(_bootstrap_tokens);
#if 0
        if (SystemKeyspace.bootstrapInProgress())
            logger.warn("Detected previous bootstrap failure; retrying");
        else
            SystemKeyspace.setBootstrapState(SystemKeyspace.BootstrapState.IN_PROGRESS);
        setMode(Mode.JOINING, "waiting for ring information", true);
        // first sleep the delay to make sure we see all our peers
        for (int i = 0; i < delay; i += 1000)
        {
            // if we see schema, we can proceed to the next check directly
            if (!Schema.instance.getVersion().equals(Schema.emptyVersion))
            {
                logger.debug("got schema: {}", Schema.instance.getVersion());
                break;
            }
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        }
        // if our schema hasn't matched yet, keep sleeping until it does
        // (post CASSANDRA-1391 we don't expect this to be necessary very often, but it doesn't hurt to be careful)
        while (!MigrationManager.isReadyForBootstrap())
        {
            setMode(Mode.JOINING, "waiting for schema information to complete", true);
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        }
        setMode(Mode.JOINING, "schema complete, ready to bootstrap", true);
        setMode(Mode.JOINING, "waiting for pending range calculation", true);
        PendingRangeCalculatorService.instance.blockUntilFinished();
        setMode(Mode.JOINING, "calculation complete, ready to bootstrap", true);


        if (logger.isDebugEnabled())
            logger.debug("... got ring + schema info");

        if (Boolean.parseBoolean(System.getProperty("cassandra.consistent.rangemovement", "true")) &&
                (
                    _token_metadata.getBootstrapTokens().valueSet().size() > 0 ||
                    _token_metadata.getLeavingEndpoints().size() > 0 ||
                    _token_metadata.getMovingEndpoints().size() > 0
                ))
            throw new UnsupportedOperationException("Other bootstrapping/leaving/moving nodes detected, cannot bootstrap while cassandra.consistent.rangemovement is true");

        if (!DatabaseDescriptor.isReplacing())
        {
            if (_token_metadata.isMember(FBUtilities.getBroadcastAddress()))
            {
                String s = "This node is already a member of the token ring; bootstrap aborted. (If replacing a dead node, remove the old one from the ring first.)";
                throw new UnsupportedOperationException(s);
            }
            setMode(Mode.JOINING, "getting bootstrap token", true);
            _bootstrap_tokens = BootStrapper.getBootstrapTokens(_token_metadata);
        }
        else
        {
            if (!DatabaseDescriptor.getReplaceAddress().equals(FBUtilities.getBroadcastAddress()))
            {
                try
                {
                    // Sleep additionally to make sure that the server actually is not alive
                    // and giving it more time to gossip if alive.
                    Thread.sleep(LoadBroadcaster.BROADCAST_INTERVAL);
                }
                catch (InterruptedException e)
                {
                    throw new AssertionError(e);
                }

                // check for operator errors...
                for (Token token : _bootstrap_tokens)
                {
                    InetAddress existing = _token_metadata.getEndpoint(token);
                    if (existing != null)
                    {
                        long nanoDelay = delay * 1000000L;
                        if (Gossiper.instance.getEndpointStateForEndpoint(existing).getUpdateTimestamp() > (System.nanoTime() - nanoDelay))
                            throw new UnsupportedOperationException("Cannot replace a live node... ");
                        current.add(existing);
                    }
                    else
                    {
                        throw new UnsupportedOperationException("Cannot replace token " + token + " which does not exist!");
                    }
                }
            }
            else
            {
                try
                {
                    Thread.sleep(RING_DELAY);
                }
                catch (InterruptedException e)
                {
                    throw new AssertionError(e);
                }

            }
            setMode(Mode.JOINING, "Replacing a node with token(s): " + _bootstrap_tokens, true);
        }

        bootstrap(_bootstrap_tokens);
        assert !_is_bootstrap_mode; // bootstrap will block until finished
#endif
    } else {
        // FIXME: DatabaseDescriptor.getNumTokens()
        size_t num_tokens = 3;
        _bootstrap_tokens = boot_strapper::get_random_tokens(_token_metadata, num_tokens);
#if 0
        _bootstrap_tokens = SystemKeyspace.getSavedTokens();
        if (_bootstrap_tokens.isEmpty())
        {
            Collection<String> initialTokens = DatabaseDescriptor.getInitialTokens();
            if (initialTokens.size() < 1)
            {
                _bootstrap_tokens = BootStrapper.getRandomTokens(_token_metadata, DatabaseDescriptor.getNumTokens());
                if (DatabaseDescriptor.getNumTokens() == 1)
                    logger.warn("Generated random token {}. Random tokens will result in an unbalanced ring; see http://wiki.apache.org/cassandra/Operations", _bootstrap_tokens);
                else
                    logger.info("Generated random tokens. tokens are {}", _bootstrap_tokens);
            }
            else
            {
                _bootstrap_tokens = new ArrayList<Token>(initialTokens.size());
                for (String token : initialTokens)
                    _bootstrap_tokens.add(getPartitioner().getTokenFactory().fromString(token));
                logger.info("Saved tokens not found. Using configuration value: {}", _bootstrap_tokens);
            }
        }
        else
        {
            if (_bootstrap_tokens.size() != DatabaseDescriptor.getNumTokens())
                throw new ConfigurationException("Cannot change the number of tokens from " + _bootstrap_tokens.size() + " to " + DatabaseDescriptor.getNumTokens());
            else
                logger.info("Using saved tokens {}", _bootstrap_tokens);
        }
#endif
    }

    return f.then([this] {
        set_tokens(_bootstrap_tokens);
#if 0
    // if we don't have system_traces keyspace at this point, then create it manually
    if (Schema.instance.getKSMetaData(TraceKeyspace.NAME) == null)
        MigrationManager.announceNewKeyspace(TraceKeyspace.definition(), 0, false);

    if (!_is_survey_mode)
    {
        // start participating in the ring.
        SystemKeyspace.setBootstrapState(SystemKeyspace.BootstrapState.COMPLETED);
        set_tokens(_bootstrap_tokens);
        // remove the existing info about the replaced node.
        if (!current.isEmpty())
            for (InetAddress existing : current)
                Gossiper.instance.replacedEndpoint(existing);
        assert _token_metadata.sortedTokens().size() > 0;

        Auth.setup();
    }
    else
    {
        logger.info("Startup complete, but write survey mode is active, not becoming an active ring member. Use JMX (StorageService->joinRing()) to finalize ring joining.");
    }
#endif
    });
}

void storage_service::join_ring() {
#if 0
    if (!joined) {
        logger.info("Joining ring by operator request");
        try
        {
            joinTokenRing(0);
        }
        catch (ConfigurationException e)
        {
            throw new IOException(e.getMessage());
        }
    } else if (_is_survey_mode) {
        set_tokens(SystemKeyspace.getSavedTokens());
        SystemKeyspace.setBootstrapState(SystemKeyspace.BootstrapState.COMPLETED);
        _is_survey_mode = false;
        logger.info("Leaving write survey mode and joining ring at operator request");
        assert _token_metadata.sortedTokens().size() > 0;

        Auth.setup();
    }
#endif
}

future<> storage_service::bootstrap(std::unordered_set<token> tokens) {
    _is_bootstrap_mode = true;
    // SystemKeyspace.updateTokens(tokens); // DON'T use setToken, that makes us part of the ring locally which is incorrect until we are done bootstrapping
    // FIXME: DatabaseDescriptor.isReplacing()
    auto is_replacing = false;
    auto sleep_time = std::chrono::milliseconds(1);
    if (!is_replacing) {
        // if not an existing token then bootstrap
        auto& gossiper = gms::get_local_gossiper();
        gossiper.add_local_application_state(gms::application_state::TOKENS, value_factory.tokens(tokens));
        gossiper.add_local_application_state(gms::application_state::STATUS, value_factory.bootstrapping(tokens));
        sleep_time = std::chrono::milliseconds(RING_DELAY);
        // setMode(Mode.JOINING, "sleeping " + RING_DELAY + " ms for pending range setup", true);
    } else {
        // Dont set any state for the node which is bootstrapping the existing token...
        for (auto t : tokens) {
            _token_metadata.update_normal_token(t, get_broadcast_address());
        }
        // SystemKeyspace.removeEndpoint(DatabaseDescriptor.getReplaceAddress());
    }
    return sleep(sleep_time).then([] {
        auto& gossiper = gms::get_local_gossiper();
        if (!gossiper.seen_any_seed()) {
             throw std::runtime_error("Unable to contact any seeds!");
        }
        // setMode(Mode.JOINING, "Starting to bootstrap...", true);
        // new BootStrapper(FBUtilities.getBroadcastAddress(), tokens, _token_metadata).bootstrap(); // handles token update
        // logger.info("Bootstrap completed! for the tokens {}", tokens);
        return make_ready_future<>();
    });
}

void storage_service::handle_state_bootstrap(inet_address endpoint) {
    ss_debug("SS::handle_state_bootstrap endpoint=%s\n", endpoint);
    // explicitly check for TOKENS, because a bootstrapping node might be bootstrapping in legacy mode; that is, not using vnodes and no token specified
    auto tokens = get_tokens_for(endpoint);

    // if (logger.isDebugEnabled())
    //     logger.debug("Node {} state bootstrapping, token {}", endpoint, tokens);

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
            // logger.info("Node {} state jump to bootstrap", endpoint);
        }
        // _token_metadata.removeEndpoint(endpoint);
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
    ss_debug("SS::handle_state_bootstrap endpoint=%s\n", endpoint);
    auto tokens = get_tokens_for(endpoint);
    auto& gossiper = gms::get_local_gossiper();

    std::unordered_set<token> tokensToUpdateInMetadata;
    std::unordered_set<token> tokensToUpdateInSystemKeyspace;
    std::unordered_set<token> localTokensToRemove;
    std::unordered_set<inet_address> endpointsToRemove;

    // if (logger.isDebugEnabled())
    //     logger.debug("Node {} state normal, token {}", endpoint, tokens);

    if (_token_metadata.is_member(endpoint)) {
        // logger.info("Node {} state jump to normal", endpoint);
    }
    // FIXME:
    // updatePeerInfo(endpoint);
#if 1
    // Order Matters, TM.updateHostID() should be called before TM.updateNormalToken(), (see CASSANDRA-4300).
    if (gossiper.uses_host_id(endpoint)) {
        auto host_id = gossiper.get_host_id(endpoint);
        //inet_address existing = _token_metadata.get_endpoint_for_host_id(host_id);
        // if (DatabaseDescriptor.isReplacing() &&
        //     Gossiper.instance.getEndpointStateForEndpoint(DatabaseDescriptor.getReplaceAddress()) != null &&
        //     (hostId.equals(Gossiper.instance.getHostId(DatabaseDescriptor.getReplaceAddress())))) {
        if (false) {
            // logger.warn("Not updating token metadata for {} because I am replacing it", endpoint);
        } else {
            if (false /*existing != null && !existing.equals(endpoint)*/) {
#if 0
                if (existing == get_broadcast_address()) {
                    logger.warn("Not updating host ID {} for {} because it's mine", hostId, endpoint);
                    _token_metadata.removeEndpoint(endpoint);
                    endpointsToRemove.add(endpoint);
                } else if (gossiper.compare_endpoint_startup(endpoint, existing) > 0) {
                    logger.warn("Host ID collision for {} between {} and {}; {} is the new owner", hostId, existing, endpoint, endpoint);
                    _token_metadata.removeEndpoint(existing);
                    endpointsToRemove.add(existing);
                    _token_metadata.update_host_id(hostId, endpoint);
                } else {
                    logger.warn("Host ID collision for {} between {} and {}; ignored {}", hostId, existing, endpoint, endpoint);
                    _token_metadata.removeEndpoint(endpoint);
                    endpointsToRemove.add(endpoint);
                }
#endif
            } else {
                _token_metadata.update_host_id(host_id, endpoint);
            }
        }
    }
#endif

    for (auto t : tokens) {
        // we don't want to update if this node is responsible for the token and it has a later startup time than endpoint.
        auto current_owner = _token_metadata.get_endpoint(t);
        if (!current_owner) {
            // logger.debug("New node {} at token {}", endpoint, t);
            tokensToUpdateInMetadata.insert(t);
            tokensToUpdateInSystemKeyspace.insert(t);
        } else if (endpoint == *current_owner) {
            // set state back to normal, since the node may have tried to leave, but failed and is now back up
            tokensToUpdateInMetadata.insert(t);
            tokensToUpdateInSystemKeyspace.insert(t);
        } else if (gossiper.compare_endpoint_startup(endpoint, *current_owner) > 0) {
            tokensToUpdateInMetadata.insert(t);
            tokensToUpdateInSystemKeyspace.insert(t);
#if 0

            // currentOwner is no longer current, endpoint is.  Keep track of these moves, because when
            // a host no longer has any tokens, we'll want to remove it.
            Multimap<InetAddress, Token> epToTokenCopy = getTokenMetadata().getEndpointToTokenMapForReading();
            epToTokenCopy.get(currentOwner).remove(token);
            if (epToTokenCopy.get(currentOwner).size() < 1)
                endpointsToRemove.add(currentOwner);

            logger.info(String.format("Nodes %s and %s have the same token %s.  %s is the new owner",
                                      endpoint,
                                      currentOwner,
                                      token,
                                      endpoint));
#endif
        } else {
#if 0
            logger.info(String.format("Nodes %s and %s have the same token %s.  Ignoring %s",
                                       endpoint,
                                       currentOwner,
                                       token,
                                       endpoint));
#endif
        }
    }

    bool is_moving = _token_metadata.is_moving(endpoint); // capture because updateNormalTokens clears moving status
    _token_metadata.update_normal_tokens(tokensToUpdateInMetadata, endpoint);
    // for (auto ep : endpointsToRemove) {
        // removeEndpoint(ep);
        // if (DatabaseDescriptor.isReplacing() && DatabaseDescriptor.getReplaceAddress().equals(ep))
        //     Gossiper.instance.replacementQuarantine(ep); // quarantine locally longer than normally; see CASSANDRA-8260
    // }
    if (!tokensToUpdateInSystemKeyspace.empty()) {
        // SystemKeyspace.updateTokens(endpoint, tokensToUpdateInSystemKeyspace);
    }
    if (!localTokensToRemove.empty()) {
        // SystemKeyspace.updateLocalTokens(Collections.<Token>emptyList(), localTokensToRemove);
    }

    if (is_moving) {
        // _token_metadata.remove_from_moving(endpoint);
        // for (IEndpointLifecycleSubscriber subscriber : lifecycleSubscribers)
        //     subscriber.onMove(endpoint);
    } else {
        // for (IEndpointLifecycleSubscriber subscriber : lifecycleSubscribers)
        //     subscriber.onJoinCluster(endpoint);
    }

    // PendingRangeCalculatorService.instance.update();
}

void storage_service::handle_state_leaving(inet_address endpoint) {
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
    ss_debug("SS::on_join endpoint=%s\n", endpoint);
    auto tokens = get_tokens_for(endpoint);
    for (auto t : tokens) {
        ss_debug("t=%s\n", t);
    }
    for (auto e : ep_state.get_application_state_map()) {
        on_change(endpoint, e.first, e.second);
    }
    // MigrationManager.instance.scheduleSchemaPull(endpoint, epState);
}

void storage_service::on_alive(gms::inet_address endpoint, gms::endpoint_state state) {
    ss_debug("SS::on_alive endpoint=%s\n", endpoint);
#if 0
    MigrationManager.instance.scheduleSchemaPull(endpoint, state);

    if (_token_metadata.isMember(endpoint))
    {
        HintedHandOffManager.instance.scheduleHintDelivery(endpoint, true);
        for (IEndpointLifecycleSubscriber subscriber : lifecycleSubscribers)
            subscriber.onUp(endpoint);
    }
#endif
}

void storage_service::before_change(gms::inet_address endpoint, gms::endpoint_state current_state, gms::application_state new_state_key, gms::versioned_value new_value) {
    // no-op
}

void storage_service::on_change(inet_address endpoint, application_state state, versioned_value value) {
    ss_debug("SS::on_change endpoint=%s\n", endpoint);
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
            // logger.debug("Ignoring state change for dead or unknown endpoint: {}", endpoint);
            return;
        }

        if (state == application_state::RELEASE_VERSION) {
            // SystemKeyspace.updatePeerInfo(endpoint, "release_version", value.value);
        } else if (state == application_state::DC) {
            // SystemKeyspace.updatePeerInfo(endpoint, "data_center", value.value);
        } else if (state == application_state::RACK) {
            // SystemKeyspace.updatePeerInfo(endpoint, "rack", value.value);
        } else if (state == application_state::RPC_ADDRESS) {
            // try {
            //     SystemKeyspace.updatePeerInfo(endpoint, "rpc_address", InetAddress.getByName(value.value));
            // } catch (UnknownHostException e) {
            //     throw new RuntimeException(e);
            // }
        } else if (state == application_state::SCHEMA) {
            // SystemKeyspace.updatePeerInfo(endpoint, "schema_version", UUID.fromString(value.value));
            // MigrationManager.instance.scheduleSchemaPull(endpoint, epState);
        } else if (state == application_state::HOST_ID) {
            // SystemKeyspace.updatePeerInfo(endpoint, "host_id", UUID.fromString(value.value));
        }
    }
}


void storage_service::on_remove(gms::inet_address endpoint) {
#if 0
    _token_metadata.removeEndpoint(endpoint);
    PendingRangeCalculatorService.instance.update();
#endif
}

void storage_service::on_dead(gms::inet_address endpoint, gms::endpoint_state state) {
#if 0
    MessagingService.instance().convict(endpoint);
    for (IEndpointLifecycleSubscriber subscriber : lifecycleSubscribers)
        subscriber.onDown(endpoint);
#endif
}

void storage_service::on_restart(gms::inet_address endpoint, gms::endpoint_state state) {
#if 0
    // If we have restarted before the node was even marked down, we need to reset the connection pool
    if (state.isAlive())
        onDead(endpoint, state);
#endif
}


} // namespace service
