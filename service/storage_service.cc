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
    if (!joined) {
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
#if 0
    joined = true;

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
        assert !isBootstrapMode; // bootstrap will block until finished
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

    if (!isSurveyMode)
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

future<> storage_service::bootstrap(std::unordered_set<token> tokens) {
    // isBootstrapMode = true;
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


}
