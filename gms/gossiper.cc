#include "gms/gossiper.hh"
#include "gms/failure_detector.hh"

namespace gms {

gossiper::gossiper()
    : _scheduled_gossip_task([this] { run(); }) {
    // half of QUARATINE_DELAY, to ensure _just_removed_endpoints has enough leeway to prevent re-gossip
    fat_client_timeout = (int64_t) (QUARANTINE_DELAY / 2);
    /* register with the Failure Detector for receiving Failure detector events */
    get_local_failure_detector().register_failure_detection_event_listener(this->shared_from_this());
    // Register this instance with JMX
    init_messaging_service_handler();
}

void gossiper::init_messaging_service_handler() {
    ms().register_handler(messaging_verb::ECHO, [] (empty_msg msg) {
        return make_ready_future<empty_msg>();
    });
    ms().register_handler_oneway(messaging_verb::GOSSIP_SHUTDOWN, [] (inet_address from) {
        // TODO: Implement processing of incoming SHUTDOWN message
        get_local_failure_detector().force_conviction(from);
        return messaging_service::no_wait();
    });
    ms().register_handler(messaging_verb::GOSSIP_DIGEST_SYN, [] (gossip_digest_syn msg) {
        // TODO: Implement processing of incoming ACK2 message
        print("gossiper: Server got syn msg = %s\n", msg);
        auto ep = inet_address("2.2.2.2");
        int32_t gen = 800;
        int32_t ver = 900;
        std::vector<gms::gossip_digest> digests{
            {ep, gen++, ver++},
        };
        std::map<inet_address, endpoint_state> eps{
            {ep, endpoint_state()},
        };
        gms::gossip_digest_ack ack(std::move(digests), std::move(eps));
        return make_ready_future<gossip_digest_ack>(ack);
    });
    ms().register_handler_oneway(messaging_verb::GOSSIP_DIGEST_ACK2, [] (gossip_digest_ack2 msg) {
        print("gossiper: Server got ack2 msg = %s\n", msg);
        // TODO: Implement processing of incoming ACK2 message
        return messaging_service::no_wait();
    });
}

bool gossiper::send_gossip(gossip_digest_syn message, std::set<inet_address> epset) {
    std::vector<inet_address> __live_endpoints(epset.begin(), epset.end());
    size_t size = __live_endpoints.size();
    if (size < 1) {
        return false;
    }
    /* Generate a random number from 0 -> size */
    std::uniform_int_distribution<int> dist(0, size - 1);
    int index = dist(_random);
    inet_address to = __live_endpoints[index];
    // if (logger.isTraceEnabled())
    //     logger.trace("Sending a GossipDigestSyn to {} ...", to);
    using RetMsg = gossip_digest_ack;
    auto id = get_shard_id(to);
    print("send_gossip: Sending to shard %s\n", id);
    ms().send_message<RetMsg>(messaging_verb::GOSSIP_DIGEST_SYN, std::move(id), std::move(message)).then([this, id] (RetMsg ack) {
        print("send_gossip: Client sent gossip_digest_syn got gossip_digest_ack reply = %s\n", ack);
        // TODO: Implement processing of incoming ACK message
        auto ep1 = inet_address("3.3.3.3");
        std::map<inet_address, endpoint_state> eps{
            {ep1, endpoint_state()},
        };
        gms::gossip_digest_ack2 ack2(std::move(eps));
        return ms().send_message_oneway<void>(messaging_verb::GOSSIP_DIGEST_ACK2, std::move(id), std::move(ack2)).then([] () {
            print("send_gossip: Client sent gossip_digest_ack2 got reply = void\n");
            return make_ready_future<>();
        });
    });
    return _seeds.count(to);
}


void gossiper::notify_failure_detector(inet_address endpoint, endpoint_state remote_endpoint_state) {
    /*
     * If the local endpoint state exists then report to the FD only
     * if the versions workout.
    */
    auto it = endpoint_state_map.find(endpoint);
    if (it != endpoint_state_map.end()) {
        auto& local_endpoint_state = it->second;
        i_failure_detector& fd = get_local_failure_detector();
        int local_generation = local_endpoint_state.get_heart_beat_state().get_generation();
        int remote_generation = remote_endpoint_state.get_heart_beat_state().get_generation();
        if (remote_generation > local_generation) {
            local_endpoint_state.update_timestamp();
            // this node was dead and the generation changed, this indicates a reboot, or possibly a takeover
            // we will clean the fd intervals for it and relearn them
            if (!local_endpoint_state.is_alive()) {
                //logger.debug("Clearing interval times for {} due to generation change", endpoint);
                fd.remove(endpoint);
            }
            fd.report(endpoint);
            return;
        }

        if (remote_generation == local_generation) {
            int local_version = get_max_endpoint_state_version(local_endpoint_state);
            int remote_version = remote_endpoint_state.get_heart_beat_state().get_heart_beat_version();
            if (remote_version > local_version) {
                local_endpoint_state.update_timestamp();
                // just a version change, report to the fd
                fd.report(endpoint);
            }
        }
    }
}

void gossiper::apply_state_locally(std::map<inet_address, endpoint_state>& map) {
    for (auto& entry : map) {
        auto& ep = entry.first;
        if (ep == get_broadcast_address() && !is_in_shadow_round()) {
            continue;
        }
        if (_just_removed_endpoints.count(ep)) {
            // if (logger.isTraceEnabled())
            //     logger.trace("Ignoring gossip for {} because it is quarantined", ep);
            continue;
        }
        /*
           If state does not exist just add it. If it does then add it if the remote generation is greater.
           If there is a generation tie, attempt to break it by heartbeat version.
           */
        endpoint_state& remote_state = entry.second;
        auto it = endpoint_state_map.find(ep);
        if (it != endpoint_state_map.end()) {
            endpoint_state& local_ep_state_ptr = it->second;
            int local_generation = local_ep_state_ptr.get_heart_beat_state().get_generation();
            int remote_generation = remote_state.get_heart_beat_state().get_generation();
            // if (logger.isTraceEnabled()) {
            //     logger.trace("{} local generation {}, remote generation {}", ep, local_generation, remote_generation);
            // }
            if (local_generation != 0 && remote_generation > local_generation + MAX_GENERATION_DIFFERENCE) {
                // assume some peer has corrupted memory and is broadcasting an unbelievable generation about another peer (or itself)
                // logger.warn("received an invalid gossip generation for peer {}; local generation = {}, received generation = {}",
                //         ep, local_generation, remote_generation);
            } else if (remote_generation > local_generation) {
                // if (logger.isTraceEnabled())
                //     logger.trace("Updating heartbeat state generation to {} from {} for {}", remote_generation, local_generation, ep);
                // major state change will handle the update by inserting the remote state directly
                handle_major_state_change(ep, remote_state);
            } else if (remote_generation == local_generation) {  //generation has not changed, apply new states
                /* find maximum state */
                int local_max_version = get_max_endpoint_state_version(local_ep_state_ptr);
                int remote_max_version = get_max_endpoint_state_version(remote_state);
                if (remote_max_version > local_max_version) {
                    // apply states, but do not notify since there is no major change
                    apply_new_states(ep, local_ep_state_ptr, remote_state);
                } else {
                    // if (logger.isTraceEnabled()) {
                    //     logger.trace("Ignoring remote version {} <= {} for {}", remote_max_version, local_max_version, ep);
                }
                if (!local_ep_state_ptr.is_alive() && !is_dead_state(local_ep_state_ptr)) { // unless of course, it was dead
                    mark_alive(ep, local_ep_state_ptr);
                }
            } else {
                // if (logger.isTraceEnabled())
                //     logger.trace("Ignoring remote generation {} < {}", remote_generation, local_generation);
            }
        } else {
            // this is a new node, report it to the FD in case it is the first time we are seeing it AND it's not alive
            get_local_failure_detector().report(ep);
            handle_major_state_change(ep, remote_state);
            fail(unimplemented::cause::GOSSIP);
        }
    }
}

void gossiper::remove_endpoint(inet_address endpoint) {
    // do subscribers first so anything in the subscriber that depends on gossiper state won't get confused
    for (shared_ptr<i_endpoint_state_change_subscriber>& subscriber : _subscribers) {
        subscriber->on_remove(endpoint);
    }

    if(_seeds.count(endpoint)) {
        build_seeds_list();
        _seeds.erase(endpoint);
        //logger.info("removed {} from _seeds, updated _seeds list = {}", endpoint, _seeds);
    }

    _live_endpoints.erase(endpoint);
    _unreachable_endpoints.erase(endpoint);
    // do not remove endpointState until the quarantine expires
    get_local_failure_detector().remove(endpoint);
    // FIXME: MessagingService
    //MessagingService.instance().resetVersion(endpoint);
    fail(unimplemented::cause::GOSSIP);
    quarantine_endpoint(endpoint);
    // FIXME: MessagingService
    //MessagingService.instance().destroyConnectionPool(endpoint);
    // if (logger.isDebugEnabled())
    //     logger.debug("removing endpoint {}", endpoint);
}

void gossiper::do_status_check() {
    // if (logger.isTraceEnabled())
    //     logger.trace("Performing status check ...");

    int64_t now = now_millis();

    // FIXME:
    // int64_t pending = ((JMXEnabledThreadPoolExecutor) StageManager.getStage(Stage.GOSSIP)).getPendingTasks();
    int64_t pending = 1;
    if (pending > 0 && _last_processed_message_at < now - 1000) {
        // FIXME: SLEEP
        // if some new messages just arrived, give the executor some time to work on them
        //Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);

        // still behind?  something's broke
        if (_last_processed_message_at < now - 1000) {
            // logger.warn("Gossip stage has {} pending tasks; skipping status check (no nodes will be marked down)", pending);
            return;
        }
    }

    for (auto& entry : endpoint_state_map) {
        const inet_address& endpoint = entry.first;
        if (endpoint == get_broadcast_address()) {
            continue;
        }

        get_local_failure_detector().interpret(endpoint);
        fail(unimplemented::cause::GOSSIP);

        auto it = endpoint_state_map.find(endpoint);
        if (it != endpoint_state_map.end()) {
            endpoint_state& ep_state = it->second;
            // check if this is a fat client. fat clients are removed automatically from
            // gossip after FatClientTimeout.  Do not remove dead states here.
            if (is_gossip_only_member(endpoint)
                && !_just_removed_endpoints.count(endpoint)
                && ((now - ep_state.get_update_timestamp().time_since_epoch().count()) > fat_client_timeout)) {
                // logger.info("FatClient {} has been silent for {}ms, removing from gossip", endpoint, FatClientTimeout);
                remove_endpoint(endpoint); // will put it in _just_removed_endpoints to respect quarantine delay
                evict_from_membershipg(endpoint); // can get rid of the state immediately
            }

            // check for dead state removal
            int64_t expire_time = get_expire_time_for_endpoint(endpoint);
            if (!ep_state.is_alive() && (now > expire_time)) {
                /* && (!StorageService.instance.getTokenMetadata().isMember(endpoint))) */
                // if (logger.isDebugEnabled()) {
                //     logger.debug("time is expiring for endpoint : {} ({})", endpoint, expire_time);
                // }
                evict_from_membershipg(endpoint);
            }
        }
    }

    for (auto it = _just_removed_endpoints.begin(); it != _just_removed_endpoints.end();) {
        auto& t= it->second;
        if ((now - t) > QUARANTINE_DELAY) {
            // if (logger.isDebugEnabled())
            //     logger.debug("{} elapsed, {} gossip quarantine over", QUARANTINE_DELAY, entry.getKey());
            it = _just_removed_endpoints.erase(it);
        } else {
            it++;
        }
    }
}

void gossiper::run() {
    print("---> In Gossip::run() \n");
    //wait on messaging service to start listening
    // MessagingService.instance().waitUntilListening();


    /* Update the local heartbeat counter. */
    //endpoint_state_map.get(FBUtilities.getBroadcastAddress()).get_heart_beat_state().updateHeartBeat();
    // if (logger.isTraceEnabled())
    //     logger.trace("My heartbeat is now {}", endpoint_state_map.get(FBUtilities.getBroadcastAddress()).get_heart_beat_state().get_heart_beat_version());
    std::vector<gossip_digest> g_digests;
    this->make_random_gossip_digest(g_digests);

    // FIXME: hack
    if (g_digests.size() > 0 || true) {
        sstring cluster_name("my cluster_name");
        sstring partioner_name("my partioner name");
        gossip_digest_syn message(cluster_name, partioner_name, g_digests);

        /* Gossip to some random live member */
        _live_endpoints.emplace(inet_address("127.0.0.1")); // FIXME: hack
        bool gossiped_to_seed = do_gossip_to_live_member(message);

        /* Gossip to some unreachable member with some probability to check if he is back up */
        do_gossip_to_unreachable_member(message);

        /* Gossip to a seed if we did not do so above, or we have seen less nodes
           than there are seeds.  This prevents partitions where each group of nodes
           is only gossiping to a subset of the seeds.

           The most straightforward check would be to check that all the seeds have been
           verified either as live or unreachable.  To avoid that computation each round,
           we reason that:

           either all the live nodes are seeds, in which case non-seeds that come online
           will introduce themselves to a member of the ring by definition,

           or there is at least one non-seed node in the list, in which case eventually
           someone will gossip to it, and then do a gossip to a random seed from the
           gossipedToSeed check.

           See CASSANDRA-150 for more exposition. */
        if (!gossiped_to_seed || _live_endpoints.size() < _seeds.size()) {
            do_gossip_to_seed(message);
        }

        do_status_check();
    }
}

} // namespace gms
