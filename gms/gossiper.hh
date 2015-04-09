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
 */

#pragma once

#include "core/distributed.hh"
#include "core/shared_ptr.hh"
#include "core/print.hh"
#include "unimplemented.hh"
#include "gms/inet_address.hh"
#include "gms/endpoint_state.hh"
#include "gms/i_failure_detection_event_listener.hh"
#include "gms/i_endpoint_state_change_subscriber.hh"
#include "gms/i_failure_detector.hh"
#include "gms/gossip_digest.hh"
#include "utils/UUID.hh"
#include "gms/gossip_digest_syn.hh"
#include "gms/versioned_value.hh"

#include <boost/algorithm/string.hpp>
#include <experimental/optional>
#include <algorithm>

namespace gms {


// FIXME: Stub
template<typename T>
class MessageOut {
    T _msg;
};

/**
 * This module is responsible for Gossiping information for the local endpoint. This abstraction
 * maintains the list of live and dead endpoints. Periodically i.e. every 1 second this module
 * chooses a random node and initiates a round of Gossip with it. A round of Gossip involves 3
 * rounds of messaging. For instance if node A wants to initiate a round of Gossip with node B
 * it starts off by sending node B a GossipDigestSynMessage. Node B on receipt of this message
 * sends node A a GossipDigestAckMessage. On receipt of this message node A sends node B a
 * GossipDigestAck2Message which completes a round of Gossip. This module as and when it hears one
 * of the three above mentioned messages updates the Failure Detector with the liveness information.
 * Upon hearing a GossipShutdownMessage, this module will instantly mark the remote node as down in
 * the Failure Detector.
 */
class gossiper : public i_failure_detection_event_listener, public enable_shared_from_this<gossiper> {
private:
    inet_address get_broadcast_address() {
        // FIXME: Helper for FBUtilities.getBroadcastAddress
        return inet_address(0xffffff);
    }
public:
    static int64_t now_millis() {
        return db_clock::now().time_since_epoch().count();
    }
    static int64_t now_nanos() {
        return now_millis() * 1000;
    }
public:
    /* map where key is the endpoint and value is the state associated with the endpoint */
    std::map<inet_address, endpoint_state> endpoint_state_map;

    const std::vector<sstring> DEAD_STATES = { versioned_value::REMOVING_TOKEN, versioned_value::REMOVED_TOKEN,
                                               versioned_value::STATUS_LEFT, versioned_value::HIBERNATE };
    static constexpr const int INTERVAL_IN_MILLIS = 1000;
    // FIXME: Define StorageService.RING_DELAY -> cassandra.ring_delay_ms
    static constexpr const int QUARANTINE_DELAY = (30 * 1000) * 2; // StorageService.RING_DELAY * 2;
    static constexpr const int64_t A_VERY_LONG_TIME = 259200 * 1000; // 3 days in milliseconds

    /** Maximimum difference in generation and version values we are willing to accept about a peer */
    static constexpr const int64_t MAX_GENERATION_DIFFERENCE = 86400 * 365;
    int64_t fat_client_timeout;
private:
    std::random_device _random;
    /* subscribers for interest in EndpointState change */
    std::list<shared_ptr<i_endpoint_state_change_subscriber>> _subscribers;

    /* live member set */
    std::set<inet_address> _live_endpoints;

    /* unreachable member set */
    std::map<inet_address, int64_t> _unreachable_endpoints;

    /* initial seeds for joining the cluster */
    std::set<inet_address> _seeds;

    /* map where key is endpoint and value is timestamp when this endpoint was removed from
     * gossip. We will ignore any gossip regarding these endpoints for QUARANTINE_DELAY time
     * after removal to prevent nodes from falsely reincarnating during the time when removal
     * gossip gets propagated to all nodes */
    std::map<inet_address, int64_t> _just_removed_endpoints;

    std::map<inet_address, int64_t> _expire_time_endpoint_map;

    bool _in_shadow_round = false;

    int64_t _last_processed_message_at = now_millis();

#if 0
    private class GossipTask implements Runnable
    {
        public void run()
        {
            try
            {
                //wait on messaging service to start listening
                MessagingService.instance().waitUntilListening();

                taskLock.lock();

                /* Update the local heartbeat counter. */
                endpoint_state_map.get(FBUtilities.getBroadcastAddress()).get_heart_beat_state().updateHeartBeat();
                if (logger.isTraceEnabled())
                    logger.trace("My heartbeat is now {}", endpoint_state_map.get(FBUtilities.getBroadcastAddress()).get_heart_beat_state().get_heart_beat_version());
                final List<GossipDigest> g_digests = new ArrayList<GossipDigest>();
                Gossiper.instance.make_random_gossip_digest(g_digests);

                if (g_digests.size() > 0)
                {
                    GossipDigestSyn digestSynMessage = new GossipDigestSyn(DatabaseDescriptor.getClusterName(),
                                                                           DatabaseDescriptor.getPartitionerName(),
                                                                           g_digests);
                    MessageOut<GossipDigestSyn> message = new MessageOut<GossipDigestSyn>(MessagingService.Verb.GOSSIP_DIGEST_SYN,
                                                                                          digestSynMessage,
                                                                                          GossipDigestSyn.serializer);
                    /* Gossip to some random live member */
                    boolean gossipedToSeed = do_gossip_to_live_member(message);

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
                    if (!gossipedToSeed || _live_endpoints.size() < _seeds.size())
                        do_gossip_to_seed(message);

                    do_status_check();
                }
            }
            catch (Exception e)
            {
                JVMStabilityInspector.inspectThrowable(e);
                logger.error("Gossip error", e);
            }
            finally
            {
                taskLock.unlock();
            }
        }
    }
#endif

public:
    gossiper();
    void set_last_processed_message_at(int64_t time_in_millis) {
        _last_processed_message_at = time_in_millis;
    }

    bool seen_any_seed() {
        for (auto& entry : endpoint_state_map) {
            if (_seeds.count(entry.first)) {
                return true;
            }
            auto& state = entry.second;
            if (state.get_application_state_map().count(application_state::INTERNAL_IP) &&
                _seeds.count(inet_address(state.get_application_state(application_state::INTERNAL_IP)->value))) {
                return true;
            }
        }
        return false;
    }

    /**
     * Register for interesting state changes.
     *
     * @param subscriber module which implements the IEndpointStateChangeSubscriber
     */
    void register_(shared_ptr<i_endpoint_state_change_subscriber> subscriber) {
        _subscribers.push_back(std::move(subscriber));
    }

    /**
     * Unregister interest for state changes.
     *
     * @param subscriber module which implements the IEndpointStateChangeSubscriber
     */
    void unregister_(shared_ptr<i_endpoint_state_change_subscriber> subscriber) {
        _subscribers.remove(subscriber);
    }

    std::set<inet_address> get_live_members() {
        std::set<inet_address> live_members(_live_endpoints);
        if (!live_members.count(get_broadcast_address())) {
             live_members.insert(get_broadcast_address());
        }
        return live_members;
    }

    std::set<inet_address> get_live_token_owners() {
        std::set<inet_address> token_owners;
        for (auto& member : get_live_members()) {
            auto it = endpoint_state_map.find(member);
            // FIXME: StorageService.instance.getTokenMetadata
            if (it != endpoint_state_map.end() && !is_dead_state(it->second) /* && StorageService.instance.getTokenMetadata().isMember(member) */) {
                token_owners.insert(member);
            }
            fail(unimplemented::cause::GOSSIP);
        }
        return token_owners;
    }

    /**
     * @return a list of unreachable gossip participants, including fat clients
     */
    std::set<inet_address> get_unreachable_members() {
        std::set<inet_address> ret;
        for (auto&& x : _unreachable_endpoints) {
            ret.insert(x.first);
        }
        return ret;
    }

    /**
     * @return a list of unreachable token owners
     */
    std::set<inet_address> get_unreachable_token_owners() {
        std::set<inet_address> token_owners;
        for (auto&& x : _unreachable_endpoints) {
            auto& endpoint = x.first;
            fail(unimplemented::cause::GOSSIP);
            if (true /* StorageService.instance.getTokenMetadata().isMember(endpoint) */) {
                token_owners.insert(endpoint);
            }
        }
        return token_owners;
    }

    int64_t get_endpoint_downtime(inet_address ep) {
        auto it = _unreachable_endpoints.find(ep);
        if (it != _unreachable_endpoints.end()) {
            auto& downtime = it->second;
            return (now_nanos() - downtime) / 1000;
        } else {
            return 0L;
        }
    }

    /**
     * This method is part of IFailureDetectionEventListener interface. This is invoked
     * by the Failure Detector when it convicts an end point.
     *
     * @param endpoint end point that is convicted.
     */
    virtual void convict(inet_address endpoint, double phi) override {
        auto it = endpoint_state_map.find(endpoint);
        if (it == endpoint_state_map.end()) {
            return;
        }
        auto& state = it->second;
        if (state.is_alive() && is_dead_state(state)) {
            mark_dead(endpoint, state);
        } else {
            state.mark_dead();
        }
    }


    /**
     * Return either: the greatest heartbeat or application state
     *
     * @param ep_state
     * @return
     */
    int get_max_endpoint_state_version(endpoint_state state) {
        int max_version = state.get_heart_beat_state().get_heart_beat_version();
        for (auto& entry : state.get_application_state_map()) {
            auto& value = entry.second;
            max_version = std::max(max_version, value.version);
        }
        return max_version;
    }


private:
    /**
     * Removes the endpoint from gossip completely
     *
     * @param endpoint endpoint to be removed from the current membership.
     */
    void evict_from_membershipg(inet_address endpoint) {
        _unreachable_endpoints.erase(endpoint);
        endpoint_state_map.erase(endpoint);
        _expire_time_endpoint_map.erase(endpoint);
        quarantine_endpoint(endpoint);
        // if (logger.isDebugEnabled())
        //     logger.debug("evicting {} from gossip", endpoint);
    }
public:
    /**
     * Removes the endpoint from Gossip but retains endpoint state
     */
    void remove_endpoint(inet_address endpoint);
private:
    /**
     * Quarantines the endpoint for QUARANTINE_DELAY
     *
     * @param endpoint
     */
    void quarantine_endpoint(inet_address endpoint) {
        quarantine_endpoint(endpoint, now_millis());
    }

    /**
     * Quarantines the endpoint until quarantine_expiration + QUARANTINE_DELAY
     *
     * @param endpoint
     * @param quarantine_expiration
     */
    void quarantine_endpoint(inet_address endpoint, int64_t quarantine_expiration) {
        _just_removed_endpoints[endpoint] = quarantine_expiration;
    }

public:
    /**
     * Quarantine endpoint specifically for replacement purposes.
     * @param endpoint
     */
    void replacement_quarantine(inet_address endpoint) {
        // remember, quarantine_endpoint will effectively already add QUARANTINE_DELAY, so this is 2x
        // logger.debug("");
        quarantine_endpoint(endpoint, now_millis() + QUARANTINE_DELAY);
    }

    /**
     * Remove the Endpoint and evict immediately, to avoid gossiping about this node.
     * This should only be called when a token is taken over by a new IP address.
     *
     * @param endpoint The endpoint that has been replaced
     */
    void replaced_endpoint(inet_address endpoint) {
        remove_endpoint(endpoint);
        evict_from_membershipg(endpoint);
        replacement_quarantine(endpoint);
    }

private:
    /**
     * The gossip digest is built based on randomization
     * rather than just looping through the collection of live endpoints.
     *
     * @param g_digests list of Gossip Digests.
     */
    void make_random_gossip_digest(std::list<gossip_digest>& g_digests) {
        int generation = 0;
        int max_version = 0;

        // local epstate will be part of endpoint_state_map
        std::vector<inet_address> endpoints;
        for (auto&& x : endpoint_state_map) {
            endpoints.push_back(x.first);
        }
        std::random_shuffle(endpoints.begin(), endpoints.end());
        for (auto& endpoint : endpoints) {
            auto it = endpoint_state_map.find(endpoint);
            if (it != endpoint_state_map.end()) {
                auto& eps = it->second;
                generation = eps.get_heart_beat_state().get_generation();
                max_version = get_max_endpoint_state_version(eps);
            }
            g_digests.push_back(gossip_digest(endpoint, generation, max_version));
        }
#if 0
        if (logger.isTraceEnabled()) {
            StringBuilder sb = new StringBuilder();
            for (GossipDigest g_digest : g_digests)
            {
                sb.append(g_digest);
                sb.append(" ");
            }
            logger.trace("Gossip Digests are : {}", sb);
        }
#endif
    }

public:
    /**
     * This method will begin removing an existing endpoint from the cluster by spoofing its state
     * This should never be called unless this coordinator has had 'removenode' invoked
     *
     * @param endpoint    - the endpoint being removed
     * @param host_id      - the ID of the host being removed
     * @param local_host_id - my own host ID for replication coordination
     */
    void advertise_removing(inet_address endpoint, utils::UUID host_id, utils::UUID local_host_id) {
        auto& state = endpoint_state_map.at(endpoint);
        // remember this node's generation
        int generation = state.get_heart_beat_state().get_generation();
        // logger.info("Removing host: {}", host_id);
        // logger.info("Sleeping for {}ms to ensure {} does not change", StorageService.RING_DELAY, endpoint);
        // FIXME: sleep
        fail(unimplemented::cause::GOSSIP);
        // Uninterruptibles.sleepUninterruptibly(StorageService.RING_DELAY, TimeUnit.MILLISECONDS);
        // make sure it did not change
        auto& eps = endpoint_state_map.at(endpoint);
        if (eps.get_heart_beat_state().get_generation() != generation) {
            throw std::runtime_error(sprint("Endpoint %s generation changed while trying to remove it", endpoint));
        }

        // update the other node's generation to mimic it as if it had changed it itself
        //logger.info("Advertising removal for {}", endpoint);
        eps.update_timestamp(); // make sure we don't evict it too soon
        eps.get_heart_beat_state().force_newer_generation_unsafe();
        // FIXME:  StorageService.instance.valueFactory
        // eps.add_application_state(application_state::STATUS, StorageService.instance.valueFactory.removingNonlocal(host_id));
        // eps.add_application_state(application_state::REMOVAL_COORDINATOR, StorageService.instance.valueFactory.removalCoordinator(local_host_id));
        endpoint_state_map[endpoint] = eps;
    }

    /**
     * Handles switching the endpoint's state from REMOVING_TOKEN to REMOVED_TOKEN
     * This should only be called after advertise_removing
     *
     * @param endpoint
     * @param host_id
     */
    void advertise_token_removed(inet_address endpoint, utils::UUID host_id) {
        auto& eps = endpoint_state_map.at(endpoint);
        eps.update_timestamp(); // make sure we don't evict it too soon
        eps.get_heart_beat_state().force_newer_generation_unsafe();
        int64_t expire_time = compute_expire_time();
        // FIXME: StorageService.instance.valueFactory.removedNonlocal
        // eps.add_application_state(application_state::STATUS, StorageService.instance.valueFactory.removedNonlocal(host_id, expire_time));
        //logger.info("Completing removal of {}", endpoint);
        add_expire_time_for_endpoint(endpoint, expire_time);
        endpoint_state_map[endpoint] = eps;
        // ensure at least one gossip round occurs before returning
        // FIXME: sleep
        //Uninterruptibles.sleepUninterruptibly(INTERVAL_IN_MILLIS * 2, TimeUnit.MILLISECONDS);
        fail(unimplemented::cause::GOSSIP);
    }

    void unsafe_assassinate_endpoint(sstring address) {
        //logger.warn("Gossiper.unsafeAssassinateEndpoint is deprecated and will be removed in the next release; use assassinate_endpoint instead");
        assassinate_endpoint(address);
    }

    /**
     * Do not call this method unless you know what you are doing.
     * It will try extremely hard to obliterate any endpoint from the ring,
     * even if it does not know about it.
     *
     * @param address
     * @throws UnknownHostException
     */
    void assassinate_endpoint(sstring address) {
        inet_address endpoint(address);
        auto is_exist = endpoint_state_map.count(endpoint);
        endpoint_state&& ep_state = is_exist ? endpoint_state_map.at(endpoint) :
            endpoint_state(heart_beat_state((int) ((now_millis() + 60000) / 1000), 9999));
        //Collection<Token> tokens = null;
        // logger.warn("Assassinating {} via gossip", endpoint);
        if (is_exist) {
            // FIXME:
            fail(unimplemented::cause::GOSSIP);
#if 0
            try {
                tokens = StorageService.instance.getTokenMetadata().getTokens(endpoint);
            } catch (Throwable th) {
                JVMStabilityInspector.inspectThrowable(th);
                // TODO this is broken
                logger.warn("Unable to calculate tokens for {}.  Will use a random one", address);
                tokens = Collections.singletonList(StorageService.getPartitioner().getRandomToken());
            }
#endif
            int generation = ep_state.get_heart_beat_state().get_generation();
            int heartbeat = ep_state.get_heart_beat_state().get_heart_beat_version();
            //logger.info("Sleeping for {}ms to ensure {} does not change", StorageService.RING_DELAY, endpoint);
            //Uninterruptibles.sleepUninterruptibly(StorageService.RING_DELAY, TimeUnit.MILLISECONDS);
            // make sure it did not change

            auto it = endpoint_state_map.find(endpoint);
            if (it == endpoint_state_map.end()) {
                // logger.warn("Endpoint {} disappeared while trying to assassinate, continuing anyway", endpoint);
            } else {
                auto& new_state = it->second;
                if (new_state.get_heart_beat_state().get_generation() != generation) {
                    throw std::runtime_error(sprint("Endpoint still alive: %s generation changed while trying to assassinate it", endpoint));
                } else if (new_state.get_heart_beat_state().get_heart_beat_version() != heartbeat) {
                    throw std::runtime_error(sprint("Endpoint still alive: %s heartbeat changed while trying to assassinate it", endpoint));
                }
            }
            ep_state.update_timestamp(); // make sure we don't evict it too soon
            ep_state.get_heart_beat_state().force_newer_generation_unsafe();
        }

        // do not pass go, do not collect 200 dollars, just gtfo
        // FIXME: StorageService.instance and Sleep
        // ep_state.add_application_state(application_state::STATUS, StorageService.instance.valueFactory.left(tokens, compute_expire_time()));
        handle_major_state_change(endpoint, ep_state);
        // Uninterruptibles.sleepUninterruptibly(INTERVAL_IN_MILLIS * 4, TimeUnit.MILLISECONDS);
        //logger.warn("Finished assassinating {}", endpoint);
    }

public:
    bool is_known_endpoint(inet_address endpoint) {
        return endpoint_state_map.count(endpoint);
    }

    int get_current_generation_number(inet_address endpoint) {
        return endpoint_state_map.at(endpoint).get_heart_beat_state().get_generation();
    }
private:
    /**
     * Returns true if the chosen target was also a seed. False otherwise
     *
     * @param message
     * @param epSet   a set of endpoint from which a random endpoint is chosen.
     * @return true if the chosen endpoint is also a seed.
     */
    bool send_gossip(MessageOut<gossip_digest_syn> message, std::set<inet_address> epset) {
        std::vector<inet_address> _live_endpoints(epset.begin(), epset.end());
        size_t size = _live_endpoints.size();
        if (size < 1) {
            return false;
        }
        /* Generate a random number from 0 -> size */
        std::uniform_int_distribution<int> dist(0, size - 1);
        int index = dist(_random);
        inet_address to = _live_endpoints[index];
        // if (logger.isTraceEnabled())
        //     logger.trace("Sending a GossipDigestSyn to {} ...", to);
        // FIXME: Add MessagingService.instance().sendOneWay
        // MessagingService.instance().sendOneWay(message, to);
        return _seeds.count(to);
    }

    /* Sends a Gossip message to a live member and returns true if the recipient was a seed */
    bool do_gossip_to_live_member(MessageOut<gossip_digest_syn> message) {
        size_t size = _live_endpoints.size();
        if (size == 0) {
            return false;
        }
        return send_gossip(message, _live_endpoints);
    }

    /* Sends a Gossip message to an unreachable member */
    void do_gossip_to_unreachable_member(MessageOut<gossip_digest_syn> message) {
        double live_endpoint_count = _live_endpoints.size();
        double unreachable_endpoint_count = _unreachable_endpoints.size();
        if (unreachable_endpoint_count > 0) {
            /* based on some probability */
            double prob = unreachable_endpoint_count / (live_endpoint_count + 1);
            std::uniform_real_distribution<double> dist(0, 1);
            double rand_dbl = dist(_random);
            if (rand_dbl < prob) {
                std::set<inet_address> addrs;
                for (auto&& x : _unreachable_endpoints) {
                    addrs.insert(x.first);
                }
                send_gossip(message, addrs);
            }
        }
    }

    /* Gossip to a seed for facilitating partition healing */
    void do_gossip_to_seed(MessageOut<gossip_digest_syn> prod) {
        size_t size = _seeds.size();
        if (size > 0) {
            // FIXME: FBUtilities.getBroadcastAddress
            if (size == 1 /* && _seeds.contains(FBUtilities.getBroadcastAddress())*/) {
                return;
            }

            if (_live_endpoints.size() == 0) {
                send_gossip(prod, _seeds);
            } else {
                /* Gossip with the seed with some probability. */
                double probability = _seeds.size() / (double) (_live_endpoints.size() + _unreachable_endpoints.size());
                std::uniform_real_distribution<double> dist(0, 1);
                double rand_dbl = dist(_random);
                if (rand_dbl <= probability) {
                    send_gossip(prod, _seeds);
                }
            }
        }
    }

    bool is_gossip_only_member(inet_address endpoint) {
        auto it = endpoint_state_map.find(endpoint);
        if (it == endpoint_state_map.end()) {
            return false;
        }
        auto& eps = it->second;
        // FIXME: StorageService.instance.getTokenMetadata
        return !is_dead_state(eps) /* && !StorageService.instance.getTokenMetadata().isMember(endpoint); */;
    }

    void do_status_check();

public:
    int64_t get_expire_time_for_endpoint(inet_address endpoint) {
        /* default expire_time is A_VERY_LONG_TIME */
        auto it = _expire_time_endpoint_map.find(endpoint);
        if (it == _expire_time_endpoint_map.end()) {
            return compute_expire_time();
        } else {
            int64_t stored_time = it->second;
            return stored_time;
        }
    }

    std::experimental::optional<endpoint_state> get_endpoint_state_for_endpoint(inet_address ep) {
        auto it = endpoint_state_map.find(ep);
        if (it == endpoint_state_map.end()) {
            return {};
       } else {
            return it->second;
        }
    }

    // removes ALL endpoint states; should only be called after shadow gossip
    void reset_endpoint_state_map() {
        endpoint_state_map.clear();
        _unreachable_endpoints.clear();
        _live_endpoints.clear();
    }


    std::map<inet_address, endpoint_state>& get_endpoint_states() {
        return endpoint_state_map;
    }

    bool uses_host_id(inet_address endpoint) {
        // FIXME
        fail(unimplemented::cause::GOSSIP);
        if (true /* MessagingService.instance().knowsVersion(endpoint) */) {
             return true;
        } else if (get_endpoint_state_for_endpoint(endpoint)->get_application_state(application_state::NET_VERSION)) {
            return true;
        }
        return false;
    }

    bool uses_vnodes(inet_address endpoint) {
        return uses_host_id(endpoint) && get_endpoint_state_for_endpoint(endpoint)->get_application_state(application_state::TOKENS);
    }

    utils::UUID get_host_id(inet_address endpoint) {
        if (!uses_host_id(endpoint)) {
            throw std::runtime_error(sprint("Host %s does not use new-style tokens!", endpoint));
        }
        sstring uuid = get_endpoint_state_for_endpoint(endpoint)->get_application_state(application_state::HOST_ID)->value;
        // FIXME: Add UUID(const sstring& id) constructor
        fail(unimplemented::cause::GOSSIP);
        return utils::UUID(0, 0);
    }

    std::experimental::optional<endpoint_state> get_state_for_version_bigger_than(inet_address for_endpoint, int version) {
        std::experimental::optional<endpoint_state> reqd_endpoint_state;
        auto it = endpoint_state_map.find(for_endpoint);
        if (it != endpoint_state_map.end()) {
            auto& eps = it->second;
            /*
             * Here we try to include the Heart Beat state only if it is
             * greater than the version passed in. It might happen that
             * the heart beat version maybe lesser than the version passed
             * in and some application state has a version that is greater
             * than the version passed in. In this case we also send the old
             * heart beat and throw it away on the receiver if it is redundant.
            */
            int local_hb_version = eps.get_heart_beat_state().get_heart_beat_version();
            if (local_hb_version > version) {
                reqd_endpoint_state.emplace(eps.get_heart_beat_state());
                // if (logger.isTraceEnabled())
                //     logger.trace("local heartbeat version {} greater than {} for {}", local_hb_version, version, for_endpoint);
            }
            /* Accumulate all application states whose versions are greater than "version" variable */
            for (auto& entry : eps.get_application_state_map()) {
                auto& value = entry.second;
                if (value.version > version) {
                    if (!reqd_endpoint_state) {
                        reqd_endpoint_state.emplace(eps.get_heart_beat_state());
                    }
                    auto& key = entry.first;
                    // if (logger.isTraceEnabled())
                    //     logger.trace("Adding state {}: {}" , key, value.value);
                    reqd_endpoint_state->add_application_state(key, value);
                }
            }
        }
        return reqd_endpoint_state;
    }

    /**
     * determine which endpoint started up earlier
     */
    int compare_endpoint_startup(inet_address addr1, inet_address addr2) {
        auto ep1 = get_endpoint_state_for_endpoint(addr1);
        auto ep2 = get_endpoint_state_for_endpoint(addr2);
        assert(ep1 && ep2);
        return ep1->get_heart_beat_state().get_generation() - ep2->get_heart_beat_state().get_generation();
    }

    void notify_failure_detector(std::map<inet_address, endpoint_state> remoteEpStateMap) {
        for (auto& entry : remoteEpStateMap) {
            notify_failure_detector(entry.first, entry.second);
        }
    }


    void notify_failure_detector(inet_address endpoint, endpoint_state remote_endpoint_state);

private:
    void mark_alive(inet_address addr, endpoint_state local_state) {
        fail(unimplemented::cause::GOSSIP);

        // if (MessagingService.instance().getVersion(addr) < MessagingService.VERSION_20) {
        //     real_mark_alive(addr, local_state);
        //     return;
        // }

        local_state.mark_dead();

#if 0
        MessageOut<EchoMessage> echoMessage = new MessageOut<EchoMessage>(MessagingService.Verb.ECHO, new EchoMessage(), EchoMessage.serializer);
        logger.trace("Sending a EchoMessage to {}", addr);
        IAsyncCallback echoHandler = new IAsyncCallback()
        {
            public boolean isLatencyForSnitch()
            {
                return false;
            }

            public void response(MessageIn msg)
            {
                real_mark_alive(addr, local_state);
            }
        };
        MessagingService.instance().sendRR(echoMessage, addr, echoHandler);
#endif
    }

    void real_mark_alive(inet_address addr, endpoint_state local_state) {
        // if (logger.isTraceEnabled())
        //         logger.trace("marking as alive {}", addr);
        local_state.mark_alive();
        local_state.update_timestamp(); // prevents do_status_check from racing us and evicting if it was down > A_VERY_LONG_TIME
        _live_endpoints.insert(addr);
        _unreachable_endpoints.erase(addr);
        _expire_time_endpoint_map.erase(addr);
        // logger.debug("removing expire time for endpoint : {}", addr);
        // logger.info("inet_address {} is now UP", addr);
        for (shared_ptr<i_endpoint_state_change_subscriber>& subscriber : _subscribers)
            subscriber->on_alive(addr, local_state);
        // if (logger.isTraceEnabled())
        //     logger.trace("Notified {}", _subscribers);
    }

    void mark_dead(inet_address addr, endpoint_state local_state) {
        // if (logger.isTraceEnabled())
        //     logger.trace("marking as down {}", addr);
        local_state.mark_dead();
        _live_endpoints.erase(addr);
        _unreachable_endpoints[addr] = now_nanos();
        // logger.info("inet_address {} is now DOWN", addr);
        for (shared_ptr<i_endpoint_state_change_subscriber>& subscriber : _subscribers)
            subscriber->on_dead(addr, local_state);
        // if (logger.isTraceEnabled())
        //     logger.trace("Notified {}", _subscribers);
    }

    /**
     * This method is called whenever there is a "big" change in ep state (a generation change for a known node).
     *
     * @param ep      endpoint
     * @param ep_state EndpointState for the endpoint
     */
    void handle_major_state_change(inet_address ep, endpoint_state eps) {
        if (!is_dead_state(eps)) {
            if (endpoint_state_map.count(ep))  {
                //logger.info("Node {} has restarted, now UP", ep);
            } else {
                //logger.info("Node {} is now part of the cluster", ep);
            }
        }
        // if (logger.isTraceEnabled())
        //     logger.trace("Adding endpoint state for {}", ep);
        endpoint_state_map[ep] = eps;

        // the node restarted: it is up to the subscriber to take whatever action is necessary
        for (auto& subscriber : _subscribers) {
            subscriber->on_restart(ep, eps);
        }

        if (!is_dead_state(eps)) {
            mark_alive(ep, eps);
        } else {
            //logger.debug("Not marking {} alive due to dead state", ep);
            mark_dead(ep, eps);
        }
        for (auto& subscriber : _subscribers) {
            subscriber->on_join(ep, eps);
        }
    }

public:
    bool is_dead_state(endpoint_state eps) {
        if (!eps.get_application_state(application_state::STATUS)) {
            return false;
        }
        auto value = eps.get_application_state(application_state::STATUS)->value;
        std::vector<sstring> pieces;
        boost::split(pieces, value, boost::is_any_of(","));
        assert(pieces.size() > 0);
        sstring state = pieces[0];
        for (auto& deadstate : DEAD_STATES) {
            if (state == deadstate) {
                return true;
            }
        }
        return false;
    }

    void apply_state_locally(std::map<inet_address, endpoint_state>& map);

private:
    void apply_new_states(inet_address addr, endpoint_state local_state, endpoint_state remote_state) {
        // don't assert here, since if the node restarts the version will go back to zero
        //int oldVersion = local_state.get_heart_beat_state().get_heart_beat_version();

        local_state.set_heart_beat_state(remote_state.get_heart_beat_state());
        // if (logger.isTraceEnabled()) {
        //     logger.trace("Updating heartbeat state version to {} from {} for {} ...",
        //     local_state.get_heart_beat_state().get_heart_beat_version(), oldVersion, addr);
        // }

        // we need to make two loops here, one to apply, then another to notify,
        // this way all states in an update are present and current when the notifications are received
        for (auto& remote_entry : remote_state.get_application_state_map()) {
            auto& remote_key = remote_entry.first;
            auto& remote_value = remote_entry.second;
            assert(remote_state.get_heart_beat_state().get_generation() == local_state.get_heart_beat_state().get_generation());
            local_state.add_application_state(remote_key, remote_value);
        }
        for (auto& entry : remote_state.get_application_state_map()) {
            do_on_change_notifications(addr, entry.first, entry.second);
        }
    }

    // notify that a local application state is going to change (doesn't get triggered for remote changes)
    void do_before_change_notifications(inet_address addr, endpoint_state& ep_state, application_state& ap_state, versioned_value& new_value) {
        for (auto& subscriber : _subscribers) {
            subscriber->before_change(addr, ep_state, ap_state, new_value);
        }
    }

    // notify that an application state has changed
    void do_on_change_notifications(inet_address addr, const application_state& state, versioned_value& value) {
        for (auto& subscriber : _subscribers) {
            subscriber->on_change(addr, state, value);
        }
    }
    /* Request all the state for the endpoint in the g_digest */
    void request_all(gossip_digest g_digest, std::vector<gossip_digest> delta_gossip_digest_list, int remote_generation) {
        /* We are here since we have no data for this endpoint locally so request everthing. */
        delta_gossip_digest_list.emplace_back(g_digest.get_endpoint(), remote_generation, 0);
        // if (logger.isTraceEnabled())
        //     logger.trace("request_all for {}", g_digest.get_endpoint());
    }

    /* Send all the data with version greater than max_remote_version */
    void send_all(gossip_digest g_digest, std::map<inet_address, endpoint_state>& delta_ep_state_map, int max_remote_version) {
        auto ep = g_digest.get_endpoint();
        auto local_ep_state_ptr = get_state_for_version_bigger_than(ep, max_remote_version);
        if (local_ep_state_ptr) {
            delta_ep_state_map[ep] = *local_ep_state_ptr;
        }
    }

public:
    /*
        This method is used to figure the state that the Gossiper has but Gossipee doesn't. The delta digests
        and the delta state are built up.
    */
    void examine_gossiper(std::vector<gossip_digest>& g_digest_list,
                         std::vector<gossip_digest>& delta_gossip_digest_list,
                         std::map<inet_address, endpoint_state>& delta_ep_state_map) {
        if (g_digest_list.size() == 0) {
            /* we've been sent a *completely* empty syn, which should normally
             * never happen since an endpoint will at least send a syn with
             * itself.  If this is happening then the node is attempting shadow
             * gossip, and we should reply with everything we know.
             */
            // logger.debug("Shadow request received, adding all states");
            for (auto& entry : endpoint_state_map) {
                g_digest_list.emplace_back(entry.first, 0, 0);
            }
        }
        for (gossip_digest& g_digest : g_digest_list) {
            int remote_generation = g_digest.get_generation();
            int max_remote_version = g_digest.get_max_version();
            /* Get state associated with the end point in digest */
            auto it = endpoint_state_map.find(g_digest.get_endpoint());
            /* Here we need to fire a GossipDigestAckMessage. If we have some
             * data associated with this endpoint locally then we follow the
             * "if" path of the logic. If we have absolutely nothing for this
             * endpoint we need to request all the data for this endpoint. 
             */
            if (it != endpoint_state_map.end()) {
                endpoint_state& ep_state_ptr = it->second;
                int local_generation = ep_state_ptr.get_heart_beat_state().get_generation();
                /* get the max version of all keys in the state associated with this endpoint */
                int max_local_version = get_max_endpoint_state_version(ep_state_ptr);
                if (remote_generation == local_generation && max_remote_version == max_local_version) {
                    continue;
                }

                if (remote_generation > local_generation) {
                    /* we request everything from the gossiper */
                    request_all(g_digest, delta_gossip_digest_list, remote_generation);
                } else if (remote_generation < local_generation) {
                    /* send all data with generation = localgeneration and version > 0 */
                    send_all(g_digest, delta_ep_state_map, 0);
                } else if (remote_generation == local_generation) {
                    /*
                     * If the max remote version is greater then we request the
                     * remote endpoint send us all the data for this endpoint
                     * with version greater than the max version number we have
                     * locally for this endpoint.
                     *
                     * If the max remote version is lesser, then we send all
                     * the data we have locally for this endpoint with version
                     * greater than the max remote version.
                     */
                    if (max_remote_version > max_local_version) {
                        delta_gossip_digest_list.emplace_back(g_digest.get_endpoint(), remote_generation, max_local_version);
                    } else if (max_remote_version < max_local_version) {
                        /* send all data with generation = localgeneration and version > max_remote_version */
                        send_all(g_digest, delta_ep_state_map, max_remote_version);
                    }
                }
            } else {
                /* We are here since we have no data for this endpoint locally so request everything. */
                request_all(g_digest, delta_gossip_digest_list, remote_generation);
            }
        }
    }
#if 0
public:
    void start(int generationNumber) {
        start(generationNumber, new HashMap<ApplicationState, VersionedValue>());
    }

    /**
     * Start the gossiper with the generation number, preloading the map of application states before starting
     */
    void start(int generation_nbr, Map<ApplicationState, VersionedValue> preloadLocalStates) {
        build_seeds_list();
        /* initialize the heartbeat state for this localEndpoint */
        maybe_initialize_local_state(generation_nbr);
        endpoint_state local_state = endpoint_state_map.get(FBUtilities.getBroadcastAddress());
        for (Map.Entry<ApplicationState, VersionedValue> entry : preloadLocalStates.entrySet())
            local_state.add_application_state(entry.getKey(), entry.getValue());

        //notify snitches that Gossiper is about to start
        DatabaseDescriptor.getEndpointSnitch().gossiperStarting();
        if (logger.isTraceEnabled())
            logger.trace("gossip started with generation {}", local_state.get_heart_beat_state().get_generation());

        scheduledGossipTask = executor.scheduleWithFixedDelay(new GossipTask(),
                                                              Gossiper.INTERVAL_IN_MILLIS,
                                                              Gossiper.INTERVAL_IN_MILLIS,
                                                              TimeUnit.MILLISECONDS);
    }
#endif

#if 0
    /**
     *  Do a single 'shadow' round of gossip, where we do not modify any state
     *  Only used when replacing a node, to get and assume its states
     */
    public void doShadowRound()
    {
        build_seeds_list();
        // send a completely empty syn
        List<GossipDigest> g_digests = new ArrayList<GossipDigest>();
        GossipDigestSyn digestSynMessage = new GossipDigestSyn(DatabaseDescriptor.getClusterName(),
                DatabaseDescriptor.getPartitionerName(),
                g_digests);
        MessageOut<GossipDigestSyn> message = new MessageOut<GossipDigestSyn>(MessagingService.Verb.GOSSIP_DIGEST_SYN,
                digestSynMessage,
                GossipDigestSyn.serializer);
        _in_shadow_round = true;
        for (inet_address seed : _seeds)
            MessagingService.instance().sendOneWay(message, seed);
        int slept = 0;
        try
        {
            while (true)
            {
                Thread.sleep(1000);
                if (!_in_shadow_round)
                    break;
                slept += 1000;
                if (slept > StorageService.RING_DELAY)
                    throw new RuntimeException("Unable to gossip with any _seeds");
            }
        }
        catch (InterruptedException wtf)
        {
            throw new RuntimeException(wtf);
        }
    }
#endif
private:
    void build_seeds_list() {
        // for (inet_address seed : DatabaseDescriptor.getSeeds())
        // {
        //     if (seed.equals(FBUtilities.getBroadcastAddress()))
        //         continue;
        //     _seeds.add(seed);
        // }
    }

public:
    // initialize local HB state if needed, i.e., if gossiper has never been started before.
    void maybe_initialize_local_state(int generation_nbr) {
        heart_beat_state hb_state(generation_nbr);
        endpoint_state local_state(hb_state);
        local_state.mark_alive();
        // FIXME
        // endpoint_state_map.putIfAbsent(FBUtilities.getBroadcastAddress(), local_state);
        inet_address ep = get_broadcast_address();
        auto it = endpoint_state_map.find(ep);
        if (it == endpoint_state_map.end()) {
            endpoint_state_map.emplace(ep, local_state);
        }
    }

    /**
     * Add an endpoint we knew about previously, but whose state is unknown
     */
    void add_saved_endpoint(inet_address ep) {
        if (ep == get_broadcast_address()) {
            // logger.debug("Attempt to add self as saved endpoint");
            return;
        }

        //preserve any previously known, in-memory data about the endpoint (such as DC, RACK, and so on)
        auto ep_state = endpoint_state(heart_beat_state(0));
        auto it = endpoint_state_map.find(ep);
        if (it != endpoint_state_map.end()) {
            ep_state = it->second;
            // logger.debug("not replacing a previous ep_state for {}, but reusing it: {}", ep, ep_state);
            ep_state.set_heart_beat_state(heart_beat_state(0));
        }
        ep_state.mark_dead();
        endpoint_state_map[ep] = ep_state;
        _unreachable_endpoints[ep] = now_nanos();
        // if (logger.isTraceEnabled())
        //     logger.trace("Adding saved endpoint {} {}", ep, ep_state.get_heart_beat_state().get_generation());
    }

    void add_local_application_state(application_state state, versioned_value value) {
        inet_address ep_addr = get_broadcast_address();
        assert(endpoint_state_map.count(ep_addr));
        endpoint_state& ep_state = endpoint_state_map.at(ep_addr);
        // Fire "before change" notifications:
        do_before_change_notifications(ep_addr, ep_state, state, value);
        // Notifications may have taken some time, so preventively raise the version
        // of the new value, otherwise it could be ignored by the remote node
        // if another value with a newer version was received in the meantime:
        // FIXME:
        // value = StorageService.instance.valueFactory.cloneWithHigherVersion(value);
        // Add to local application state and fire "on change" notifications:
        ep_state.add_application_state(state, value);
        do_on_change_notifications(ep_addr, state, value);
    }

    void add_lccal_application_states(std::list<std::pair<application_state, versioned_value>> states) {
        // Note: The taskLock in Origin code is removed, we can probaby use a
        // simple data structure here
        for (std::pair<application_state, versioned_value>& pair : states) {
            add_local_application_state(pair.first, pair.second);
        }
    }

#if 0
    public void stop()
    {
    	if (scheduledGossipTask != null)
    		scheduledGossipTask.cancel(false);
        logger.info("Announcing shutdown");
        Uninterruptibles.sleepUninterruptibly(INTERVAL_IN_MILLIS * 2, TimeUnit.MILLISECONDS);
        MessageOut message = new MessageOut(MessagingService.Verb.GOSSIP_SHUTDOWN);
        for (inet_address ep : _live_endpoints)
            MessagingService.instance().sendOneWay(message, ep);
    }

    public boolean isEnabled()
    {
        return (scheduledGossipTask != null) && (!scheduledGossipTask.isCancelled());
    }
#endif

public:
    void finish_shadow_round() {
        if (_in_shadow_round) {
            _in_shadow_round = false;
        }
    }

    bool is_in_shadow_round() {
        return _in_shadow_round;
    }

#if 0
    @VisibleForTesting
    public void initializeNodeUnsafe(inet_address addr, UUID uuid, int generation_nbr)
    {
        HeartBeatState hb_state = new HeartBeatState(generation_nbr);
        endpoint_state new_state = new endpoint_state(hb_state);
        new_state.mark_alive();
        endpoint_state oldState = endpoint_state_map.putIfAbsent(addr, new_state);
        endpoint_state local_state = oldState == null ? new_state : oldState;

        // always add the version state
        local_state.add_application_state(application_state::NET_VERSION, StorageService.instance.valueFactory.networkVersion());
        local_state.add_application_state(application_state::HOST_ID, StorageService.instance.valueFactory.host_id(uuid));
    }

    @VisibleForTesting
    public void injectApplicationState(inet_address endpoint, application_state state, versioned_value value)
    {
        endpoint_state local_state = endpoint_state_map.get(endpoint);
        local_state.add_application_state(state, value);
    }

    public int64_t get_endpoint_downtime(String address) throws UnknownHostException
    {
        return get_endpoint_downtime(inet_address.getByName(address));
    }

    public int getCurrentGenerationNumber(String address) throws UnknownHostException
    {
        return getCurrentGenerationNumber(inet_address.getByName(address));
    }
#endif

public:
    void add_expire_time_for_endpoint(inet_address endpoint, int64_t expire_time) {
        // if (logger.isDebugEnabled()) {
        //     logger.debug("adding expire time for endpoint : {} ({})", endpoint, expire_time);
        // }
        _expire_time_endpoint_map[endpoint] = expire_time;
    }

    static int64_t compute_expire_time() {
        return now_millis() + A_VERY_LONG_TIME;
    }
};

extern distributed<gossiper> _the_gossiper;
inline gossiper& get_local_gossiper() {
    assert(engine().cpu_id() == 0);
    return _the_gossiper.local();
}

} // namespace gms
