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

#pragma once

#include "unimplemented.hh"
#include <seastar/core/distributed.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/print.hh>
#include "utils/UUID.hh"
#include "utils/fb_utilities.hh"
#include "gms/i_failure_detection_event_listener.hh"
#include "gms/failure_detector.hh"
#include "gms/versioned_value.hh"
#include "gms/application_state.hh"
#include "gms/endpoint_state.hh"
#include "gms/feature.hh"
#include "gms/gossip_digest_syn.hh"
#include "gms/gossip_digest.hh"
#include "utils/loading_shared_values.hh"
#include "utils/in.hh"
#include "message/messaging_service_fwd.hh"
#include <boost/algorithm/string.hpp>
#include <optional>
#include <algorithm>
#include <chrono>
#include <set>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/metrics_registration.hh>

namespace db {
class config;
}

namespace gms {

class gossip_digest_syn;
class gossip_digest_ack;
class gossip_digest_ack2;
class gossip_digest;
class inet_address;
class i_endpoint_state_change_subscriber;

class feature_service;

struct bind_messaging_port_tag {};
using bind_messaging_port = bool_class<bind_messaging_port_tag>;

struct syn_msg_pending {
    bool pending = false;
    std::optional<gossip_digest_syn> syn_msg;
};

struct ack_msg_pending {
    bool pending = false;
    std::optional<utils::chunked_vector<gossip_digest>> ack_msg_digest;
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
class gossiper : public i_failure_detection_event_listener, public seastar::async_sharded_service<gossiper>, public seastar::peering_sharded_service<gossiper> {
public:
    using clk = seastar::lowres_system_clock;
    using ignore_features_of_local_node = bool_class<class ignore_features_of_local_node_tag>;
private:
    using messaging_verb = netw::messaging_verb;
    using messaging_service = netw::messaging_service;
    using msg_addr = netw::msg_addr;
    netw::messaging_service& ms() {
        return netw::get_local_messaging_service();
    }
    void init_messaging_service_handler(bind_messaging_port do_bind = bind_messaging_port::yes);
    void uninit_messaging_service_handler();
    future<> handle_syn_msg(msg_addr from, gossip_digest_syn syn_msg);
    future<> handle_ack_msg(msg_addr from, gossip_digest_ack ack_msg);
    future<> handle_ack2_msg(gossip_digest_ack2 msg);
    future<> handle_echo_msg();
    future<> handle_shutdown_msg(inet_address from);
    future<> do_send_ack_msg(msg_addr from, gossip_digest_syn syn_msg);
    future<> do_send_ack2_msg(msg_addr from, utils::chunked_vector<gossip_digest> ack_msg_digest);
    static constexpr uint32_t _default_cpuid = 0;
    msg_addr get_msg_addr(inet_address to);
    void do_sort(utils::chunked_vector<gossip_digest>& g_digest_list);
    timer<lowres_clock> _scheduled_gossip_task;
    bool _enabled = false;
    std::set<inet_address> _seeds_from_config;
    sstring _cluster_name;
    semaphore _callback_running{1};
    semaphore _apply_state_locally_semaphore{100};
    std::unordered_map<gms::inet_address, syn_msg_pending> _syn_handlers;
    std::unordered_map<gms::inet_address, ack_msg_pending> _ack_handlers;
public:
    sstring get_cluster_name();
    sstring get_partitioner_name();
    inet_address get_broadcast_address() const {
        return utils::fb_utilities::get_broadcast_address();
    }
    void set_cluster_name(sstring name);
    std::set<inet_address> get_seeds();
    void set_seeds(std::set<inet_address> _seeds);
public:
    static clk::time_point inline now() { return clk::now(); }
public:
    using endpoint_locks_map = utils::loading_shared_values<inet_address, semaphore>;
    struct endpoint_permit {
        endpoint_locks_map::entry_ptr _ptr;
        semaphore_units<> _units;
    };
    future<endpoint_permit> lock_endpoint(inet_address);
public:
    /* map where key is the endpoint and value is the state associated with the endpoint */
    std::unordered_map<inet_address, endpoint_state> endpoint_state_map;
    // Used for serializing changes to endpoint_state_map and running of associated change listeners.
    endpoint_locks_map endpoint_locks;

    const std::vector<sstring> DEAD_STATES = {
        versioned_value::REMOVING_TOKEN,
        versioned_value::REMOVED_TOKEN,
        versioned_value::STATUS_LEFT,
        versioned_value::HIBERNATE
    };
    const std::vector<sstring> SILENT_SHUTDOWN_STATES = {
        versioned_value::REMOVING_TOKEN,
        versioned_value::REMOVED_TOKEN,
        versioned_value::STATUS_LEFT,
        versioned_value::HIBERNATE,
        versioned_value::STATUS_BOOTSTRAPPING,
    };
    static constexpr std::chrono::milliseconds INTERVAL{1000};
    static constexpr std::chrono::hours A_VERY_LONG_TIME{24 * 3};

    /** Maximimum difference in generation and version values we are willing to accept about a peer */
    static constexpr int64_t MAX_GENERATION_DIFFERENCE = 86400 * 365;
    std::chrono::milliseconds fat_client_timeout;

    std::chrono::milliseconds quarantine_delay();
private:

    std::default_random_engine _random_engine{std::random_device{}()};

    /**
     * subscribers for interest in EndpointState change
     *
     * @class subscribers_list - allows modifications of the list at the same
     *        time as it's being iterated using for_each() method.
     */
    class subscribers_list {
        std::list<shared_ptr<i_endpoint_state_change_subscriber>> _l;
    public:
        auto push_back(shared_ptr<i_endpoint_state_change_subscriber> s) {
            return _l.push_back(s);
        }

       /**
        * Remove the element pointing to the same object as the given one.
        * @param s shared_ptr pointing to the same object as one of the elements
        *          in the list.
        */
        void remove(shared_ptr<i_endpoint_state_change_subscriber> s) {
            _l.remove(s);
        }

        /**
         * Make a copy of the current list and iterate over a copy.
         *
         * @param Func - function to apply on each list element
         */
        template <typename Func>
        void for_each(Func&& f) {
            auto list_copy(_l);

            std::for_each(list_copy.begin(), list_copy.end(), std::forward<Func>(f));
        }
    } _subscribers;

    /* live member set */
    utils::chunked_vector<inet_address> _live_endpoints;
    std::list<inet_address> _live_endpoints_just_added;

    /* nodes are being marked as alive */
    std::unordered_set<inet_address> _pending_mark_alive_endpoints;

    /* unreachable member set */
    std::unordered_map<inet_address, clk::time_point> _unreachable_endpoints;

    /* initial seeds for joining the cluster */
    std::set<inet_address> _seeds;

    /* map where key is endpoint and value is timestamp when this endpoint was removed from
     * gossip. We will ignore any gossip regarding these endpoints for QUARANTINE_DELAY time
     * after removal to prevent nodes from falsely reincarnating during the time when removal
     * gossip gets propagated to all nodes */
    std::map<inet_address, clk::time_point> _just_removed_endpoints;

    std::map<inet_address, clk::time_point> _expire_time_endpoint_map;

    bool _in_shadow_round = false;

    clk::time_point _last_processed_message_at = now();

    std::unordered_map<inet_address, clk::time_point> _shadow_unreachable_endpoints;
    utils::chunked_vector<inet_address> _shadow_live_endpoints;

    void run();
    // Replicates given endpoint_state to all other shards.
    // The state state doesn't have to be kept alive around until completes.
    future<> replicate(inet_address, const endpoint_state&);
    // Replicates "states" from "src" to all other shards.
    // "src" and "states" must be kept alive until completes and must not change.
    future<> replicate(inet_address, const std::map<application_state, versioned_value>& src, const utils::chunked_vector<application_state>& states);
    // Replicates given value to all other shards.
    // The value must be kept alive until completes and not change.
    future<> replicate(inet_address, application_state key, const versioned_value& value);
public:
    explicit gossiper(feature_service& features, db::config& cfg);

    void set_last_processed_message_at();
    void set_last_processed_message_at(clk::time_point tp);

    void check_seen_seeds();

    /**
     * Register for interesting state changes.
     *
     * @param subscriber module which implements the IEndpointStateChangeSubscriber
     */
    void register_(shared_ptr<i_endpoint_state_change_subscriber> subscriber);

    /**
     * Unregister interest for state changes.
     *
     * @param subscriber module which implements the IEndpointStateChangeSubscriber
     */
    void unregister_(shared_ptr<i_endpoint_state_change_subscriber> subscriber);

    std::set<inet_address> get_live_members();

    std::set<inet_address> get_live_token_owners();

    /**
     * @return a list of unreachable gossip participants, including fat clients
     */
    std::set<inet_address> get_unreachable_members();

    /**
     * @return a list of unreachable token owners
     */
    std::set<inet_address> get_unreachable_token_owners();

    int64_t get_endpoint_downtime(inet_address ep);

    /**
     * This method is part of IFailureDetectionEventListener interface. This is invoked
     * by the Failure Detector when it convicts an end point.
     *
     * @param endpoint end point that is convicted.
     */
    virtual void convict(inet_address endpoint, double phi) override;

    /**
     * Return either: the greatest heartbeat or application state
     *
     * @param ep_state
     * @return
     */
    int get_max_endpoint_state_version(endpoint_state state);


private:
    /**
     * Removes the endpoint from gossip completely
     *
     * @param endpoint endpoint to be removed from the current membership.
     */
    void evict_from_membership(inet_address endpoint);
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
    void quarantine_endpoint(inet_address endpoint);

    /**
     * Quarantines the endpoint until quarantine_expiration + QUARANTINE_DELAY
     *
     * @param endpoint
     * @param quarantine_expiration
     */
    void quarantine_endpoint(inet_address endpoint, clk::time_point quarantine_expiration);

public:
    /**
     * Quarantine endpoint specifically for replacement purposes.
     * @param endpoint
     */
    void replacement_quarantine(inet_address endpoint);

    /**
     * Remove the Endpoint and evict immediately, to avoid gossiping about this node.
     * This should only be called when a token is taken over by a new IP address.
     *
     * @param endpoint The endpoint that has been replaced
     */
    void replaced_endpoint(inet_address endpoint);

private:
    /**
     * The gossip digest is built based on randomization
     * rather than just looping through the collection of live endpoints.
     *
     * @param g_digests list of Gossip Digests.
     */
    void make_random_gossip_digest(utils::chunked_vector<gossip_digest>& g_digests);

public:
    /**
     * This method will begin removing an existing endpoint from the cluster by spoofing its state
     * This should never be called unless this coordinator has had 'removenode' invoked
     *
     * @param endpoint    - the endpoint being removed
     * @param host_id      - the ID of the host being removed
     * @param local_host_id - my own host ID for replication coordination
     */
    future<> advertise_removing(inet_address endpoint, utils::UUID host_id, utils::UUID local_host_id);

    /**
     * Handles switching the endpoint's state from REMOVING_TOKEN to REMOVED_TOKEN
     * This should only be called after advertise_removing
     *
     * @param endpoint
     * @param host_id
     */
    future<> advertise_token_removed(inet_address endpoint, utils::UUID host_id);

    future<> unsafe_assassinate_endpoint(sstring address);

    /**
     * Do not call this method unless you know what you are doing.
     * It will try extremely hard to obliterate any endpoint from the ring,
     * even if it does not know about it.
     *
     * @param address
     * @throws UnknownHostException
     */
    future<> assassinate_endpoint(sstring address);

public:
    bool is_known_endpoint(inet_address endpoint);

    future<int> get_current_generation_number(inet_address endpoint);
    future<int> get_current_heart_beat_version(inet_address endpoint);

    bool is_gossip_only_member(inet_address endpoint);
    bool is_safe_for_bootstrap(inet_address endpoint);
private:
    /**
     * Returns true if the chosen target was also a seed. False otherwise
     *
     * @param message
     * @param epSet   a set of endpoint from which a random endpoint is chosen.
     * @return true if the chosen endpoint is also a seed.
     */
    future<> send_gossip(gossip_digest_syn message, std::set<inet_address> epset);

    /* Sends a Gossip message to a live member */
    future<> do_gossip_to_live_member(gossip_digest_syn message, inet_address ep);

    /* Sends a Gossip message to an unreachable member */
    future<> do_gossip_to_unreachable_member(gossip_digest_syn message);

    /* Gossip to a seed for facilitating partition healing */
    future<> do_gossip_to_seed(gossip_digest_syn prod);

    void do_status_check();

public:
    clk::time_point get_expire_time_for_endpoint(inet_address endpoint);

    const endpoint_state* get_endpoint_state_for_endpoint_ptr(inet_address ep) const;
    endpoint_state& get_endpoint_state(inet_address ep);

    endpoint_state* get_endpoint_state_for_endpoint_ptr(inet_address ep);

    const versioned_value* get_application_state_ptr(inet_address endpoint, application_state appstate) const;

    // Use with caution, copies might be expensive (see #764)
    std::optional<endpoint_state> get_endpoint_state_for_endpoint(inet_address ep) const;

    // removes ALL endpoint states; should only be called after shadow gossip
    future<> reset_endpoint_state_map();

    std::unordered_map<inet_address, endpoint_state>& get_endpoint_states();

    bool uses_host_id(inet_address endpoint);

    utils::UUID get_host_id(inet_address endpoint);

    std::optional<endpoint_state> get_state_for_version_bigger_than(inet_address for_endpoint, int version);

    /**
     * determine which endpoint started up earlier
     */
    int compare_endpoint_startup(inet_address addr1, inet_address addr2);

    void notify_failure_detector(const std::map<inet_address, endpoint_state>& remoteEpStateMap);


    void notify_failure_detector(inet_address endpoint, const endpoint_state& remote_endpoint_state);

private:
    void mark_alive(inet_address addr, endpoint_state& local_state);

    void real_mark_alive(inet_address addr, endpoint_state& local_state);

    void mark_dead(inet_address addr, endpoint_state& local_state);

    /**
     * This method is called whenever there is a "big" change in ep state (a generation change for a known node).
     *
     * @param ep      endpoint
     * @param ep_state EndpointState for the endpoint
     */
    void handle_major_state_change(inet_address ep, const endpoint_state& eps);

public:
    bool is_alive(inet_address ep) const;
    bool is_dead_state(const endpoint_state& eps) const;

    future<> apply_state_locally(std::map<inet_address, endpoint_state> map);

private:
    void apply_new_states(inet_address addr, endpoint_state& local_state, const endpoint_state& remote_state);

    // notify that a local application state is going to change (doesn't get triggered for remote changes)
    void do_before_change_notifications(inet_address addr, const endpoint_state& ep_state, const application_state& ap_state, const versioned_value& new_value);

    // notify that an application state has changed
    void do_on_change_notifications(inet_address addr, const application_state& state, const versioned_value& value);
    /* Request all the state for the endpoint in the g_digest */

    void request_all(gossip_digest& g_digest, utils::chunked_vector<gossip_digest>& delta_gossip_digest_list, int remote_generation);

    /* Send all the data with version greater than max_remote_version */
    void send_all(gossip_digest& g_digest, std::map<inet_address, endpoint_state>& delta_ep_state_map, int max_remote_version);

public:
    /*
        This method is used to figure the state that the Gossiper has but Gossipee doesn't. The delta digests
        and the delta state are built up.
    */
    void examine_gossiper(utils::chunked_vector<gossip_digest>& g_digest_list,
                         utils::chunked_vector<gossip_digest>& delta_gossip_digest_list,
                         std::map<inet_address, endpoint_state>& delta_ep_state_map);

public:
    future<> start_gossiping(int generation_number,
            bind_messaging_port do_bind = bind_messaging_port::yes);

    /**
     * Start the gossiper with the generation number, preloading the map of application states before starting
     */
    future<> start_gossiping(int generation_nbr, std::map<application_state, versioned_value> preload_local_states,
            bind_messaging_port do_bind = bind_messaging_port::yes);

public:
    /**
     *  Do a single 'shadow' round of gossip, where we do not modify any state
     *  Only used when replacing a node, to get and assume its states
     */
    future<> do_shadow_round();

private:
    void build_seeds_list();

public:
    // initialize local HB state if needed, i.e., if gossiper has never been started before.
    void maybe_initialize_local_state(int generation_nbr);

    /**
     * Add an endpoint we knew about previously, but whose state is unknown
     */
    void add_saved_endpoint(inet_address ep);

    future<> add_local_application_state(application_state state, versioned_value value);

    /**
     * Applies all states in set "atomically", as in guaranteed monotonic versions and
     * inserted into endpoint state together (and assuming same grouping, overwritten together).
     */
    future<> add_local_application_state(std::list<std::pair<application_state, versioned_value>>);

    /**
     * Intentionally overenginered to avoid very rare string copies.
     */
    future<> add_local_application_state(std::initializer_list<std::pair<application_state, utils::in<versioned_value>>>);

    // Needed by seastar::sharded
    future<> stop();
    future<> do_stop_gossiping();

public:
    bool is_enabled() const;

    void finish_shadow_round();

    bool is_in_shadow_round() const;

    void goto_shadow_round();

public:
    void add_expire_time_for_endpoint(inet_address endpoint, clk::time_point expire_time);

    static clk::time_point compute_expire_time();
public:
    void dump_endpoint_state_map();
    void debug_show();
public:
    bool is_seed(const inet_address& endpoint) const;
    bool is_shutdown(const inet_address& endpoint) const;
    bool is_normal(const inet_address& endpoint) const;
    bool is_cql_ready(const inet_address& endpoint) const;
    bool is_silent_shutdown_state(const endpoint_state& ep_state) const;
    void mark_as_shutdown(const inet_address& endpoint);
    void force_newer_generation();
public:
    sstring get_gossip_status(const endpoint_state& ep_state) const;
    sstring get_gossip_status(const inet_address& endpoint) const;
public:
    future<> wait_for_gossip_to_settle();
    future<> wait_for_range_setup();
private:
    future<> wait_for_gossip(std::chrono::milliseconds, std::optional<int32_t> = {});

    uint64_t _nr_run = 0;
    uint64_t _msg_processing = 0;
    bool _ms_registered = false;
    bool _gossiped_to_seed = false;
    bool _gossip_settled = false;

    class msg_proc_guard;
private:
    condition_variable _features_condvar;
    feature_service& _feature_service;
    db::config& _cfg;
    failure_detector _fd;
    friend class feature;
    // Get features supported by a particular node
    std::set<sstring> get_supported_features(inet_address endpoint) const;
    // Get features supported by all the nodes this node knows about
    std::set<sstring> get_supported_features(const std::unordered_map<gms::inet_address, sstring>& loaded_peer_features, ignore_features_of_local_node ignore_local_node) const;
public:
    void check_knows_remote_features(sstring local_features_string, const std::unordered_map<inet_address, sstring>& loaded_peer_features) const;
    void maybe_enable_features();
private:
    seastar::metrics::metric_groups _metrics;
    gms::versioned_value::factory _value_factory;
public:
    void append_endpoint_state(std::stringstream& ss, const endpoint_state& state);
public:
    sstring get_all_endpoint_states();
    std::map<sstring, sstring> get_simple_states();
    int get_down_endpoint_count();
    int get_up_endpoint_count();
    sstring get_endpoint_state(sstring address);
    failure_detector& fd() { return _fd; }
};

extern distributed<gossiper> _the_gossiper;

inline gossiper& get_local_gossiper() {
    return _the_gossiper.local();
}

inline distributed<gossiper>& get_gossiper() {
    return _the_gossiper;
}

future<> stop_gossiping();

inline future<sstring> get_all_endpoint_states() {
    return smp::submit_to(0, [] {
        return get_local_gossiper().get_all_endpoint_states();
    });
}

inline future<sstring> get_endpoint_state(sstring address) {
    return smp::submit_to(0, [address] {
        return get_local_gossiper().get_endpoint_state(address);
    });
}

inline future<std::map<sstring, sstring>> get_simple_states() {
    return smp::submit_to(0, [] {
        return get_local_gossiper().get_simple_states();
    });
}

inline future<int> get_down_endpoint_count() {
    return smp::submit_to(0, [] {
        return get_local_gossiper().get_down_endpoint_count();
    });
}

inline future<int> get_up_endpoint_count() {
    return smp::submit_to(0, [] {
        return get_local_gossiper().get_up_endpoint_count();
    });
}

inline future<> set_phi_convict_threshold(double phi) {
    return smp::submit_to(0, [phi] {
        get_local_gossiper().fd().set_phi_convict_threshold(phi);
    });
}

inline future<double> get_phi_convict_threshold() {
    return smp::submit_to(0, [] {
        return get_local_gossiper().fd().get_phi_convict_threshold();
    });
}

inline future<std::map<inet_address, arrival_window>> get_arrival_samples() {
    return smp::submit_to(0, [] {
        return get_local_gossiper().fd().arrival_samples();
    });
}


} // namespace gms
