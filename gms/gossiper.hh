/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "unimplemented.hh"
#include <seastar/core/distributed.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/print.hh>
#include <seastar/rpc/rpc_types.hh>
#include "utils/atomic_vector.hh"
#include "utils/UUID.hh"
#include "utils/fb_utilities.hh"
#include "gms/failure_detector.hh"
#include "gms/versioned_value.hh"
#include "gms/application_state.hh"
#include "gms/endpoint_state.hh"
#include "gms/feature.hh"
#include "gms/gossip_digest_syn.hh"
#include "gms/gossip_digest.hh"
#include "utils/loading_shared_values.hh"
#include "utils/updateable_value.hh"
#include "utils/in.hh"
#include "message/messaging_service_fwd.hh"
#include "direct_failure_detector/failure_detector.hh"
#include <optional>
#include <algorithm>
#include <chrono>
#include <set>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/abort_source.hh>
#include <seastar/core/scheduling.hh>
#include "locator/token_metadata.hh"

namespace db {
class config;
class system_keyspace;
}

namespace gms {

class gossip_digest_syn;
class gossip_digest_ack;
class gossip_digest_ack2;
class gossip_digest;
class inet_address;
class i_endpoint_state_change_subscriber;
class gossip_get_endpoint_states_request;
class gossip_get_endpoint_states_response;

class feature_service;

using advertise_myself = bool_class<class advertise_myself_tag>;

struct syn_msg_pending {
    bool pending = false;
    std::optional<gossip_digest_syn> syn_msg;
};

struct ack_msg_pending {
    bool pending = false;
    std::optional<utils::chunked_vector<gossip_digest>> ack_msg_digest;
};

struct gossip_config {
    seastar::scheduling_group gossip_scheduling_group = seastar::scheduling_group();
    sstring cluster_name;
    std::set<inet_address> seeds;
    sstring partitioner;
    uint32_t ring_delay_ms = 30 * 1000;
    uint32_t shadow_round_ms = 300 * 1000;
    uint32_t shutdown_announce_ms = 2 * 1000;
    uint32_t skip_wait_for_gossip_to_settle = -1;
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
class gossiper : public seastar::async_sharded_service<gossiper>, public seastar::peering_sharded_service<gossiper> {
public:
    using clk = seastar::lowres_system_clock;
    using ignore_features_of_local_node = bool_class<class ignore_features_of_local_node_tag>;
private:
    using messaging_verb = netw::messaging_verb;
    using messaging_service = netw::messaging_service;
    using msg_addr = netw::msg_addr;

    void init_messaging_service_handler();
    future<> uninit_messaging_service_handler();
    future<> handle_syn_msg(msg_addr from, gossip_digest_syn syn_msg);
    future<> handle_ack_msg(msg_addr from, gossip_digest_ack ack_msg);
    future<> handle_ack2_msg(msg_addr from, gossip_digest_ack2 msg);
    future<> handle_echo_msg(inet_address from, std::optional<int64_t> generation_number_opt);
    future<> handle_shutdown_msg(inet_address from, std::optional<int64_t> generation_number_opt);
    future<> do_send_ack_msg(msg_addr from, gossip_digest_syn syn_msg);
    future<> do_send_ack2_msg(msg_addr from, utils::chunked_vector<gossip_digest> ack_msg_digest);
    future<gossip_get_endpoint_states_response> handle_get_endpoint_states_msg(gossip_get_endpoint_states_request request);
    static constexpr uint32_t _default_cpuid = 0;
    msg_addr get_msg_addr(inet_address to) const noexcept;
    void do_sort(utils::chunked_vector<gossip_digest>& g_digest_list);
    timer<lowres_clock> _scheduled_gossip_task;
    bool _enabled = false;
    semaphore _callback_running{1};
    semaphore _apply_state_locally_semaphore{100};
    seastar::gate _background_msg;
    std::unordered_map<gms::inet_address, syn_msg_pending> _syn_handlers;
    std::unordered_map<gms::inet_address, ack_msg_pending> _ack_handlers;
    bool _advertise_myself = true;
    // Map ip address and generation number
    std::unordered_map<gms::inet_address, int32_t> _advertise_to_nodes;
    future<> _failure_detector_loop_done{make_ready_future<>()} ;

    rpc::no_wait_type background_msg(sstring type, noncopyable_function<future<>(gossiper&)> fn);

public:
    // Get current generation number for the given nodes
    future<std::unordered_map<gms::inet_address, int32_t>>
    get_generation_for_nodes(std::unordered_set<gms::inet_address> nodes);
    // Only respond echo message listed in nodes with the generation number
    future<> advertise_to_nodes(std::unordered_map<gms::inet_address, int32_t> advertise_to_nodes = {});
    const sstring& get_cluster_name() const noexcept;

    const sstring& get_partitioner_name() const noexcept {
        return _gcfg.partitioner;
    }

    inet_address get_broadcast_address() const noexcept {
        return utils::fb_utilities::get_broadcast_address();
    }
    const std::set<inet_address>& get_seeds() const noexcept;

public:
    static clk::time_point inline now() noexcept { return clk::now(); }
public:
    using endpoint_locks_map = utils::loading_shared_values<inet_address, semaphore>;
    struct endpoint_permit {
        endpoint_locks_map::entry_ptr _ptr;
        semaphore_units<> _units;
    };
    future<endpoint_permit> lock_endpoint(inet_address);

private:
    /* map where key is the endpoint and value is the state associated with the endpoint */
    std::unordered_map<inet_address, endpoint_state> _endpoint_state_map;
    // Used for serializing changes to _endpoint_state_map and running of associated change listeners.
    endpoint_locks_map _endpoint_locks;

public:
    const std::vector<sstring> DEAD_STATES = {
        versioned_value::REMOVING_TOKEN,
        versioned_value::REMOVED_TOKEN,
        versioned_value::STATUS_LEFT,
    };
    const std::vector<sstring> SILENT_SHUTDOWN_STATES = {
        versioned_value::REMOVING_TOKEN,
        versioned_value::REMOVED_TOKEN,
        versioned_value::STATUS_LEFT,
        versioned_value::HIBERNATE,
        versioned_value::STATUS_BOOTSTRAPPING,
        versioned_value::STATUS_UNKNOWN,
    };
    static constexpr std::chrono::milliseconds INTERVAL{1000};
    static constexpr std::chrono::hours A_VERY_LONG_TIME{24 * 3};

    static constexpr std::chrono::milliseconds GOSSIP_SETTLE_MIN_WAIT_MS{5000};

    // Maximimum difference between remote generation value and generation
    // value this node would get if this node were restarted that we are
    // willing to accept about a peer.
    static constexpr int64_t MAX_GENERATION_DIFFERENCE = 86400 * 365;
    std::chrono::milliseconds fat_client_timeout;

    std::chrono::milliseconds quarantine_delay() const noexcept;
private:
    std::default_random_engine _random_engine{std::random_device{}()};

    /**
     * subscribers for interest in EndpointState change
     */
    atomic_vector<shared_ptr<i_endpoint_state_change_subscriber>> _subscribers;

    std::list<std::vector<inet_address>> _endpoints_to_talk_with;

    /* live member set */
    utils::chunked_vector<inet_address> _live_endpoints;
    uint64_t _live_endpoints_version = 0;

    /* nodes are being marked as alive */
    std::unordered_set<inet_address> _pending_mark_alive_endpoints;

    /* unreachable member set */
    std::unordered_map<inet_address, clk::time_point> _unreachable_endpoints;

    semaphore _endpoint_update_semaphore = semaphore(1);

    /* initial seeds for joining the cluster */
    std::set<inet_address> _seeds;

    /* map where key is endpoint and value is timestamp when this endpoint was removed from
     * gossip. We will ignore any gossip regarding these endpoints for QUARANTINE_DELAY time
     * after removal to prevent nodes from falsely reincarnating during the time when removal
     * gossip gets propagated to all nodes */
    std::map<inet_address, clk::time_point> _just_removed_endpoints;

    std::map<inet_address, clk::time_point> _expire_time_endpoint_map;

    bool _in_shadow_round = false;

    std::unordered_map<inet_address, clk::time_point> _shadow_unreachable_endpoints;
    utils::chunked_vector<inet_address> _shadow_live_endpoints;

    // replicate shard 0 live endpoints across all other shards.
    // _endpoint_update_semaphore must be held for the whole duration
    future<> replicate_live_endpoints_on_change();

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
    explicit gossiper(abort_source& as, feature_service& features, const locator::shared_token_metadata& stm, netw::messaging_service& ms, sharded<db::system_keyspace>& sys_ks, const db::config& cfg, gossip_config gcfg);

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
    future<> unregister_(shared_ptr<i_endpoint_state_change_subscriber> subscriber);

    std::set<inet_address> get_live_members() const;

    std::set<inet_address> get_live_token_owners() const;

    /**
     * @return a list of unreachable gossip participants, including fat clients
     */
    std::set<inet_address> get_unreachable_members() const;

    /**
     * @return a list of unreachable token owners
     */
    std::set<inet_address> get_unreachable_token_owners() const;

    int64_t get_endpoint_downtime(inet_address ep) const noexcept;

    /**
     * @param endpoint end point that is convicted.
     */
    future<> convict(inet_address endpoint);

    /**
     * Return either: the greatest heartbeat or application state
     *
     * @param ep_state
     * @return
     */
    int get_max_endpoint_state_version(endpoint_state state) const noexcept;


private:
    /**
     * Removes the endpoint from gossip completely
     *
     * @param endpoint endpoint to be removed from the current membership.
     */
    future<> evict_from_membership(inet_address endpoint);
public:
    /**
     * Removes the endpoint from Gossip but retains endpoint state
     */
    future<> remove_endpoint(inet_address endpoint);
    future<> force_remove_endpoint(inet_address endpoint);
private:
    /**
     * Quarantines the endpoint for QUARANTINE_DELAY
     *
     * @param endpoint
     */
    void quarantine_endpoint(inet_address endpoint);

    /**
     * Quarantines the endpoint until quarantine_start + QUARANTINE_DELAY
     *
     * @param endpoint
     * @param quarantine_start
     */
    void quarantine_endpoint(inet_address endpoint, clk::time_point quarantine_start);

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
    future<> advertise_removing(inet_address endpoint, locator::host_id host_id, locator::host_id local_host_id);

    /**
     * Handles switching the endpoint's state from REMOVING_TOKEN to REMOVED_TOKEN
     * This should only be called after advertise_removing
     *
     * @param endpoint
     * @param host_id
     */
    future<> advertise_token_removed(inet_address endpoint, locator::host_id host_id);

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
    future<int> get_current_generation_number(inet_address endpoint);
    future<int> get_current_heart_beat_version(inet_address endpoint);

    bool is_gossip_only_member(inet_address endpoint);
    bool is_safe_for_bootstrap(inet_address endpoint);
    bool is_safe_for_restart(inet_address endpoint, locator::host_id host_id);
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

    future<> do_status_check();

public:
    clk::time_point get_expire_time_for_endpoint(inet_address endpoint) const noexcept;

    const endpoint_state* get_endpoint_state_for_endpoint_ptr(inet_address ep) const noexcept;
    endpoint_state& get_endpoint_state(inet_address ep);

    endpoint_state* get_endpoint_state_for_endpoint_ptr(inet_address ep) noexcept;

    const versioned_value* get_application_state_ptr(inet_address endpoint, application_state appstate) const noexcept;
    sstring get_application_state_value(inet_address endpoint, application_state appstate) const;

    // removes ALL endpoint states; should only be called after shadow gossip
    future<> reset_endpoint_state_map();

    const std::unordered_map<inet_address, endpoint_state>& get_endpoint_states() const noexcept;

    std::vector<inet_address> get_endpoints() const;

    bool uses_host_id(inet_address endpoint) const;

    locator::host_id get_host_id(inet_address endpoint) const;

    std::set<gms::inet_address> get_nodes_with_host_id(locator::host_id host_id) const;

    std::optional<endpoint_state> get_state_for_version_bigger_than(inet_address for_endpoint, int version);

    /**
     * determine which endpoint started up earlier
     */
    int compare_endpoint_startup(inet_address addr1, inet_address addr2);

    /**
     * Return the rpc address associated with an endpoint as a string.
     * @param endpoint The endpoint to get rpc address for
     * @return the rpc address
     */
    sstring get_rpc_address(const inet_address& endpoint) const;
private:
    void update_timestamp_for_nodes(const std::map<inet_address, endpoint_state>& map);

    void mark_alive(inet_address addr, endpoint_state& local_state);

    future<> real_mark_alive(inet_address addr, endpoint_state& local_state);

    future<> mark_dead(inet_address addr, endpoint_state& local_state);

    /**
     * This method is called whenever there is a "big" change in ep state (a generation change for a known node).
     *
     * @param ep      endpoint
     * @param ep_state EndpointState for the endpoint
     */
    future<> handle_major_state_change(inet_address ep, const endpoint_state& eps);

public:
    bool is_alive(inet_address ep) const;
    bool is_dead_state(const endpoint_state& eps) const;
    // Wait for nodes to be alive on all shards
    future<> wait_alive(std::vector<gms::inet_address> nodes, std::chrono::milliseconds timeout);

    // Get live members synchronized to all shards
    future<std::set<inet_address>> get_live_members_synchronized();

    // Get live members synchronized to all shards
    future<std::set<inet_address>> get_unreachable_members_synchronized();

    future<> apply_state_locally(std::map<inet_address, endpoint_state> map);

private:
    future<> do_apply_state_locally(gms::inet_address node, const endpoint_state& remote_state, bool listener_notification);
    future<> apply_state_locally_without_listener_notification(std::unordered_map<inet_address, endpoint_state> map);

    future<> apply_new_states(inet_address addr, endpoint_state& local_state, const endpoint_state& remote_state);

    // notify that a local application state is going to change (doesn't get triggered for remote changes)
    future<> do_before_change_notifications(inet_address addr, const endpoint_state& ep_state, const application_state& ap_state, const versioned_value& new_value);

    // notify that an application state has changed
    future<> do_on_change_notifications(inet_address addr, const application_state& state, const versioned_value& value);
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
    /**
     * Start the gossiper with the generation number, preloading the map of application states before starting
     *
     * If advertise is set to false, gossip will not respond to gossip echo
     * message, so that other nodes will not mark this node as alive.
     *
     * Note 1: In practice, advertise is set to false only when the local node is
     * replacing a dead node using the same ip address of the dead node, i.e.,
     * replacing_a_node_with_same_ip is set to true, because the issue (#7312)
     * that the advertise flag fixes is limited to replacing a node with the
     * same ip address only.
     *
     * Note 2: When a node with a new ip address joins the cluster, e.g.,
     * replacing a dead node using the different ip address, with advertise =
     * false, existing nodes will not mark the node as up. So existing nodes
     * will not send gossip syn messages to the new node because the new node
     * is not in either live node list or unreachable node list.
     *
     * The new node will only include itself in the gossip syn messages, so the
     * syn message from new node to existing node will not exchange gossip
     * application states of existing nodes. Gossip exchanges node information
     * for node listed in SYN messages only.
     *
     * As a result, the new node will not learn other existing nodes in gossip
     * and existing nodes will learn the new node.
     *
     * Note 3: When a node replaces a dead node using the same ip address of
     * the dead node, with advertise = false, existing nodes will send syn
     * messages to the replacing node, because the replacing node is listed
     * in the unreachable node list.
     *
     * As a result, the replacing node will learn other existing nodes in
     * gossip and existing nodes will learn the new replacing node. Yes,
     * unreachable node is contacted with some probability, but all of the
     * existing nodes can talk to the replacing node. So the probability of
     * replacing node being talked to is pretty high.
     */
    future<> start_gossiping(int generation_nbr, std::map<application_state, versioned_value> preload_local_states = {},
            gms::advertise_myself advertise = gms::advertise_myself::yes);

public:
    /**
     *  Do a single 'shadow' round of gossip, where we do not modify any state
     */
    future<> do_shadow_round(std::unordered_set<gms::inet_address> nodes = {});

private:
    void build_seeds_list();

public:
    /**
     * Add an endpoint we knew about previously, but whose state is unknown
     */
    future<> add_saved_endpoint(inet_address ep);

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

    future<> start();
    future<> shutdown();
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
public:
    bool is_seed(const inet_address& endpoint) const;
    bool is_shutdown(const inet_address& endpoint) const;
    bool is_normal(const inet_address& endpoint) const;
    bool is_left(const inet_address& endpoint) const;
    // Check if a node is in NORMAL or SHUTDOWN status which means the node is
    // part of the token ring from the gossip point of view and operates in
    // normal status or was in normal status but is shutdown.
    bool is_normal_ring_member(const inet_address& endpoint) const;
    bool is_cql_ready(const inet_address& endpoint) const;
    bool is_silent_shutdown_state(const endpoint_state& ep_state) const;
    future<> mark_as_shutdown(const inet_address& endpoint);
    void force_newer_generation();
public:
    std::string_view get_gossip_status(const endpoint_state& ep_state) const noexcept;
    std::string_view get_gossip_status(const inet_address& endpoint) const noexcept;
public:
    future<> wait_for_gossip_to_settle();
    future<> wait_for_range_setup();
private:
    future<> wait_for_gossip(std::chrono::milliseconds, std::optional<int32_t> = {});

    uint64_t _nr_run = 0;
    uint64_t _msg_processing = 0;
    bool _gossip_settled = false;

    class msg_proc_guard;
private:
    abort_source& _abort_source;
    feature_service& _feature_service;
    const locator::shared_token_metadata& _shared_token_metadata;
    netw::messaging_service& _messaging;
    sharded<db::system_keyspace>& _sys_ks;
    utils::updateable_value<uint32_t> _failure_detector_timeout_ms;
    utils::updateable_value<int32_t> _force_gossip_generation;
    gossip_config _gcfg;
    // Get features supported by a particular node
    std::set<sstring> get_supported_features(inet_address endpoint) const;
    // Get features supported by all the nodes this node knows about
    std::set<sstring> get_supported_features(const std::unordered_map<gms::inet_address, sstring>& loaded_peer_features, ignore_features_of_local_node ignore_local_node) const;
    locator::token_metadata_ptr get_token_metadata_ptr() const noexcept;
public:
    void check_knows_remote_features(std::set<std::string_view>& local_features, const std::unordered_map<inet_address, sstring>& loaded_peer_features) const;
    future<> maybe_enable_features();
private:
    seastar::metrics::metric_groups _metrics;
public:
    void append_endpoint_state(std::stringstream& ss, const endpoint_state& state);
public:
    void check_snitch_name_matches(sstring local_snitch_name) const;
    int get_down_endpoint_count() const noexcept;
    int get_up_endpoint_count() const noexcept;
private:
    future<> failure_detector_loop();
    future<> failure_detector_loop_for_node(gms::inet_address node, int64_t gossip_generation, uint64_t live_endpoints_version);
    future<> update_live_endpoints_version();
};


struct gossip_get_endpoint_states_request {
    // Application states the sender requested
    std::unordered_set<gms::application_state> application_states;
};

struct gossip_get_endpoint_states_response {
    std::unordered_map<gms::inet_address, gms::endpoint_state> endpoint_state_map;
};

} // namespace gms
