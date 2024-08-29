/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <seastar/core/distributed.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/print.hh>
#include <seastar/rpc/rpc_types.hh>
#include <seastar/util/source_location-compat.hh>
#include "utils/atomic_vector.hh"
#include "utils/UUID.hh"
#include "gms/generation-number.hh"
#include "gms/versioned_value.hh"
#include "gms/application_state.hh"
#include "gms/endpoint_state.hh"
#include "gms/gossip_digest_syn.hh"
#include "gms/gossip_digest.hh"
#include "utils/loading_shared_values.hh"
#include "utils/updateable_value.hh"
#include "message/messaging_service_fwd.hh"
#include <optional>
#include <chrono>
#include <set>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/abort_source.hh>
#include <seastar/core/scheduling.hh>
#include "locator/token_metadata.hh"
#include "locator/types.hh"

namespace gms {

class gossip_digest_syn;
class gossip_digest_ack;
class gossip_digest_ack2;
class gossip_digest;
class inet_address;
class i_endpoint_state_change_subscriber;
class gossip_get_endpoint_states_request;
class gossip_get_endpoint_states_response;

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
    utils::UUID group0_id;
    std::set<inet_address> seeds;
    sstring partitioner;
    uint32_t ring_delay_ms = 30 * 1000;
    uint32_t shadow_round_ms = 300 * 1000;
    uint32_t shutdown_announce_ms = 2 * 1000;
    uint32_t skip_wait_for_gossip_to_settle = -1;
    utils::updateable_value<uint32_t> failure_detector_timeout_ms;
    utils::updateable_value<int32_t> force_gossip_generation;
};

struct loaded_endpoint_state {
    gms::inet_address endpoint;
    std::unordered_set<dht::token> tokens;
    std::optional<locator::endpoint_dc_rack> opt_dc_rack;
    std::optional<gms::versioned_value> opt_status;
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
    using generation_for_nodes = std::unordered_map<gms::inet_address, generation_type>;
private:
    using messaging_verb = netw::messaging_verb;
    using messaging_service = netw::messaging_service;
    using msg_addr = netw::msg_addr;

    void init_messaging_service_handler();
    future<> uninit_messaging_service_handler();
    future<> handle_syn_msg(msg_addr from, gossip_digest_syn syn_msg);
    future<> handle_ack_msg(msg_addr from, gossip_digest_ack ack_msg);
    future<> handle_ack2_msg(msg_addr from, gossip_digest_ack2 msg);
    future<> handle_echo_msg(inet_address from, const locator::host_id* id, seastar::rpc::opt_time_point, std::optional<int64_t> generation_number_opt, bool notify_up);
    future<> handle_shutdown_msg(inet_address from, std::optional<int64_t> generation_number_opt);
    future<> do_send_ack_msg(msg_addr from, gossip_digest_syn syn_msg);
    future<> do_send_ack2_msg(msg_addr from, utils::chunked_vector<gossip_digest> ack_msg_digest);
    future<gossip_get_endpoint_states_response> handle_get_endpoint_states_msg(gossip_get_endpoint_states_request request);
    static constexpr uint32_t _default_cpuid = 0;
    msg_addr get_msg_addr(inet_address to) const noexcept;
    void do_sort(utils::chunked_vector<gossip_digest>& g_digest_list) const;
    timer<lowres_clock> _scheduled_gossip_task;
    bool _enabled = false;
    semaphore _callback_running{1};
    semaphore _apply_state_locally_semaphore{100};
    seastar::gate _background_msg;
    std::unordered_map<gms::inet_address, syn_msg_pending> _syn_handlers;
    std::unordered_map<gms::inet_address, ack_msg_pending> _ack_handlers;
    bool _advertise_myself = true;
    // Map ip address and generation number
    generation_for_nodes _advertise_to_nodes;
    future<> _failure_detector_loop_done{make_ready_future<>()} ;

    future<rpc::no_wait_type> background_msg(sstring type, noncopyable_function<future<>(gossiper&)> fn);

public:
    // Get current generation number for the given nodes
    future<generation_for_nodes>
    get_generation_for_nodes(std::unordered_set<gms::inet_address> nodes) const;
    // Only respond echo message listed in nodes with the generation number
    future<> advertise_to_nodes(generation_for_nodes advertise_to_nodes = {});
    const sstring& get_cluster_name() const noexcept;
    void set_group0_id(utils::UUID group0_id);
    const utils::UUID& get_group0_id() const noexcept;

    const sstring& get_partitioner_name() const noexcept {
        return _gcfg.partitioner;
    }

    locator::host_id my_host_id() const noexcept {
        return get_token_metadata_ptr()->get_topology().my_host_id();
    }
    inet_address get_broadcast_address() const noexcept {
        return get_token_metadata_ptr()->get_topology().my_address();
    }
    const std::set<inet_address>& get_seeds() const noexcept;

public:
    static clk::time_point inline now() noexcept { return clk::now(); }
public:
    struct endpoint_lock_entry {
        semaphore sem;
        permit_id pid;
        semaphore_units<> units;
        size_t holders = 0;
        std::optional<seastar::compat::source_location> first_holder;
        // last_holder is the caller of endpoint_permit who last took this entry,
        // it might not be a current holder (the permit might've been destroyed)
        std::optional<seastar::compat::source_location> last_holder;

        endpoint_lock_entry() noexcept;
    };
    using endpoint_locks_map = utils::loading_shared_values<inet_address, endpoint_lock_entry>;
    class endpoint_permit {
        endpoint_locks_map::entry_ptr _ptr;
        permit_id _permit_id;
        inet_address _addr;
        seastar::compat::source_location _caller;
    public:
        endpoint_permit(endpoint_locks_map::entry_ptr&& ptr, inet_address addr, seastar::compat::source_location caller) noexcept;
        endpoint_permit(endpoint_permit&&) noexcept;
        ~endpoint_permit();
        bool release() noexcept;
        const permit_id& id() const noexcept { return _permit_id; }
    };
    // Must be called on shard 0
    future<endpoint_permit> lock_endpoint(inet_address, permit_id pid, seastar::compat::source_location l = seastar::compat::source_location::current());

private:
    void permit_internal_error(const inet_address& addr, permit_id pid);
    void verify_permit(const inet_address& addr, permit_id pid) {
        if (!pid) {
            permit_internal_error(addr, pid);
        }
    }

    /* map where key is the endpoint and value is the state associated with the endpoint */
    std::unordered_map<inet_address, endpoint_state_ptr> _endpoint_state_map;
    // Used for serializing changes to _endpoint_state_map and running of associated change listeners.
    endpoint_locks_map _endpoint_locks;

public:
    const std::vector<sstring> DEAD_STATES = {
        versioned_value::REMOVED_TOKEN,
        versioned_value::STATUS_LEFT,
    };
    const std::vector<sstring> SILENT_SHUTDOWN_STATES = {
        versioned_value::REMOVED_TOKEN,
        versioned_value::STATUS_LEFT,
        versioned_value::STATUS_BOOTSTRAPPING,
        versioned_value::STATUS_UNKNOWN,
    };
    static constexpr std::chrono::milliseconds INTERVAL{1000};
    static constexpr std::chrono::hours A_VERY_LONG_TIME{24 * 3};

    static constexpr std::chrono::milliseconds GOSSIP_SETTLE_MIN_WAIT_MS{5000};

    // Maximum difference between remote generation value and generation
    // value this node would get if this node were restarted that we are
    // willing to accept about a peer.
    static constexpr generation_type::value_type MAX_GENERATION_DIFFERENCE = 86400 * 365;
    std::chrono::milliseconds fat_client_timeout;

    std::chrono::milliseconds quarantine_delay() const noexcept;
private:
    mutable std::default_random_engine _random_engine{std::random_device{}()};

    /**
     * subscribers for interest in EndpointState change
     */
    atomic_vector<shared_ptr<i_endpoint_state_change_subscriber>> _subscribers;

    std::list<std::vector<inet_address>> _endpoints_to_talk_with;

    /* live member set */
    std::unordered_set<inet_address> _live_endpoints;
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

    service::topology_state_machine* _topo_sm = nullptr;

    // Must be called on shard 0.
    future<semaphore_units<>> lock_endpoint_update_semaphore();

    struct live_and_unreachable_endpoints {
        std::unordered_set<inet_address> live;
        std::unordered_map<inet_address, clk::time_point> unreachable;
    };

    // Must be called on shard 0.
    // Update _live_endpoints and/or _unreachable_endpoints
    // on shard 0, after acquiring `lock_endpoint_update_semaphore`.
    //
    // The called function modifies a copy of live_and_unreachable_endpoints
    // which is then copied to temporary copies for all shards
    // and then applied atomcically on all shards.
    //
    // Finally, the `on_success` callback is called on shard 0 iff replication was successful.
    future<> mutate_live_and_unreachable_endpoints(std::function<void(live_and_unreachable_endpoints&)> func,
            std::function<void(gossiper&)> on_success = [] (gossiper&) {});

    // replicate shard 0 live and unreachable endpoints sets across all other shards.
    // _endpoint_update_semaphore must be held for the whole duration
    future<> replicate_live_endpoints_on_change(foreign_ptr<std::unique_ptr<live_and_unreachable_endpoints>>, uint64_t new_live_endpoints_version);

    void run();
    // Replicates given endpoint_state to all other shards.
    // The state state doesn't have to be kept alive around until completes.
    // Must be called under lock_endpoint.
    future<> replicate(inet_address, endpoint_state, permit_id);
public:
    explicit gossiper(abort_source& as, const locator::shared_token_metadata& stm, netw::messaging_service& ms, gossip_config gcfg);

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

    /**
     * @return a list of unreachable nodes
     */
    std::set<inet_address> get_unreachable_nodes() const;

    int64_t get_endpoint_downtime(inet_address ep) const noexcept;

    /**
     * Return either: the greatest heartbeat or application state
     *
     * @param ep_state
     * @return
     */
    version_type get_max_endpoint_state_version(const endpoint_state& state) const noexcept;

    void set_topology_state_machine(service::topology_state_machine* m) {
        _topo_sm = m;
        // In raft topology mode the coodinator maintains banned nodes list
        _just_removed_endpoints.clear();
    }

private:
    /**
     * @param endpoint end point that is convicted.
     */
    future<> convict(inet_address endpoint);

    /**
     * Removes the endpoint from gossip completely
     *
     * @param endpoint endpoint to be removed from the current membership.
     *
     * Must be called under lock_endpoint.
     */
    future<> evict_from_membership(inet_address endpoint, permit_id);
public:
    /**
     * Removes the endpoint from Gossip but retains endpoint state
     */
    future<> remove_endpoint(inet_address endpoint, permit_id);
    future<> force_remove_endpoint(inet_address endpoint, permit_id);
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
    void make_random_gossip_digest(utils::chunked_vector<gossip_digest>& g_digests) const;

public:
    /**
     * Handles switching the endpoint's state from REMOVING_TOKEN to REMOVED_TOKEN
     *
     * @param endpoint
     * @param host_id
     */
    future<> advertise_token_removed(inet_address endpoint, locator::host_id host_id, permit_id);

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
    future<generation_type> get_current_generation_number(inet_address endpoint) const;
    future<version_type> get_current_heart_beat_version(inet_address endpoint) const;

    bool is_gossip_only_member(inet_address endpoint) const;
    bool is_safe_for_bootstrap(inet_address endpoint) const;
    bool is_safe_for_restart(inet_address endpoint, locator::host_id host_id) const;
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

    const std::unordered_map<inet_address, endpoint_state_ptr>& get_endpoint_states() const noexcept;

public:
    clk::time_point get_expire_time_for_endpoint(inet_address endpoint) const noexcept;

    // Gets a shared pointer to the endpoint_state, if exists.
    // Otherwise, returns a null ptr.
    // The endpoint_state is immutable (except for its update_timestamp), guaranteed not to change while
    // the endpoint_state_ptr is held.
    endpoint_state_ptr get_endpoint_state_ptr(inet_address ep) const noexcept;

    // Return this node's endpoint_state_ptr
    endpoint_state_ptr get_this_endpoint_state_ptr() const noexcept {
        return get_endpoint_state_ptr(get_broadcast_address());
    }

    const versioned_value* get_application_state_ptr(inet_address endpoint, application_state appstate) const noexcept;
    sstring get_application_state_value(inet_address endpoint, application_state appstate) const;

    // removes ALL endpoint states; should only be called after shadow gossip.
    // Must be called on shard 0
    future<> reset_endpoint_state_map();

    std::vector<inet_address> get_endpoints() const;

    size_t num_endpoints() const noexcept {
        return _endpoint_state_map.size();
    }

    // Calls func for each endpoint_state.
    // Called function must not yield
    void for_each_endpoint_state(std::function<void(const inet_address&, const endpoint_state&)> func) const {
        for_each_endpoint_state_until([func = std::move(func)] (const inet_address& node, const endpoint_state& eps) {
            func(node, eps);
            return stop_iteration::no;
        });
    }

    // Calls func for each endpoint_state until it returns stop_iteration::yes
    // Returns stop_iteration::yes iff `func` returns stop_iteration::yes.
    // Called function must not yield
    stop_iteration for_each_endpoint_state_until(std::function<stop_iteration(const inet_address&, const endpoint_state&)>) const;

    locator::host_id get_host_id(inet_address endpoint) const;

    std::set<gms::inet_address> get_nodes_with_host_id(locator::host_id host_id) const;

    std::optional<endpoint_state> get_state_for_version_bigger_than(inet_address for_endpoint, version_type version) const;

    /**
     * determine which endpoint started up earlier
     */
    std::strong_ordering compare_endpoint_startup(inet_address addr1, inet_address addr2) const;

    /**
     * Return the rpc address associated with an endpoint as a string.
     * @param endpoint The endpoint to get rpc address for
     * @return the rpc address
     */
    sstring get_rpc_address(const inet_address& endpoint) const;

    future<> real_mark_alive(inet_address addr);
private:
    // FIXME: for now, allow modifying the endpoint_state's heartbeat_state in place
    // Gets or creates endpoint_state for this node
    endpoint_state& get_or_create_endpoint_state(inet_address ep);
    endpoint_state& my_endpoint_state() {
        return get_or_create_endpoint_state(get_broadcast_address());
    }

    // Use with care, as the endpoint_state_ptr in the endpoint_state_map is considered
    // immutable, with one exception - the update_timestamp.
    void update_timestamp(const endpoint_state_ptr& eps) noexcept;
    const endpoint_state& get_endpoint_state(inet_address ep) const;

    void update_timestamp_for_nodes(const std::map<inet_address, endpoint_state>& map);

    void mark_alive(inet_address addr);

    // Must be called under lock_endpoint.
    future<> mark_dead(inet_address addr, endpoint_state_ptr local_state, permit_id);

    // Must be called under lock_endpoint.
    future<> mark_as_shutdown(const inet_address& endpoint, permit_id);

    /**
     * This method is called whenever there is a "big" change in ep state (a generation change for a known node).
     *
     * @param ep      endpoint
     * @param ep_state EndpointState for the endpoint
     *
     * Must be called under lock_endpoint.
     */
    future<> handle_major_state_change(inet_address ep, endpoint_state eps, permit_id);

public:
    bool is_alive(inet_address ep) const;
    bool is_dead_state(const endpoint_state& eps) const;
    // Wait for nodes to be alive on all shards
    future<> wait_alive(std::vector<gms::inet_address> nodes, std::chrono::milliseconds timeout);
    future<> wait_alive(noncopyable_function<std::vector<gms::inet_address>()> get_nodes, std::chrono::milliseconds timeout);

    // Wait for `n` live nodes to show up in gossip (including ourself).
    future<> wait_for_live_nodes_to_show_up(size_t n);

    // Get live members synchronized to all shards
    future<std::set<inet_address>> get_live_members_synchronized();

    // Get live members synchronized to all shards
    future<std::set<inet_address>> get_unreachable_members_synchronized();

    future<> apply_state_locally(std::map<inet_address, endpoint_state> map);

private:
    future<> do_apply_state_locally(gms::inet_address node, endpoint_state remote_state, bool listener_notification);
    future<> apply_state_locally_without_listener_notification(std::unordered_map<inet_address, endpoint_state> map);

    // Must be called under lock_endpoint.
    future<> apply_new_states(inet_address addr, endpoint_state local_state, const endpoint_state& remote_state, permit_id);

    // notify that an application state has changed
    // Must be called under lock_endpoint.
    future<> do_on_change_notifications(inet_address addr, const application_state_map& states, permit_id) const;

    // notify that a node is DOWN (dead)
    // Must be called under lock_endpoint.
    future<> do_on_dead_notifications(inet_address addr, endpoint_state_ptr state, permit_id) const;

    /* Request all the state for the endpoint in the g_digest */

    void request_all(gossip_digest& g_digest, utils::chunked_vector<gossip_digest>& delta_gossip_digest_list, generation_type remote_generation) const;

    /* Send all the data with version greater than max_remote_version */
    void send_all(gossip_digest& g_digest, std::map<inet_address, endpoint_state>& delta_ep_state_map, version_type max_remote_version) const;

public:
    /*
        This method is used to figure the state that the Gossiper has but Gossipee doesn't. The delta digests
        and the delta state are built up.
    */
    void examine_gossiper(utils::chunked_vector<gossip_digest>& g_digest_list,
                         utils::chunked_vector<gossip_digest>& delta_gossip_digest_list,
                         std::map<inet_address, endpoint_state>& delta_ep_state_map) const;

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
    future<> start_gossiping(gms::generation_type generation_nbr, application_state_map preload_local_states = {},
            gms::advertise_myself advertise = gms::advertise_myself::yes);

public:
    using mandatory = bool_class<class mandatory_tag>;
    /**
     *  Do a single 'shadow' round of gossip, where we do not modify any state
     */
    future<> do_shadow_round(std::unordered_set<gms::inet_address> nodes, mandatory is_mandatory);

private:
    void build_seeds_list();
    // Must be called on shard 0
    future<> do_stop_gossiping();

public:
    /**
     * Add an endpoint we knew about previously, but whose state is unknown
     */
    future<> add_saved_endpoint(locator::host_id host_id, loaded_endpoint_state st, permit_id);

    future<> add_local_application_state(application_state state, versioned_value value);

    /**
     * Applies all states in set "atomically", as in guaranteed monotonic versions and
     * inserted into endpoint state together (and assuming same grouping, overwritten together).
     */
    future<> add_local_application_state(application_state_map states);

    // Add multiple application states
    future<> add_local_application_state(std::convertible_to<std::pair<const application_state, versioned_value>> auto&&... states);

    future<> start();
    future<> shutdown();
    // Needed by seastar::sharded
    future<> stop();

public:
    bool is_enabled() const;

    void finish_shadow_round();

    bool is_in_shadow_round() const;

    void goto_shadow_round();

public:
    void add_expire_time_for_endpoint(inet_address endpoint, clk::time_point expire_time);

    static clk::time_point compute_expire_time();
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
    void force_newer_generation();
public:
    std::string_view get_gossip_status(const endpoint_state& ep_state) const noexcept;
    std::string_view get_gossip_status(const inet_address& endpoint) const noexcept;
public:
    future<> wait_for_gossip_to_settle() const;
    future<> wait_for_range_setup() const;
private:
    future<> wait_for_gossip(std::chrono::milliseconds, std::optional<int32_t> = {}) const;

    uint64_t _nr_run = 0;
    uint64_t _msg_processing = 0;

    class msg_proc_guard;
private:
    abort_source& _abort_source;
    const locator::shared_token_metadata& _shared_token_metadata;
    netw::messaging_service& _messaging;
    gossip_config _gcfg;
    // Get features supported by a particular node
    std::set<sstring> get_supported_features(inet_address endpoint) const;
    locator::token_metadata_ptr get_token_metadata_ptr() const noexcept;
public:
    void check_knows_remote_features(std::set<std::string_view>& local_features, const std::unordered_map<inet_address, sstring>& loaded_peer_features) const;
    // Get features supported by all the nodes this node knows about
    std::set<sstring> get_supported_features(const std::unordered_map<gms::inet_address, sstring>& loaded_peer_features, ignore_features_of_local_node ignore_local_node) const;
private:
    seastar::metrics::metric_groups _metrics;
public:
    void append_endpoint_state(std::stringstream& ss, const endpoint_state& state);
public:
    void check_snitch_name_matches(sstring local_snitch_name) const;
    int get_down_endpoint_count() const noexcept;
    int get_up_endpoint_count() const noexcept;
    // Send UP notification to all nodes in the set
    future<> notify_nodes_on_up(std::unordered_set<inet_address>);
private:
    future<> failure_detector_loop();
    future<> failure_detector_loop_for_node(gms::inet_address node, generation_type gossip_generation, uint64_t live_endpoints_version);
};


struct gossip_get_endpoint_states_request {
    // Application states the sender requested
    std::unordered_set<gms::application_state> application_states;
};

struct gossip_get_endpoint_states_response {
    std::unordered_map<gms::inet_address, gms::endpoint_state> endpoint_state_map;
};

future<>
gossiper::add_local_application_state(std::convertible_to<std::pair<const application_state, versioned_value>> auto&&... states) {
    application_state_map tmp;
    (..., tmp.emplace(std::forward<decltype(states)>(states)));
    return add_local_application_state(std::move(tmp));
}


} // namespace gms

template <>
struct fmt::formatter<gms::loaded_endpoint_state> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const gms::loaded_endpoint_state&, fmt::format_context& ctx) const -> decltype(ctx.out());
};
