/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "token_metadata.hh"
#include <optional>
#include "locator/snitch_base.hh"
#include "locator/abstract_replication_strategy.hh"
#include "locator/tablets.hh"
#include "log.hh"
#include "partition_range_compat.hh"
#include <unordered_map>
#include <algorithm>
#include <boost/icl/interval.hpp>
#include <boost/icl/interval_map.hpp>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <boost/range/adaptors.hpp>
#include <seastar/core/smp.hh>
#include "utils/stall_free.hh"

namespace locator {

static logging::logger tlogger("token_metadata");

template <typename NodeId>
inline static constexpr const topology::key_kind kind_for_node_id_type
    = std::is_same_v<NodeId, gms::inet_address>
      ? topology::key_kind::inet_address
      : topology::key_kind::host_id;

template <typename C, typename V>
static void remove_by_value(C& container, V value) {
    for (auto it = container.begin(); it != container.end();) {
        if (it->second == value) {
            it = container.erase(it);
        } else {
            it++;
        }
    }
}

template <typename NodeId>
class token_metadata_impl final {
private:
    /**
     * Maintains token to endpoint map of every node in the cluster.
     * Each Token is associated with exactly one Address, but each Address may have
     * multiple tokens.  Hence, the BiMultiValMap collection.
     */
    // FIXME: have to be BiMultiValMap
    std::unordered_map<token, NodeId> _token_to_endpoint_map;

    // Track the unique set of nodes in _token_to_endpoint_map
    std::unordered_set<NodeId> _normal_token_owners;

    std::unordered_map<token, NodeId> _bootstrap_tokens;
    std::unordered_set<NodeId> _leaving_endpoints;
    // The map between the existing node to be replaced and the replacing node
    std::unordered_map<NodeId, NodeId> _replacing_endpoints;

    std::optional<topology_change_info<NodeId>> _topology_change_info;

    std::vector<token> _sorted_tokens;

    tablet_metadata _tablets;

    topology _topology;

    token_metadata::read_new_t _read_new = token_metadata::read_new_t::no;

    long _ring_version = 0;
    static thread_local long _static_ring_version;

    // Zero means that token_metadata versions are not supported,
    // this will be used in RPC handling to decide whether we
    // need to apply fencing or not.
    // The initial valid version is 1;
    token_metadata::version_t _version = 0;
    token_metadata::version_tracker_t _version_tracker;

    // Note: if any member is added to this class
    // clone_async() must be updated to copy that member.

    void sort_tokens();

    const tablet_metadata& tablets() const { return _tablets; }
    tablet_metadata& tablets() { return _tablets; }

    void set_tablets(tablet_metadata&& tablets) {
        _tablets = std::move(tablets);
        invalidate_cached_rings();
    }

    struct shallow_copy {};
public:
    token_metadata_impl(shallow_copy, const token_metadata_impl& o) noexcept
        : _topology(topology::config{}, kind_for_node_id_type<NodeId>)
    {}
    token_metadata_impl(token_metadata::config cfg) noexcept : _topology(std::move(cfg.topo_cfg), kind_for_node_id_type<NodeId>) {};
    token_metadata_impl(const token_metadata_impl&) = delete; // it's too huge for direct copy, use clone_async()
    token_metadata_impl(token_metadata_impl&&) noexcept = default;
    const std::vector<token>& sorted_tokens() const;
    future<> update_normal_tokens(std::unordered_set<token> tokens, NodeId endpoint);
    const token& first_token(const token& start) const;
    size_t first_token_index(const token& start) const;
    std::optional<NodeId> get_endpoint(const token& token) const;
    std::vector<token> get_tokens(const NodeId& addr) const;
    const std::unordered_map<token, NodeId>& get_token_to_endpoint() const {
        return _token_to_endpoint_map;
    }

    const std::unordered_set<NodeId>& get_leaving_endpoints() const {
        return _leaving_endpoints;
    }

    const std::unordered_map<token, NodeId>& get_bootstrap_tokens() const {
        return _bootstrap_tokens;
    }

    void update_topology(inet_address ep, std::optional<endpoint_dc_rack> opt_dr, std::optional<node::state> opt_st, std::optional<shard_id> shard_count = std::nullopt) {
        _topology.add_or_update_endpoint(ep, std::nullopt, std::move(opt_dr), std::move(opt_st), std::move(shard_count));
    }

    void update_topology(host_id ep, std::optional<endpoint_dc_rack> opt_dr, std::optional<node::state> opt_st, std::optional<shard_id> shard_count = std::nullopt) {
        _topology.add_or_update_endpoint(std::nullopt, ep, std::move(opt_dr), std::move(opt_st), std::move(shard_count));
    }

    /**
     * Creates an iterable range of the sorted tokens starting at the token next
     * after the given one.
     *
     * @param start A token that will define the beginning of the range
     *
     * @return The requested range (see the description above)
     */
    boost::iterator_range<typename generic_token_metadata<NodeId>::tokens_iterator> ring_range(const token& start) const;

    boost::iterator_range<typename generic_token_metadata<NodeId>::tokens_iterator> ring_range(dht::ring_position_view pos) const;

    topology& get_topology() {
        return _topology;
    }

    const topology& get_topology() const {
        return _topology;
    }

    void debug_show() const;

    /**
     * Store an end-point to host ID mapping.  Each ID must be unique, and
     * cannot be changed after the fact.
     *
     * @param hostId
     * @param endpoint
     */
    void update_host_id(const host_id& host_id, inet_address endpoint);

    /** Return the unique host ID for an end-point. */
    host_id get_host_id(inet_address endpoint) const;

    /// Return the unique host ID for an end-point or nullopt if not found.
    std::optional<host_id> get_host_id_if_known(inet_address endpoint) const;

    /** Return the end-point for a unique host ID or nullopt if not found.*/
    std::optional<inet_address> get_endpoint_for_host_id_if_known(host_id) const;

    /** Return the end-point for a unique host ID.*/
    inet_address get_endpoint_for_host_id(host_id) const;

    /** @return a copy of the endpoint-to-id map for read-only operations */
    std::unordered_map<inet_address, host_id> get_endpoint_to_host_id_map_for_reading() const;

    void add_bootstrap_token(token t, NodeId endpoint);

    void add_bootstrap_tokens(std::unordered_set<token> tokens, NodeId endpoint);

    void remove_bootstrap_tokens(std::unordered_set<token> tokens);

    void add_leaving_endpoint(NodeId endpoint);
    void del_leaving_endpoint(NodeId endpoint);
public:
    void remove_endpoint(NodeId endpoint);

    bool is_normal_token_owner(NodeId endpoint) const;

    bool is_leaving(NodeId endpoint) const;

    // Is this node being replaced by another node
    bool is_being_replaced(NodeId endpoint) const;

    // Is any node being replaced by another node
    bool is_any_node_being_replaced() const;

    void add_replacing_endpoint(NodeId existing_node, NodeId replacing_node);

    void del_replacing_endpoint(NodeId existing_node);
public:

    /**
     * Create a full copy of token_metadata_impl using asynchronous continuations.
     * The caller must ensure that the cloned object will not change if
     * the function yields.
     */
    future<std::unique_ptr<token_metadata_impl>> clone_async() const noexcept;

    /**
     * Create a copy of TokenMetadata with only tokenToEndpointMap. That is, pending ranges,
     * bootstrap tokens and leaving endpoints are not included in the copy.
     * The caller must ensure that the cloned object will not change if
     * the function yields.
     */
    future<std::unique_ptr<token_metadata_impl>> clone_only_token_map(bool clone_sorted_tokens = true) const noexcept;

    /**
     * Create a copy of TokenMetadata with tokenToEndpointMap reflecting situation after all
     * current leave operations have finished.
     *
     * @return new token metadata
     */
    future<std::unique_ptr<token_metadata_impl>> clone_after_all_left() const noexcept {
        return clone_only_token_map(false).then([this] (std::unique_ptr<token_metadata_impl> all_left_metadata) {
            for (auto endpoint : _leaving_endpoints) {
                all_left_metadata->remove_endpoint(endpoint);
            }
            all_left_metadata->sort_tokens();

            return all_left_metadata;
        });
    }

    /**
     * Destroy the token_metadata members using continuations
     * to prevent reactor stalls.
     */
    future<> clear_gently() noexcept;

public:
    dht::token_range_vector get_primary_ranges_for(std::unordered_set<token> tokens) const;

    dht::token_range_vector get_primary_ranges_for(token right) const;
    static boost::icl::interval<token>::interval_type range_to_interval(range<dht::token> r);
    static range<dht::token> interval_to_range(boost::icl::interval<token>::interval_type i);

public:
    future<> update_topology_change_info(dc_rack_fn<NodeId>& get_dc_rack);
    const std::optional<topology_change_info<NodeId>>& get_topology_change_info() const {
        return _topology_change_info;
    }
public:

    token get_predecessor(token t) const;

    // Returns nodes that are officially part of the ring. It does not include
    // node that is still joining the cluster, e.g., a node that is still
    // streaming data before it finishes the bootstrap process and turns into
    // NORMAL status.
    const std::unordered_set<NodeId>& get_all_endpoints() const noexcept {
        return _normal_token_owners;
    }

    /* Returns the number of different endpoints that own tokens in the ring.
     * Bootstrapping tokens are not taken into account. */
    size_t count_normal_token_owners() const;
private:
    future<> update_normal_token_owners();
public:
    void set_read_new(token_metadata::read_new_t read_new) {
        _read_new = read_new;
    }

public:
    long get_ring_version() const {
        return _ring_version;
    }

    void invalidate_cached_rings() {
        _ring_version = ++_static_ring_version;
        tlogger.debug("ring_version={}", _ring_version);
    }

    token_metadata::version_t get_version() const {
        return _version;
    }
    void set_version(token_metadata::version_t version) {
        if (version <= 0) {
            on_internal_error(tlogger,
                format("token_metadata_impl<NodeId>::set_version: invalid new version {}", version));
        }
        if (version < _version) {
            on_internal_error(tlogger,
                format("token_metadata_impl<NodeId>::set_version: new version can't be smaller than the previous one, "
                       "new version {}, previous version {}", version, _version));
        }
        _version = version;
    }
    void set_version_tracker(token_metadata::version_tracker_t tracker) {
        _version_tracker = std::move(tracker);
    }

    friend class generic_token_metadata<NodeId>;
};

template <typename NodeId>
thread_local long token_metadata_impl<NodeId>::_static_ring_version;

template <typename NodeId>
generic_token_metadata<NodeId>::tokens_iterator::tokens_iterator(const token& start, const token_metadata_impl<NodeId>* token_metadata)
    : _token_metadata(token_metadata) {
    _cur_it = _token_metadata->sorted_tokens().begin() + _token_metadata->first_token_index(start);
    _remaining = _token_metadata->sorted_tokens().size();
}

template <typename NodeId>
bool generic_token_metadata<NodeId>::tokens_iterator::operator==(const tokens_iterator& it) const {
    return _remaining == it._remaining;
}

template <typename NodeId>
const token& generic_token_metadata<NodeId>::tokens_iterator::operator*() const {
    return *_cur_it;
}

template <typename NodeId>
typename generic_token_metadata<NodeId>::tokens_iterator& generic_token_metadata<NodeId>::tokens_iterator::operator++() {
    ++_cur_it;
    if (_cur_it == _token_metadata->sorted_tokens().end()) {
        _cur_it = _token_metadata->sorted_tokens().begin();
    }
    --_remaining;
    return *this;
}

template <typename NodeId>
host_id generic_token_metadata<NodeId>::get_my_id() const {
    return get_topology().get_config().this_host_id;
}

template <typename NodeId>
inline
boost::iterator_range<typename generic_token_metadata<NodeId>::tokens_iterator>
token_metadata_impl<NodeId>::ring_range(const token& start) const {
    auto begin = typename generic_token_metadata<NodeId>::tokens_iterator(start, this);
    auto end = typename generic_token_metadata<NodeId>::tokens_iterator();
    return boost::make_iterator_range(begin, end);
}

template <typename NodeId>
future<std::unique_ptr<token_metadata_impl<NodeId>>> token_metadata_impl<NodeId>::clone_async() const noexcept {
    auto ret = co_await clone_only_token_map();
    ret->_bootstrap_tokens.reserve(_bootstrap_tokens.size());
    for (const auto& p : _bootstrap_tokens) {
        ret->_bootstrap_tokens.emplace(p);
        co_await coroutine::maybe_yield();
    }
    ret->_leaving_endpoints = _leaving_endpoints;
    ret->_replacing_endpoints = _replacing_endpoints;
    ret->_ring_version = _ring_version;
    ret->_version = _version;
    co_return ret;
}

template <typename NodeId>
future<std::unique_ptr<token_metadata_impl<NodeId>>> token_metadata_impl<NodeId>::clone_only_token_map(bool clone_sorted_tokens) const noexcept {
    auto ret = std::make_unique<token_metadata_impl>(shallow_copy{}, *this);
    ret->_token_to_endpoint_map.reserve(_token_to_endpoint_map.size());
    for (const auto& p : _token_to_endpoint_map) {
        ret->_token_to_endpoint_map.emplace(p);
        co_await coroutine::maybe_yield();
    }
    ret->_normal_token_owners = _normal_token_owners;
    ret->_topology = co_await _topology.clone_gently();
    if (clone_sorted_tokens) {
        ret->_sorted_tokens = _sorted_tokens;
        co_await coroutine::maybe_yield();
    }
    ret->_tablets = _tablets;
    ret->_read_new = _read_new;
    co_return ret;
}

template <typename NodeId>
future<> token_metadata_impl<NodeId>::clear_gently() noexcept {
    co_await utils::clear_gently(_token_to_endpoint_map);
    co_await utils::clear_gently(_normal_token_owners);
    co_await utils::clear_gently(_bootstrap_tokens);
    co_await utils::clear_gently(_leaving_endpoints);
    co_await utils::clear_gently(_replacing_endpoints);
    co_await utils::clear_gently(_sorted_tokens);
    co_await _topology.clear_gently();
    co_await _tablets.clear_gently();
    co_return;
}

template <typename NodeId>
void token_metadata_impl<NodeId>::sort_tokens() {
    std::vector<token> sorted;
    sorted.reserve(_token_to_endpoint_map.size());

    for (auto&& i : _token_to_endpoint_map) {
        sorted.push_back(i.first);
    }

    std::sort(sorted.begin(), sorted.end());

    _sorted_tokens = std::move(sorted);
}

template <typename NodeId>
const tablet_metadata& generic_token_metadata<NodeId>::tablets() const {
    return _impl->tablets();
}

template <typename NodeId>
tablet_metadata& generic_token_metadata<NodeId>::tablets() {
    return _impl->tablets();
}

template <typename NodeId>
void generic_token_metadata<NodeId>::set_tablets(tablet_metadata tm) {
    _impl->set_tablets(std::move(tm));
}

template <typename NodeId>
const std::vector<token>& token_metadata_impl<NodeId>::sorted_tokens() const {
    return _sorted_tokens;
}

template <typename NodeId>
std::vector<token> token_metadata_impl<NodeId>::get_tokens(const NodeId& addr) const {
    std::vector<token> res;
    for (auto&& i : _token_to_endpoint_map) {
        if (i.second == addr) {
            res.push_back(i.first);
        }
    }
    std::sort(res.begin(), res.end());
    return res;
}

template <typename NodeId>
future<> token_metadata_impl<NodeId>::update_normal_tokens(std::unordered_set<token> tokens, NodeId endpoint) {
    if (tokens.empty()) {
        co_return;
    }

    if (!_topology.has_node(endpoint)) {
        on_internal_error(tlogger, format("token_metadata_impl: {} must be a member of topology to update normal tokens", endpoint));
    }

    bool should_sort_tokens = false;

    // Phase 1: erase all tokens previously owned by the endpoint.
    for(auto it = _token_to_endpoint_map.begin(), ite = _token_to_endpoint_map.end(); it != ite;) {
        co_await coroutine::maybe_yield();
        if(it->second == endpoint) {
            auto tokit = tokens.find(it->first);
            if (tokit == tokens.end()) {
                // token no longer owned by endpoint
                it = _token_to_endpoint_map.erase(it);
                continue;
            }
            // token ownership did not change,
            // no further update needed for it.
            tokens.erase(tokit);
        }
        ++it;
    }

    // Phase 2:
    // a. ...
    // b. update pending _bootstrap_tokens and _leaving_endpoints
    // c. update _token_to_endpoint_map with the new endpoint->token mappings
    //    - set `should_sort_tokens` if new tokens were added
    remove_by_value(_bootstrap_tokens, endpoint);
    _leaving_endpoints.erase(endpoint);
    invalidate_cached_rings();
    for (const token& t : tokens)
    {
        co_await coroutine::maybe_yield();
        auto prev = _token_to_endpoint_map.insert(std::pair<token, NodeId>(t, endpoint));
        should_sort_tokens |= prev.second; // new token inserted -> sort
        if (prev.first->second != endpoint) {
            tlogger.debug("Token {} changing ownership from {} to {}", t, prev.first->second, endpoint);
            prev.first->second = endpoint;
        }
    }

    co_await update_normal_token_owners();

    // New tokens were added to _token_to_endpoint_map
    // so re-sort all tokens.
    if (should_sort_tokens) {
        sort_tokens();
    }
    co_return;
}

template <typename NodeId>
size_t token_metadata_impl<NodeId>::first_token_index(const token& start) const {
    if (_sorted_tokens.empty()) {
        auto msg = format("sorted_tokens is empty in first_token_index!");
        tlogger.error("{}", msg);
        throw std::runtime_error(msg);
    }
    auto it = std::lower_bound(_sorted_tokens.begin(), _sorted_tokens.end(), start);
    if (it == _sorted_tokens.end()) {
        return 0;
    } else {
        return std::distance(_sorted_tokens.begin(), it);
    }
}

template <typename NodeId>
const token& token_metadata_impl<NodeId>::first_token(const token& start) const {
    return _sorted_tokens[first_token_index(start)];
}

template <typename NodeId>
std::optional<NodeId> token_metadata_impl<NodeId>::get_endpoint(const token& token) const {
    auto it = _token_to_endpoint_map.find(token);
    if (it == _token_to_endpoint_map.end()) {
        return std::nullopt;
    } else {
        return it->second;
    }
}

template <typename NodeId>
void token_metadata_impl<NodeId>::debug_show() const {
    auto reporter = std::make_shared<timer<lowres_clock>>();
    reporter->set_callback ([reporter, this] {
        fmt::print("Endpoint -> Token\n");
        for (auto x : _token_to_endpoint_map) {
            fmt::print("inet_address={}, token={}\n", x.second, x.first);
        }
        fmt::print("Sorted Token\n");
        for (auto x : _sorted_tokens) {
            fmt::print("token={}\n", x);
        }
    });
    reporter->arm_periodic(std::chrono::seconds(1));
}

template <typename NodeId>
void token_metadata_impl<NodeId>::update_host_id(const host_id& host_id, inet_address endpoint) {
    _topology.add_or_update_endpoint(endpoint, host_id);
}

template <typename NodeId>
host_id token_metadata_impl<NodeId>::get_host_id(inet_address endpoint) const {
    if (const auto* node = _topology.find_node(endpoint)) [[likely]] {
        return node->host_id();
    } else {
        on_internal_error(tlogger, format("host_id for endpoint {} is not found", endpoint));
    }
}

template <typename NodeId>
std::optional<host_id> token_metadata_impl<NodeId>::get_host_id_if_known(inet_address endpoint) const {
    if (const auto* node = _topology.find_node(endpoint)) [[likely]] {
        return node->host_id();
    } else {
        return std::nullopt;
    }
}

template <typename NodeId>
std::optional<inet_address> token_metadata_impl<NodeId>::get_endpoint_for_host_id_if_known(host_id host_id) const {
    if (const auto* node = _topology.find_node(host_id)) [[likely]] {
        return node->endpoint();
    } else {
        return std::nullopt;
    }
}

template <typename NodeId>
inet_address token_metadata_impl<NodeId>::get_endpoint_for_host_id(host_id host_id) const {
    if (const auto* node = _topology.find_node(host_id)) [[likely]] {
        return node->endpoint();
    } else {
        on_internal_error(tlogger, format("endpoint for host_id {} is not found", host_id));
    }
}

template <typename NodeId>
std::unordered_map<inet_address, host_id> token_metadata_impl<NodeId>::get_endpoint_to_host_id_map_for_reading() const {
    const auto& nodes = _topology.get_nodes_by_endpoint();
    std::unordered_map<inet_address, host_id> map;
    map.reserve(nodes.size());
    for (const auto& [endpoint, node] : nodes) {
        // Restrict to token-owners
        if (!(node->is_normal() || node->is_leaving())) {
            continue;
        }
        if (const auto& host_id = node->host_id()) {
            map[endpoint] = host_id;
        } else {
            tlogger.info("get_endpoint_to_host_id_map_for_reading: endpoint {} has null host_id: state={}", endpoint, node->get_state());
        }
    }
    return map;
}

template <typename NodeId>
bool token_metadata_impl<NodeId>::is_normal_token_owner(NodeId endpoint) const {
    return _normal_token_owners.contains(endpoint);
}

template <typename NodeId>
void token_metadata_impl<NodeId>::add_bootstrap_token(token t, NodeId endpoint) {
    std::unordered_set<token> tokens{t};
    add_bootstrap_tokens(tokens, endpoint);
}

template <typename NodeId>
boost::iterator_range<typename generic_token_metadata<NodeId>::tokens_iterator>
token_metadata_impl<NodeId>::ring_range(const dht::ring_position_view start) const {
    return ring_range(start.token());
}

template <typename NodeId>
void token_metadata_impl<NodeId>::add_bootstrap_tokens(std::unordered_set<token> tokens, NodeId endpoint) {
    for (auto t : tokens) {
        auto old_endpoint = _bootstrap_tokens.find(t);
        if (old_endpoint != _bootstrap_tokens.end() && (*old_endpoint).second != endpoint) {
            auto msg = format("Bootstrap Token collision between {} and {} (token {}", (*old_endpoint).second, endpoint, t);
            throw std::runtime_error(msg);
        }

        auto old_endpoint2 = _token_to_endpoint_map.find(t);
        if (old_endpoint2 != _token_to_endpoint_map.end() && (*old_endpoint2).second != endpoint) {
            auto msg = format("Bootstrap Token collision between {} and {} (token {}", (*old_endpoint2).second, endpoint, t);
            throw std::runtime_error(msg);
        }
    }

    std::erase_if(_bootstrap_tokens, [endpoint] (const std::pair<token, NodeId>& n) { return n.second == endpoint; });

    for (auto t : tokens) {
        _bootstrap_tokens[t] = endpoint;
    }
}

template <typename NodeId>
void token_metadata_impl<NodeId>::remove_bootstrap_tokens(std::unordered_set<token> tokens) {
    if (tokens.empty()) {
        tlogger.warn("tokens is empty in remove_bootstrap_tokens!");
        return;
    }
    for (auto t : tokens) {
        _bootstrap_tokens.erase(t);
    }
}

template <typename NodeId>
bool token_metadata_impl<NodeId>::is_leaving(NodeId endpoint) const {
    return _leaving_endpoints.contains(endpoint);
}

template <typename NodeId>
bool token_metadata_impl<NodeId>::is_being_replaced(NodeId endpoint) const {
    return _replacing_endpoints.contains(endpoint);
}

template <typename NodeId>
bool token_metadata_impl<NodeId>::is_any_node_being_replaced() const {
    return !_replacing_endpoints.empty();
}

template <typename NodeId>
void token_metadata_impl<NodeId>::remove_endpoint(NodeId endpoint) {
    remove_by_value(_bootstrap_tokens, endpoint);
    remove_by_value(_token_to_endpoint_map, endpoint);
    _normal_token_owners.erase(endpoint);
    _topology.remove_endpoint(endpoint);
    _leaving_endpoints.erase(endpoint);
    del_replacing_endpoint(endpoint);
    invalidate_cached_rings();
}

template <typename NodeId>
token token_metadata_impl<NodeId>::get_predecessor(token t) const {
    auto& tokens = sorted_tokens();
    auto it = std::lower_bound(tokens.begin(), tokens.end(), t);
    if (it == tokens.end() || *it != t) {
        auto msg = format("token error in get_predecessor!");
        tlogger.error("{}", msg);
        throw std::runtime_error(msg);
    }
    if (it == tokens.begin()) {
        // If the token is the first element, its preprocessor is the last element
        return tokens.back();
    } else {
        return *(--it);
    }
}

template <typename NodeId>
dht::token_range_vector token_metadata_impl<NodeId>::get_primary_ranges_for(std::unordered_set<token> tokens) const {
    dht::token_range_vector ranges;
    ranges.reserve(tokens.size() + 1); // one of the ranges will wrap
    for (auto right : tokens) {
        auto left = get_predecessor(right);
        ::compat::unwrap_into(
                wrapping_range<token>(range_bound<token>(left, false), range_bound<token>(right)),
                dht::token_comparator(),
                [&] (auto&& rng) { ranges.push_back(std::move(rng)); });
    }
    return ranges;
}

template <typename NodeId>
dht::token_range_vector token_metadata_impl<NodeId>::get_primary_ranges_for(token right) const {
    return get_primary_ranges_for(std::unordered_set<token>{right});
}

template <typename NodeId>
boost::icl::interval<token>::interval_type
token_metadata_impl<NodeId>::range_to_interval(range<dht::token> r) {
    bool start_inclusive = false;
    bool end_inclusive = false;
    token start = dht::minimum_token();
    token end = dht::maximum_token();

    if (r.start()) {
        start = r.start()->value();
        start_inclusive = r.start()->is_inclusive();
    }

    if (r.end()) {
        end = r.end()->value();
        end_inclusive = r.end()->is_inclusive();
    }

    if (start_inclusive == false && end_inclusive == false) {
        return boost::icl::interval<token>::open(std::move(start), std::move(end));
    } else if (start_inclusive == false && end_inclusive == true) {
        return boost::icl::interval<token>::left_open(std::move(start), std::move(end));
    } else if (start_inclusive == true && end_inclusive == false) {
        return boost::icl::interval<token>::right_open(std::move(start), std::move(end));
    } else {
        return boost::icl::interval<token>::closed(std::move(start), std::move(end));
    }
}

template <typename NodeId>
range<dht::token>
token_metadata_impl<NodeId>::interval_to_range(boost::icl::interval<token>::interval_type i) {
    bool start_inclusive;
    bool end_inclusive;
    auto bounds = i.bounds().bits();
    if (bounds == boost::icl::interval_bounds::static_open) {
        start_inclusive = false;
        end_inclusive = false;
    } else if (bounds == boost::icl::interval_bounds::static_left_open) {
        start_inclusive = false;
        end_inclusive = true;
    } else if (bounds == boost::icl::interval_bounds::static_right_open) {
        start_inclusive = true;
        end_inclusive = false;
    } else if (bounds == boost::icl::interval_bounds::static_closed) {
        start_inclusive = true;
        end_inclusive = true;
    } else {
        throw std::runtime_error("Invalid boost::icl::interval<token> bounds");
    }
    return range<dht::token>({{i.lower(), start_inclusive}}, {{i.upper(), end_inclusive}});
}

template <typename NodeId>
future<> token_metadata_impl<NodeId>::update_topology_change_info(dc_rack_fn<NodeId>& get_dc_rack) {
    if (_bootstrap_tokens.empty() && _leaving_endpoints.empty() && _replacing_endpoints.empty()) {
        co_await utils::clear_gently(_topology_change_info);
        _topology_change_info.reset();
        co_return;
    }

    // true if there is a node replaced with the same IP
    bool replace_with_same_endpoint = false;
    // target_token_metadata incorporates all the changes from leaving, bootstrapping and replacing
    auto target_token_metadata = co_await clone_only_token_map(false);
    {
        // construct new_normal_tokens based on _bootstrap_tokens and _replacing_endpoints
        std::unordered_map<NodeId, std::unordered_set<token>> new_normal_tokens;
        if (!_replacing_endpoints.empty()) {
            for (const auto& [token, inet_address]: _token_to_endpoint_map) {
                const auto it = _replacing_endpoints.find(inet_address);
                if (it == _replacing_endpoints.end()) {
                    continue;
                }
                new_normal_tokens[it->second].insert(token);
            }
            for (const auto& [replace_from, replace_to]: _replacing_endpoints) {
                if (replace_from == replace_to) {
                    replace_with_same_endpoint = true;
                } else {
                    target_token_metadata->remove_endpoint(replace_from);
                }
            }
        }
        for (const auto& [token, inet_address]: _bootstrap_tokens) {
            new_normal_tokens[inet_address].insert(token);
        }
        // apply new_normal_tokens
        for (auto& [endpoint, tokens]: new_normal_tokens) {
            target_token_metadata->update_topology(endpoint, get_dc_rack(endpoint), node::state::normal);
            co_await target_token_metadata->update_normal_tokens(std::move(tokens), endpoint);
        }
        // apply leaving endpoints
        for (const auto& endpoint: _leaving_endpoints) {
            target_token_metadata->remove_endpoint(endpoint);
        }
        target_token_metadata->sort_tokens();
    }

    // We require a distinct token_metadata instance when replace_from equals replace_to,
    // as it ensures the node is included in pending_ranges.
    // Otherwise, the node would be excluded from both pending_ranges and
    // get_natural_endpoints_without_node_being_replaced,
    // causing the coordinator to overlook it entirely.
    std::unique_ptr<token_metadata_impl> base_token_metadata;
    if (replace_with_same_endpoint) {
        base_token_metadata = co_await clone_only_token_map(false);
        for (const auto& [replace_from, replace_to]: _replacing_endpoints) {
            if (replace_from == replace_to) {
                base_token_metadata->remove_endpoint(replace_from);
            }
        }
        base_token_metadata->sort_tokens();
    }

    // merge tokens from token_to_endpoint and bootstrap_tokens,
    // preserving tokens of leaving endpoints
    auto all_tokens = std::vector<dht::token>();
    all_tokens.reserve(sorted_tokens().size() + get_bootstrap_tokens().size());
    all_tokens.resize(sorted_tokens().size());
    std::copy(begin(sorted_tokens()), end(sorted_tokens()), begin(all_tokens));
    for (const auto& p: get_bootstrap_tokens()) {
        all_tokens.push_back(p.first);
    }
    std::sort(begin(all_tokens), end(all_tokens));

    auto prev_value = std::move(_topology_change_info);
    _topology_change_info.emplace(make_lw_shared<generic_token_metadata<NodeId>>(std::move(target_token_metadata)),
        base_token_metadata ? make_lw_shared<generic_token_metadata<NodeId>>(std::move(base_token_metadata)): nullptr,
        std::move(all_tokens),
        _read_new);
    co_await utils::clear_gently(prev_value);
}

template <typename NodeId>
size_t token_metadata_impl<NodeId>::count_normal_token_owners() const {
    return _normal_token_owners.size();
}

template <typename NodeId>
future<> token_metadata_impl<NodeId>::update_normal_token_owners() {
    std::unordered_set<NodeId> eps;
    for (auto [t, ep]: _token_to_endpoint_map) {
        eps.insert(ep);
        co_await coroutine::maybe_yield();
    }
    _normal_token_owners = std::move(eps);
}

template <typename NodeId>
void token_metadata_impl<NodeId>::add_leaving_endpoint(NodeId endpoint) {
     _leaving_endpoints.emplace(endpoint);
}

template <typename NodeId>
void token_metadata_impl<NodeId>::del_leaving_endpoint(NodeId endpoint) {
     _leaving_endpoints.erase(endpoint);
}

template <typename NodeId>
void token_metadata_impl<NodeId>::add_replacing_endpoint(NodeId existing_node, NodeId replacing_node) {
    tlogger.info("Added node {} as pending replacing endpoint which replaces existing node {}",
            replacing_node, existing_node);
    _replacing_endpoints[existing_node] = replacing_node;
}

template <typename NodeId>
void token_metadata_impl<NodeId>::del_replacing_endpoint(NodeId existing_node) {
    if (_replacing_endpoints.contains(existing_node)) {
        tlogger.info("Removed node {} as pending replacing endpoint which replaces existing node {}",
                _replacing_endpoints[existing_node], existing_node);
    }
    _replacing_endpoints.erase(existing_node);
}

template <typename NodeId>
topology_change_info<NodeId>::topology_change_info(lw_shared_ptr<generic_token_metadata<NodeId>> target_token_metadata_,
        lw_shared_ptr<generic_token_metadata<NodeId>> base_token_metadata_,
    std::vector<dht::token> all_tokens_,
    token_metadata::read_new_t read_new_)
    : target_token_metadata(std::move(target_token_metadata_))
    , base_token_metadata(std::move(base_token_metadata_))
    , all_tokens(std::move(all_tokens_))
    , read_new(read_new_)
{
}

template <typename NodeId>
future<> topology_change_info<NodeId>::clear_gently() {
    co_await utils::clear_gently(target_token_metadata);
    co_await utils::clear_gently(base_token_metadata);
    co_await utils::clear_gently(all_tokens);
}

template <typename NodeId>
generic_token_metadata<NodeId>::generic_token_metadata(std::unique_ptr<token_metadata_impl<NodeId>> impl)
    : _impl(std::move(impl))
{
}

template <typename NodeId>
template <typename T>
requires std::is_same_v<T, gms::inet_address>
generic_token_metadata<NodeId>::generic_token_metadata(std::unique_ptr<token_metadata_impl<NodeId>> impl,
    token_metadata2 new_value)
    : _impl(std::move(impl))
    , _new_value(make_token_metadata2_ptr(std::move(new_value)))
{
}

template <typename NodeId>
template <typename T>
requires std::is_same_v<T, gms::inet_address>
generic_token_metadata<NodeId>::generic_token_metadata(token_metadata2_ptr new_value)
    : _impl(nullptr)
    , _new_value(std::move(new_value))
{
}

template <typename NodeId>
generic_token_metadata<NodeId>::generic_token_metadata(config cfg)
        : _impl(std::make_unique<token_metadata_impl<NodeId>>(cfg))
{
    if constexpr (std::is_same_v<NodeId, gms::inet_address>) {
        _new_value = make_token_metadata2_ptr(std::move(cfg));
    }
}

template <typename NodeId>
generic_token_metadata<NodeId>::~generic_token_metadata() = default;

template <typename NodeId>
generic_token_metadata<NodeId>::generic_token_metadata(generic_token_metadata&&) noexcept = default;

template <typename NodeId>
generic_token_metadata<NodeId>& generic_token_metadata<NodeId>::generic_token_metadata<NodeId>::operator=(generic_token_metadata&&) noexcept = default;

template <typename NodeId>
const std::vector<token>&
generic_token_metadata<NodeId>::sorted_tokens() const {
    return _impl->sorted_tokens();
}

template <typename NodeId>
future<>
generic_token_metadata<NodeId>::update_normal_tokens(std::unordered_set<token> tokens, NodeId endpoint) {
    return _impl->update_normal_tokens(std::move(tokens), endpoint);
}

template <typename NodeId>
const token&
generic_token_metadata<NodeId>::first_token(const token& start) const {
    return _impl->first_token(start);
}

template <typename NodeId>
size_t
generic_token_metadata<NodeId>::first_token_index(const token& start) const {
    return _impl->first_token_index(start);
}

template <typename NodeId>
std::optional<NodeId>
generic_token_metadata<NodeId>::get_endpoint(const token& token) const {
    return _impl->get_endpoint(token);
}

template <typename NodeId>
std::vector<token>
generic_token_metadata<NodeId>::get_tokens(const NodeId& addr) const {
    return _impl->get_tokens(addr);
}

template <typename NodeId>
const std::unordered_map<token, NodeId>&
generic_token_metadata<NodeId>::get_token_to_endpoint() const {
    return _impl->get_token_to_endpoint();
}

template <typename NodeId>
const std::unordered_set<NodeId>&
generic_token_metadata<NodeId>::get_leaving_endpoints() const {
    return _impl->get_leaving_endpoints();
}

template <typename NodeId>
const std::unordered_map<token, NodeId>&
generic_token_metadata<NodeId>::get_bootstrap_tokens() const {
    return _impl->get_bootstrap_tokens();
}

template <typename NodeId>
void
generic_token_metadata<NodeId>::update_topology(NodeId ep, std::optional<endpoint_dc_rack> opt_dr, std::optional<node::state> opt_st, std::optional<shard_id> shard_count) {
    _impl->update_topology(ep, std::move(opt_dr), std::move(opt_st), std::move(shard_count));
}

template <typename NodeId>
boost::iterator_range<typename generic_token_metadata<NodeId>::tokens_iterator>
generic_token_metadata<NodeId>::ring_range(const token& start) const {
    return _impl->ring_range(start);
}

template <typename NodeId>
boost::iterator_range<typename generic_token_metadata<NodeId>::tokens_iterator>
generic_token_metadata<NodeId>::ring_range(dht::ring_position_view start) const {
    return _impl->ring_range(start);
}

class token_metadata_ring_splitter : public locator::token_range_splitter {
    token_metadata2_ptr _tmptr;
    boost::iterator_range<token_metadata2::tokens_iterator> _range;
public:
    token_metadata_ring_splitter(token_metadata2_ptr tmptr)
        : _tmptr(std::move(tmptr))
        , _range(_tmptr->sorted_tokens().empty() // ring_range() throws if the ring is empty
                ? boost::make_iterator_range(token_metadata2::tokens_iterator(), token_metadata2::tokens_iterator())
                : _tmptr->ring_range(dht::minimum_token()))
    { }

    void reset(dht::ring_position_view pos) override {
        _range = _tmptr->ring_range(pos);
    }

    std::optional<dht::token> next_token() override {
        if (_range.empty()) {
            return std::nullopt;
        }
        auto t = *_range.begin();
        _range.drop_front();
        return t;
    }
};

std::unique_ptr<locator::token_range_splitter> make_splitter(token_metadata2_ptr tmptr) {
    return std::make_unique<token_metadata_ring_splitter>(std::move(tmptr));
}

template <typename NodeId>
topology&
generic_token_metadata<NodeId>::get_topology() {
    return _impl->get_topology();
}

template <typename NodeId>
const topology&
generic_token_metadata<NodeId>::get_topology() const {
    return _impl->get_topology();
}

template <typename NodeId>
void
generic_token_metadata<NodeId>::debug_show() const {
    _impl->debug_show();
}

template <typename NodeId>
void
generic_token_metadata<NodeId>::update_host_id(const host_id& host_id, inet_address endpoint) {
    _impl->update_host_id(host_id, endpoint);
}

template <typename NodeId>
host_id
generic_token_metadata<NodeId>::get_host_id(inet_address endpoint) const {
    return _impl->get_host_id(endpoint);
}

template <typename NodeId>
std::optional<host_id>
generic_token_metadata<NodeId>::get_host_id_if_known(inet_address endpoint) const {
    return _impl->get_host_id_if_known(endpoint);
}

template <typename NodeId>
std::optional<typename generic_token_metadata<NodeId>::inet_address>
generic_token_metadata<NodeId>::get_endpoint_for_host_id_if_known(host_id host_id) const {
    return _impl->get_endpoint_for_host_id_if_known(host_id);
}

template <typename NodeId>
typename generic_token_metadata<NodeId>::inet_address
generic_token_metadata<NodeId>::get_endpoint_for_host_id(host_id host_id) const {
    return _impl->get_endpoint_for_host_id(host_id);
}

template <typename NodeId>
host_id_or_endpoint generic_token_metadata<NodeId>::parse_host_id_and_endpoint(const sstring& host_id_string) const {
    auto res = host_id_or_endpoint(host_id_string);
    res.resolve(*this);
    return res;
}

template <typename NodeId>
std::unordered_map<inet_address, host_id>
generic_token_metadata<NodeId>::get_endpoint_to_host_id_map_for_reading() const {
    return _impl->get_endpoint_to_host_id_map_for_reading();
}

template <typename NodeId>
void
generic_token_metadata<NodeId>::add_bootstrap_token(token t, NodeId endpoint) {
    _impl->add_bootstrap_token(t, endpoint);
}

template <typename NodeId>
void
generic_token_metadata<NodeId>::add_bootstrap_tokens(std::unordered_set<token> tokens, NodeId endpoint) {
    _impl->add_bootstrap_tokens(std::move(tokens), endpoint);
}

template <typename NodeId>
void
generic_token_metadata<NodeId>::remove_bootstrap_tokens(std::unordered_set<token> tokens) {
    _impl->remove_bootstrap_tokens(std::move(tokens));
}

template <typename NodeId>
void
generic_token_metadata<NodeId>::add_leaving_endpoint(NodeId endpoint) {
    _impl->add_leaving_endpoint(endpoint);
}

template <typename NodeId>
void
generic_token_metadata<NodeId>::del_leaving_endpoint(NodeId endpoint) {
    _impl->del_leaving_endpoint(endpoint);
}

template <typename NodeId>
void
generic_token_metadata<NodeId>::remove_endpoint(NodeId endpoint) {
    _impl->remove_endpoint(endpoint);
    _impl->sort_tokens();
}

template <typename NodeId>
bool
generic_token_metadata<NodeId>::is_normal_token_owner(NodeId endpoint) const {
    return _impl->is_normal_token_owner(endpoint);
}

template <typename NodeId>
bool
generic_token_metadata<NodeId>::is_leaving(NodeId endpoint) const {
    return _impl->is_leaving(endpoint);
}

template <typename NodeId>
bool
generic_token_metadata<NodeId>::is_being_replaced(NodeId endpoint) const {
    return _impl->is_being_replaced(endpoint);
}

template <typename NodeId>
bool
generic_token_metadata<NodeId>::is_any_node_being_replaced() const {
    return _impl->is_any_node_being_replaced();
}

template <typename NodeId>
void generic_token_metadata<NodeId>::add_replacing_endpoint(NodeId existing_node, NodeId replacing_node) {
    _impl->add_replacing_endpoint(existing_node, replacing_node);
}

template <typename NodeId>
void generic_token_metadata<NodeId>::del_replacing_endpoint(NodeId existing_node) {
    _impl->del_replacing_endpoint(existing_node);
}

template <typename NodeId>
future<generic_token_metadata<NodeId>> generic_token_metadata<NodeId>::clone_async() const noexcept {
    if constexpr (std::is_same_v<NodeId, gms::inet_address>) {
        co_return !holds_alternative<std::monostate>(_new_value)
                  ? generic_token_metadata(co_await _impl->clone_async(), co_await get_new()->clone_async())
                  : generic_token_metadata(co_await _impl->clone_async());
    } else {
        co_return generic_token_metadata(co_await _impl->clone_async());
    }
}

template <typename NodeId>
future<generic_token_metadata<NodeId>>
generic_token_metadata<NodeId>::clone_only_token_map() const noexcept {
    if constexpr (std::is_same_v<NodeId, gms::inet_address>) {
        co_return !holds_alternative<std::monostate>(_new_value)
                  ? generic_token_metadata(co_await _impl->clone_only_token_map(), co_await get_new()->clone_only_token_map())
                  : generic_token_metadata(co_await _impl->clone_only_token_map());
    } else {
        co_return generic_token_metadata(co_await _impl->clone_only_token_map());
    }
}

template <typename NodeId>
future<generic_token_metadata<NodeId>>
generic_token_metadata<NodeId>::clone_after_all_left() const noexcept {
    if constexpr (std::is_same_v<NodeId, gms::inet_address>) {
        co_return !holds_alternative<std::monostate>(_new_value)
                  ? generic_token_metadata(co_await _impl->clone_after_all_left(), co_await get_new()->clone_after_all_left())
                  : generic_token_metadata(co_await _impl->clone_after_all_left());
    } else {
        co_return generic_token_metadata(co_await _impl->clone_after_all_left());
    }
}

template <typename NodeId>
future<> generic_token_metadata<NodeId>::clear_gently() noexcept {
    co_await _impl->clear_gently();
    if constexpr (std::is_same_v<NodeId, gms::inet_address>) {
        if (holds_alternative<lw_shared_ptr<token_metadata2>>(_new_value)) {
            co_await get_new()->clear_gently();
        }
    }
}

template <typename NodeId>
dht::token_range_vector
generic_token_metadata<NodeId>::get_primary_ranges_for(std::unordered_set<token> tokens) const {
    return _impl->get_primary_ranges_for(std::move(tokens));
}

template <typename NodeId>
dht::token_range_vector
generic_token_metadata<NodeId>::get_primary_ranges_for(token right) const {
    return _impl->get_primary_ranges_for(right);
}

template <typename NodeId>
boost::icl::interval<token>::interval_type
generic_token_metadata<NodeId>::range_to_interval(range<dht::token> r) {
    return token_metadata_impl<NodeId>::range_to_interval(std::move(r));
}

template <typename NodeId>
range<dht::token>
generic_token_metadata<NodeId>::interval_to_range(boost::icl::interval<token>::interval_type i) {
    return token_metadata_impl<NodeId>::interval_to_range(std::move(i));
}

template <typename NodeId>
future<>
generic_token_metadata<NodeId>::update_topology_change_info(dc_rack_fn<NodeId>& get_dc_rack) {
    return _impl->update_topology_change_info(get_dc_rack);
}

template <typename NodeId>
const std::optional<topology_change_info<NodeId>>&
generic_token_metadata<NodeId>::get_topology_change_info() const {
    return _impl->get_topology_change_info();
}

template <typename NodeId>
token
generic_token_metadata<NodeId>::get_predecessor(token t) const {
    return _impl->get_predecessor(t);
}

template <typename NodeId>
const std::unordered_set<NodeId>&
generic_token_metadata<NodeId>::get_all_endpoints() const {
    return _impl->get_all_endpoints();
}

template <typename NodeId>
template <typename T>
requires std::is_same_v<T, locator::host_id>
std::unordered_set<gms::inet_address> generic_token_metadata<NodeId>::get_all_ips() const {
    const auto& host_ids = _impl->get_all_endpoints();
    std::unordered_set<gms::inet_address> result;
    result.reserve(host_ids.size());
    for (const auto& id: host_ids) {
        result.insert(_impl->get_endpoint_for_host_id(id));
    }
    return result;
}

template <typename NodeId>
size_t
generic_token_metadata<NodeId>::count_normal_token_owners() const {
    return _impl->count_normal_token_owners();
}

template <typename NodeId>
void
generic_token_metadata<NodeId>::set_read_new(read_new_t read_new) {
    _impl->set_read_new(read_new);
}

template <typename NodeId>
long
generic_token_metadata<NodeId>::get_ring_version() const {
    return _impl->get_ring_version();
}

template <typename NodeId>
void
generic_token_metadata<NodeId>::invalidate_cached_rings() {
    _impl->invalidate_cached_rings();
}

template <typename NodeId>
auto
generic_token_metadata<NodeId>::get_version() const -> version_t {
    return _impl->get_version();
}
template <typename NodeId>
void
generic_token_metadata<NodeId>::set_version(version_t version) {
    _impl->set_version(version);
}
template <typename NodeId>
void
generic_token_metadata<NodeId>::set_version_tracker(version_tracker_t tracker) {
    _impl->set_version_tracker(std::move(tracker));
}

void shared_token_metadata::set(mutable_token_metadata_ptr tmptr) noexcept {
    if (_shared->get_ring_version() >= tmptr->get_ring_version()) {
        on_internal_error(tlogger, format("shared_token_metadata: must not set non-increasing ring_version: {} -> {}", _shared->get_ring_version(), tmptr->get_ring_version()));
    }

    if (_shared->get_version() > tmptr->get_version()) {
        on_internal_error(tlogger, format("shared_token_metadata: must not set decreasing version: {} -> {}", _shared->get_version(), tmptr->get_version()));
    } else if (_shared->get_version() < tmptr->get_version()) {
        _stale_versions_in_use = _versions_barrier.advance_and_await();
    }

    _shared = std::move(tmptr);
    _shared->set_version_tracker(_versions_barrier.start());
    tlogger.debug("new token_metadata is set, version {}", _shared->get_version());
}

void shared_token_metadata::update_fence_version(token_metadata::version_t version) {
    if (const auto current_version = _shared->get_version(); version > current_version) {
        // The generic_token_metadata<NodeId>::version under no circumstance can go backwards.
        // Even in case of topology change coordinator moving to another node
        // this condition must hold, that is why we treat its violation
        // as an internal error.
        on_internal_error(tlogger,
            format("shared_token_metadata: invalid new fence version, can't be greater than the current version, "
                   "current version {}, new fence version {}", current_version, version));
    }
    if (version < _fence_version) {
        // If topology change coordinator moved to another node,
        // it can get ahead and increment the fence version
        // while we are handling raft_topology_cmd::command::fence,
        // so we just throw an error in this case.
        throw std::runtime_error(
            format("shared_token_metadata: can't set decreasing fence version: {} -> {}",
                _fence_version, version));
    }
    _fence_version = version;
    tlogger.debug("new fence_version is set, version {}", _fence_version);
}

future<> shared_token_metadata::mutate_token_metadata(seastar::noncopyable_function<future<> (token_metadata&)> func) {
    auto lk = co_await get_lock();
    auto tm = co_await _shared->clone_async();
    // bump the token_metadata ring_version
    // to invalidate cached token/replication mappings
    // when the modified token_metadata is committed.
    tm.invalidate_cached_rings();
    co_await func(tm);
    set(make_token_metadata_ptr(std::move(tm)));
}

future<> shared_token_metadata::mutate_on_all_shards(sharded<shared_token_metadata>& stm, seastar::noncopyable_function<future<> (token_metadata&)> func) {
    auto base_shard = this_shard_id();
    assert(base_shard == 0);
    auto lk = co_await stm.local().get_lock();

    std::vector<mutable_token_metadata_ptr> pending_token_metadata_ptr;
    pending_token_metadata_ptr.resize(smp::count);
    auto tmptr = make_token_metadata_ptr(co_await stm.local().get()->clone_async());
    auto& tm = *tmptr;
    // bump the token_metadata ring_version
    // to invalidate cached token/replication mappings
    // when the modified token_metadata is committed.
    tm.invalidate_cached_rings();
    co_await func(tm);

    // Apply the mutated token_metadata only after successfully cloning it on all shards.
    pending_token_metadata_ptr[base_shard] = tmptr;
    co_await smp::invoke_on_others(base_shard, [&] () -> future<> {
        pending_token_metadata_ptr[this_shard_id()] = make_token_metadata_ptr(co_await tm.clone_async());
    });

    co_await stm.invoke_on_all([&] (shared_token_metadata& stm) {
        stm.set(std::move(pending_token_metadata_ptr[this_shard_id()]));
    });
}

host_id_or_endpoint::host_id_or_endpoint(const sstring& s, param_type restrict) {
    switch (restrict) {
    case param_type::host_id:
        try {
            id = host_id(utils::UUID(s));
        } catch (const marshal_exception& e) {
            throw std::invalid_argument(format("Invalid host_id {}: {}", s, e.what()));
        }
        break;
    case param_type::endpoint:
        try {
            endpoint = gms::inet_address(s);
        } catch (std::invalid_argument& e) {
            throw std::invalid_argument(format("Invalid inet_address {}: {}", s, e.what()));
        }
        break;
    case param_type::auto_detect:
        try {
            id = host_id(utils::UUID(s));
        } catch (const marshal_exception& e) {
            try {
                endpoint = gms::inet_address(s);
            } catch (std::invalid_argument& e) {
                throw std::invalid_argument(format("Invalid host_id or inet_address {}", s));
            }
        }
    }
}

template <typename NodeId>
void host_id_or_endpoint::resolve(const generic_token_metadata<NodeId>& tm) {
    if (id) {
        auto endpoint_opt = tm.get_endpoint_for_host_id_if_known(id);
        if (!endpoint_opt) {
            throw std::runtime_error(format("Host ID {} not found in the cluster", id));
        }
        endpoint = *endpoint_opt;
    } else {
        auto opt_id = tm.get_host_id_if_known(endpoint);
        if (!opt_id) {
            throw std::runtime_error(format("Host inet address {} not found in the cluster", endpoint));
        }
        id = *opt_id;
    }
}

template class generic_token_metadata<locator::host_id>;
template class generic_token_metadata<gms::inet_address>;
template void host_id_or_endpoint::resolve(const token_metadata& tm);
template void host_id_or_endpoint::resolve(const token_metadata2& tm);
template token_metadata2* generic_token_metadata<gms::inet_address>::get_new<>();
template const token_metadata2* generic_token_metadata<gms::inet_address>::get_new<>() const;
template lw_shared_ptr<const token_metadata2> generic_token_metadata<gms::inet_address>::get_new_strong<>() const;
template generic_token_metadata<gms::inet_address>::generic_token_metadata(std::unique_ptr<token_metadata_impl<gms::inet_address>>, token_metadata2);
template generic_token_metadata<gms::inet_address>::generic_token_metadata(token_metadata2_ptr);
template std::unordered_set<gms::inet_address> generic_token_metadata<locator::host_id>::get_all_ips<>() const;

} // namespace locator
