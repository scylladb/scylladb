/*
 * Copyright (C) 2015-present ScyllaDB
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

#include <memory>
#include <functional>
#include <unordered_map>

#include <boost/icl/interval.hpp>
#include <boost/icl/interval_map.hpp>

#include <seastar/core/coroutine.hh>

#include "gms/inet_address.hh"
#include "locator/snitch_base.hh"
#include "dht/i_partitioner.hh"
#include "token_metadata.hh"
#include "snitch_base.hh"
#include <seastar/util/bool_class.hh>
#include "utils/maybe_yield.hh"

// forward declaration since replica/database.hh includes this file
namespace replica {
class keyspace;
}

namespace locator {

extern logging::logger rslogger;

using inet_address = gms::inet_address;
using token = dht::token;

enum class replication_strategy_type {
    simple,
    local,
    network_topology,
    everywhere_topology,
};

using can_yield = utils::can_yield;

using replication_strategy_config_options = std::map<sstring, sstring>;

using replication_map = std::unordered_map<token, inet_address_vector_replica_set>;

class effective_replication_map;
class effective_replication_map_factory;

class abstract_replication_strategy {
    friend class effective_replication_map;
protected:
    replication_strategy_config_options _config_options;
    snitch_ptr& _snitch;
    replication_strategy_type _my_type;

    template <typename... Args>
    void err(const char* fmt, Args&&... args) const {
        rslogger.error(fmt, std::forward<Args>(args)...);
    }

    template <typename... Args>
    void warn(const char* fmt, Args&&... args) const {
        rslogger.warn(fmt, std::forward<Args>(args)...);
    }

    template <typename... Args>
    void debug(const char* fmt, Args&&... args) const {
        rslogger.debug(fmt, std::forward<Args>(args)...);
    }

public:
    using ptr_type = seastar::shared_ptr<abstract_replication_strategy>;

    abstract_replication_strategy(
        snitch_ptr& snitch,
        const replication_strategy_config_options& config_options,
        replication_strategy_type my_type);

    // The returned vector has size O(number of normal token owners), which is O(number of nodes in the cluster).
    // Note: it is not guaranteed that the function will actually yield. If the complexity of a particular implementation
    // is small, that implementation may not yield since by itself it won't cause a reactor stall (assuming practical
    // cluster sizes and number of tokens per node). The caller is responsible for yielding if they call this function
    // in a loop.
    virtual future<inet_address_vector_replica_set> calculate_natural_endpoints(const token& search_token, const token_metadata& tm) const  = 0;

    virtual ~abstract_replication_strategy() {}
    static ptr_type create_replication_strategy(const sstring& strategy_name, const replication_strategy_config_options& config_options);
    static void validate_replication_strategy(const sstring& ks_name,
                                              const sstring& strategy_name,
                                              const replication_strategy_config_options& config_options,
                                              const topology& topology);
    static void validate_replication_factor(sstring rf);

    static sstring to_qualified_class_name(std::string_view strategy_class_name);

    virtual inet_address_vector_replica_set get_natural_endpoints(const token& search_token, const effective_replication_map& erm) const;
    virtual void validate_options() const = 0;
    virtual std::optional<std::set<sstring>> recognized_options(const topology&) const = 0;
    virtual size_t get_replication_factor(const token_metadata& tm) const = 0;
    // Decide if the replication strategy allow removing the node being
    // replaced from the natural endpoints when a node is being replaced in the
    // cluster. LocalStrategy is the not allowed to do so because it always
    // returns the node itself as the natural_endpoints and the node will not
    // appear in the pending_endpoints.
    virtual bool allow_remove_node_being_replaced_from_natural_endpoints() const = 0;
    replication_strategy_type get_type() const noexcept { return _my_type; }
    const replication_strategy_config_options get_config_options() const noexcept { return _config_options; }

    // Use the token_metadata provided by the caller instead of _token_metadata
    // Note: must be called with initialized, non-empty token_metadata.
    future<dht::token_range_vector> get_ranges(inet_address ep, token_metadata_ptr tmptr) const;

    // Caller must ensure that token_metadata will not change throughout the call.
    future<std::unordered_map<dht::token_range, inet_address_vector_replica_set>> get_range_addresses(const token_metadata& tm) const;

    future<dht::token_range_vector> get_pending_address_ranges(const token_metadata_ptr tmptr, token pending_token, inet_address pending_address) const;

    future<dht::token_range_vector> get_pending_address_ranges(const token_metadata_ptr tmptr, std::unordered_set<token> pending_tokens, inet_address pending_address) const;

private:
    future<dht::token_range_vector> get_address_ranges(const token_metadata& tm, inet_address endpoint) const;
};

// Holds the full replication_map resulting from applying the
// effective replication strategy over the given token_metadata
// and replication_strategy_config_options.
class effective_replication_map : public enable_lw_shared_from_this<effective_replication_map> {
public:
    struct factory_key {
        replication_strategy_type rs_type;
        long ring_version;
        replication_strategy_config_options rs_config_options;

        factory_key(replication_strategy_type rs_type_, const replication_strategy_config_options& rs_config_options_, long ring_version_)
            : rs_type(std::move(rs_type_))
            , ring_version(ring_version_)
            , rs_config_options(std::move(rs_config_options_))
        {}

        factory_key(factory_key&&) = default;
        factory_key(const factory_key&) = default;

        bool operator==(const factory_key& o) const = default;
        bool operator!=(const factory_key& o) const = default;

        sstring to_sstring() const;
    };

private:
    using address_ranges = std::unordered_multimap<inet_address, dht::token_range>;
    using pending_ranges = std::unordered_multimap<range<token>, inet_address>;
    using pending_ranges_interval_map = boost::icl::interval_map<token, std::unordered_set<inet_address>>;

    abstract_replication_strategy::ptr_type _rs;
    token_metadata_ptr _tmptr;
    replication_map _replication_map;
    size_t _replication_factor;
    pending_ranges_interval_map _pending_ranges_interval_map;
    std::optional<factory_key> _factory_key = std::nullopt;
    effective_replication_map_factory* _factory = nullptr;

    friend class abstract_replication_strategy;
    friend class effective_replication_map_factory;
public:
    explicit effective_replication_map(abstract_replication_strategy::ptr_type rs, token_metadata_ptr tmptr, replication_map replication_map, size_t replication_factor) noexcept
        : _rs(std::move(rs))
        , _tmptr(std::move(tmptr))
        , _replication_map(std::move(replication_map))
        , _replication_factor(replication_factor)
    { }
    effective_replication_map() = delete;
    effective_replication_map(effective_replication_map&&) = default;
    ~effective_replication_map();

    const token_metadata_ptr& get_token_metadata_ptr() const noexcept {
        return _tmptr;
    }

    const replication_map& get_replication_map() const noexcept {
        return _replication_map;
    }

    const size_t get_replication_factor() const noexcept {
        return _replication_factor;
    }

    future<> clear_gently() noexcept;

    future<replication_map> clone_endpoints_gently() const;

    inet_address_vector_replica_set get_natural_endpoints(const token& search_token) const;
    inet_address_vector_replica_set get_natural_endpoints_without_node_being_replaced(const token& search_token) const;

    // get_ranges() returns the list of ranges held by the given endpoint.
    // The list is sorted, and its elements are non overlapping and non wrap-around.
    // It the analogue of Origin's getAddressRanges().get(endpoint).
    // This function is not efficient, and not meant for the fast path.
    //
    // Note: must be called after token_metadata has been initialized.
    dht::token_range_vector get_ranges(inet_address ep) const;

    // get_primary_ranges() returns the list of "primary ranges" for the given
    // endpoint. "Primary ranges" are the ranges that the node is responsible
    // for storing replica primarily, which means this is the first node
    // returned calculate_natural_endpoints().
    // This function is the analogue of Origin's
    // StorageService.getPrimaryRangesForEndpoint().
    //
    // Note: must be called after token_metadata has been initialized.
    dht::token_range_vector get_primary_ranges(inet_address ep) const;

    // get_primary_ranges_within_dc() is similar to get_primary_ranges()
    // except it assigns a primary node for each range within each dc,
    // instead of one node globally.
    //
    // Note: must be called after token_metadata has been initialized.
    dht::token_range_vector get_primary_ranges_within_dc(inet_address ep) const;

    std::unordered_map<dht::token_range, inet_address_vector_replica_set>
    get_range_addresses() const;

    // returns empty vector if token not found.
    inet_address_vector_topology_change pending_endpoints_for(const token& token) const;

    bool has_pending_ranges(inet_address endpoint) const noexcept;

private:
    dht::token_range_vector do_get_ranges(noncopyable_function<bool(inet_address_vector_replica_set)> should_add_range) const;

    future<address_ranges> calculate_address_ranges() const;

    future<> calculate_pending_ranges_for_replacing(
        const address_ranges& address_ranges,
        pending_ranges& new_pending_ranges,
        const std::unordered_map<inet_address, inet_address>& replacing_endpoints) const;
    future<> calculate_pending_ranges_for_leaving(
        const address_ranges& address_ranges,
        pending_ranges& new_pending_ranges,
        const token_metadata& all_left_metadata,
        const std::unordered_set<inet_address>& leaving_endpoints) const;
    future<> calculate_pending_ranges_for_bootstrap(
        pending_ranges& new_pending_ranges,
        token_metadata& all_left_metadata,
        const std::unordered_map<token, inet_address>& bootstrap_tokens) const;

    future<> set_pending_ranges(pending_ranges new_pending_ranges);

     /**
      * Calculate pending ranges according to bootsrapping and leaving nodes. Reasoning is:
      *
      * (1) When in doubt, it is better to write too much to a node than too little. That is, if
      * there are multiple nodes moving, calculate the biggest ranges a node could have. Cleaning
      * up unneeded data afterwards is better than missing writes during movement.
      * (2) When a node leaves, ranges for other nodes can only grow (a node might get additional
      * ranges, but it will not lose any of its current ranges as a result of a leave). Therefore
      * we will first remove _all_ leaving tokens for the sake of calculation and then check what
      * ranges would go where if all nodes are to leave. This way we get the biggest possible
      * ranges with regard current leave operations, covering all subsets of possible final range
      * values.
      * (3) When a node bootstraps, ranges of other nodes can only get smaller. Without doing
      * complex calculations to see if multiple bootstraps overlap, we simply base calculations
      * on the same token ring used before (reflecting situation after all leave operations have
      * completed). Bootstrapping nodes will be added and removed one by one to that metadata and
      * checked what their ranges would be. This will give us the biggest possible ranges the
      * node could have. It might be that other bootstraps make our actual final ranges smaller,
      * but it does not matter as we can clean up the data afterwards.
      *
      * NOTE: This is heavy and ineffective operation. This will be done only once when a node
      * changes state in the cluster, so it should be manageable.
      */
    future<> update_pending_ranges();

    friend future<lw_shared_ptr<effective_replication_map>> calculate_effective_replication_map(abstract_replication_strategy::ptr_type rs, token_metadata_ptr tmptr);
public:
    static factory_key make_factory_key(const abstract_replication_strategy::ptr_type& rs, const token_metadata_ptr& tmptr);

    const factory_key& get_factory_key() const noexcept {
        return *_factory_key;
    }

    void set_factory(effective_replication_map_factory& factory, factory_key key) noexcept {
        _factory = &factory;
        _factory_key.emplace(std::move(key));
    }

    bool is_registered() const noexcept {
        return _factory != nullptr;
    }

    void unregister() noexcept {
        _factory = nullptr;
    }
};

using effective_replication_map_ptr = lw_shared_ptr<const effective_replication_map>;
using mutable_effective_replication_map_ptr = lw_shared_ptr<effective_replication_map>;

inline mutable_effective_replication_map_ptr make_effective_replication_map(abstract_replication_strategy::ptr_type rs, token_metadata_ptr tmptr, replication_map replication_map, size_t replication_factor) {
    return make_lw_shared<effective_replication_map>(std::move(rs), std::move(tmptr), std::move(replication_map), replication_factor);
}

// Apply the replication strategy over the current configuration and the given token_metadata.
future<mutable_effective_replication_map_ptr> calculate_effective_replication_map(abstract_replication_strategy::ptr_type rs, token_metadata_ptr tmptr);

} // namespace locator

std::ostream& operator<<(std::ostream& os, locator::replication_strategy_type);
std::ostream& operator<<(std::ostream& os, const locator::effective_replication_map::factory_key& key);

template <>
struct fmt::formatter<locator::effective_replication_map::factory_key> {
    constexpr auto parse(format_parse_context& ctx) {
        return ctx.end();
    }

    template <typename FormatContext>
    auto format(const locator::effective_replication_map::factory_key& key, FormatContext& ctx) {
        std::ostringstream os;
        os << key;
        return format_to(ctx.out(), "{}", os.str());
    }
};

template<>
struct appending_hash<locator::effective_replication_map::factory_key> {
    template<typename Hasher>
    void operator()(Hasher& h, const locator::effective_replication_map::factory_key& key) const {
        feed_hash(h, key.rs_type);
        feed_hash(h, key.ring_version);
        for (const auto& [opt, val] : key.rs_config_options) {
            h.update(opt.c_str(), opt.size());
            h.update(val.c_str(), val.size());
        }
    }
};

struct factory_key_hasher : public hasher {
    XXH64_state_t _state;
    factory_key_hasher(uint64_t seed = 0) noexcept {
        XXH64_reset(&_state, seed);
    }
    void update(const char* ptr, size_t length) noexcept {
        XXH64_update(&_state, ptr, length);
    }
    size_t finalize() {
        return static_cast<size_t>(XXH64_digest(&_state));
    }
};

namespace std {

template <>
struct hash<locator::effective_replication_map::factory_key> {
    size_t operator()(const locator::effective_replication_map::factory_key& key) const {
        factory_key_hasher h;
        appending_hash<locator::effective_replication_map::factory_key>{}(h, key);
        return h.finalize();
    }
};

} // namespace std

namespace locator {

class effective_replication_map_factory : public peering_sharded_service<effective_replication_map_factory> {
    std::unordered_map<effective_replication_map::factory_key, effective_replication_map*> _effective_replication_maps;
    future<> _background_work = make_ready_future<>();
    bool _stopped = false;

public:
    // looks up the effective_replication_map on the local shard.
    // If not found, tries to look one up for reference on shard 0
    // so its replication map can be cloned.  Otherwise, calculates the
    // effective_replication_map for the local shard.
    //
    // Therefore create should be called first on shard 0, then on all other shards.
    future<effective_replication_map_ptr> create_effective_replication_map(abstract_replication_strategy::ptr_type rs, token_metadata_ptr tmptr);

    future<> stop() noexcept;

    bool stopped() const noexcept {
        return _stopped;
    }

    // Note that func is called with a const effective_replication_map&
    // If it yields, it must keep erm.shared_from_this() to extend its lifetime
    future<> do_for_each(std::invocable<const effective_replication_map&> auto func) const noexcept {
        for (const auto& [key, p] : _effective_replication_maps) {
            co_await futurize_invoke(func, *p);
        }
    }
private:
    effective_replication_map_ptr find_effective_replication_map(const effective_replication_map::factory_key& key) const;
    effective_replication_map_ptr insert_effective_replication_map(mutable_effective_replication_map_ptr erm, effective_replication_map::factory_key key);

    bool erase_effective_replication_map(effective_replication_map* erm);

    void submit_background_work(future<> fut);

    friend class effective_replication_map;
};

}
