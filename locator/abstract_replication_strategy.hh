/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <memory>
#include <functional>
#include <unordered_map>
#include "gms/inet_address.hh"
#include "gms/feature_service.hh"
#include "locator/snitch_base.hh"
#include "dht/i_partitioner.hh"
#include "token_metadata.hh"
#include "snitch_base.hh"
#include <seastar/util/bool_class.hh>
#include "utils/maybe_yield.hh"
#include "utils/sequenced_set.hh"

// forward declaration since replica/database.hh includes this file
namespace replica {
class database;
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

using endpoint_set = utils::basic_sequenced_set<inet_address, inet_address_vector_replica_set>;

class vnode_effective_replication_map;
class effective_replication_map_factory;

class abstract_replication_strategy {
    friend class vnode_effective_replication_map;
protected:
    replication_strategy_config_options _config_options;
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
        const replication_strategy_config_options& config_options,
        replication_strategy_type my_type);

    // Evaluates to true iff calculate_natural_endpoints
    // returns different results for different tokens.
    virtual bool natural_endpoints_depend_on_token() const noexcept { return true; }

    // The returned vector has size O(number of normal token owners), which is O(number of nodes in the cluster).
    // Note: it is not guaranteed that the function will actually yield. If the complexity of a particular implementation
    // is small, that implementation may not yield since by itself it won't cause a reactor stall (assuming practical
    // cluster sizes and number of tokens per node). The caller is responsible for yielding if they call this function
    // in a loop.
    virtual future<endpoint_set> calculate_natural_endpoints(const token& search_token, const token_metadata& tm) const  = 0;

    virtual ~abstract_replication_strategy() {}
    static ptr_type create_replication_strategy(const sstring& strategy_name, const replication_strategy_config_options& config_options);
    static void validate_replication_strategy(const sstring& ks_name,
                                              const sstring& strategy_name,
                                              const replication_strategy_config_options& config_options,
                                              const gms::feature_service& fs,
                                              const topology& topology);
    static void validate_replication_factor(sstring rf);

    static sstring to_qualified_class_name(std::string_view strategy_class_name);

    virtual inet_address_vector_replica_set get_natural_endpoints(const token& search_token, const vnode_effective_replication_map& erm) const;
    // Returns the last stop_iteration result of the called func
    virtual stop_iteration for_each_natural_endpoint_until(const token& search_token, const vnode_effective_replication_map& erm, const noncopyable_function<stop_iteration(const inet_address&)>& func) const;
    virtual void validate_options(const gms::feature_service&) const = 0;
    virtual std::optional<std::unordered_set<sstring>> recognized_options(const topology&) const = 0;
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
    future<dht::token_range_vector> get_ranges(inet_address ep, const token_metadata& tm) const;

    // Caller must ensure that token_metadata will not change throughout the call.
    future<std::unordered_map<dht::token_range, inet_address_vector_replica_set>> get_range_addresses(const token_metadata& tm) const;

    future<dht::token_range_vector> get_pending_address_ranges(const token_metadata_ptr tmptr, std::unordered_set<token> pending_tokens, inet_address pending_address, locator::endpoint_dc_rack dr) const;
};

using replication_strategy_ptr = seastar::shared_ptr<const abstract_replication_strategy>;
using mutable_replication_strategy_ptr = seastar::shared_ptr<abstract_replication_strategy>;

/// \brief Represents effective replication (assignment of replicas to keys).
///
/// It's a result of application of a given replication strategy instance
/// over a particular token metadata version, for a given table.
/// Can be shared by multiple tables if they have the same replication.
///
/// Immutable, users can assume that it doesn't change.
///
/// Holding to this object keeps the associated token_metadata_ptr alive,
/// keeping the token metadata version alive and seen as in use.
class effective_replication_map {
protected:
    replication_strategy_ptr _rs;
    token_metadata_ptr _tmptr;
    size_t _replication_factor;
public:
    effective_replication_map(replication_strategy_ptr, token_metadata_ptr, size_t replication_factor) noexcept;
    virtual ~effective_replication_map() = default;

    const abstract_replication_strategy& get_replication_strategy() const noexcept { return *_rs; }
    const token_metadata& get_token_metadata() const noexcept { return *_tmptr; }
    const token_metadata_ptr& get_token_metadata_ptr() const noexcept { return _tmptr; }
    const topology& get_topology() const noexcept { return _tmptr->get_topology(); }
    size_t get_replication_factor() const noexcept { return _replication_factor; }

    /// Returns addresses of replicas for a given token.
    /// Does not include pending replicas except for a pending replica which
    /// has the same address as one of the old replicas. This can be the case during "nodetool replace"
    /// operation which adds a replica which has the same address as the replaced replica.
    /// Use get_natural_endpoints_without_node_being_replaced() to get replicas without any pending replicas.
    /// This won't be necessary after we implement https://github.com/scylladb/scylladb/issues/6403.
    virtual inet_address_vector_replica_set get_natural_endpoints(const token& search_token) const = 0;

    /// Returns addresses of replicas for a given token.
    /// Does not include pending replicas.
    virtual inet_address_vector_replica_set get_natural_endpoints_without_node_being_replaced(const token& search_token) const = 0;

    /// Returns the set of pending replicas for a given token.
    /// Pending replica is a replica which gains ownership of data.
    /// Non-empty only during topology change.
    virtual inet_address_vector_topology_change get_pending_endpoints(const token& search_token, const sstring& ks_name) const = 0;
};

using effective_replication_map_ptr = seastar::shared_ptr<const effective_replication_map>;
using mutable_effective_replication_map_ptr = seastar::shared_ptr<effective_replication_map>;

// Holds the full replication_map resulting from applying the
// effective replication strategy over the given token_metadata
// and replication_strategy_config_options.
// Used for token-based replication strategies.
class vnode_effective_replication_map : public enable_shared_from_this<vnode_effective_replication_map>
                                      , public effective_replication_map {
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
    replication_map _replication_map;
    std::optional<factory_key> _factory_key = std::nullopt;
    effective_replication_map_factory* _factory = nullptr;

    friend class abstract_replication_strategy;
    friend class effective_replication_map_factory;
public: // effective_replication_map
    inet_address_vector_replica_set get_natural_endpoints(const token& search_token) const override;
    inet_address_vector_replica_set get_natural_endpoints_without_node_being_replaced(const token& search_token) const override;
    inet_address_vector_topology_change get_pending_endpoints(const token& search_token, const sstring& ks_name) const override;
public:
    explicit vnode_effective_replication_map(replication_strategy_ptr rs, token_metadata_ptr tmptr, replication_map replication_map, size_t replication_factor) noexcept
        : effective_replication_map(std::move(rs), std::move(tmptr), replication_factor)
        , _replication_map(std::move(replication_map))
    { }
    vnode_effective_replication_map() = delete;
    vnode_effective_replication_map(vnode_effective_replication_map&&) = default;
    ~vnode_effective_replication_map();

    const replication_map& get_replication_map() const noexcept {
        return _replication_map;
    }

    future<> clear_gently() noexcept;

    future<replication_map> clone_endpoints_gently() const;

    stop_iteration for_each_natural_endpoint_until(const token& search_token, const noncopyable_function<stop_iteration(const inet_address&)>& func) const;

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

    future<std::unordered_map<dht::token_range, inet_address_vector_replica_set>>
    get_range_addresses() const;

private:
    dht::token_range_vector do_get_ranges(noncopyable_function<stop_iteration(bool& add_range, const inet_address& natural_endpoint)> consider_range_for_endpoint) const;

public:
    static factory_key make_factory_key(const replication_strategy_ptr& rs, const token_metadata_ptr& tmptr);

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

using vnode_effective_replication_map_ptr = shared_ptr<const vnode_effective_replication_map>;
using mutable_vnode_effective_replication_map_ptr = shared_ptr<vnode_effective_replication_map>;
using vnode_erm_ptr = vnode_effective_replication_map_ptr;
using mutable_vnode_erm_ptr = mutable_vnode_effective_replication_map_ptr;

inline mutable_vnode_erm_ptr make_effective_replication_map(replication_strategy_ptr rs, token_metadata_ptr tmptr, replication_map replication_map, size_t replication_factor) {
    return seastar::make_shared<vnode_effective_replication_map>(
            std::move(rs), std::move(tmptr), std::move(replication_map), replication_factor);
}

// Apply the replication strategy over the current configuration and the given token_metadata.
future<mutable_vnode_erm_ptr> calculate_effective_replication_map(replication_strategy_ptr rs, token_metadata_ptr tmptr);

// Class to hold a coherent view of a keyspace
// effective replication map on all shards
class global_vnode_effective_replication_map {
    std::vector<foreign_ptr<vnode_erm_ptr>> _erms;

public:
    global_vnode_effective_replication_map() : _erms(smp::count) {}
    global_vnode_effective_replication_map(global_vnode_effective_replication_map&&) = default;
    global_vnode_effective_replication_map& operator=(global_vnode_effective_replication_map&&) = default;

    future<> get_keyspace_erms(sharded<replica::database>& sharded_db, std::string_view keyspace_name);

    const vnode_effective_replication_map& get() const noexcept {
        return *_erms[this_shard_id()];
    }

    const vnode_effective_replication_map& operator*() const noexcept {
        return get();
    }

    const vnode_effective_replication_map* operator->() const noexcept {
        return &get();
    }
};

future<global_vnode_effective_replication_map> make_global_effective_replication_map(sharded<replica::database>& sharded_db, std::string_view keyspace_name);

} // namespace locator

std::ostream& operator<<(std::ostream& os, locator::replication_strategy_type);
std::ostream& operator<<(std::ostream& os, const locator::vnode_effective_replication_map::factory_key& key);

template <>
struct fmt::formatter<locator::vnode_effective_replication_map::factory_key> {
    constexpr auto parse(format_parse_context& ctx) {
        return ctx.end();
    }

    template <typename FormatContext>
    auto format(const locator::vnode_effective_replication_map::factory_key& key, FormatContext& ctx) {
        std::ostringstream os;
        os << key;
        return fmt::format_to(ctx.out(), "{}", os.str());
    }
};

template<>
struct appending_hash<locator::vnode_effective_replication_map::factory_key> {
    template<typename Hasher>
    void operator()(Hasher& h, const locator::vnode_effective_replication_map::factory_key& key) const {
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
struct hash<locator::vnode_effective_replication_map::factory_key> {
    size_t operator()(const locator::vnode_effective_replication_map::factory_key& key) const {
        factory_key_hasher h;
        appending_hash<locator::vnode_effective_replication_map::factory_key>{}(h, key);
        return h.finalize();
    }
};

} // namespace std

namespace locator {

class effective_replication_map_factory : public peering_sharded_service<effective_replication_map_factory> {
    std::unordered_map<vnode_effective_replication_map::factory_key, vnode_effective_replication_map*> _effective_replication_maps;
    future<> _background_work = make_ready_future<>();
    bool _stopped = false;

public:
    // looks up the vnode_effective_replication_map on the local shard.
    // If not found, tries to look one up for reference on shard 0
    // so its replication map can be cloned.  Otherwise, calculates the
    // vnode_effective_replication_map for the local shard.
    //
    // Therefore create should be called first on shard 0, then on all other shards.
    future<vnode_erm_ptr> create_effective_replication_map(replication_strategy_ptr rs, token_metadata_ptr tmptr);

    future<> stop() noexcept;

    bool stopped() const noexcept {
        return _stopped;
    }

private:
    vnode_erm_ptr find_effective_replication_map(const vnode_effective_replication_map::factory_key& key) const;
    vnode_erm_ptr insert_effective_replication_map(mutable_vnode_erm_ptr erm, vnode_effective_replication_map::factory_key key);

    bool erase_effective_replication_map(vnode_effective_replication_map* erm);

    void submit_background_work(future<> fut);

    friend class vnode_effective_replication_map;
};

}
