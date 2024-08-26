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
#include <boost/icl/interval.hpp>
#include <boost/icl/interval_map.hpp>
#include "gms/inet_address.hh"
#include "locator/snitch_base.hh"
#include "locator/token_range_splitter.hh"
#include "dht/token-sharding.hh"
#include "token_metadata.hh"
#include "snitch_base.hh"
#include <seastar/util/bool_class.hh>
#include "utils/maybe_yield.hh"
#include "utils/sequenced_set.hh"
#include "utils/simple_hashers.hh"
#include "tablets.hh"

// forward declaration since replica/database.hh includes this file
namespace replica {
class database;
class keyspace;
}

namespace gms {
class feature_service;
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
struct replication_strategy_params {
    const replication_strategy_config_options options;
    std::optional<unsigned> initial_tablets;
    explicit replication_strategy_params(const replication_strategy_config_options& o, std::optional<unsigned> it) noexcept : options(o), initial_tablets(it) {}
};

using replication_map = std::unordered_map<token, host_id_vector_replica_set>;

using endpoint_set = utils::basic_sequenced_set<inet_address, inet_address_vector_replica_set>;
using host_id_set = utils::basic_sequenced_set<locator::host_id, host_id_vector_replica_set>;

class vnode_effective_replication_map;
class effective_replication_map_factory;
class per_table_replication_strategy;
class tablet_aware_replication_strategy;


class abstract_replication_strategy : public seastar::enable_shared_from_this<abstract_replication_strategy> {
    friend class vnode_effective_replication_map;
    friend class per_table_replication_strategy;
    friend class tablet_aware_replication_strategy;
protected:
    replication_strategy_config_options _config_options;
    replication_strategy_type _my_type;
    bool _per_table = false;
    bool _uses_tablets = false;
    bool _natural_endpoints_depend_on_token = true;

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
        replication_strategy_params params,
        replication_strategy_type my_type);

    // Evaluates to true iff calculate_natural_endpoints
    // returns different results for different tokens.
    bool natural_endpoints_depend_on_token() const noexcept { return _natural_endpoints_depend_on_token; }

    // The returned vector has size O(number of normal token owners), which is O(number of nodes in the cluster).
    // Note: it is not guaranteed that the function will actually yield. If the complexity of a particular implementation
    // is small, that implementation may not yield since by itself it won't cause a reactor stall (assuming practical
    // cluster sizes and number of tokens per node). The caller is responsible for yielding if they call this function
    // in a loop.
    virtual future<host_id_set> calculate_natural_endpoints(const token& search_token, const token_metadata& tm) const  = 0;
    future<endpoint_set> calculate_natural_ips(const token& search_token, const token_metadata& tm) const;

    virtual ~abstract_replication_strategy() {}
    static ptr_type create_replication_strategy(const sstring& strategy_name, replication_strategy_params params);
    static void validate_replication_strategy(const sstring& ks_name,
                                              const sstring& strategy_name,
                                              replication_strategy_params params,
                                              const gms::feature_service& fs,
                                              const topology& topology);
    static long parse_replication_factor(sstring rf);

    static sstring to_qualified_class_name(std::string_view strategy_class_name);

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

    // If returns true then tables governed by this replication strategy have separate
    // effective_replication_maps.
    // If returns false, they share the same effective_replication_map, which is per keyspace.
    // If returns true, then this replication strategy extends per_table_replication_strategy.
    // Note, a replication strategy may extend per_table_replication_strategy while !is_per_table(),
    // depending on actual strategy options.
    bool is_per_table() const { return _per_table; }
    const per_table_replication_strategy* maybe_as_per_table() const;

    // Returns true iff this replication strategy is based on vnodes.
    // If this is the case, all tables governed by this replication strategy share the effective replication map.
    bool is_vnode_based() const {
        return !is_per_table();
    }

    bool uses_tablets() const { return _uses_tablets; }
    const tablet_aware_replication_strategy* maybe_as_tablet_aware() const;

    // Use the token_metadata provided by the caller instead of _token_metadata
    // Note: must be called with initialized, non-empty token_metadata.
    future<dht::token_range_vector> get_ranges(locator::host_id ep, token_metadata_ptr tmptr) const;
    future<dht::token_range_vector> get_ranges(locator::host_id ep, const token_metadata& tm) const;

    // Caller must ensure that token_metadata will not change throughout the call.
    future<std::unordered_map<dht::token_range, inet_address_vector_replica_set>> get_range_addresses(const token_metadata& tm) const;

    future<dht::token_range_vector> get_pending_address_ranges(const token_metadata_ptr tmptr, std::unordered_set<token> pending_tokens, locator::host_id pending_address, locator::endpoint_dc_rack dr) const;
};

using ring_mapping = boost::icl::interval_map<token, std::unordered_set<locator::host_id>>;
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
    std::unique_ptr<abort_source> _validity_abort_source;
public:
    effective_replication_map(replication_strategy_ptr, token_metadata_ptr, size_t replication_factor) noexcept;
    effective_replication_map(effective_replication_map&&) noexcept = default;
    virtual ~effective_replication_map() = default;

    const abstract_replication_strategy& get_replication_strategy() const noexcept { return *_rs; }
    const token_metadata& get_token_metadata() const noexcept { return *_tmptr; }
    const token_metadata_ptr& get_token_metadata_ptr() const noexcept { return _tmptr; }
    const topology& get_topology() const noexcept { return _tmptr->get_topology(); }
    size_t get_replication_factor() const noexcept { return _replication_factor; }

    void invalidate() const noexcept {
        _validity_abort_source->request_abort();
    }

    /// Returns a reference to abort_source which is aborted when this effective replication map
    /// is no longer the latest table's effective replication map.
    abort_source& get_validity_abort_source() const {
        return *_validity_abort_source;
    }

    /// Returns addresses of replicas for a given token.
    /// Does not include pending replicas except for a pending replica which
    /// has the same address as one of the old replicas. This can be the case during "nodetool replace"
    /// operation which adds a replica which has the same address as the replaced replica.
    /// Use get_natural_endpoints_without_node_being_replaced() to get replicas without any pending replicas.
    /// This won't be necessary after we implement https://github.com/scylladb/scylladb/issues/6403.
    ///
    /// Excludes replicas which are in the left state. After replace, the replaced replica may
    /// still be in the replica set of the tablet until tablet scheduler rebuilds the replacing replica.
    /// The old replica will not be listed here. This is necessary to support replace-with-the-same-ip
    /// scenario. Since we return IPs here, writes to the old replica would be incorrectly routed to the
    /// new replica.
    ///
    /// The returned addresses are present in the topology object associated with this instance.
    virtual inet_address_vector_replica_set get_natural_endpoints(const token& search_token) const = 0;

    /// Returns a subset of replicas returned by get_natural_endpoints() without the pending replica.
    virtual inet_address_vector_replica_set get_natural_endpoints_without_node_being_replaced(const token& search_token) const = 0;

    /// Returns the set of pending replicas for a given token.
    /// Pending replica is a replica which gains ownership of data.
    /// Non-empty only during topology change.
    virtual inet_address_vector_topology_change get_pending_endpoints(const token& search_token) const = 0;

    /// Returns a list of nodes to which a read request should be directed.
    virtual inet_address_vector_replica_set get_endpoints_for_reading(const token& search_token) const = 0;

    /// Returns replicas for a given token.
    /// During topology change returns replicas which should be targets for writes, excluding the pending replica.
    /// Unlike get_natural_endpoints(), the replica set may include nodes in the left state which were
    /// replaced but not yet rebuilt.
    virtual host_id_vector_replica_set get_replicas(const token& search_token) const = 0;

    virtual std::optional<tablet_routing_info> check_locality(const token& token) const = 0;


    /// Returns true if there are any pending ranges for this endpoint.
    /// This operation is expensive, for vnode_erm it iterates
    /// over all pending ranges which is O(number of tokens).
    virtual bool has_pending_ranges(locator::host_id endpoint) const = 0;

    /// Returns a token_range_splitter which is line with the replica assignment of this replication map.
    /// The splitter can live longer than this instance.
    virtual std::unique_ptr<token_range_splitter> make_splitter() const = 0;

    /// Returns a sharder which reflects shard replica assignment of this replication map.
    /// The sharder is valid as long as this instance is kept alive.
    virtual const dht::sharder& get_sharder(const schema& s) const = 0;

    // get_ranges() returns the list of ranges held by the given endpoint.
    // The list is sorted, and its elements are non overlapping and non wrap-around.
    // It the analogue of Origin's getAddressRanges().get(endpoint).
    // This function is not efficient, and not meant for the fast path.
    //
    // Note: must be called after token_metadata has been initialized.
    virtual future<dht::token_range_vector> get_ranges(inet_address ep) const = 0;

    shard_id shard_for_reads(const schema& s, dht::token t) const {
        return get_sharder(s).shard_for_reads(t);
    }

    dht::shard_replica_set shard_for_writes(const schema& s, dht::token t) const {
        return get_sharder(s).shard_for_writes(t);
    }
};

using effective_replication_map_ptr = seastar::shared_ptr<const effective_replication_map>;
using mutable_effective_replication_map_ptr = seastar::shared_ptr<effective_replication_map>;

/// Replication strategies which support per-table replication extend this trait.
///
/// It will be accessed only if the replication strategy actually works in per-table mode,
/// that is after mark_as_per_table() is called, and as a result
/// abstract_replication_strategy::is_per_table() returns true.
class per_table_replication_strategy {
protected:
    void mark_as_per_table(abstract_replication_strategy& self) {
        self._per_table = true;
    }
public:
    virtual ~per_table_replication_strategy() = default;
    virtual effective_replication_map_ptr make_replication_map(table_id, token_metadata_ptr) const = 0;
};

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
    };
private:
    replication_map _replication_map;
    ring_mapping _pending_endpoints;
    ring_mapping _read_endpoints;
    std::unordered_set<locator::host_id> _dirty_endpoints;
    std::optional<factory_key> _factory_key = std::nullopt;
    effective_replication_map_factory* _factory = nullptr;

    friend class abstract_replication_strategy;
    friend class effective_replication_map_factory;
public: // effective_replication_map
    inet_address_vector_replica_set get_natural_endpoints(const token& search_token) const override;
    inet_address_vector_replica_set get_natural_endpoints_without_node_being_replaced(const token& search_token) const override;
    inet_address_vector_topology_change get_pending_endpoints(const token& search_token) const override;
    inet_address_vector_replica_set get_endpoints_for_reading(const token& search_token) const override;
    host_id_vector_replica_set get_replicas(const token& search_token) const override;
    std::optional<tablet_routing_info> check_locality(const token& token) const override;
    bool has_pending_ranges(locator::host_id endpoint) const override;
    std::unique_ptr<token_range_splitter> make_splitter() const override;
    const dht::sharder& get_sharder(const schema& s) const override;
    future<dht::token_range_vector> get_ranges(inet_address ep) const override;
public:
    explicit vnode_effective_replication_map(replication_strategy_ptr rs, token_metadata_ptr tmptr, replication_map replication_map,
            ring_mapping pending_endpoints, ring_mapping read_endpoints, std::unordered_set<locator::host_id> dirty_endpoints, size_t replication_factor) noexcept
        : effective_replication_map(std::move(rs), std::move(tmptr), replication_factor)
        , _replication_map(std::move(replication_map))
        , _pending_endpoints(std::move(pending_endpoints))
        , _read_endpoints(std::move(read_endpoints))
        , _dirty_endpoints(std::move(dirty_endpoints))
    { }
    vnode_effective_replication_map() = delete;
    vnode_effective_replication_map(vnode_effective_replication_map&&) = default;
    ~vnode_effective_replication_map();

    struct cloned_data {
        replication_map replication_map;
        ring_mapping pending_endpoints;
        ring_mapping read_endpoints;
        std::unordered_set<locator::host_id> dirty_endpoints;
    };
    // boost::icl::interval_map is not no_throw_move_constructible -> can't return cloned_data by val,
    // since future_state requires T to be no_throw_move_constructible.
    future<std::unique_ptr<cloned_data>> clone_data_gently() const;

    // get_primary_ranges() returns the list of "primary ranges" for the given
    // endpoint. "Primary ranges" are the ranges that the node is responsible
    // for storing replica primarily, which means this is the first node
    // returned calculate_natural_endpoints().
    // This function is the analogue of Origin's
    // StorageService.getPrimaryRangesForEndpoint().
    //
    // Note: must be called after token_metadata has been initialized.
    future<dht::token_range_vector> get_primary_ranges(inet_address ep) const;

    // get_primary_ranges_within_dc() is similar to get_primary_ranges()
    // except it assigns a primary node for each range within each dc,
    // instead of one node globally.
    //
    // Note: must be called after token_metadata has been initialized.
    future<dht::token_range_vector> get_primary_ranges_within_dc(inet_address ep) const;

    future<std::unordered_map<dht::token_range, inet_address_vector_replica_set>>
    get_range_addresses() const;

    // Returns a set of dirty endpoint. An endpoint is dirty if it may have a data
    // for a range it does not own any longer. Will be empty if there is no topology
    // change. During topology can be empty as well (for instance for everywhere strategy)
    const std::unordered_set<locator::host_id>& get_dirty_endpoints() const {
        return _dirty_endpoints;
    }

    std::unordered_set<locator::host_id> get_all_pending_nodes() const;

private:
    future<dht::token_range_vector> do_get_ranges(noncopyable_function<stop_iteration(bool& add_range, const inet_address& natural_endpoint)> consider_range_for_endpoint) const;
    inet_address_vector_replica_set do_get_natural_endpoints(const token& tok, bool is_vnode) const;
    host_id_vector_replica_set do_get_replicas(const token& tok, bool is_vnode) const;
    stop_iteration for_each_natural_endpoint_until(const token& vnode_tok, const noncopyable_function<stop_iteration(const inet_address&)>& func) const;

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

inline mutable_vnode_erm_ptr make_effective_replication_map(replication_strategy_ptr rs, token_metadata_ptr tmptr, replication_map replication_map, ring_mapping pending_endpoints,
    ring_mapping read_endpoints, std::unordered_set<locator::host_id> dirty_endpoints, size_t replication_factor) {
    return seastar::make_shared<vnode_effective_replication_map>(
            std::move(rs), std::move(tmptr), std::move(replication_map),
        std::move(pending_endpoints), std::move(read_endpoints), std::move(dirty_endpoints), replication_factor);
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

template <>
struct fmt::formatter<locator::replication_strategy_type> : fmt::formatter<string_view> {
    auto format(locator::replication_strategy_type, fmt::format_context& ctx) const -> decltype(ctx.out());
};

template <>
struct fmt::formatter<locator::vnode_effective_replication_map::factory_key> : fmt::formatter<string_view> {
    auto format(const locator::vnode_effective_replication_map::factory_key&, fmt::format_context& ctx) const -> decltype(ctx.out());
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

namespace std {

template <>
struct hash<locator::vnode_effective_replication_map::factory_key> {
    size_t operator()(const locator::vnode_effective_replication_map::factory_key& key) const {
        simple_xx_hasher h;
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

void maybe_remove_node_being_replaced(const token_metadata&,
                                      const abstract_replication_strategy&,
                                      inet_address_vector_replica_set& natural_endpoints);

}
