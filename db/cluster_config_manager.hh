/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <functional>
#include <optional>
#include <string_view>
#include <tuple>
#include <cstdint>
#include <vector>

#include <absl/container/flat_hash_map.h>

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>

#include "service/migration_listener.hh"
#include "seastarx.hh"
#include "utils/UUID.hh"
#include "utils/config_file.hh"
#include "utils/hash.hh"

namespace cql3 {
class query_processor;
}

namespace replica {
class database;
}

namespace db {

class config;

namespace cluster_config_registry {
struct option;
}

class cluster_config_manager : public peering_sharded_service<cluster_config_manager> {
public:
    struct lookup_context {
        std::optional<sstring> dc_name;
        std::optional<sstring> rack_name;
        std::optional<utils::UUID> node_uuid;
        std::optional<sstring> keyspace_name;
        std::optional<sstring> table_name;
    };

    cluster_config_manager(seastar::sharded<cluster_config_manager>& owner, replica::database& db, cql3::query_processor& qp);

    future<> stop();
    future<> refresh();
    future<> wait_until_ready();

    std::optional<sstring> resolve_config(std::string_view config_name, const lookup_context& ctx) const;

    // Resolve an option and return its effective value in the option's native type, falling
    // back to the default registered for it when no scope in the chain stores an override.
    // These are what a consuming subsystem normally calls: unlike resolve_config(), which
    // reports absence so callers that need to distinguish "no override" (DESCRIBE, validation)
    // can, these always yield a usable value, and the default comes from the registry rather
    // than from the call site.
    //
    // `opt` is a registry entry (cluster_config_registry::find()), looked up once by the
    // consumer rather than by name on every read. Its type must match the accessor.
    bool resolve_boolean_config(const cluster_config_registry::option& opt, const lookup_context& ctx) const;
    int64_t resolve_integer_config(const cluster_config_registry::option& opt, const lookup_context& ctx) const;
    double resolve_floating_point_config(const cluster_config_registry::option& opt, const lookup_context& ctx) const;
    sstring resolve_text_config(const cluster_config_registry::option& opt, const lookup_context& ctx) const;

    // A consumer registers a callback to be told the effective value of an option for each
    // target the option resolves for: once initially, and again whenever a refresh pass finds
    // the value changed since the one last delivered. A table-oriented option has one target
    // per table, so a keyspace-scope value reaches the callback only once a table exists in
    // that keyspace; a node-oriented option has the local node as its single target. The
    // callback receives the resolved text, or std::nullopt when no scope stores an override
    // and the consumer should use the registered default (the registry's to_* converters do
    // that).
    //
    // The callback is registered on, and invoked only on, the calling shard. The caller owns
    // its safety there: captures must be valid on that shard and outlive the manager. Register
    // on every shard (invoke_on_all) for per-shard behavior, or on one shard to fire once. A
    // pass that fails midway is retried and re-delivers what the failed pass delivered, so the
    // callback must be idempotent.
    using config_callback_function = std::function<future<>(const lookup_context&, std::optional<sstring>)>;

    class config_callback_registration {
    public:
        config_callback_registration() noexcept = default;
        config_callback_registration(const config_callback_registration&) = delete;
        config_callback_registration& operator=(const config_callback_registration&) = delete;
        config_callback_registration(config_callback_registration&& other) noexcept;
        config_callback_registration& operator=(config_callback_registration&& other) noexcept;
        ~config_callback_registration();

        void unregister() noexcept;

    private:
        friend class cluster_config_manager;
        config_callback_registration(cluster_config_manager& owner, uint64_t callback_id) noexcept;

        cluster_config_manager* _owner = nullptr;
        uint64_t _callback_id = 0;
    };

    config_callback_registration register_config_callback(sstring config_name, config_callback_function on_change);

private:
    using config_map = absl::flat_hash_map<sstring, sstring>;
    using rack_key = std::tuple<sstring, sstring>;
    using table_key = std::tuple<sstring, sstring>;
    // A callback invocation target: (keyspace, table) for a table-oriented option, a single
    // empty key for a node-oriented one (whose only target is the local node).
    using callback_target_key = std::tuple<sstring, sstring>;
    using callback_values = absl::flat_hash_map<callback_target_key, std::optional<sstring>, utils::tuple_hash>;

    // One refresh pass's fully loaded view of every config table. Loaded once per pass on
    // the coordinating shard (load_caches) and then copied onto each shard (apply_refresh),
    // so the full-scan reads run once per pass instead of once per shard.
    struct scope_caches {
        config_map cluster_configs;
        absl::flat_hash_map<sstring, config_map> dc_configs;
        absl::flat_hash_map<rack_key, config_map, utils::tuple_hash> rack_configs;
        absl::flat_hash_map<utils::UUID, config_map> node_configs;
        absl::flat_hash_map<sstring, config_map> keyspace_configs;
        absl::flat_hash_map<table_key, config_map, utils::tuple_hash> table_configs;
    };

    // Listens for schema merges that touched a cluster-config table. Registered on shard 0
    // only: the notification fires on every shard, but a refresh is coordinated from shard 0
    // anyway, so registering everywhere would just trigger smp_count identical refreshes.
    //
    // The notification handler is purely synchronous: it records the request and wakes the
    // manager's refresh fiber. It never creates a future, so there is no dropped continuation
    // whose lifetime the manager would have to reason about.
    class config_change_listener final : public service::migration_listener::empty_listener {
    public:
        explicit config_change_listener(cluster_config_manager& owner) noexcept;
        void on_cluster_config_change() override;

    private:
        cluster_config_manager& _owner;
    };

    struct config_callback {
        uint64_t id = 0;
        sstring config_name;
        config_callback_function on_change;
        bool registered = true;
        // The value last delivered to on_change for each target the option resolves for.
        // A refresh pass invokes the callback only for targets whose resolved value differs
        // from the entry here (or that have no entry yet); entries for targets that
        // disappear (dropped tables) are pruned at the end of each pass.
        callback_values last_pushed;
    };

    void request_refresh();
    future<> refresh_fiber();
    future<scope_caches> load_caches();
    future<> apply_refresh(const scope_caches& caches);
    future<> run_config_callbacks();
    void unregister_config_callback(uint64_t callback_id) noexcept;
    void sweep_unregistered_callbacks() noexcept;

    void signal_ready();
    std::optional<sstring> get_config_for(const config_map& configs, std::string_view config_name) const;

    // Per-scope lookups used by resolve_config(). Deliberately private: a consumer reads an
    // option through resolve_config() or the typed resolve_*_config() accessors, so no call
    // site can consult a single scope and bypass the precedence chain.
    std::optional<sstring> get_cluster_config(std::string_view config_name) const;
    std::optional<sstring> get_dc_config(std::string_view dc_name, std::string_view config_name) const;
    std::optional<sstring> get_rack_config(std::string_view dc_name, std::string_view rack_name, std::string_view config_name) const;
    std::optional<sstring> get_node_config(const utils::UUID& node_uuid, std::string_view config_name) const;
    std::optional<sstring> get_keyspace_config(std::string_view keyspace_name, std::string_view config_name) const;
    std::optional<sstring> get_table_config(std::string_view keyspace_name, std::string_view table_name, std::string_view config_name) const;

    // Look up `config_name` in a keyed scope map (dc/rack/node/keyspace/table):
    // nullopt if the key is absent or holds no such override. Shared by the
    // get_*_config accessors so every scope handles a miss identically.
    template <typename Map, typename Key>
    std::optional<sstring> lookup_in_scope(const Map& scope_map, const Key& key, std::string_view config_name) const {
        auto it = scope_map.find(key);
        if (it == scope_map.end()) {
            return std::nullopt;
        }
        return get_config_for(it->second, config_name);
    }

private:
    seastar::sharded<cluster_config_manager>& _owner;
    replica::database& _db;
    cql3::query_processor& _qp;

    config_change_listener _listener;
    bool _listener_registered = false;
    gate _gate;
    // Fired by stop() so a refresh retry-backoff (refresh_fiber) wakes promptly instead of
    // delaying shutdown by the remaining backoff.
    abort_source _abort_source;
    // All refresh execution happens in one long-lived fiber on shard 0, started at
    // construction and joined by stop(). request_refresh() bumps
    // _requested_refresh_generation and signals _refresh_cv; the fiber runs passes until
    // _completed_refresh_generation catches up, so requests arriving during a pass coalesce
    // into a single follow-up pass. Owning the background work as a joined member future -
    // rather than detaching a continuation per notification - makes the lifetime structural:
    // the fiber is provably finished before the manager is destroyed.
    future<> _refresh_fiber = make_ready_future<>();
    condition_variable _refresh_cv;
    // Broadcast whenever _completed_refresh_generation advances (and on stop());
    // refresh() waits on it to keep its wait-for-completion contract.
    condition_variable _refresh_done_cv;
    uint64_t _requested_refresh_generation = 0;
    uint64_t _completed_refresh_generation = 0;
    bool _stopping = false;
    // Latches refreshes off during startup. The migration listener is registered at
    // construction, long before boot has applied the persisted group0 state, so schema
    // merges replayed while starting up would otherwise trigger refreshes that read the
    // config tables mid-replay - and, worse, signal_ready() would open the
    // wait_until_ready() barrier before the cache reflects an authoritative read. Until
    // refresh() flips this flag (main calls it once the group0 state machine is enabled),
    // triggers only bump _requested_refresh_generation while the fiber stays parked; the
    // first authoritative pass then covers everything requested so far.
    bool _authoritative_refresh_enabled = false;
    bool _is_ready = false;
    // Broadcast when _is_ready flips and on stop(); wait_until_ready() waits on it.
    condition_variable _ready_cv;

    config_map _cluster_configs;
    absl::flat_hash_map<sstring, config_map> _dc_configs;
    absl::flat_hash_map<rack_key, config_map, utils::tuple_hash> _rack_configs;
    absl::flat_hash_map<utils::UUID, config_map> _node_configs;
    absl::flat_hash_map<sstring, config_map> _keyspace_configs;
    absl::flat_hash_map<table_key, config_map, utils::tuple_hash> _table_configs;
    uint64_t _next_callback_id = 0;
    // True while run_config_callbacks() is invoking this shard's callbacks; a
    // callback that unregisters itself mid-pass defers the sweep until the pass ends.
    bool _callbacks_in_progress = false;
    std::vector<config_callback> _config_callbacks;
};

}
