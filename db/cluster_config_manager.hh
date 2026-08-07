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

    future<> start();
    future<> stop();
    future<> refresh();
    future<> wait_until_ready();

    std::optional<sstring> get_cluster_config(std::string_view config_name) const;
    std::optional<sstring> get_dc_config(std::string_view dc_name, std::string_view config_name) const;
    std::optional<sstring> get_rack_config(std::string_view dc_name, std::string_view rack_name, std::string_view config_name) const;
    std::optional<sstring> get_node_config(const utils::UUID& node_uuid, std::string_view config_name) const;
    std::optional<sstring> get_keyspace_config(std::string_view keyspace_name, std::string_view config_name) const;
    std::optional<sstring> get_table_config(std::string_view keyspace_name, std::string_view table_name, std::string_view config_name) const;

    std::optional<sstring> resolve_config(std::string_view config_name, const lookup_context& ctx) const;

    // Registers a callback for an option on the calling shard only. The manager invokes the
    // callback exclusively on the shard it was registered on (the callback never crosses a
    // shard boundary), once per target the option resolves for on that shard (see
    // apply_runtime_config_updates).
    //
    // The caller owns the callback's safety on the shard it registers it on: its captures must
    // be valid to invoke on that shard and outlive the manager there. To get per-shard
    // behavior, register on every shard (e.g. invoke_on_all); each shard's callback may then
    // capture that shard's local state. To get fire-once behavior, register on a single shard
    // (e.g. local() on shard 0).
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

    future<config_callback_registration> register_config_callback(sstring config_name, config_callback_function on_change);

private:
    using config_map = absl::flat_hash_map<sstring, sstring>;
    using rack_key = std::tuple<sstring, sstring>;
    using table_key = std::tuple<sstring, sstring>;

    // Listens for schema merges that touched a cluster-config table. Registered on shard 0
    // only: the notification fires on every shard, but a refresh is coordinated from shard 0
    // anyway, so registering everywhere would just trigger smp_count identical refreshes.
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
    };

    future<> on_config_table_write();
    future<> schedule_refresh_all();
    future<> do_refresh_all();
    future<> reload_local_cache();
    future<> apply_runtime_config_updates();
    future<> invoke_callback(const config_callback_function& on_change, std::string_view config_name, const lookup_context& ctx) const;
    void unregister_config_callback(uint64_t callback_id) noexcept;
    void sweep_unregistered_callbacks() noexcept;

    void signal_ready();
    std::optional<sstring> get_config_for(const config_map& configs, std::string_view config_name) const;

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
    // Fired by stop() so a refresh retry-backoff (do_refresh_all) wakes promptly
    // instead of holding _gate open for the full backoff during shutdown.
    abort_source _abort_source;
    bool _refresh_in_progress = false;
    bool _refresh_requested = false;
    bool _authoritative_refresh_enabled = false;
    bool _is_ready = false;
    std::vector<promise<>> _ready_waiters;

    config_map _cluster_configs;
    absl::flat_hash_map<sstring, config_map> _dc_configs;
    absl::flat_hash_map<rack_key, config_map, utils::tuple_hash> _rack_configs;
    absl::flat_hash_map<utils::UUID, config_map> _node_configs;
    absl::flat_hash_map<sstring, config_map> _keyspace_configs;
    absl::flat_hash_map<table_key, config_map, utils::tuple_hash> _table_configs;
    uint64_t _next_callback_id = 0;
    unsigned _callback_invocation_depth = 0;
    std::vector<config_callback> _config_callbacks;
};

}
