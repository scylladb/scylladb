/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "db/cluster_config_manager.hh"

#include <utility>

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>

#include "cql3/query_processor.hh"
#include "cql3/untyped_result_set.hh"
#include "db/cluster_config_registry.hh"
#include "db/config.hh"
#include "db/schema_tables.hh"
#include "locator/topology.hh"
#include "replica/database.hh"
#include "utils/assert.hh"

namespace db {

namespace {

logging::logger cluster_config_logger("cluster_config_manager");

// Backoff between retries when a full cache refresh fails transiently. Kept short
// so the cache converges quickly once the underlying read recovers; the wait is
// abortable, so shutdown is not delayed by it.
constexpr std::chrono::seconds refresh_retry_backoff{1};

}

cluster_config_manager::config_callback_registration::config_callback_registration(cluster_config_manager& owner, uint64_t callback_id) noexcept
    : _owner(&owner)
    , _callback_id(callback_id) {
}

cluster_config_manager::config_callback_registration::config_callback_registration(config_callback_registration&& other) noexcept
    : _owner(std::exchange(other._owner, nullptr))
    , _callback_id(std::exchange(other._callback_id, 0)) {
}

cluster_config_manager::config_callback_registration& cluster_config_manager::config_callback_registration::operator=(config_callback_registration&& other) noexcept {
    if (this != &other) {
        unregister();
        _owner = std::exchange(other._owner, nullptr);
        _callback_id = std::exchange(other._callback_id, 0);
    }
    return *this;
}

cluster_config_manager::config_callback_registration::~config_callback_registration() {
    unregister();
}

void cluster_config_manager::config_callback_registration::unregister() noexcept {
    if (!_owner) {
        return;
    }
    _owner->unregister_config_callback(_callback_id);
    _owner = nullptr;
    _callback_id = 0;
}

cluster_config_manager::config_change_listener::config_change_listener(cluster_config_manager& owner) noexcept
    : _owner(owner) {
}

void cluster_config_manager::config_change_listener::on_cluster_config_change() {
    (void)_owner.on_config_table_write().handle_exception([] (std::exception_ptr ep) {
        try {
            std::rethrow_exception(ep);
        } catch (const seastar::gate_closed_exception&) {
            // Shutting down; the cache no longer matters.
        } catch (const std::exception& e) {
            cluster_config_logger.warn("Failed to refresh cluster config cache after config-table write: {}", e);
        } catch (...) {
            cluster_config_logger.warn("Failed to refresh cluster config cache after config-table write: unknown exception");
        }
    });
}

cluster_config_manager::cluster_config_manager(seastar::sharded<cluster_config_manager>& owner, replica::database& db, cql3::query_processor& qp)
    : _owner(owner)
    , _db(db)
    , _qp(qp)
    , _listener(*this) {
}

future<> cluster_config_manager::start() {
    // Only shard 0 needs the notification: schema merges notify every shard, but
    // schedule_refresh_all() is coordinated on shard 0 regardless.
    if (this_shard_id() == 0) {
        _db.get_notifier().register_listener(&_listener);
        _listener_registered = true;
    }
    co_return;
}

future<> cluster_config_manager::stop() {
    // Wake any in-flight refresh retry-backoff so it does not hold _gate open for
    // the remaining backoff below.
    if (!_abort_source.abort_requested()) {
        _abort_source.request_abort();
    }
    if (_listener_registered) {
        _listener_registered = false;
        co_await _db.get_notifier().unregister_listener(&_listener);
    }
    // Wake any wait_until_ready() waiter still parked on this shard (the barrier only ever
    // opens on a successful refresh, so a waiter is parked whenever none has completed yet).
    // Without this the promises are destroyed unsatisfied and waiters observe an implicit
    // broken_promise; instead surface the gate closing as a clean shutdown signal. stop()
    // never marks the manager ready.
    for (auto& waiter : _ready_waiters) {
        waiter.set_exception(std::make_exception_ptr(seastar::gate_closed_exception()));
    }
    _ready_waiters.clear();
    // Close the gate before touching _config_callbacks. A callback pass running under
    // apply_runtime_config_updates() holds this shard's gate across its suspension points and
    // indexes into _config_callbacks after resuming, so clearing the vector first would leave
    // it reading freed storage.
    co_await _gate.close();
    _config_callbacks.clear();
}

future<> cluster_config_manager::refresh() {
    return _owner.invoke_on(0, [] (cluster_config_manager& manager) {
        manager._authoritative_refresh_enabled = true;
        return manager.schedule_refresh_all();
    });
}

future<> cluster_config_manager::wait_until_ready() {
    if (_is_ready) {
        co_return;
    }
    promise<> done;
    auto fut = done.get_future();
    _ready_waiters.push_back(std::move(done));
    co_await std::move(fut);
}

future<cluster_config_manager::config_callback_registration> cluster_config_manager::register_config_callback(sstring config_name, config_callback_function on_change) {
    // Registered on the calling shard only. apply_runtime_config_updates() iterates each
    // shard's own _config_callbacks, so this callback is invoked exclusively on this shard;
    // it never crosses a shard boundary. The caller owns the callback's safety on this shard
    // (valid captures, lifetime >= manager on this shard). To get per-shard behavior, register
    // on every shard (e.g. invoke_on_all); to fire once, register on a single shard.
    const auto callback_id = ++_next_callback_id;
    _config_callbacks.push_back(config_callback{
        .id = callback_id,
        .config_name = std::move(config_name),
        .on_change = std::move(on_change),
        .registered = true,
    });
    return make_ready_future<config_callback_registration>(config_callback_registration(*this, callback_id));
}

future<> cluster_config_manager::on_config_table_write() {
    // Always invoked on shard 0: the config_change_listener is registered on shard 0 only,
    // and schema_applier::post_commit() fires the notification on shard 0's notifier alone.
    // Refresh coordination therefore runs here directly, with no shard hop.
    SCYLLA_ASSERT(this_shard_id() == 0);
    return schedule_refresh_all();
}

future<> cluster_config_manager::schedule_refresh_all() {
    _refresh_requested = true;
    if (!_authoritative_refresh_enabled) {
        co_return;
    }
    if (_refresh_in_progress) {
        co_return;
    }
    _refresh_in_progress = true;
    try {
        co_await with_gate(_gate, [this] {
            return do_refresh_all();
        });
    } catch (seastar::gate_closed_exception&) {
        _refresh_in_progress = false;
        co_return;
    } catch (...) {
        _refresh_in_progress = false;
        throw;
    }
}

future<> cluster_config_manager::do_refresh_all() {
    // Drain _refresh_requested: a request coalesced by schedule_refresh_all() while
    // a pass was already running is picked up by the loop condition.
    //
    // A transient failure of the pass (reload_local_cache()/apply_runtime_config_updates()
    // throwing, e.g. an internal SELECT failing under load) must NOT drop the pending
    // request and leave every shard's cache stale until some unrelated later write
    // happens to trigger another refresh. So a failed pass re-arms itself and retries
    // after a short, abortable backoff instead of propagating. Shutdown breaks out:
    // gate_closed propagates (schedule_refresh_all swallows it), and stop() fires
    // _abort_source so the backoff wakes immediately rather than pinning _gate open.
    while (_refresh_requested) {
        _refresh_requested = false;
        // Capture a failure rather than reacting in the catch handler: co_await
        // (the backoff below) is not permitted inside a catch block.
        std::exception_ptr failure;
        try {
            co_await _owner.invoke_on_all([] (cluster_config_manager& manager) {
                return manager.reload_local_cache();
            });
            co_await apply_runtime_config_updates();
            co_await _owner.invoke_on_all([] (cluster_config_manager& manager) {
                manager.signal_ready();
            });
        } catch (const seastar::gate_closed_exception&) {
            throw;
        } catch (...) {
            failure = std::current_exception();
        }
        if (failure) {
            cluster_config_logger.warn(
                    "Failed to refresh cluster config cache, will retry in {}s: {}",
                    refresh_retry_backoff.count(), failure);
            _refresh_requested = true;
            try {
                co_await seastar::sleep_abortable(refresh_retry_backoff, _abort_source);
            } catch (const seastar::sleep_aborted&) {
                break;
            }
        }
    }

    _refresh_in_progress = false;
    co_return;
}

future<> cluster_config_manager::apply_runtime_config_updates() {
    co_await _owner.invoke_on_all([] (cluster_config_manager& manager) -> future<> {
        // Enter THIS shard's gate. schedule_refresh_all() only holds shard 0's gate, which
        // says nothing about shard N: sharded<>::stop() runs stop() on all shards
        // concurrently, so without this the pass below could resume after shard N's stop()
        // had already torn down _config_callbacks.
        if (manager._gate.is_closed()) {
            co_return;
        }
        co_await with_gate(manager._gate, [&manager] () -> future<> {
        // Runs on every shard, but each shard fires only the callbacks registered on it
        // (register_config_callback stores per-shard). A callback registered on a single
        // shard is invoked only there; one registered on all shards is invoked on each.
        //
        // The local node's identity, used to resolve node-oriented options through the
        // node -> rack -> dc -> cluster precedence chain.
        lookup_context node_ctx;
        const auto& topology = manager._db.get_token_metadata().get_topology();
        node_ctx.dc_name = topology.get_datacenter();
        node_ctx.rack_name = topology.get_rack();
        node_ctx.node_uuid = topology.my_host_id().uuid();

        manager._callback_invocation_depth++;
        auto cleanup_callbacks = seastar::defer([&manager] {
            SCYLLA_ASSERT(manager._callback_invocation_depth > 0);
            manager._callback_invocation_depth--;
            if (manager._callback_invocation_depth == 0) {
                manager.sweep_unregistered_callbacks();
            }
        });

        const auto callback_count = manager._config_callbacks.size();
        for (size_t callback_idx = 0; callback_idx < callback_count; ++callback_idx) {
            if (!manager._config_callbacks[callback_idx].registered) {
                continue;
            }
            auto config_name = manager._config_callbacks[callback_idx].config_name;
            // Copy the callback closure into this frame before awaiting it: a table-oriented
            // option below awaits once per table, and a callback that suspends must not hold a
            // reference into _config_callbacks, since a concurrent register_config_callback()
            // can push_back and reallocate the vector, moving the stored closure away.
            auto on_change = manager._config_callbacks[callback_idx].on_change;
            const auto* opt = db::cluster_config_registry::find(config_name);
            const bool table_oriented = opt
                    && (db::cluster_config_registry::supports_scope(*opt, db::cluster_config_registry::scope::keyspace)
                        || db::cluster_config_registry::supports_scope(*opt, db::cluster_config_registry::scope::table));

            if (table_oriented) {
                // A table-oriented option resolves per table (table -> keyspace -> cluster).
                // The callback is invoked once for each table; re-invoking with an unchanged
                // value is harmless, so the manager does not track what it last pushed.
                std::vector<lookup_context> table_contexts;
                manager._db.get_tables_metadata().for_each_table([&] (table_id, lw_shared_ptr<replica::table> t) {
                    const auto& s = t->schema();
                    lookup_context ctx;
                    ctx.keyspace_name = s->ks_name();
                    ctx.table_name = s->cf_name();
                    table_contexts.push_back(std::move(ctx));
                });
                for (const auto& ctx : table_contexts) {
                    if (!manager._config_callbacks[callback_idx].registered) {
                        break;
                    }
                    co_await manager.invoke_callback(on_change, config_name, ctx);
                }
            } else {
                co_await manager.invoke_callback(on_change, config_name, node_ctx);
            }
        }
        co_return;
        });
    });
}

future<> cluster_config_manager::invoke_callback(const config_callback_function& on_change, std::string_view config_name, const lookup_context& ctx) const {
    // The callback receives an effective value when one resolves for this target, or
    // std::nullopt when the consumer should restore its own default. The callback is
    // expected to be idempotent, so the manager invokes it on every refresh without
    // remembering what it last pushed.
    //
    // on_change must reference a copy that lives in the caller's frame (see
    // apply_runtime_config_updates), not the entry stored in _config_callbacks: a callback
    // that suspends captures this closure, and a concurrent register_config_callback() may
    // reallocate the vector and move that entry away underneath a suspended callback.
    return on_change(ctx, resolve_config(config_name, ctx));
}

void cluster_config_manager::unregister_config_callback(uint64_t callback_id) noexcept {
    for (auto& callback : _config_callbacks) {
        if (callback.id == callback_id) {
            callback.registered = false;
            break;
        }
    }

    if (_callback_invocation_depth == 0) {
        sweep_unregistered_callbacks();
    }
}

void cluster_config_manager::sweep_unregistered_callbacks() noexcept {
    std::erase_if(_config_callbacks, [] (const config_callback& callback) {
        return !callback.registered;
    });
}

future<> cluster_config_manager::reload_local_cache() {
    auto cluster_configs = config_map{};
    auto dc_configs = absl::flat_hash_map<sstring, config_map>{};
    auto rack_configs = absl::flat_hash_map<rack_key, config_map, utils::tuple_hash>{};
    auto node_configs = absl::flat_hash_map<utils::UUID, config_map>{};
    auto keyspace_configs = absl::flat_hash_map<sstring, config_map>{};
    auto table_configs = absl::flat_hash_map<table_key, config_map, utils::tuple_hash>{};

    // Scan one config table and hand each non-empty row's configs map to `store`,
    // which keys it into the right scope map. `query` and `store` are taken by
    // value so they outlive the internal query across the co_await.
    auto load_scope = [this] (sstring query, auto store) -> future<> {
        auto rows = co_await _qp.execute_internal(query, cql3::query_processor::cache_internal::no);
        for (const auto& row : *rows) {
            if (!row.has("configs")) {
                continue;
            }
            auto configs = row.template get_map<sstring, sstring>("configs");
            if (configs.empty()) {
                continue;
            }
            store(row, config_map(configs.begin(), configs.end()));
        }
    };

    co_await load_scope(
            format("SELECT cluster_name, configs FROM {}.{} WHERE cluster_name = '{}'",
                    schema_tables::v3::NAME, schema_tables::v3::SCYLLA_CLUSTERS, schema_tables::CLUSTER_CONFIG_SINGLETON_KEY),
            [&] (const auto&, config_map m) { cluster_configs.insert(m.begin(), m.end()); });

    co_await load_scope(
            format("SELECT dc_name, configs FROM {}.{}", schema_tables::v3::NAME, schema_tables::v3::SCYLLA_DATACENTERS),
            [&] (const auto& row, config_map m) { dc_configs.emplace(row.template get_as<sstring>("dc_name"), std::move(m)); });

    co_await load_scope(
            format("SELECT dc_name, rack_name, configs FROM {}.{}", schema_tables::v3::NAME, schema_tables::v3::SCYLLA_RACKS),
            [&] (const auto& row, config_map m) {
                rack_configs.emplace(
                        std::make_tuple(row.template get_as<sstring>("dc_name"), row.template get_as<sstring>("rack_name")),
                        std::move(m));
            });

    co_await load_scope(
            format("SELECT host_id, configs FROM {}.{}", schema_tables::v3::NAME, schema_tables::v3::SCYLLA_NODES),
            [&] (const auto& row, config_map m) { node_configs.emplace(row.template get_as<utils::UUID>("host_id"), std::move(m)); });

    co_await load_scope(
            format("SELECT keyspace_name, configs FROM {}.{}", schema_tables::v3::NAME, schema_tables::v3::SCYLLA_KEYSPACES),
            [&] (const auto& row, config_map m) { keyspace_configs.emplace(row.template get_as<sstring>("keyspace_name"), std::move(m)); });

    co_await load_scope(
            format("SELECT keyspace_name, table_name, configs FROM {}.{}", schema_tables::v3::NAME, schema_tables::v3::SCYLLA_TABLES),
            [&] (const auto& row, config_map m) {
                table_configs.emplace(
                        std::make_tuple(row.template get_as<sstring>("keyspace_name"), row.template get_as<sstring>("table_name")),
                        std::move(m));
            });

    _cluster_configs = std::move(cluster_configs);
    _dc_configs = std::move(dc_configs);
    _rack_configs = std::move(rack_configs);
    _node_configs = std::move(node_configs);
    _keyspace_configs = std::move(keyspace_configs);
    _table_configs = std::move(table_configs);
    co_return;
}

void cluster_config_manager::signal_ready() {
    if (_is_ready) {
        return;
    }
    _is_ready = true;
    for (auto& waiter : _ready_waiters) {
        waiter.set_value();
    }
    _ready_waiters.clear();
}

std::optional<sstring> cluster_config_manager::get_config_for(const config_map& configs, std::string_view config_name) const {
    auto it = configs.find(sstring(config_name));
    if (it == configs.end()) {
        return std::nullopt;
    }
    return it->second;
}

std::optional<sstring> cluster_config_manager::get_cluster_config(std::string_view config_name) const {
    return get_config_for(_cluster_configs, config_name);
}

std::optional<sstring> cluster_config_manager::get_dc_config(std::string_view dc_name, std::string_view config_name) const {
    return lookup_in_scope(_dc_configs, sstring(dc_name), config_name);
}

std::optional<sstring> cluster_config_manager::get_rack_config(std::string_view dc_name, std::string_view rack_name, std::string_view config_name) const {
    return lookup_in_scope(_rack_configs, std::make_tuple(sstring(dc_name), sstring(rack_name)), config_name);
}

std::optional<sstring> cluster_config_manager::get_node_config(const utils::UUID& node_uuid, std::string_view config_name) const {
    return lookup_in_scope(_node_configs, node_uuid, config_name);
}

std::optional<sstring> cluster_config_manager::get_keyspace_config(std::string_view keyspace_name, std::string_view config_name) const {
    return lookup_in_scope(_keyspace_configs, sstring(keyspace_name), config_name);
}

std::optional<sstring> cluster_config_manager::get_table_config(std::string_view keyspace_name, std::string_view table_name, std::string_view config_name) const {
    return lookup_in_scope(_table_configs, std::make_tuple(sstring(keyspace_name), sstring(table_name)), config_name);
}

std::optional<sstring> cluster_config_manager::resolve_config(std::string_view config_name, const lookup_context& ctx) const {
    // A lookup context must address a single resolution domain: either the table-oriented
    // chain (keyspace/table) or the node-oriented chain (dc/rack/node), both sharing the
    // cluster fallback. Mixing them would interleave two independent precedence chains.
    const bool has_table_oriented = ctx.keyspace_name.has_value() || ctx.table_name.has_value();
    const bool has_node_oriented = ctx.dc_name.has_value() || ctx.rack_name.has_value() || ctx.node_uuid.has_value();
    SCYLLA_ASSERT(!(has_table_oriented && has_node_oriented));

    if (ctx.keyspace_name && ctx.table_name) {
        if (auto value = get_table_config(*ctx.keyspace_name, *ctx.table_name, config_name)) {
            return value;
        }
    }
    if (ctx.node_uuid) {
        if (auto value = get_node_config(*ctx.node_uuid, config_name)) {
            return value;
        }
    }
    if (ctx.keyspace_name) {
        if (auto value = get_keyspace_config(*ctx.keyspace_name, config_name)) {
            return value;
        }
    }
    if (ctx.dc_name && ctx.rack_name) {
        if (auto value = get_rack_config(*ctx.dc_name, *ctx.rack_name, config_name)) {
            return value;
        }
    }
    if (ctx.dc_name) {
        if (auto value = get_dc_config(*ctx.dc_name, config_name)) {
            return value;
        }
    }
    return get_cluster_config(config_name);
}

}
