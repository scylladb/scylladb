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
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/core/when_all.hh>

#include "cql3/query_processor.hh"
#include "cql3/untyped_result_set.hh"
#include "db/cluster_config_registry.hh"
#include "db/config.hh"
#include "db/schema_tables.hh"
#include "locator/topology.hh"
#include "replica/database.hh"
#include "utils/on_internal_error.hh"

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
    // Purely synchronous: record the request and wake the refresh fiber. No future is
    // created here, so there is no detached continuation that could outlive the manager -
    // the fiber that does the actual work is joined by stop().
    _owner.request_refresh();
}

cluster_config_manager::cluster_config_manager(seastar::sharded<cluster_config_manager>& owner, replica::database& db, cql3::query_processor& qp)
    : _owner(owner)
    , _db(db)
    , _qp(qp)
    , _listener(*this) {
    // Only shard 0 needs the notification: schema merges notify every shard, but refresh
    // execution is coordinated on shard 0 regardless.
    if (this_shard_id() == 0) {
        _db.get_notifier().register_listener(&_listener);
        _listener_registered = true;
        _refresh_fiber = refresh_fiber();
    }
}

future<> cluster_config_manager::stop() {
    _stopping = true;
    // Wake any in-flight refresh retry-backoff so joining the fiber below does not wait
    // out the remaining backoff.
    if (!_abort_source.abort_requested()) {
        _abort_source.request_abort();
    }
    if (_listener_registered) {
        _listener_registered = false;
        co_await _db.get_notifier().unregister_listener(&_listener);
    }
    // Join the refresh fiber (started on shard 0 only; a ready future elsewhere). After
    // this, no refresh pass is in flight and none can start, so nothing coroutine-shaped
    // can touch this manager once stop() completes. Wake the fiber's wait first, and
    // release any refresh() caller still parked on the completion condition.
    _refresh_cv.signal();
    _refresh_done_cv.broadcast();
    co_await std::exchange(_refresh_fiber, make_ready_future<>());
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
    // apply_refresh() holds this shard's gate across its suspension points and
    // indexes into _config_callbacks after resuming, so clearing the vector first would leave
    // it reading freed storage.
    co_await _gate.close();
    _config_callbacks.clear();
}

future<> cluster_config_manager::refresh() {
    return _owner.invoke_on(0, [] (cluster_config_manager& manager) -> future<> {
        manager._authoritative_refresh_enabled = true;
        manager.request_refresh();
        // Wait until a pass that started no earlier than this request completes, keeping
        // refresh()'s contract: when it resolves, every shard's cache reflects at least the
        // state as of the call. A transient pass failure keeps the generation unfinished, so
        // the wait spans the fiber's retries. Shutdown releases the waiter instead.
        const auto target_generation = manager._requested_refresh_generation;
        co_await manager._refresh_done_cv.when([&manager, target_generation] {
            return manager._stopping || manager._completed_refresh_generation >= target_generation;
        });
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
    // Registered on the calling shard only. run_config_callbacks() iterates each
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

void cluster_config_manager::request_refresh() {
    // Always invoked on shard 0: the config_change_listener is registered on shard 0 only
    // and schema_applier::post_commit() fires the notification on shard 0's notifier alone,
    // and refresh() hops to shard 0 explicitly.
    if (this_shard_id() != 0) {
        utils::on_internal_error("cluster_config_manager::request_refresh() called off shard 0; "
                "the request would be lost, since only shard 0 runs the refresh fiber");
    }
    ++_requested_refresh_generation;
    _refresh_cv.signal();
}

future<> cluster_config_manager::refresh_fiber() {
    // Runs on shard 0 only, started at construction and joined by stop(). All refresh
    // execution funnels through this single fiber, which is also what serializes passes:
    // at most one is ever in flight, as run_config_callbacks() checks.
    //
    // A transient failure of a pass (load_caches()/apply_refresh()
    // throwing, e.g. an internal SELECT failing under load) must NOT drop the pending
    // request and leave every shard's cache stale until some unrelated later write
    // happens to trigger another refresh. A failed pass leaves the requested generation
    // unfinished, so after a short abortable backoff the wait condition is still true and
    // the pass re-runs. stop() fires _abort_source so the backoff wakes immediately.
    while (true) {
        co_await _refresh_cv.when([this] {
            // Startup latch: see _authoritative_refresh_enabled.
            return _stopping
                    || (_authoritative_refresh_enabled
                        && _completed_refresh_generation < _requested_refresh_generation);
        });
        if (_stopping) {
            co_return;
        }
        // Requests arriving while the pass below runs bump _requested_refresh_generation
        // and are handled by the next iteration; capturing the target up front is what
        // coalesces them into a single follow-up pass.
        const auto target_generation = _requested_refresh_generation;
        // Capture a failure rather than reacting in the catch handler: co_await
        // (the backoff below) is not permitted inside a catch block.
        std::exception_ptr failure;
        try {
            // Read the config tables once, on this shard: every read is a full scan of a
            // schema table (the keyspace and table tables can hold thousands of rows), so
            // scanning from every shard would multiply the work by the shard count only to
            // read back identical values. The loaded caches are then installed on every
            // shard, which also runs that shard's callbacks and opens its readiness
            // barrier - a single invoke_on_all covers all three stages, since none of them
            // needs cross-shard ordering.
            const auto caches = co_await load_caches();
            co_await _owner.invoke_on_all([&caches] (cluster_config_manager& manager) {
                return manager.apply_refresh(caches);
            });
        } catch (...) {
            failure = std::current_exception();
        }
        if (!failure) {
            _completed_refresh_generation = target_generation;
            _refresh_done_cv.broadcast();
            continue;
        }
        if (_stopping) {
            co_return;
        }
        cluster_config_logger.warn(
                "Failed to refresh cluster config cache, will retry in {}s: {}",
                refresh_retry_backoff.count(), failure);
        try {
            co_await seastar::sleep_abortable(refresh_retry_backoff, _abort_source);
        } catch (const seastar::sleep_aborted&) {
            co_return;
        }
    }
}

// Runs on every shard, invoked by the refresh fiber through a single invoke_on_all per
// pass: installs the caches loaded on the coordinating shard, runs this shard's callbacks
// against them, and opens this shard's readiness barrier.
future<> cluster_config_manager::apply_refresh(const scope_caches& caches) {
    // Enter THIS shard's gate. The refresh fiber only lives on shard 0, and holding shard
    // 0's gate says nothing about shard N: sharded<>::stop() runs stop() on all shards
    // concurrently, so without this the callback pass below could resume after this
    // shard's stop() had already torn down _config_callbacks.
    if (_gate.is_closed()) {
        co_return;
    }
    auto gate_held = _gate.hold();
    // Install the caches on this shard. The copy executes here, so the new maps are
    // allocated on this shard even though the source lives on the coordinating shard
    // (which keeps it alive for the duration of the invoke_on_all).
    _cluster_configs = caches.cluster_configs;
    _dc_configs = caches.dc_configs;
    _rack_configs = caches.rack_configs;
    _node_configs = caches.node_configs;
    _keyspace_configs = caches.keyspace_configs;
    _table_configs = caches.table_configs;

    co_await run_config_callbacks();
    signal_ready();
}

future<> cluster_config_manager::run_config_callbacks() {
    // Each shard fires only the callbacks registered on it (register_config_callback
    // stores per-shard). A callback registered on a single shard is invoked only there;
    // one registered on all shards is invoked on each.
    //
    // The local node's identity, used to resolve node-oriented options through the
    // node -> rack -> dc -> cluster precedence chain.
    lookup_context node_ctx;
    const auto& topology = _db.get_token_metadata().get_topology();
    node_ctx.dc_name = topology.get_datacenter();
    node_ctx.rack_name = topology.get_rack();
    node_ctx.node_uuid = topology.my_host_id().uuid();

    // At most one callback pass runs per shard at a time: all refresh execution happens
    // in shard 0's refresh_fiber(), which awaits this pass before starting the next one.
    if (_callbacks_in_progress) {
        utils::on_internal_error("cluster config callback pass started while a previous pass is still running");
    }
    _callbacks_in_progress = true;
    auto cleanup_callbacks = seastar::defer([this] noexcept {
        _callbacks_in_progress = false;
        sweep_unregistered_callbacks();
    });

    const auto callback_count = _config_callbacks.size();
    for (size_t callback_idx = 0; callback_idx < callback_count; ++callback_idx) {
        if (!_config_callbacks[callback_idx].registered) {
            continue;
        }
        auto config_name = _config_callbacks[callback_idx].config_name;
        // Copy the callback closure into this frame before awaiting it: a callback that
        // suspends must not hold a reference into _config_callbacks, since a concurrent
        // register_config_callback() can push_back and reallocate the vector, moving the
        // stored closure away.
        auto on_change = _config_callbacks[callback_idx].on_change;
        const auto* opt = db::cluster_config_registry::find(config_name);
        const bool table_oriented = opt
                && (db::cluster_config_registry::supports_scope(*opt, db::cluster_config_registry::scope::keyspace)
                    || db::cluster_config_registry::supports_scope(*opt, db::cluster_config_registry::scope::table));

        // A table-oriented option resolves per table (table -> keyspace -> cluster), so
        // there is one target per table; a node-oriented one has the local node as its
        // single target.
        std::vector<lookup_context> contexts;
        if (table_oriented) {
            _db.get_tables_metadata().for_each_table([&] (table_id, lw_shared_ptr<replica::table> t) {
                const auto& s = t->schema();
                lookup_context ctx;
                ctx.keyspace_name = s->ks_name();
                ctx.table_name = s->cf_name();
                contexts.push_back(std::move(ctx));
            });
        } else {
            contexts.push_back(node_ctx);
        }

        // The values delivered (or confirmed unchanged) by this pass. Replaces the
        // callback's last_pushed at the end, so entries for targets that no longer exist
        // (dropped tables) are pruned instead of accumulating.
        callback_values pushed_this_pass;
        pushed_this_pass.reserve(contexts.size());
        for (const auto& ctx : contexts) {
            // There is one context per table on this shard and the body below only
            // suspends when a target's value changed, so on a steady-state pass this
            // loop would otherwise run synchronously over thousands of tables, once
            // per registered callback.
            co_await coroutine::maybe_yield();
            if (!_config_callbacks[callback_idx].registered) {
                break;
            }
            auto resolved = resolve_config(config_name, ctx);
            auto target = callback_target_key(ctx.keyspace_name.value_or(""), ctx.table_name.value_or(""));
            // This is where a change is detected: the callback is invoked only when the
            // resolved value for this target differs from the value last delivered to it.
            // A target with no recorded value (a fresh registration, or a table created
            // since the last pass) always fires, so the consumer learns its initial state:
            // an effective value when one resolves, std::nullopt when it should restore its
            // own default. A pass that fails midway is retried without having committed
            // pushed_this_pass, so it may re-deliver a value - the callback is required to
            // be idempotent.
            const auto& last_pushed = _config_callbacks[callback_idx].last_pushed;
            if (auto it = last_pushed.find(target); it == last_pushed.end() || it->second != resolved) {
                co_await on_change(ctx, resolved);
            }
            pushed_this_pass.emplace(std::move(target), std::move(resolved));
        }
        // Re-index rather than holding a reference across the awaits above: a concurrent
        // registration may have reallocated the vector.
        if (_config_callbacks[callback_idx].registered) {
            _config_callbacks[callback_idx].last_pushed = std::move(pushed_this_pass);
        }
    }
}

void cluster_config_manager::unregister_config_callback(uint64_t callback_id) noexcept {
    for (auto& callback : _config_callbacks) {
        if (callback.id == callback_id) {
            callback.registered = false;
            break;
        }
    }

    if (!_callbacks_in_progress) {
        sweep_unregistered_callbacks();
    }
}

void cluster_config_manager::sweep_unregistered_callbacks() noexcept {
    std::erase_if(_config_callbacks, [] (const config_callback& callback) {
        return !callback.registered;
    });
}

future<cluster_config_manager::scope_caches> cluster_config_manager::load_caches() {
    scope_caches caches;

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

    // The six scans are independent and each is a full scan of its table, so run them
    // concurrently rather than back to back. Each store callback fills a distinct member
    // of `caches`, so they never step on one another.
    co_await when_all_succeed(
            load_scope(
                    format("SELECT cluster_name, configs FROM {}.{} WHERE cluster_name = '{}'",
                            schema_tables::v3::NAME, schema_tables::v3::SCYLLA_CLUSTERS, schema_tables::CLUSTER_CONFIG_SINGLETON_KEY),
                    [&caches] (const auto&, config_map m) { caches.cluster_configs.insert(m.begin(), m.end()); }),
            load_scope(
                    format("SELECT dc_name, configs FROM {}.{}", schema_tables::v3::NAME, schema_tables::v3::SCYLLA_DATACENTERS),
                    [&caches] (const auto& row, config_map m) { caches.dc_configs.emplace(row.template get_as<sstring>("dc_name"), std::move(m)); }),
            load_scope(
                    format("SELECT dc_name, rack_name, configs FROM {}.{}", schema_tables::v3::NAME, schema_tables::v3::SCYLLA_RACKS),
                    [&caches] (const auto& row, config_map m) {
                        caches.rack_configs.emplace(
                                std::make_tuple(row.template get_as<sstring>("dc_name"), row.template get_as<sstring>("rack_name")),
                                std::move(m));
                    }),
            load_scope(
                    format("SELECT host_id, configs FROM {}.{}", schema_tables::v3::NAME, schema_tables::v3::SCYLLA_NODES),
                    [&caches] (const auto& row, config_map m) { caches.node_configs.emplace(row.template get_as<utils::UUID>("host_id"), std::move(m)); }),
            load_scope(
                    format("SELECT keyspace_name, configs FROM {}.{}", schema_tables::v3::NAME, schema_tables::v3::SCYLLA_KEYSPACES),
                    [&caches] (const auto& row, config_map m) { caches.keyspace_configs.emplace(row.template get_as<sstring>("keyspace_name"), std::move(m)); }),
            load_scope(
                    format("SELECT keyspace_name, table_name, configs FROM {}.{}", schema_tables::v3::NAME, schema_tables::v3::SCYLLA_TABLES),
                    [&caches] (const auto& row, config_map m) {
                        caches.table_configs.emplace(
                                std::make_tuple(row.template get_as<sstring>("keyspace_name"), row.template get_as<sstring>("table_name")),
                                std::move(m));
                    }));

    co_return caches;
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
    if (has_table_oriented && has_node_oriented) {
        utils::on_internal_error(format(
                "cluster config lookup context for '{}' mixes table-oriented and node-oriented targets", config_name));
    }

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

// Looked up unversioned: a registered option's default is compiled in, so it is usable before
// the cluster enables the registry feature. Until then no scope can store an override for it
// (the ALTER path rejects the option), so resolution yields absence and these return the
// default - which is the value the node should be using anyway.
static const cluster_config_registry::option& registered_option(std::string_view config_name) {
    const auto* opt = cluster_config_registry::find(config_name);
    if (!opt) {
        utils::on_internal_error(format(
                "typed cluster config accessor called for unregistered config '{}'", config_name));
    }
    return *opt;
}

bool cluster_config_manager::resolve_boolean_config(std::string_view config_name, const lookup_context& ctx) const {
    const auto& opt = registered_option(config_name);
    return cluster_config_registry::to_boolean(opt, resolve_config(config_name, ctx));
}

uint32_t cluster_config_manager::resolve_uint32_config(std::string_view config_name, const lookup_context& ctx) const {
    const auto& opt = registered_option(config_name);
    return cluster_config_registry::to_uint32(opt, resolve_config(config_name, ctx));
}

double cluster_config_manager::resolve_floating_point_config(std::string_view config_name, const lookup_context& ctx) const {
    const auto& opt = registered_option(config_name);
    return cluster_config_registry::to_floating_point(opt, resolve_config(config_name, ctx));
}

sstring cluster_config_manager::resolve_text_config(std::string_view config_name, const lookup_context& ctx) const {
    const auto& opt = registered_option(config_name);
    return cluster_config_registry::to_text(opt, resolve_config(config_name, ctx));
}

}
