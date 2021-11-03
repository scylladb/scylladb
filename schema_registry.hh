/*
 * Copyright 2015-present ScyllaDB
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

#include <unordered_map>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/timer.hh>
#include <seastar/core/shared_future.hh>
#include "schema_fwd.hh"
#include "frozen_schema.hh"

namespace db {
class schema_ctxt;
}

class schema_registry;

using async_frozen_schema_loader = std::function<future<frozen_schema>(table_schema_version)>;
using frozen_schema_loader = std::function<frozen_schema(table_schema_version)>;
using schema_loader = std::function<schema_ptr(table_schema_version)>;

class schema_version_not_found : public std::runtime_error {
public:
    schema_version_not_found(table_schema_version v);
};

class schema_version_loading_failed : public std::runtime_error {
public:
    schema_version_loading_failed(table_schema_version v);
};

//
// Presence in schema_registry is controlled by different processes depending on
// life cycle stage:
//   1) Initially it's controlled by the loader. When loading fails, entry is removed by the loader.
//   2) When loading succeeds, the entry is controlled by live schema_ptr. It remains present as long as
//      there's any live schema_ptr.
//   3) When last schema_ptr dies, entry is deactivated. Currently it is removed immediately, later we may
//      want to keep it around for some time to reduce cache misses.
//
// In addition to the above the entry is controlled by lw_shared_ptr<> to cope with races between loaders.
//
class schema_registry_entry : public enable_lw_shared_from_this<schema_registry_entry> {
    using schema_factory = std::function<schema_ptr()>;
    using erase_clock = seastar::lowres_clock;

    enum class state {
        INITIAL, LOADING, LOADED
    };

    state _state;
    table_schema_version _version; // always valid
    schema_registry& _registry; // always valid

    async_frozen_schema_loader _loader; // valid when state == LOADING
    shared_promise<schema_ptr> _schema_promise; // valid when state == LOADING

    schema_factory _factory; // engaged when state == LOADED
    std::optional<frozen_schema> _frozen_schema; // engaged after ensure_frozen()
    // valid when state == LOADED
    // This is != nullptr when there is an alive schema_ptr associated with this entry.
    std::vector<const ::schema*> _schemas;

    enum class sync_state { NOT_SYNCED, SYNCING, SYNCED };
    sync_state _sync_state;
    shared_promise<> _synced_promise; // valid when _sync_state == SYNCING
    timer<erase_clock> _erase_timer;

    friend class schema_registry;
private:
    void pair_with(const schema& s);
    schema_ptr do_load(schema_factory factory);
public:
    schema_registry_entry(table_schema_version v, schema_registry& r);
    schema_registry_entry(schema_registry_entry&&) = delete;
    schema_registry_entry(const schema_registry_entry&) = delete;
    ~schema_registry_entry();
    schema_ptr load(frozen_schema);
    schema_ptr load(schema_loader);
    schema_registry& registry() const { return _registry; }
    future<schema_ptr> start_loading(async_frozen_schema_loader);
    schema_ptr get_schema(); // call only when state >= LOADED
    // Can be called from other shards
    bool is_synced() const;
    // Initiates asynchronous schema sync or returns ready future when is already synced.
    future<> maybe_sync(std::function<future<>()> sync);
    // Marks this schema version as synced. Syncing cannot be in progress.
    void mark_synced();
    void ensure_frozen();
    // Can be called from other shards
    // Call ensure_frozen() before first use
    frozen_schema frozen() const;
    // Can be called from other shards
    table_schema_version version() const { return _version; }
public:
    // Called by class schema
    void detach_schema(const schema& s) noexcept;
};

//
// Keeps track of different versions of table schemas. A per-shard object.
//
// For every schema_ptr obtained through getters, as long as the schema pointed to is
// alive the registry will keep its entry. To ensure remote nodes can query current node
// for schema version, make sure that schema_ptr for the request is alive around the call.
//
class schema_registry {
    std::unordered_map<table_schema_version, lw_shared_ptr<schema_registry_entry>> _entries;
    std::unique_ptr<db::schema_ctxt> _ctxt;

    friend class schema_registry_entry;
    friend class schema;
    schema_registry_entry& get_entry(table_schema_version) const;
    void pair_with_entry(const schema& s);
    // Duration for which unused entries are kept alive to avoid
    // too frequent re-requests and syncs. Default is 1 second.
    schema_registry_entry::erase_clock::duration grace_period() const;

    schema_ptr get_or_load(table_schema_version, std::function<schema_ptr(schema_registry_entry&)> loader);

public:
    ~schema_registry();
    // workaround to this object being magically appearing from nowhere.
    void init(const db::schema_ctxt&);

    db::schema_ctxt& get_context() { return *_ctxt; }

    // Looks up schema by version or loads it using supplied loader.
    schema_ptr get_or_load(table_schema_version, const frozen_schema_loader&);

    // Looks up schema by version or loads it using supplied loader.
    schema_ptr get_or_load(table_schema_version, const schema_loader&);

    // Looks up schema by version or returns an empty pointer if not available.
    schema_ptr get_or_null(table_schema_version) const;

    // Like get_or_load() which takes frozen_schema_loader but the loader may be
    // deferring. The loader is copied must be alive only until this method
    // returns. If the loader fails, the future resolves with
    // schema_version_loading_failed.
    future<schema_ptr> get_or_load(table_schema_version, const async_frozen_schema_loader&);

    // Looks up schema version. Throws schema_version_not_found when not found
    // or loading is in progress.
    schema_ptr get(table_schema_version) const;

    // Attempts to add given schema to the registry. If the registry already
    // knows about the schema, returns existing entry, otherwise returns back
    // the schema which was passed as argument. Users should prefer to use the
    // schema_ptr returned by this method instead of the one passed to it,
    // because doing so ensures that the entry will be kept in the registry as
    // long as the schema is actively used.
    schema_ptr learn(const schema_ptr&);
};

schema_registry& local_schema_registry();

// Schema pointer which can be safely accessed/passed across shards via
// const&. Useful for ensuring that schema version obtained on one shard is
// automatically propagated to other shards, no matter how long the processing
// chain will last.
class global_schema_ptr {
    schema_ptr _ptr;
    schema_ptr _base_schema;
    unsigned _cpu_of_origin;
public:
    // Note: the schema_ptr must come from the current shard and can't be nullptr.
    global_schema_ptr(const schema_ptr&);
    // The other may come from a different shard.
    global_schema_ptr(const global_schema_ptr& other);
    // The other must come from current shard.
    global_schema_ptr(global_schema_ptr&& other) noexcept;
    // May be invoked across shards. Always returns an engaged pointer.
    schema_ptr get() const;
    operator schema_ptr() const { return get(); }
};
