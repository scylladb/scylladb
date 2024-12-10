/*
 * Copyright 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <unordered_map>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/timer.hh>
#include <seastar/core/shared_future.hh>
#include "schema_fwd.hh"
#include "frozen_schema.hh"
#include "replica/database_fwd.hh"

namespace db {
class schema_ctxt;
}

class schema_registry;

struct base_and_view_schemas {
    frozen_schema schema;
    std::optional<schema_ptr> base_schema;
};

using async_schema_loader = std::function<future<base_and_view_schemas>(table_schema_version)>;
using schema_loader = std::function<base_and_view_schemas(table_schema_version)>;

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
    using erase_clock = seastar::lowres_clock;

    enum class state {
        INITIAL, LOADING, LOADED
    };

    state _state;
    table_schema_version _version; // always valid
    schema_registry& _registry; // always valid

    async_schema_loader _loader; // valid when state == LOADING
    shared_promise<schema_ptr> _schema_promise; // valid when state == LOADING

    std::optional<frozen_schema> _frozen_schema; // engaged when state == LOADED
    std::optional<schema_ptr> _base_schema;// engaged when state == LOADED for view schemas

    // valid when state == LOADED
    // This is != nullptr when there is an alive schema_ptr associated with this entry.
    const ::schema* _schema = nullptr;
    weak_ptr<replica::table> _table;

    enum class sync_state { NOT_SYNCED, SYNCING, SYNCED };
    sync_state _sync_state;
    shared_promise<> _synced_promise; // valid when _sync_state == SYNCING
    timer<erase_clock> _erase_timer;

    friend class schema_registry;
public:
    schema_registry_entry(table_schema_version v, schema_registry& r);
    schema_registry_entry(schema_registry_entry&&) = delete;
    schema_registry_entry(const schema_registry_entry&) = delete;
    ~schema_registry_entry();
    schema_ptr load(base_and_view_schemas);
    schema_ptr load(schema_ptr);
    future<schema_ptr> start_loading(async_schema_loader);
    schema_ptr get_schema(); // call only when state >= LOADED
    // Can be called from other shards
    bool is_synced() const;
    // Initiates asynchronous schema sync or returns ready future when is already synced.
    future<> maybe_sync(std::function<future<>()> sync);
    // Marks this schema version as synced. Syncing cannot be in progress.
    void mark_synced();
    // Updates the frozen base schema for a view, should be called when updating the base info
    // Is not needed when we set the base info for the first time - that means this schema is not in the registry
    void update_base_schema(schema_ptr);
    // Can be called from other shards
    frozen_schema frozen() const;
    // Can be called from other shards
    table_schema_version version() const { return _version; }

    // Returns a pointer to the table if local database contains the table referenced by the schema object.
    // The table pointer is set even if the schema version of this object is different from the current schema of the table.
    replica::table* table() const { return _table.get(); }
    void set_table(weak_ptr<replica::table> table) { _table = std::move(table); }
public:
    // Called by class schema
    void detach_schema() noexcept;
};

//
// Keeps track of different versions of table schemas. A per-shard object.
//
// For every schema_ptr obtained through getters, as long as the schema pointed to is
// alive the registry will keep its entry. To ensure remote nodes can query current node
// for schema version, make sure that schema_ptr for the request is alive around the call.
//
// Schemas of views returned by this registry always have base_info set.
class schema_registry {
    std::unordered_map<table_schema_version, lw_shared_ptr<schema_registry_entry>> _entries;
    std::unique_ptr<db::schema_ctxt> _ctxt;

    friend class schema_registry_entry;
    schema_registry_entry& get_entry(table_schema_version) const;
    // Duration for which unused entries are kept alive to avoid
    // too frequent re-requests and syncs. Default is 1 second.
    schema_registry_entry::erase_clock::duration grace_period() const;
private:
    void attach_table(schema_registry_entry&) noexcept;
public:
    ~schema_registry();
    // workaround to this object being magically appearing from nowhere.
    void init(const db::schema_ctxt&);

    // Looks up schema by version or loads it using supplied loader.
    // If the schema refers to a view, the loader must return both view and base schemas.
    schema_ptr get_or_load(table_schema_version, const schema_loader&);

    // Looks up schema by version or returns an empty pointer if not available.
    schema_ptr get_or_null(table_schema_version) const;

    // Like get_or_load() which takes schema_loader but the loader may be
    // deferring. The loader is copied must be alive only until this method
    // returns. If the loader fails, the future resolves with
    // schema_version_loading_failed.
    // If the schema refers to a view, the loader must return both view and base schemas.
    future<schema_ptr> get_or_load(table_schema_version, const async_schema_loader&);

    // Looks up schema version. Throws schema_version_not_found when not found
    // or loading is in progress.
    schema_ptr get(table_schema_version) const;

    // Looks up schema version. Throws schema_version_not_found when not found
    // or loading is in progress.
    frozen_schema get_frozen(table_schema_version) const;

    // Attempts to add given schema to the registry. If the registry already
    // knows about the schema, returns existing entry, otherwise returns back
    // the schema which was passed as argument.
    // The schema instance pointed to by the argument will be attached to the registry
    // entry and will keep it alive.
    // If the schema refers to a view, it must have base_info set.
    schema_ptr learn(const schema_ptr&);

    // Removes all entries from the registry. This in turn removes all dependencies
    // on the Seastar reactor.
    //
    // Prerequisite: all futures from get_or_load() are resolved.
    void clear();
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
