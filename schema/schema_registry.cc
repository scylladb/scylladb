/*
 * Copyright 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "utils/assert.hh"
#include <seastar/core/sharded.hh>

#include "schema_registry.hh"
#include "utils/log.hh"
#include "db/schema_tables.hh"
#include "view_info.hh"
#include "replica/database.hh"

static logging::logger slogger("schema_registry");

static thread_local schema_registry registry;

schema_version_not_found::schema_version_not_found(table_schema_version v)
        : std::runtime_error{format("Schema version {} not found", v)}
{ }

schema_version_loading_failed::schema_version_loading_failed(table_schema_version v)
        : std::runtime_error{format("Failed to load schema version {}", v)}
{ }

schema_registry_entry::~schema_registry_entry() {
    if (_schema) {
        _schema->_registry_entry = nullptr;
    }
}

schema_registry_entry::schema_registry_entry(table_schema_version v, schema_registry& r)
    : _state(state::INITIAL)
    , _version(v)
    , _registry(r)
    , _sync_state(sync_state::NOT_SYNCED)
{
    _erase_timer.set_callback([this] {
        slogger.debug("Dropping {}", _version);
        SCYLLA_ASSERT(!_schema);
        try {
            _registry._entries.erase(_version);
        } catch (...) {
            slogger.error("Failed to erase schema version {}: {}", _version, std::current_exception());
        }
    });
}

schema_registry::~schema_registry() = default;

void schema_registry::init(const db::schema_ctxt& ctxt) {
    _ctxt = std::make_unique<db::schema_ctxt>(ctxt);
}

void schema_registry::attach_table(schema_registry_entry& e) noexcept {
    if (e._table) {
        return;
    }
    replica::database* db = _ctxt->get_db();
    if (!db) {
        return;
    }
    try {
        auto& table = db->find_column_family(e.get_schema()->id());
        e.set_table(table.weak_from_this());
    } catch (const replica::no_such_column_family&) {
        if (slogger.is_enabled(seastar::log_level::debug)) {
            slogger.debug("No table for schema version {} of {}.{}: {}", e._version,
                          e.get_schema()->ks_name(), e.get_schema()->cf_name(), seastar::current_backtrace());
        }
        // ignore
    }
}

schema_ptr schema_registry::learn(const schema_ptr& s) {
    if (s->registry_entry()) {
        return std::move(s);
    }
    auto i = _entries.find(s->version());
    if (i != _entries.end()) {
        schema_registry_entry& e = *i->second;
        if (e._state == schema_registry_entry::state::LOADING) {
            e.load(s);
            attach_table(e);
        }
        return e.get_schema();
    }
    slogger.debug("Learning about version {} of {}.{}", s->version(), s->ks_name(), s->cf_name());
    auto e_ptr = make_lw_shared<schema_registry_entry>(s->version(), *this);
    auto loaded_s = e_ptr->load(s);
    attach_table(*e_ptr);
    _entries.emplace(s->version(), e_ptr);
    return loaded_s;
}

schema_registry_entry& schema_registry::get_entry(table_schema_version v) const {
    auto i = _entries.find(v);
    if (i == _entries.end()) {
        throw schema_version_not_found(v);
    }
    schema_registry_entry& e = *i->second;
    if (e._state != schema_registry_entry::state::LOADED) {
        throw schema_version_not_found(v);
    }
    return e;
}

schema_registry_entry::erase_clock::duration schema_registry::grace_period() const {
    return std::chrono::seconds(_ctxt->schema_registry_grace_period());
}

schema_ptr schema_registry::get(table_schema_version v) const {
    return get_entry(v).get_schema();
}

frozen_schema schema_registry::get_frozen(table_schema_version v) const {
    return get_entry(v).frozen();
}

future<schema_ptr> schema_registry::get_or_load(table_schema_version v, const async_schema_loader& loader) {
    auto i = _entries.find(v);
    if (i == _entries.end()) {
        auto e_ptr = make_lw_shared<schema_registry_entry>(v, *this);
        auto f = e_ptr->start_loading(loader);
        _entries.emplace(v, e_ptr);
        return f;
    }
    schema_registry_entry& e = *i->second;
    if (e._state == schema_registry_entry::state::LOADING) {
        return e._schema_promise.get_shared_future();
    }
    return make_ready_future<schema_ptr>(e.get_schema());
}

schema_ptr schema_registry::get_or_null(table_schema_version v) const {
    auto i = _entries.find(v);
    if (i == _entries.end()) {
        return nullptr;
    }
    schema_registry_entry& e = *i->second;
    if (e._state != schema_registry_entry::state::LOADED) {
        return nullptr;
    }
    return e.get_schema();
}

schema_ptr schema_registry::get_or_load(table_schema_version v, const schema_loader& loader) {
    auto i = _entries.find(v);
    if (i == _entries.end()) {
        auto e_ptr = make_lw_shared<schema_registry_entry>(v, *this);
        auto s = e_ptr->load(loader(v));
        attach_table(*e_ptr);
        _entries.emplace(v, e_ptr);
        return s;
    }
    schema_registry_entry& e = *i->second;
    if (e._state == schema_registry_entry::state::LOADING) {
        auto s = e.load(loader(v));
        attach_table(e);
        return s;
    }
    return e.get_schema();
}

void schema_registry::clear() {
    _entries.clear();
}

schema_ptr schema_registry_entry::load(base_and_view_schemas fs) {
    _frozen_schema = std::move(fs.schema);
    if (fs.base_schema) {
        _base_schema = std::move(fs.base_schema);
    }
    auto s = get_schema();
    if (_state == state::LOADING) {
        _schema_promise.set_value(s);
        _schema_promise = {};
    }
    _state = state::LOADED;
    slogger.trace("Loaded {} = {}", _version, *s);
    return s;
}

schema_ptr schema_registry_entry::load(schema_ptr s) {
    _frozen_schema = frozen_schema(s);
    if (s->is_view()) {
        _base_schema = s->view_info()->base_info()->base_schema();
    }
    _schema = &*s;
    _schema->_registry_entry = this;
    _erase_timer.cancel();
    if (_state == state::LOADING) {
        _schema_promise.set_value(s);
        _schema_promise = {};
    }
    _state = state::LOADED;
    slogger.trace("Loaded {} = {}", _version, *s);
    return s;
}

future<schema_ptr> schema_registry_entry::start_loading(async_schema_loader loader) {
    _loader = std::move(loader);
    auto f = _loader(_version);
    auto sf = _schema_promise.get_shared_future();
    _state = state::LOADING;
    slogger.trace("Loading {}", _version);
    // Move to background.
    (void)f.then_wrapped([self = shared_from_this(), this] (future<base_and_view_schemas>&& f) {
        _loader = {};
        if (_state != state::LOADING) {
            slogger.trace("Loading of {} aborted", _version);
            return;
        }
        try {
            try {
                load(f.get());
                _registry.attach_table(*this);
            } catch (...) {
                std::throw_with_nested(schema_version_loading_failed(_version));
            }
        } catch (...) {
            slogger.debug("Loading of {} failed: {}", _version, std::current_exception());
            _schema_promise.set_exception(std::current_exception());
            _registry._entries.erase(_version);
        }
    });
    return sf;
}

schema_ptr schema_registry_entry::get_schema() {
    if (!_schema) {
        slogger.trace("Activating {}", _version);
        auto s = _frozen_schema->unfreeze(*_registry._ctxt);
        if (s->version() != _version) {
            throw std::runtime_error(format("Unfrozen schema version doesn't match entry version ({}): {}", _version, *s));
        }
        if (s->is_view()) {
            // We may encounter a no_such_column_family here, which means that the base table was deleted and we should fail the request
            s->view_info()->set_base_info(s->view_info()->make_base_dependent_view_info(**_base_schema));
        }
        _erase_timer.cancel();
        s->_registry_entry = this;
        _schema = &*s;
        return s;
    } else {
        return _schema->shared_from_this();
    }
}

void schema_registry_entry::detach_schema() noexcept {
    slogger.trace("Deactivating {}", _version);
    _schema = nullptr;
    _erase_timer.arm(_registry.grace_period());
}

frozen_schema schema_registry_entry::frozen() const {
    SCYLLA_ASSERT(_state >= state::LOADED);
    return *_frozen_schema;
}

void schema_registry_entry::update_base_schema(schema_ptr s) {
    _base_schema = s;
}

future<> schema_registry_entry::maybe_sync(std::function<future<>()> syncer) {
    switch (_sync_state) {
        case schema_registry_entry::sync_state::SYNCED:
            return make_ready_future<>();
        case schema_registry_entry::sync_state::SYNCING:
            return _synced_promise.get_shared_future();
        case schema_registry_entry::sync_state::NOT_SYNCED: {
            slogger.debug("Syncing {}", _version);
            _synced_promise = {};
            auto f = do_with(std::move(syncer), [] (auto& syncer) {
                return syncer();
            });
            auto sf = _synced_promise.get_shared_future();
            _sync_state = schema_registry_entry::sync_state::SYNCING;
            // Move to background.
            (void)f.then_wrapped([this, self = shared_from_this()] (auto&& f) {
                if (_sync_state != sync_state::SYNCING) {
                    f.ignore_ready_future();
                    return;
                }
                if (f.failed()) {
                    slogger.debug("Syncing of {} failed", _version);
                    _sync_state = schema_registry_entry::sync_state::NOT_SYNCED;
                    _synced_promise.set_exception(f.get_exception());
                } else {
                    slogger.debug("Synced {}", _version);
                    _registry.attach_table(*this);
                    _sync_state = schema_registry_entry::sync_state::SYNCED;
                    _synced_promise.set_value();
                }
            });
            return sf;
        }
    }
    abort();
}

bool schema_registry_entry::is_synced() const {
    return _sync_state == sync_state::SYNCED;
}

void schema_registry_entry::mark_synced() {
    if (_sync_state == sync_state::SYNCED) {
        return;
    }
    if (_sync_state == sync_state::SYNCING) {
        _synced_promise.set_value();
    }
    _registry.attach_table(*this);
    _sync_state = sync_state::SYNCED;
    slogger.debug("Marked {} as synced", _version);
}

schema_registry& local_schema_registry() {
    return registry;
}

global_schema_ptr::global_schema_ptr(const global_schema_ptr& o)
    : global_schema_ptr(o.get())
{ }

global_schema_ptr::global_schema_ptr(global_schema_ptr&& o) noexcept {
    auto current = this_shard_id();
    SCYLLA_ASSERT(o._cpu_of_origin == current);
    _ptr = std::move(o._ptr);
    _cpu_of_origin = current;
    _base_schema = std::move(o._base_schema);
}

schema_ptr global_schema_ptr::get() const {
    if (this_shard_id() == _cpu_of_origin) {
        return _ptr;
    } else {
        auto registered_schema = [](const schema_registry_entry& e, std::optional<schema_ptr> base_schema = std::nullopt) -> schema_ptr {
            schema_ptr ret = local_schema_registry().get_or_null(e.version());
            if (!ret) {
                ret = local_schema_registry().get_or_load(e.version(), [&e, &base_schema](table_schema_version) -> base_and_view_schemas {
                    return {e.frozen(), base_schema};
                });
            }
            return ret;
        };

        std::optional<schema_ptr> registered_bs;
        // the following code contains registry entry dereference of a foreign shard
        // however, it is guaranteed to succeed since we made sure in the constructor
        // that _bs_schema and _ptr will have a registry on the foreign shard where this
        // object originated so as long as this object lives the registry entries lives too
        // and it is safe to reference them on foreign shards.
        if (_base_schema) {
            registered_bs = registered_schema(*_base_schema->registry_entry());
            if (_base_schema->registry_entry()->is_synced()) {
                registered_bs.value()->registry_entry()->mark_synced();
            }
        }
        schema_ptr s = registered_schema(*_ptr->registry_entry(), registered_bs);
        if (_ptr->registry_entry()->is_synced()) {
            s->registry_entry()->mark_synced();
        }
        return s;
    }
}

global_schema_ptr::global_schema_ptr(const schema_ptr& ptr)
        : _cpu_of_origin(this_shard_id()) {
    // _ptr must always have an associated registry entry,
    // if ptr doesn't, we need to load it into the registry.
    auto ensure_registry_entry = [] (const schema_ptr& s) {
        schema_registry_entry* e = s->registry_entry();
        if (e) {
            return s;
        } else {
            return local_schema_registry().get_or_load(s->version(), [&s] (table_schema_version) -> base_and_view_schemas {
                if (s->is_view()) {
                    if (!s->view_info()->base_info()) {
                        on_internal_error(slogger, format("Tried to build a global schema for view {}.{} with an uninitialized base info", s->ks_name(), s->cf_name()));
                    }
                    return {frozen_schema(s), s->view_info()->base_info()->base_schema()};
                } else {
                    return {frozen_schema(s)};
                }
            });
        }
    };

    schema_ptr s = ensure_registry_entry(ptr);
    if (s->is_view()) {
        _base_schema = ensure_registry_entry(s->view_info()->base_info()->base_schema());
    }
    _ptr = s;
}
