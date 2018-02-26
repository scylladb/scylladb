/*
 * Copyright 2015 ScyllaDB
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

#include <seastar/core/sharded.hh>

#include "schema_registry.hh"
#include "log.hh"
#include "db/schema_tables.hh"

static logging::logger slogger("schema_registry");

static thread_local schema_registry registry;

schema_version_not_found::schema_version_not_found(table_schema_version v)
        : std::runtime_error{sprint("Schema version %s not found", v)}
{ }

schema_version_loading_failed::schema_version_loading_failed(table_schema_version v)
        : std::runtime_error{sprint("Failed to load schema version %s", v)}
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
        assert(!_schema);
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

schema_ptr schema_registry::learn(const schema_ptr& s) {
    if (s->registry_entry()) {
        return std::move(s);
    }
    auto i = _entries.find(s->version());
    if (i != _entries.end()) {
        return i->second->get_schema();
    }
    slogger.debug("Learning about version {} of {}.{}", s->version(), s->ks_name(), s->cf_name());
    auto e_ptr = make_lw_shared<schema_registry_entry>(s->version(), *this);
    auto loaded_s = e_ptr->load(frozen_schema(s));
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
        _entries.emplace(v, e_ptr);
        return s;
    }
    schema_registry_entry& e = *i->second;
    if (e._state == schema_registry_entry::state::LOADING) {
        return e.load(loader(v));
    }
    return e.get_schema();
}

schema_ptr schema_registry_entry::load(frozen_schema fs) {
    _frozen_schema = std::move(fs);
    auto s = get_schema();
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
    f.then_wrapped([self = shared_from_this(), this] (future<frozen_schema>&& f) {
        _loader = {};
        if (_state != state::LOADING) {
            slogger.trace("Loading of {} aborted", _version);
            return;
        }
        try {
            try {
                load(f.get0());
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
            throw std::runtime_error(sprint("Unfrozen schema version doesn't match entry version (%s): %s", _version, *s));
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
    assert(_state >= state::LOADED);
    return *_frozen_schema;
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
            f.then_wrapped([this, self = shared_from_this()] (auto&& f) {
                if (_sync_state != sync_state::SYNCING) {
                    return;
                }
                if (f.failed()) {
                    slogger.debug("Syncing of {} failed", _version);
                    _sync_state = schema_registry_entry::sync_state::NOT_SYNCED;
                    _synced_promise.set_exception(f.get_exception());
                } else {
                    slogger.debug("Synced {}", _version);
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
    if (_sync_state == sync_state::SYNCING) {
        _synced_promise.set_value();
    }
    _sync_state = sync_state::SYNCED;
    slogger.debug("Marked {} as synced", _version);
}

schema_registry& local_schema_registry() {
    return registry;
}

global_schema_ptr::global_schema_ptr(const global_schema_ptr& o)
    : global_schema_ptr(o.get())
{ }

global_schema_ptr::global_schema_ptr(global_schema_ptr&& o) {
    auto current = engine().cpu_id();
    if (o._cpu_of_origin != current) {
        throw std::runtime_error("Attempted to move global_schema_ptr across shards");
    }
    _ptr = std::move(o._ptr);
    _cpu_of_origin = current;
}

schema_ptr global_schema_ptr::get() const {
    if (engine().cpu_id() == _cpu_of_origin) {
        return _ptr;
    } else {
        // 'e' points to a foreign entry, but we know it won't be evicted
        // because _ptr is preventing this.
        const schema_registry_entry& e = *_ptr->registry_entry();
        schema_ptr s = local_schema_registry().get_or_null(e.version());
        if (!s) {
            s = local_schema_registry().get_or_load(e.version(), [&e](table_schema_version) {
                return e.frozen();
            });
        }
        if (e.is_synced()) {
            s->registry_entry()->mark_synced();
        }
        return s;
    }
}

global_schema_ptr::global_schema_ptr(const schema_ptr& ptr)
    : _ptr([&ptr]() {
        // _ptr must always have an associated registry entry,
        // if ptr doesn't, we need to load it into the registry.
        schema_registry_entry* e = ptr->registry_entry();
        if (e) {
            return ptr;
        }
        return local_schema_registry().get_or_load(ptr->version(), [&ptr] (table_schema_version) {
                return frozen_schema(ptr);
            });
        }())
    , _cpu_of_origin(engine().cpu_id())
{ }
