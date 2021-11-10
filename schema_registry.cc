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

#include <seastar/core/sharded.hh>
#include <seastar/core/reactor.hh>

#include "schema_registry.hh"
#include "log.hh"
#include "db/schema_tables.hh"
#include "view_info.hh"

static logging::logger slogger("schema_registry");

static thread_local schema_registry* registry = nullptr;

schema_version_not_found::schema_version_not_found(table_schema_version v)
        : std::runtime_error{format("Schema version {} not found", v)}
{ }

schema_version_loading_failed::schema_version_loading_failed(table_schema_version v)
        : std::runtime_error{format("Failed to load schema version {}", v)}
{ }

void schema_registry_entry::pair_with(const schema& s) {
    if (std::find(_schemas.begin(), _schemas.end(), &s) != _schemas.end()) {
        slogger.trace("Registry entry already paired with schema@{} of version {}", fmt::ptr(&s), s.version());
        return;
    }
    s._registry_entry = this;
    _schemas.push_back(&s);
    _erase_timer.cancel();
    if (_state == state::LOADING) {
        _schema_promise.set_value(s.shared_from_this());
        _schema_promise = {};
    }
    _state = state::LOADED;
}

schema_registry_entry::~schema_registry_entry() {
    for (auto& s : _schemas) {
        s->_registry_entry = nullptr;
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
        assert(_schemas.empty());
        try {
            _registry._entries.erase(_version);
        } catch (...) {
            slogger.error("Failed to erase schema version {}: {}", _version, std::current_exception());
        }
    });
}

schema_registry::schema_registry() = default;
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

void schema_registry::pair_with_entry(const schema& s) {
    const auto v = s.version();
    auto i = _entries.find(v);
    slogger.trace("Pairing schema @{} of version {} of {}.{} with {} entry", fmt::ptr(&s), s.version(), s.ks_name(), s.cf_name(),
            (i == _entries.end()) ? "new" : "existing");
    if (i == _entries.end()) {
        auto e_ptr = make_lw_shared<schema_registry_entry>(v, *this);
        _entries.emplace(v, e_ptr);
        e_ptr->pair_with(s);
    } else {
        i->second->pair_with(s);
    }
}

schema_registry_entry::erase_clock::duration schema_registry::grace_period() const {
    return std::chrono::seconds(_ctxt->schema_registry_grace_period());
}

schema_ptr schema_registry::get(table_schema_version v) const {
    return get_entry(v).get_schema();
}

future<schema_ptr> schema_registry::get_or_load(table_schema_version v, const async_frozen_schema_loader& loader) {
    auto i = _entries.find(v);
    if (i == _entries.end()) {
        auto e_ptr = make_lw_shared<schema_registry_entry>(v, *this);
        _entries.emplace(v, e_ptr);
        return e_ptr->start_loading(loader);
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

schema_ptr schema_registry::get_or_load(table_schema_version v, std::function<schema_ptr(schema_registry_entry&)> load) {
    auto i = _entries.find(v);
    if (i == _entries.end()) {
        auto e_ptr = make_lw_shared<schema_registry_entry>(v, *this);
        _entries.emplace(v, e_ptr);
        return load(*e_ptr);
    }
    schema_registry_entry& e = *i->second;
    if (e._state == schema_registry_entry::state::LOADING) {
        return load(e);
    }
    return e.get_schema();
}

schema_ptr schema_registry::get_or_load(table_schema_version v, const frozen_schema_loader& loader) {
    return get_or_load(v, [v, &loader] (schema_registry_entry& e) {
        return e.load(loader(v));
    });
}

schema_ptr schema_registry::get_or_load(table_schema_version v, const schema_loader& loader) {
    return get_or_load(v, [&loader] (schema_registry_entry& e) {
        return e.load(loader);
    });
}

schema_ptr schema_registry_entry::do_load(schema_factory factory) {
    _factory = std::move(factory);
    auto s = get_schema();
    slogger.trace("Loaded {} = {}", _version, *s);
    return s;
}

schema_ptr schema_registry_entry::load(frozen_schema fs) {
    return do_load([this, fs = std::move(fs)] {
        return fs.unfreeze(_registry);
    });
}

schema_ptr schema_registry_entry::load(schema_loader loader) {
    return do_load([this, loader = std::move(loader)] {
        return loader(_version);
    });
}

future<schema_ptr> schema_registry_entry::start_loading(async_frozen_schema_loader loader) {
    _loader = std::move(loader);
    auto f = _loader(_version);
    auto sf = _schema_promise.get_shared_future();
    _state = state::LOADING;
    slogger.trace("Loading {}", _version);
    // Move to background.
    (void)f.then_wrapped([self = shared_from_this(), this] (future<frozen_schema>&& f) {
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
    if (_schemas.empty()) {
        slogger.trace("Activating {}", _version);
        auto s = _factory();
        if (s->version() != _version) {
            throw std::runtime_error(format("Unfrozen schema version doesn't match entry version ({}): {}", _version, *s));
        }
        return s;
    } else {
        return _schemas.front()->shared_from_this();
    }
}

void schema_registry_entry::detach_schema(const schema& s) noexcept {
    if (auto it = std::find(_schemas.begin(), _schemas.end(), &s); it != _schemas.end()) {
        _schemas.erase(it);
    }
    if (!_schemas.empty()) {
        return;
    }
    slogger.trace("Deactivating {}", _version);
    // Some tests that use schemas don't start a reactor
    if (engine_is_ready()) [[likely]] {
        _erase_timer.arm(_registry.grace_period());
    }
}

void schema_registry_entry::ensure_frozen() {
    if (!_frozen_schema) {
        _frozen_schema.emplace(get_schema());
    }
}

frozen_schema schema_registry_entry::frozen() const {
    return _frozen_schema.value();
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


void set_local_schema_registry(schema_registry& sr) {
    registry = &sr;
}

schema_registry& local_schema_registry() {
    return *registry;
}

global_schema_ptr::global_schema_ptr(const global_schema_ptr& o)
    : global_schema_ptr(o.get())
{ }

global_schema_ptr::global_schema_ptr(global_schema_ptr&& o) noexcept {
    auto current = this_shard_id();
    assert(o._cpu_of_origin == current);
    _ptr = std::move(o._ptr);
    _cpu_of_origin = current;
    _base_schema = std::move(o._base_schema);
}

schema_ptr global_schema_ptr::get() const {
    if (this_shard_id() == _cpu_of_origin) {
        return _ptr;
    } else {
        auto registered_schema = [](const schema_registry_entry& e) {
            auto& local_registry = e.registry().container().local();
            schema_ptr ret = local_registry.get_or_null(e.version());
            if (!ret) {
                ret = local_registry.get_or_load(e.version(), [&e](table_schema_version) {
                    return e.frozen();
                });
            }
            return ret;
        };

        schema_ptr registered_bs;
        // the following code contains registry entry dereference of a foreign shard
        // however, it is guarantied to succeed since we made sure in the constructor
        // that _bs_schema and _ptr will have a registry on the foreign shard where this
        // object originated so as long as this object lives the registry entries lives too
        // and it is safe to reference them on foreign shards.
        if (_base_schema) {
            registered_bs = registered_schema(*_base_schema->registry_entry());
            if (_base_schema->registry_entry()->is_synced()) {
                registered_bs->registry_entry()->mark_synced();
            }
        }
        schema_ptr s = registered_schema(*_ptr->registry_entry());
        if (s->is_view()) {
            if (!s->view_info()->base_info()) {
                // we know that registered_bs is valid here because we make sure of it in the constructors.
                s->view_info()->set_base_info(s->view_info()->make_base_dependent_view_info(*registered_bs));
            }
        }
        if (_ptr->registry_entry()->is_synced()) {
            s->registry_entry()->mark_synced();
        }
        return s;
    }
}

global_schema_ptr::global_schema_ptr(const schema_ptr& ptr)
        : _ptr(ptr)
        , _cpu_of_origin(this_shard_id()) {
    _ptr->registry_entry()->ensure_frozen();
    if (_ptr->is_view()) {
        if (_ptr->view_info()->base_info()) {
            _base_schema = _ptr->view_info()->base_info()->base_schema();
            _base_schema->registry_entry()->ensure_frozen();
        } else {
            on_internal_error(slogger, format("Tried to build a global schema for view {}.{} with an uninitialized base info", _ptr->ks_name(), _ptr->cf_name()));
        }
    }
}
