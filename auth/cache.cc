/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "auth/cache.hh"
#include "auth/common.hh"
#include "auth/role_or_anonymous.hh"
#include "auth/roles-metadata.hh"
#include "cql3/query_processor.hh"
#include "cql3/untyped_result_set.hh"
#include "db/consistency_level_type.hh"
#include "db/system_keyspace.hh"
#include "schema/schema.hh"
#include <iterator>
#include <seastar/core/abort_source.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/core/format.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/do_with.hh>

namespace auth {

logging::logger logger("auth-cache");

cache::cache(cql3::query_processor& qp, abort_source& as) noexcept
    : _current_version(0)
    , _qp(qp)
    , _loading_sem(1)
    , _as(as)
    , _permission_loader(nullptr)
    , _permission_loader_sem(8) {
    namespace sm = seastar::metrics;
    _metrics.add_group("auth_cache", {
        sm::make_gauge("roles", [this] { return _roles.size(); },
                sm::description("Number of roles currently cached")),
        sm::make_gauge("permissions", [this] {
            return _cached_permissions_count;
        }, sm::description("Total number of permission sets currently cached across all roles"))
    });
}

void cache::set_permission_loader(permission_loader_func loader) {
    _permission_loader = std::move(loader);
}

lw_shared_ptr<const cache::role_record> cache::get(const role_name_t& role) const noexcept {
    auto it = _roles.find(role);
    if (it == _roles.end()) {
        return {};
    }
    return it->second;
}

void cache::for_each_role(const std::function<void(const role_name_t&, const role_record&)>& func) const {
    for (const auto& [name, record] : _roles) {
        func(name, *record);
    }
}

size_t cache::roles_count() const noexcept {
    return _roles.size();
}

future<permission_set> cache::get_permissions(const role_or_anonymous& role, const resource& r) {
    std::unordered_map<resource, permission_set>* perms_cache;
    lw_shared_ptr<role_record> role_ptr;

    if (is_anonymous(role)) {
        perms_cache = &_anonymous_permissions;
    } else {
        const auto& role_name = *role.name;
        auto role_it = _roles.find(role_name);
        if (role_it == _roles.end()) {
            // Role might have been deleted but there are some connections
            // left which reference it. They should no longer have access to anything.
            return make_ready_future<permission_set>(permissions::NONE);
        }
        role_ptr = role_it->second;
        perms_cache = &role_ptr->cached_permissions;
    }

    if (auto it = perms_cache->find(r); it != perms_cache->end()) {
        return make_ready_future<permission_set>(it->second);
    }
    // keep alive role_ptr as it holds perms_cache (except anonymous)
    return do_with(std::move(role_ptr), [this, &role, &r, perms_cache] (auto& role_ptr) {
        return load_permissions(role, r, perms_cache);
    });
}

future<permission_set> cache::load_permissions(const role_or_anonymous& role, const resource& r, std::unordered_map<resource, permission_set>* perms_cache) {
    SCYLLA_ASSERT(_permission_loader);
    auto units = co_await get_units(_permission_loader_sem, 1, _as);

    // Check again, perhaps we were blocked and other call loaded
    // the permissions already. This is a protection against misses storm.
    if (auto it = perms_cache->find(r); it != perms_cache->end()) {
        co_return it->second;
    }
    auto perms = co_await _permission_loader(role, r);
    add_permissions(*perms_cache, r, perms);
    co_return perms;
}

future<> cache::prune(const resource& r) {
    auto units = co_await get_units(_loading_sem, 1, _as);
    _anonymous_permissions.erase(r);
    for (auto& it : _roles) {
        // Prunning can run concurrently with other functions but it
        // can only cause cached_permissions extra reload via get_permissions.
        remove_permissions(it.second->cached_permissions, r);
        co_await coroutine::maybe_yield();
    }
}

future<> cache::reload_all_permissions() noexcept {
    SCYLLA_ASSERT(_permission_loader);
    auto units = co_await get_units(_loading_sem, 1, _as);
    auto copy_keys = [] (const std::unordered_map<resource, permission_set>& m) {
        std::vector<resource> keys;
        keys.reserve(m.size());
        for (const auto& [res, _] : m) {
            keys.push_back(res);
        }
        return keys;
    };
    const role_or_anonymous anon;
    for (const auto& res : copy_keys(_anonymous_permissions)) {
        _anonymous_permissions[res] = co_await _permission_loader(anon, res);
    }
    for (auto& [role, entry] : _roles) {
        auto& perms_cache = entry->cached_permissions;
        auto r = role_or_anonymous(role);
        for (const auto& res : copy_keys(perms_cache)) {
            perms_cache[res] = co_await _permission_loader(r, res);
        }
    }
    logger.debug("Reloaded auth cache with {} entries", _roles.size());
}

future<lw_shared_ptr<cache::role_record>> cache::fetch_role(const role_name_t& role) const {
    auto rec = make_lw_shared<role_record>();
    rec->version = _current_version;

    auto fetch = [this, &role](const sstring& q) {
        return _qp.execute_internal(q, db::consistency_level::LOCAL_ONE,
                internal_distributed_query_state(), {role},
                cql3::query_processor::cache_internal::yes);
    };
    // roles
    {
        static const sstring q = format("SELECT * FROM {}.{} WHERE role = ?", db::system_keyspace::NAME, meta::roles_table::name);
        auto rs = co_await fetch(q);
        if (!rs->empty()) {
            auto& r = rs->one();
            rec->is_superuser = r.get_or<bool>("is_superuser", false);
            rec->can_login = r.get_or<bool>("can_login", false);
            rec->salted_hash = r.get_or<sstring>("salted_hash", "");
            if (r.has("member_of")) {
                auto mo = r.get_set<sstring>("member_of");
                rec->member_of.insert(
                        std::make_move_iterator(mo.begin()),
                        std::make_move_iterator(mo.end()));
            }
        } else {
            // role got deleted
            co_return nullptr;
        }
    }
    // members
    {
        static const sstring q = format("SELECT role, member FROM {}.{} WHERE role = ?", db::system_keyspace::NAME, ROLE_MEMBERS_CF);
        auto rs = co_await fetch(q);
        for (const auto& r : *rs) {
            rec->members.insert(r.get_as<sstring>("member"));
            co_await coroutine::maybe_yield();
        }
    }
    // attributes
    {
        static const sstring q = format("SELECT role, name, value FROM {}.{} WHERE role = ?", db::system_keyspace::NAME, ROLE_ATTRIBUTES_CF);
        auto rs = co_await fetch(q);
        for (const auto& r : *rs) {
            rec->attributes[r.get_as<sstring>("name")] =
                    r.get_as<sstring>("value");
            co_await coroutine::maybe_yield();
        }
    }
    // permissions
    {
        static const sstring q = format("SELECT role, resource, permissions FROM {}.{} WHERE role = ?", db::system_keyspace::NAME, PERMISSIONS_CF);
        auto rs = co_await fetch(q);
        for (const auto& r : *rs) {
            auto resource = r.get_as<sstring>("resource");
            auto perms_strings = r.get_set<sstring>("permissions");
            std::unordered_set<sstring> perms_set(perms_strings.begin(), perms_strings.end());
            auto pset = permissions::from_strings(perms_set);
            rec->permissions[std::move(resource)] = std::move(pset);
            co_await coroutine::maybe_yield();
        }
    }
    co_return rec;
}

future<> cache::prune_all() noexcept {
    for (auto it = _roles.begin(); it != _roles.end(); ) {
        if (it->second->version != _current_version) {
            remove_role(it++);
            co_await coroutine::maybe_yield();
        } else {
            ++it;
        }
    }
    co_return;
}

future<> cache::load_all() {
    if (legacy_mode(_qp)) {
        co_return;
    }
    SCYLLA_ASSERT(this_shard_id() == 0);
    auto units = co_await get_units(_loading_sem, 1, _as);

    ++_current_version;

    logger.info("Loading all roles");
    const uint32_t page_size = 128;
    auto loader = [this](const cql3::untyped_result_set::row& r) -> future<stop_iteration> {
        const auto name = r.get_as<sstring>("role");
        auto role = co_await fetch_role(name);
        if (role) {
            add_role(name, role);
        }
        co_return stop_iteration::no;
    };
    co_await _qp.query_internal(format("SELECT * FROM {}.{}",
            db::system_keyspace::NAME, meta::roles_table::name),
            db::consistency_level::LOCAL_ONE, {}, page_size, loader);

    co_await prune_all();
    for (const auto& [name, role] : _roles) {
        co_await distribute_role(name, role);
    }
    co_await container().invoke_on_others([this](cache& c) -> future<> {
        auto units = co_await get_units(c._loading_sem, 1, c._as);
        c._current_version = _current_version;
        co_await c.prune_all();
    });
}

future<> cache::gather_inheriting_roles(std::unordered_set<role_name_t>& roles, lw_shared_ptr<cache::role_record> role, const role_name_t& name) {
    if (!role) {
        // Role might have been removed or not yet added, either way
        // their members will be handled by another top call to this function.
        co_return;
    }
    for (const auto& member_name : role->members) {
        bool is_new = roles.insert(member_name).second;
        if (!is_new) {
            continue;
        }
        lw_shared_ptr<cache::role_record> member_role;
        auto r = _roles.find(member_name);
        if (r != _roles.end()) {
            member_role = r->second;
        }
        co_await gather_inheriting_roles(roles, member_role, member_name);
    }
}

future<> cache::load_roles(std::unordered_set<role_name_t> roles) {
    if (legacy_mode(_qp)) {
        co_return;
    }
    SCYLLA_ASSERT(this_shard_id() == 0);
    auto units = co_await get_units(_loading_sem, 1, _as);

    std::unordered_set<role_name_t> roles_to_clear_perms;
    for (const auto& name : roles) {
        logger.info("Loading role {}", name);
        auto role = co_await fetch_role(name);
         if (role) {
            add_role(name, role);
            co_await gather_inheriting_roles(roles_to_clear_perms, role, name);
        } else {
            if (auto it = _roles.find(name); it != _roles.end()) {
                auto old_role = it->second;
                remove_role(it);
                co_await gather_inheriting_roles(roles_to_clear_perms, old_role, name);
            }
        }
        co_await distribute_role(name, role);
    }

    co_await container().invoke_on_all([&roles_to_clear_perms] (cache& c) -> future<> {
        for (const auto& name : roles_to_clear_perms) {
            c.clear_role_permissions(name);
            co_await coroutine::maybe_yield();
        }
    });
}

future<> cache::distribute_role(const role_name_t& name, lw_shared_ptr<role_record> role) {
    auto role_ptr = role.get();
    co_await container().invoke_on_others([&name, role_ptr](cache& c) -> future<> {
        auto units = co_await get_units(c._loading_sem, 1, c._as);
        if (!role_ptr) {
            c.remove_role(name);
            co_return;
        }
        auto role_copy = make_lw_shared<role_record>(*role_ptr);
        c.add_role(name, std::move(role_copy));
    });
}

bool cache::includes_table(const table_id& id) noexcept {
    return id == db::system_keyspace::roles()->id()
            || id == db::system_keyspace::role_members()->id()
            || id == db::system_keyspace::role_attributes()->id()
            || id == db::system_keyspace::role_permissions()->id();
}

void cache::add_role(const role_name_t& name, lw_shared_ptr<role_record> role) {
    if (auto it = _roles.find(name); it != _roles.end()) {
        _cached_permissions_count -= it->second->cached_permissions.size();
    }
    _cached_permissions_count += role->cached_permissions.size();
    _roles[name] = std::move(role);
}

void cache::remove_role(const role_name_t& name) {
    if (auto it = _roles.find(name); it != _roles.end()) {
        remove_role(it);
    }
}

void cache::remove_role(roles_map::iterator it) {
    _cached_permissions_count -= it->second->cached_permissions.size();
    _roles.erase(it);
}

void cache::clear_role_permissions(const role_name_t& name) {
    if (auto it = _roles.find(name); it != _roles.end()) {
        _cached_permissions_count -= it->second->cached_permissions.size();
        it->second->cached_permissions.clear();
    }
}

void cache::add_permissions(std::unordered_map<resource, permission_set>& cache, const resource& r, permission_set perms) {
    if (cache.emplace(r, perms).second) {
        ++_cached_permissions_count;
    }
}

void cache::remove_permissions(std::unordered_map<resource, permission_set>& cache, const resource& r) {
    _cached_permissions_count -= cache.erase(r);
}

} // namespace auth
