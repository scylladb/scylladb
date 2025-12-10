/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "auth/cache.hh"
#include "auth/common.hh"
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

namespace auth {

logging::logger logger("auth-cache");

cache::cache(cql3::query_processor& qp, abort_source& as) noexcept
    : _current_version(0)
    , _qp(qp)
    , _loading_sem(1)
    , _as(as) {
}

lw_shared_ptr<const cache::role_record> cache::get(const role_name_t& role) const noexcept {
    auto it = _roles.find(role);
    if (it == _roles.end()) {
        return {};
    }
    return it->second;
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
            _roles.erase(it++);
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
            _roles[name] = role;
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
        c._current_version = _current_version;
        co_await c.prune_all();
    });
}

future<> cache::load_roles(std::unordered_set<role_name_t> roles) {
    if (legacy_mode(_qp)) {
        co_return;
    }
    SCYLLA_ASSERT(this_shard_id() == 0);
    auto units = co_await get_units(_loading_sem, 1, _as);

    for (const auto& name : roles) {
        logger.info("Loading role {}", name);
        auto role = co_await fetch_role(name);
         if (role) {
            _roles[name] = role;
        } else {
            _roles.erase(name);
        }
        co_await distribute_role(name, role);
    }
}

future<> cache::distribute_role(const role_name_t& name, lw_shared_ptr<role_record> role) {
    auto role_ptr = role.get();
    co_await container().invoke_on_others([&name, role_ptr](cache& c) {
        if (!role_ptr) {
            c._roles.erase(name);
            return;
        }
        auto role_copy = make_lw_shared<role_record>(*role_ptr);
        c._roles[name] = std::move(role_copy);
    });
}

bool cache::includes_table(const table_id& id) noexcept {
    return id == db::system_keyspace::roles()->id()
            || id == db::system_keyspace::role_members()->id()
            || id == db::system_keyspace::role_attributes()->id()
            || id == db::system_keyspace::role_permissions()->id();
}

} // namespace auth
