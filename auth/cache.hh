/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/abort_source.hh>
#include <string_view>
#include <unordered_set>
#include <unordered_map>

#include <seastar/core/sstring.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/metrics_registration.hh>

#include "absl-flat_hash_map.hh"

#include "auth/permission.hh"
#include "auth/common.hh"
#include "auth/resource.hh"
#include "auth/role_or_anonymous.hh"

namespace cql3 { class query_processor; }

namespace auth {

class cache : public peering_sharded_service<cache> {
public:
    using role_name_t = sstring;
    using version_tag_t = char;
    using permission_loader_func = std::function<future<permission_set>(const role_or_anonymous&, const resource&)>;

	struct role_record {
        bool can_login = false;
        bool is_superuser = false;
        std::unordered_set<role_name_t> member_of;
        std::unordered_set<role_name_t> members;
        sstring salted_hash;
        std::unordered_map<sstring, sstring, sstring_hash, sstring_eq> attributes;
        std::unordered_map<sstring, permission_set, sstring_hash, sstring_eq> permissions;
    private:
        friend cache;
        // cached permissions include effects of role's inheritance
        std::unordered_map<resource, permission_set> cached_permissions;
        version_tag_t version; // used for seamless cache reloads
    };

    explicit cache(cql3::query_processor& qp, abort_source& as) noexcept;
    lw_shared_ptr<const role_record> get(std::string_view role) const noexcept;
    void set_permission_loader(permission_loader_func loader);
    future<permission_set> get_permissions(const role_or_anonymous& role, const resource& r);
    future<> prune(const resource& r);
    future<> reload_all_permissions() noexcept;
    future<> load_all();
    future<> load_roles(std::unordered_set<role_name_t> roles);
    static bool includes_table(const table_id&) noexcept;

    // Returns the number of roles in the cache.
    size_t roles_count() const noexcept;

    // The callback doesn't suspend (no co_await) so it observes the state
    // of the cache atomically.
    void for_each_role(const std::function<void(const role_name_t&, const role_record&)>& func) const;

private:
    using roles_map = absl::flat_hash_map<role_name_t, lw_shared_ptr<role_record>, sstring_hash, sstring_eq>;
    roles_map _roles;
    // anonymous permissions map exists mainly due to compatibility with
    // higher layers which use role_or_anonymous to get permissions.
    std::unordered_map<resource, permission_set> _anonymous_permissions;
    version_tag_t _current_version;
    cql3::query_processor& _qp;
    semaphore _loading_sem; // protects iteration of _roles map
    abort_source& _as;
    permission_loader_func _permission_loader;
    semaphore _permission_loader_sem; // protects against reload storms on a single role change
    metrics::metric_groups _metrics;
    size_t _cached_permissions_count = 0;

    future<lw_shared_ptr<role_record>> fetch_role(const role_name_t& role) const;
    future<> prune_all() noexcept;
    future<> distribute_role(const role_name_t& name, const lw_shared_ptr<role_record> role);
    future<> gather_inheriting_roles(std::unordered_set<role_name_t>& roles, lw_shared_ptr<cache::role_record> role, const role_name_t& name);

    void add_role(const role_name_t& name, lw_shared_ptr<role_record> role);
    void remove_role(const role_name_t& name);
    void remove_role(roles_map::iterator it);
    void clear_role_permissions(const role_name_t& name);
    void add_permissions(std::unordered_map<resource, permission_set>& cache, const resource& r, permission_set perms);
    void remove_permissions(std::unordered_map<resource, permission_set>& cache, const resource& r);

    future<permission_set> load_permissions(const role_or_anonymous& role, const resource& r, std::unordered_map<resource, permission_set>* perms_cache);
};

} // namespace auth
