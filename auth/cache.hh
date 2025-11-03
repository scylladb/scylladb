/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <unordered_set>
#include <unordered_map>

#include <seastar/core/sstring.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>

#include <absl/container/flat_hash_map.h>

#include "auth/permission.hh"
#include "auth/common.hh"

namespace cql3 { class query_processor; }

namespace auth {

class cache : public peering_sharded_service<cache> {
public:
    using role_name_t = sstring;
    using version_tag_t = char;

	struct role_record {
        bool can_login = false;
        bool is_superuser = false;
        std::unordered_set<role_name_t> member_of;
        std::unordered_set<role_name_t> members;
        sstring salted_hash;
        std::unordered_map<sstring, sstring> attributes;
        std::unordered_map<sstring, permission_set> permissions;
        version_tag_t version; // used for seamless cache reloads
    };

    explicit cache(cql3::query_processor& qp) noexcept;
    lw_shared_ptr<const role_record> get(const role_name_t& role) const noexcept;
    future<> load_all();
    future<> load_roles(std::unordered_set<role_name_t> roles);
    static bool includes_table(const table_id&) noexcept;

private:
    using roles_map = absl::flat_hash_map<role_name_t, lw_shared_ptr<role_record>>;
    roles_map _roles;
    version_tag_t _current_version;
    cql3::query_processor& _qp;

    future<lw_shared_ptr<role_record>> fetch_role(const role_name_t& role) const;
    future<> prune_all() noexcept;
    future<> distribute_role(const role_name_t& name, const lw_shared_ptr<role_record> role);
};

} // namespace auth
