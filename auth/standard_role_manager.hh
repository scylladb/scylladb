/*
 * Copyright (C) 2017-present ScyllaDB
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

#include "auth/role_manager.hh"

#include <string_view>
#include <unordered_set>

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include "seastarx.hh"

namespace cql3 {
class query_processor;
}

namespace service {
class migration_manager;
}

namespace auth {

class standard_role_manager final : public role_manager {
    cql3::query_processor& _qp;
    ::service::migration_manager& _migration_manager;
    future<> _stopped;
    seastar::abort_source _as;

public:
    standard_role_manager(cql3::query_processor& qp, ::service::migration_manager& mm)
            : _qp(qp)
            , _migration_manager(mm)
            , _stopped(make_ready_future<>()) {
    }

    virtual std::string_view qualified_java_name() const noexcept override;

    virtual const resource_set& protected_resources() const override;

    virtual future<> start() override;

    virtual future<> stop() override;

    virtual future<> create(std::string_view role_name, const role_config&) override;

    virtual future<> drop(std::string_view role_name) override;

    virtual future<> alter(std::string_view role_name, const role_config_update&) override;

    virtual future<> grant(std::string_view grantee_name, std::string_view role_name) override;

    virtual future<> revoke(std::string_view revokee_name, std::string_view role_name) override;

    virtual future<role_set> query_granted(std::string_view grantee_name, recursive_role_query) override;

    virtual future<role_set> query_all() override;

    virtual future<bool> exists(std::string_view role_name) override;

    virtual future<bool> is_superuser(std::string_view role_name) override;

    virtual future<bool> can_login(std::string_view role_name) override;

    virtual future<std::optional<sstring>> get_attribute(std::string_view role_name, std::string_view attribute_name) override;

    virtual future<role_manager::attribute_vals> query_attribute_for_all(std::string_view attribute_name) override;

    virtual future<> set_attribute(std::string_view role_name, std::string_view attribute_name, std::string_view attribute_value) override;

    virtual future<> remove_attribute(std::string_view role_name, std::string_view attribute_name) override;

private:
    enum class membership_change { add, remove };

    future<> create_metadata_tables_if_missing() const;

    bool legacy_metadata_exists();

    future<> migrate_legacy_metadata() const;

    future<> create_default_role_if_missing() const;

    future<> create_or_replace(std::string_view role_name, const role_config&) const;

    future<> modify_membership(std::string_view role_name, std::string_view grantee_name, membership_change) const;
};

}
