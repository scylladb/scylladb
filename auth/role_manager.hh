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

#include <string_view>
#include <memory>
#include <optional>
#include <stdexcept>
#include <unordered_set>

#include <seastar/core/future.hh>
#include <seastar/core/print.hh>
#include <seastar/core/sstring.hh>

#include "auth/resource.hh"
#include "seastarx.hh"
#include "exceptions/exceptions.hh"

namespace auth {

struct role_config final {
    bool is_superuser{false};
    bool can_login{false};
};

///
/// Differential update for altering existing roles.
///
struct role_config_update final {
    std::optional<bool> is_superuser{};
    std::optional<bool> can_login{};
};

///
/// A logical argument error for a role-management operation.
///
class roles_argument_exception : public exceptions::invalid_request_exception {
public:
    using exceptions::invalid_request_exception::invalid_request_exception;
};

class role_already_exists : public roles_argument_exception {
public:
    explicit role_already_exists(std::string_view role_name)
            : roles_argument_exception(format("Role {} already exists.", role_name)) {
    }
};

class nonexistant_role : public roles_argument_exception {
public:
    explicit nonexistant_role(std::string_view role_name)
            : roles_argument_exception(format("Role {} doesn't exist.", role_name)) {
    }
};

class role_already_included : public roles_argument_exception {
public:
    role_already_included(std::string_view grantee_name, std::string_view role_name)
            : roles_argument_exception(
                      format("{} already includes role {}.", grantee_name, role_name)) {
    }
};

class revoke_ungranted_role : public roles_argument_exception {
public:
    revoke_ungranted_role(std::string_view revokee_name, std::string_view role_name)
            : roles_argument_exception(
                      format("{} was not granted role {}, so it cannot be revoked.", revokee_name, role_name)) {
    }
};

using role_set = std::unordered_set<sstring>;

enum class recursive_role_query { yes, no };

///
/// Abstract client for managing roles.
///
/// All state necessary for managing roles is stored externally to the client instance.
///
/// All implementations should throw role-related exceptions as documented. Authorization is not addressed here, and
/// access-control should never be enforced in implementations.
///
class role_manager {
public:
    // this type represents a mapping between a role and some attribute value.
    // i.e: given attribute name  'a' this map holds role name and it's assigned
    // value of 'a'.
    using attribute_vals = std::unordered_map<sstring, sstring>;
public:
    virtual ~role_manager() = default;

    virtual std::string_view qualified_java_name() const noexcept = 0;

    virtual const resource_set& protected_resources() const = 0;

    virtual future<> start() = 0;

    virtual future<> stop() = 0;

    ///
    /// \returns an exceptional future with \ref role_already_exists for a role that has previously been created.
    ///
    virtual future<> create(std::string_view role_name, const role_config&) = 0;

    ///
    /// \returns an exceptional future with \ref nonexistant_role if the role does not exist.
    ///
    virtual future<> drop(std::string_view role_name) = 0;

    ///
    /// \returns an exceptional future with \ref nonexistant_role if the role does not exist.
    ///
    virtual future<> alter(std::string_view role_name, const role_config_update&) = 0;

    ///
    /// Grant `role_name` to `grantee_name`.
    ///
    /// \returns an exceptional future with \ref nonexistant_role if either the role or the grantee do not exist.
    ///
    /// \returns an exceptional future with \ref role_already_included if granting the role would be redundant, or
    /// create a cycle.
    ///
    virtual future<> grant(std::string_view grantee_name, std::string_view role_name) = 0;

    ///
    /// Revoke `role_name` from `revokee_name`.
    ///
    /// \returns an exceptional future with \ref nonexistant_role if either the role or the revokee do not exist.
    ///
    /// \returns an exceptional future with \ref revoke_ungranted_role if the role was not granted.
    ///
    virtual future<> revoke(std::string_view revokee_name, std::string_view role_name) = 0;

    ///
    /// \returns an exceptional future with \ref nonexistant_role if the role does not exist.
    ///
    virtual future<role_set> query_granted(std::string_view grantee, recursive_role_query) = 0;

    virtual future<role_set> query_all() = 0;

    virtual future<bool> exists(std::string_view role_name) = 0;

    ///
    /// \returns an exceptional future with \ref nonexistant_role if the role does not exist.
    ///
    virtual future<bool> is_superuser(std::string_view role_name) = 0;

    ///
    /// \returns an exceptional future with \ref nonexistant_role if the role does not exist.
    ///
    virtual future<bool> can_login(std::string_view role_name) = 0;

    ///
    /// \returns the value of the named attribute, if one is set.
    ///
    virtual future<std::optional<sstring>> get_attribute(std::string_view role_name, std::string_view attribute_name) = 0;

    ///
    /// \returns a mapping of each role's value for the named attribute, if one is set for the role.
    ///
    virtual future<attribute_vals> query_attribute_for_all(std::string_view attribute_name) = 0;

    /// Sets `attribute_name` with `attribute_value` for `role_name`.
    /// \returns an exceptional future with nonexistant_role if the role does not exist.
    ///
    virtual future<> set_attribute(std::string_view role_name, std::string_view attribute_name, std::string_view attribute_value) = 0;

    /// Removes `attribute_name` for `role_name`.
    /// \returns an exceptional future with nonexistant_role if the role does not exist.
    /// \note: This is a no-op if the role does not have the named attribute set.
    ///
    virtual future<> remove_attribute(std::string_view role_name, std::string_view attribute_name) = 0;
};
}
