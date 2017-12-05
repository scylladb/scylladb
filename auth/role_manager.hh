/*
 * Copyright (C) 2017 ScyllaDB
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

#include <experimental/optional>
#include <experimental/string_view>
#include <memory>
#include <stdexcept>
#include <unordered_set>

#include <seastar/core/future.hh>
#include <seastar/core/print.hh>
#include <seastar/core/sstring.hh>

#include "auth/data_resource.hh"
#include "seastarx.hh"
#include "stdx.hh"

namespace auth {

class authenticated_user;

struct role_config final {
    bool is_superuser{false};
    bool can_login{false};
};

// Differential update for altering existing roles.
struct role_config_update final {
    stdx::optional<bool> is_superuser{};
    stdx::optional<bool> can_login{};
};

// A logical argument error for a role-management operation.
class roles_argument_exception : public std::invalid_argument {
public:
    using std::invalid_argument::invalid_argument;
};

class role_already_exists : public roles_argument_exception {
    std::shared_ptr<sstring> _role_name;

public:
    explicit role_already_exists(stdx::string_view role_name)
            : roles_argument_exception(sprint("The '%s' role already exists.", role_name))
            , _role_name(std::make_shared<sstring>(role_name)) {
    }

    stdx::string_view role_name() const noexcept {
        return *_role_name;
    }
};

class nonexistant_role : public roles_argument_exception {
    std::shared_ptr<sstring> _role_name;

public:
    explicit nonexistant_role(stdx::string_view role_name)
            : roles_argument_exception(sprint("The role '%s' does not exist.", role_name))
            , _role_name(std::make_shared<sstring>(role_name)) {
    }

    stdx::string_view role_name() const noexcept {
        return *_role_name;
    }
};

class role_already_included : public roles_argument_exception {
    std::shared_ptr<sstring> _role_name, _grantee_name;

public:
    role_already_included(stdx::string_view grantee_name, stdx::string_view role_name)
            : roles_argument_exception(
                      sprint("'%s' already includes role '%s'.", grantee_name, role_name))
            , _role_name(std::make_shared<sstring>(role_name))
            , _grantee_name(std::make_shared<sstring>(grantee_name)) {
    }

    stdx::string_view role_name() const noexcept {
        return *_role_name;
    }

    stdx::string_view grantee_name() const noexcept {
        return *_grantee_name;
    }
};

class revoke_ungranted_role : public roles_argument_exception {
    std::shared_ptr<sstring> _role_name, _revokee_name;

public:
    revoke_ungranted_role(stdx::string_view revokee_name, stdx::string_view role_name)
            : roles_argument_exception(
                      sprint("'%s' was not granted role '%s', so it cannot be revoked.", revokee_name, role_name))
            , _role_name(std::make_shared<sstring>(role_name))
            , _revokee_name(std::make_shared<sstring>(revokee_name)) {
    }

    stdx::string_view role_name() const noexcept {
        return *_role_name;
    }

    stdx::string_view revokee_name() const noexcept {
        return *_revokee_name;
    }
};

enum class recursive_role_query { yes, no };

// Abstract role manager.
//
// All implementations should throw role-related exceptions as documented, but authorization-related checking is
// handled by the CQL layer, and not here.
class role_manager {
public:
    virtual ~role_manager() = default;

    virtual stdx::string_view qualified_java_name() const noexcept = 0;

    virtual future<> start() = 0;

    virtual future<> stop() = 0;

    // Must throw `role_already_exists` for a role that has previously been created.
    virtual future<>
    create(const authenticated_user& performer, stdx::string_view role_name, const role_config&) = 0;

    // Must throw `nonexistant_role` if the role does not exist.
    virtual future<> drop(const authenticated_user& performer, stdx::string_view role_name) = 0;

    // Must throw `nonexistant_role` if the role does not exist.
    virtual future<>
    alter(const authenticated_user& performer, stdx::string_view role_name, const role_config_update&) = 0;

    // Grant `role_name` to `grantee_name`.
    //
    // Must throw `nonexistant_role` if either the role or the grantee do not exist.
    //
    // Must throw `role_already_included` if granting the role would be redundant, or create a cycle.
    virtual future<>
    grant(const authenticated_user& performer, stdx::string_view grantee_name, stdx::string_view role_name) = 0;

    // Revoke `role_name` from `revokee_name`.
    //
    // Must throw `nonexistant_role` if either the role or the revokee do not exist.
    //
    // Must throw `revoke_ungranted_role` if the role was not granted.
    virtual future<>
    revoke(const authenticated_user& performer, stdx::string_view revokee_name, stdx::string_view role_name) = 0;

    // Must throw `nonexistant_role` if the role does not exist.
    virtual future<std::unordered_set<sstring>> query_granted(stdx::string_view grantee, recursive_role_query) const = 0;

    virtual future<std::unordered_set<sstring>> query_all() const = 0;

    virtual future<bool> exists(stdx::string_view role_name) const = 0;

    /// Must throw `nonexistant_role` if the role does not exist.
    virtual future<bool> is_superuser(stdx::string_view role_name) const = 0;

    // Must throw `nonexistant_role` if the role does not exist.
    virtual future<bool> can_login(stdx::string_view role_name) const = 0;
};

}
