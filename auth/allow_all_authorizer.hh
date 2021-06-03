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

#include "auth/authorizer.hh"
#include "exceptions/exceptions.hh"

namespace cql3 {
class query_processor;
}

namespace service {
class migration_manager;
}

namespace auth {

extern const std::string_view allow_all_authorizer_name;

class allow_all_authorizer final  : public authorizer {
public:
    allow_all_authorizer(cql3::query_processor&, ::service::migration_manager&) {
    }

    virtual future<> start() override {
        return make_ready_future<>();
    }

    virtual future<> stop() override {
        return make_ready_future<>();
    }

    virtual std::string_view qualified_java_name() const override {
        return allow_all_authorizer_name;
    }

    virtual future<permission_set> authorize(const role_or_anonymous&, const resource&) const override {
        return make_ready_future<permission_set>(permissions::ALL);
    }

    virtual future<> grant(std::string_view, permission_set, const resource&) const override {
        return make_exception_future<>(
                unsupported_authorization_operation("GRANT operation is not supported by AllowAllAuthorizer"));
    }

    virtual future<> revoke(std::string_view, permission_set, const resource&) const override {
        return make_exception_future<>(
                unsupported_authorization_operation("REVOKE operation is not supported by AllowAllAuthorizer"));
    }

    virtual future<std::vector<permission_details>> list_all() const override {
        return make_exception_future<std::vector<permission_details>>(
                unsupported_authorization_operation(
                        "LIST PERMISSIONS operation is not supported by AllowAllAuthorizer"));
    }

    virtual future<> revoke_all(std::string_view) const override {
        return make_exception_future(
                unsupported_authorization_operation("REVOKE operation is not supported by AllowAllAuthorizer"));
    }

    virtual future<> revoke_all(const resource&) const override {
        return make_exception_future(
                unsupported_authorization_operation("REVOKE operation is not supported by AllowAllAuthorizer"));
    }

    virtual const resource_set& protected_resources() const override {
        static const resource_set resources;
        return resources;
    }
};

}
