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

#include "auth/authorizer.hh"
#include "exceptions/exceptions.hh"
#include "stdx.hh"

namespace cql3 {
class query_processor;
}

namespace service {
class migration_manager;
}

namespace auth {

class service;

const sstring& allow_all_authorizer_name();

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

    virtual const sstring& qualified_java_name() const override {
        return allow_all_authorizer_name();
    }

    virtual future<permission_set> authorize(service&, stdx::string_view, const resource&) const override {
        return make_ready_future<permission_set>(permissions::ALL);
    }

    virtual future<> grant(permission_set, const resource&, stdx::string_view) override {
        throw exceptions::invalid_request_exception("GRANT operation is not supported by AllowAllAuthorizer");
    }

    virtual future<> revoke(permission_set, const resource&, stdx::string_view) override {
        throw exceptions::invalid_request_exception("REVOKE operation is not supported by AllowAllAuthorizer");
    }

    virtual future<std::vector<permission_details>> list(
            service&,
            permission_set,
            const std::optional<resource>&,
            const std::optional<stdx::string_view>&) const override {
        throw exceptions::invalid_request_exception("LIST PERMISSIONS operation is not supported by AllowAllAuthorizer");
    }

    virtual future<> revoke_all(stdx::string_view) override {
        return make_ready_future();
    }

    virtual future<> revoke_all(const resource&) override {
        return make_ready_future();
    }

    virtual const resource_set& protected_resources() const override {
        static const resource_set resources;
        return resources;
    }
};

}
