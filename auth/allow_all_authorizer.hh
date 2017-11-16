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

#include "authorizer.hh"
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

    future<> start() override {
        return make_ready_future<>();
    }

    future<> stop() override {
        return make_ready_future<>();
    }

    const sstring& qualified_java_name() const override {
        return allow_all_authorizer_name();
    }

    future<permission_set> authorize(service&, ::shared_ptr<authenticated_user>, data_resource) const override {
        return make_ready_future<permission_set>(permissions::ALL);
    }

    future<> grant(::shared_ptr<authenticated_user>, permission_set, data_resource, sstring) override {
        throw exceptions::invalid_request_exception("GRANT operation is not supported by AllowAllAuthorizer");
    }

    future<> revoke(::shared_ptr<authenticated_user>, permission_set, data_resource, sstring) override {
        throw exceptions::invalid_request_exception("REVOKE operation is not supported by AllowAllAuthorizer");
    }

    future<std::vector<permission_details>> list(
            service&,
            ::shared_ptr<authenticated_user> performer,
            permission_set,
            stdx::optional<data_resource>,
            stdx::optional<sstring>) const override {
        throw exceptions::invalid_request_exception("LIST PERMISSIONS operation is not supported by AllowAllAuthorizer");
    }

    future<> revoke_all(sstring dropped_user) override {
        return make_ready_future();
    }

    future<> revoke_all(data_resource) override {
        return make_ready_future();
    }

    const resource_ids& protected_resources() override {
        static const resource_ids ids;
        return ids;
    }

    future<> validate_configuration() const override {
        return make_ready_future();
    }
};

}
