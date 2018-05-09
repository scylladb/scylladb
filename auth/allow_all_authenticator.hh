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

#include <stdexcept>

#include "auth/authenticated_user.hh"
#include "auth/authenticator.hh"
#include "auth/common.hh"

namespace cql3 {
class query_processor;
}

namespace service {
class migration_manager;
}

namespace auth {

const sstring& allow_all_authenticator_name();

class allow_all_authenticator final : public authenticator {
public:
    allow_all_authenticator(cql3::query_processor&, ::service::migration_manager&) {
    }

    virtual future<> start() override {
        return make_ready_future<>();
    }

    virtual future<> stop() override {
        return make_ready_future<>();
    }

    virtual const sstring& qualified_java_name() const override {
        return allow_all_authenticator_name();
    }

    virtual bool require_authentication() const override {
        return false;
    }

    virtual authentication_option_set supported_options() const override {
        return authentication_option_set();
    }

    virtual authentication_option_set alterable_options() const override {
        return authentication_option_set();
    }

    future<authenticated_user> authenticate(const credentials_map& credentials) const override {
        return make_ready_future<authenticated_user>(anonymous_user());
    }

    virtual future<> create(stdx::string_view, const authentication_options& options) const override {
        return make_ready_future();
    }

    virtual future<> alter(stdx::string_view, const authentication_options& options) const override {
        return make_ready_future();
    }

    virtual future<> drop(stdx::string_view) const override {
        return make_ready_future();
    }

    virtual future<custom_options> query_custom_options(stdx::string_view role_name) const override {
        return make_ready_future<custom_options>();
    }

    virtual const resource_set& protected_resources() const override {
        static const resource_set resources;
        return resources;
    }

    virtual ::shared_ptr<sasl_challenge> new_sasl_challenge() const override {
        throw std::runtime_error("Should not reach");
    }
};

}
