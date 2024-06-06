/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

extern const std::string_view allow_all_authenticator_name;

class allow_all_authenticator final : public authenticator {
public:
    allow_all_authenticator(cql3::query_processor&, ::service::raft_group0_client&, ::service::migration_manager&) {
    }

    virtual future<> start() override {
        return make_ready_future<>();
    }

    virtual future<> stop() override {
        return make_ready_future<>();
    }

    virtual std::string_view qualified_java_name() const override {
        return allow_all_authenticator_name;
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

    virtual future<> create(std::string_view, const authentication_options& options, ::service::group0_batch&) override {
        return make_ready_future();
    }

    virtual future<> alter(std::string_view, const authentication_options& options, ::service::group0_batch&) override {
        return make_ready_future();
    }

    virtual future<> drop(std::string_view, ::service::group0_batch&) override {
        return make_ready_future();
    }

    virtual future<custom_options> query_custom_options(std::string_view role_name) const override {
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
