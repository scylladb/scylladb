/*
 * Copyright (C) 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <seastar/core/abort_source.hh>

#include "db/consistency_level_type.hh"
#include "auth/authenticator.hh"
#include "service/raft/raft_group0_client.hh"

namespace db {
    class config;
}

namespace cql3 {

class query_processor;

} // namespace cql3

namespace service {
class migration_manager;
}

namespace auth {

extern const std::string_view password_authenticator_name;

class password_authenticator : public authenticator {
    cql3::query_processor& _qp;
    ::service::raft_group0_client& _group0_client;
    ::service::migration_manager& _migration_manager;
    future<> _stopped;
    abort_source _as;
    std::string _superuser;

public:
    static db::consistency_level consistency_for_user(std::string_view role_name);
    static std::string default_superuser(const db::config&);

    password_authenticator(cql3::query_processor&, ::service::raft_group0_client&, ::service::migration_manager&);

    ~password_authenticator();

    virtual future<> start() override;

    virtual future<> stop() override;

    virtual std::string_view qualified_java_name() const override;

    virtual bool require_authentication() const override;

    virtual authentication_option_set supported_options() const override;

    virtual authentication_option_set alterable_options() const override;

    virtual future<authenticated_user> authenticate(const credentials_map& credentials) const override;

    virtual future<> create(std::string_view role_name, const authentication_options& options, ::service::group0_batch& mc) override;

    virtual future<> alter(std::string_view role_name, const authentication_options& options, ::service::group0_batch&) override;

    virtual future<> drop(std::string_view role_name, ::service::group0_batch&) override;

    virtual future<custom_options> query_custom_options(std::string_view role_name) const override;

    virtual const resource_set& protected_resources() const override;

    virtual ::shared_ptr<sasl_challenge> new_sasl_challenge() const override;

private:
    bool legacy_metadata_exists() const;

    future<> migrate_legacy_metadata() const;

    future<> create_default_if_missing();

    sstring update_row_query() const;
};

}

