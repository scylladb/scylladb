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

#include "auth/authorizer.hh"
#include "service/migration_manager.hh"
#include "service/raft/raft_group0_client.hh"

namespace cql3 {

class query_processor;

} // namespace cql3

namespace auth {

class default_authorizer : public authorizer {
    cql3::query_processor& _qp;
    ::service::raft_group0_client& _group0_client;

    ::service::migration_manager& _migration_manager;

    abort_source _as{};

    future<> _finished{make_ready_future<>()};

public:
    default_authorizer(cql3::query_processor&, ::service::raft_group0_client&, ::service::migration_manager&);

    ~default_authorizer();

    virtual future<> start() override;

    virtual future<> stop() override;

    virtual std::string_view qualified_java_name() const override;

    virtual future<permission_set> authorize(const role_or_anonymous&, const resource&) const override;

    virtual future<> grant(std::string_view, permission_set, const resource&, ::service::group0_batch&) override;

    virtual future<> revoke( std::string_view, permission_set, const resource&, ::service::group0_batch&) override;

    virtual future<std::vector<permission_details>> list_all() const override;

    virtual future<> revoke_all(std::string_view, ::service::group0_batch&) override;

    virtual future<> revoke_all(const resource&, ::service::group0_batch&) override;

    virtual const resource_set& protected_resources() const override;

private:
    future<> start_legacy();

    bool legacy_metadata_exists() const;

    future<> revoke_all_legacy(const resource&);

    future<bool> legacy_any_granted() const;

    future<> migrate_legacy_metadata();

    future<> modify(std::string_view, permission_set, const resource&, std::string_view, ::service::group0_batch&);

    void revoke_all_keyspace_resources(const resource& ks_resource, ::service::group0_batch& mc);
};

} /* namespace auth */

