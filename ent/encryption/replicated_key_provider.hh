/*
 * Copyright (C) 2015 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "encryption.hh"

namespace db {
class extensions;
class system_keyspace;
}

namespace replica {
class database;
}

namespace service {
class migration_manager;
class raft_group0_client;
class group0_guard;
}

namespace encryption {

/**
 * Manages the migration of replicated encryption keys from the legacy
 * system_replicated_keys.encrypted_keys table (using EverywhereStrategy)
 * to the system.encrypted_keys table (using Raft/group0).
 *
 * This class is only instantiated when migration is needed.
 */
class replicated_keys_migration_manager {
    encryption_context& _ctxt;

public:
    explicit replicated_keys_migration_manager(encryption_context& ctxt);

    /**
     * Perform the actual data migration from legacy table to group0 table.
     * This should be called by the topology coordinator when appropriate.
     */
    static future<> migrate_to_v1_5(db::system_keyspace& sys_ks,
                               cql3::query_processor& qp,
                               service::raft_group0_client& group0_client,
                               abort_source& as,
                               service::group0_guard&& guard);
    static future<> migrate_to_v2(db::system_keyspace& sys_ks,
                               cql3::query_processor& qp,
                               service::raft_group0_client& group0_client,
                               abort_source& as,
                               service::group0_guard&& guard);

    /**
     * Notify all replicated key provider instances about a migration state change.
     * This propagates the event to all shards.
     */
    future<> upgrade_to_v1_5();
    future<> upgrade_to_v2();
};

class replicated_key_provider_factory : public key_provider_factory {
public:
    replicated_key_provider_factory();
    ~replicated_key_provider_factory();

    shared_ptr<key_provider> get_provider(encryption_context&, const options&) override;

    static void init(db::extensions&);
    static future<> on_started(encryption_context&, ::replica::database&, service::migration_manager&);
};

}
