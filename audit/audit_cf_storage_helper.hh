/*
 * Copyright (C) 2017 ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#pragma once

#include "audit/audit.hh"
#include "table_helper.hh"
#include "storage_helper.hh"
#include "db/config.hh"
#include "service/raft/raft_group0_client.hh"

namespace cql3 {

class query_processor;

}

namespace service {

class migration_manager;

}

namespace audit {

class audit_cf_storage_helper : public storage_helper {
    static const sstring KEYSPACE_NAME;
    static const sstring TABLE_NAME;
    cql3::query_processor& _qp;
    service::migration_manager& _mm;
    table_helper _table;
    service::query_state _dummy_query_state;
    static cql3::query_options make_data(const audit_info* audit_info,
                                         socket_address node_ip,
                                         socket_address client_ip,
                                         db::consistency_level cl,
                                         const sstring& username,
                                         bool error);
    static cql3::query_options make_login_data(socket_address node_ip,
                                               socket_address client_ip,
                                               const sstring& username,
                                               bool error);

    future<> migrate_audit_table(service::group0_guard guard);

public:
    explicit audit_cf_storage_helper(cql3::query_processor& qp, service::migration_manager& mm);
    virtual ~audit_cf_storage_helper() {}
    virtual future<> start(const db::config& cfg) override;
    virtual future<> stop() override;
    virtual future<> write(const audit_info* audit_info,
                           socket_address node_ip,
                           socket_address client_ip,
                           db::consistency_level cl,
                           const sstring& username,
                           bool error) override;
    virtual future<> write_login(const sstring& username,
                                 socket_address node_ip,
                                 socket_address client_ip,
                                 bool error) override;
};

}
