/*
 * Copyright (C) 2017 ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#pragma once

#include <seastar/net/api.hh>

#include "audit/audit.hh"
#include "storage_helper.hh"
#include "db/config.hh"

namespace service {

class migration_manager;

};

namespace audit {

class audit_syslog_storage_helper : public storage_helper {
    socket_address _syslog_address;
    net::datagram_channel _sender;
public:
    explicit audit_syslog_storage_helper(cql3::query_processor&, service::migration_manager&);
    virtual ~audit_syslog_storage_helper();
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
