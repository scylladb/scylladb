/*
 * Copyright (C) 2025 ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#pragma once

#include "audit/audit.hh"
#include "storage_helper.hh"
#include "db/config.hh"

namespace service {

class migration_manager;

};

namespace cql3 {

class query_processor;

};

namespace audit {

/// Audit backend that writes audit events to the process standard output.
///
/// This backend is intended for containerised / cloud-native deployments
/// (Kubernetes, OpenShift, Docker) where the container runtime captures
/// stdout and forwards it to a centralised log aggregator (FluentBit,
/// Vector, etc.).  It avoids the need for a syslog daemon or a privileged
/// /dev/log hostPath mount.
class audit_stdout_storage_helper : public storage_helper {
public:
    explicit audit_stdout_storage_helper(cql3::query_processor&, service::migration_manager&);
    virtual ~audit_stdout_storage_helper() override = default;

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
