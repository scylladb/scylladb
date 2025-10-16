/*
 * Copyright (C) 2025 ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#pragma once

#include "audit/audit.hh"
#include <seastar/core/future.hh>

#include "storage_helper.hh"

namespace audit {

class audit_composite_storage_helper : public storage_helper {
    std::vector<std::unique_ptr<storage_helper>> _storage_helpers;

public:
    explicit audit_composite_storage_helper(std::vector<std::unique_ptr<storage_helper>>&&);
    virtual ~audit_composite_storage_helper() = default;
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

} // namespace audit
