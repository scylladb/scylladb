/*
 * Copyright (C) 2017 ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#pragma once

#include "audit/audit.hh"
#include <seastar/core/future.hh>

namespace audit {

class storage_helper {
public:
    using ptr_type = std::unique_ptr<storage_helper>;
    storage_helper() {}
    virtual ~storage_helper() {}
    virtual future<> start(const db::config& cfg) = 0;
    virtual future<> stop() = 0;
    virtual future<> write(const audit_info* audit_info,
                           socket_address node_ip,
                           socket_address client_ip,
                           db::consistency_level cl,
                           const sstring& username,
                           bool error) = 0;
    virtual future<> write_login(const sstring& username,
                                 socket_address node_ip,
                                 socket_address client_ip,
                                 bool error) = 0;
};

}
