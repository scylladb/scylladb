/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <seastar/core/shared_ptr.hh>

#include "audit/audit.hh"
#include "storage_helper.hh"

namespace audit {

struct stdout_stream;
class stdout_record_batcher;

// Writes each audit record as one line to the standard output, through a single
// stream on shard 0, so records never interleave, whatever their length.
class audit_stdout_storage_helper : public storage_helper {
    shared_ptr<stdout_stream> _stream;
    shared_ptr<stdout_record_batcher> _batcher;
public:
    // `path` stands in for the standard output. A write fails once its record waits longer than `write_timeout`,
    // or at once if other records of its shard wait and leave too little of `max_unwritten_memory` for it.
    explicit audit_stdout_storage_helper(sstring path = "/dev/stdout", std::chrono::milliseconds write_timeout = std::chrono::seconds(5),
                                         size_t max_unwritten_memory = 4 * 1024 * 1024);
    // Fails if `path` cannot take the records, without writing to it.
    static future<> check(sstring path = "/dev/stdout");
    virtual ~audit_stdout_storage_helper();
    virtual future<> start(const db::config& cfg) override;
    virtual future<> stop() override;
    virtual future<> write(audit_sink_set sinks,
                           const audit_info* audit_info,
                           socket_address node_ip,
                           socket_address client_ip,
                           std::optional<db::consistency_level> cl,
                           const sstring& username,
                           bool error) override;
    virtual future<> write_login(audit_sink_set sinks,
                                 const sstring& username,
                                 socket_address node_ip,
                                 socket_address client_ip,
                                 bool error) override;
};

}
