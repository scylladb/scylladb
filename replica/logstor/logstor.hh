/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/temporary_buffer.hh>
#include <optional>
#include <seastar/core/scheduling.hh>
#include "readers/mutation_reader.hh"
#include "types.hh"
#include "index.hh"
#include "segment_manager.hh"
#include "write_buffer.hh"
#include "mutation/mutation.hh"
#include "dht/decorated_key.hh"

namespace replica {
namespace logstor {

extern seastar::logger logstor_logger;

struct logstor_config {
    segment_manager_config segment_manager_cfg;
    seastar::scheduling_group flush_sg;
};

class logstor {

    log_index _index;
    segment_manager _segment_manager;
    buffered_writer _write_buffer;

public:

    explicit logstor(logstor_config);

    logstor(const logstor&) = delete;
    logstor& operator=(const logstor&) = delete;

    future<> start();
    future<> stop();

    static void init_crypto();
    static void free_crypto();

    void enable_auto_compaction();
    future<> disable_auto_compaction();
    future<> trigger_compaction(bool major = false);

    static index_key calculate_key(const schema&, const dht::decorated_key&);

    future<> write(const mutation&);

    future<std::optional<log_record>> read(index_key);

    future<std::optional<canonical_mutation>> read(const schema&, const dht::decorated_key&);

    /// Create a mutation reader for a specific key
    mutation_reader make_reader_for_key(schema_ptr schema,
                                       reader_permit permit,
                                       const dht::decorated_key& key,
                                       const query::partition_slice& slice,
                                       tracing::trace_state_ptr trace_state = nullptr);

};

} // namespace logstor
} // namespace replica
