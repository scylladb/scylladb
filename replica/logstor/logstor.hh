/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/temporary_buffer.hh>
#include <optional>
#include <seastar/core/scheduling.hh>
#include "readers/mutation_reader.hh"
#include "replica/compaction_group.hh"
#include "types.hh"
#include "index.hh"
#include "segment_manager.hh"
#include "write_buffer.hh"
#include "mutation/mutation.hh"
#include "dht/decorated_key.hh"

namespace replica {

class compaction_group;
class database;

namespace logstor {

extern seastar::logger logstor_logger;

struct logstor_config {
    segment_manager_config segment_manager_cfg;
    seastar::scheduling_group flush_sg;
};

class logstor {

    segment_manager _segment_manager;
    buffered_writer _write_buffer;

public:

    explicit logstor(logstor_config);

    logstor(const logstor&) = delete;
    logstor& operator=(const logstor&) = delete;

    future<> do_recovery(replica::database&);

    future<> start();
    future<> stop();

    size_t get_memory_usage() const;

    segment_manager& get_segment_manager() noexcept;
    const segment_manager& get_segment_manager() const noexcept;

    compaction_manager& get_compaction_manager() noexcept;
    const compaction_manager& get_compaction_manager() const noexcept;

    future<> write(const mutation&, compaction_group&, seastar::gate::holder cg_holder);

    future<std::optional<log_record>> read(const primary_index&, primary_index_key);

    future<std::optional<canonical_mutation>> read(const schema&, const primary_index&, const dht::decorated_key&);

    /// Create a mutation reader for a specific key
    mutation_reader make_reader(schema_ptr schema,
                                       const primary_index& index,
                                       reader_permit permit,
                                       const dht::partition_range& pr,
                                       const query::partition_slice& slice,
                                       tracing::trace_state_ptr trace_state = nullptr);

    void set_trigger_compaction_hook(std::function<void()> fn);
    void set_trigger_separator_flush_hook(std::function<void(size_t)> fn);
};

} // namespace logstor
} // namespace replica
