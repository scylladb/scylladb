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
#include "db/cache_tracker.hh"
#include "readers/mutation_reader.hh"
#include "replica/compaction_group.hh"
#include "types.hh"
#include "index.hh"
#include "segment_manager.hh"
#include "write_buffer.hh"
#include "cache.hh"
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

    struct stats {
        uint64_t write_failures{0};
    };

    segment_manager _segment_manager;
    buffered_writer _write_buffer;
    cache_tracker _cache_tracker;
    seastar::metrics::metric_groups _metrics;
    stats _stats;

public:

    logstor(logstor_config, ::cache_tracker& shared_cache_tracker);

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

    cache_tracker& get_cache_tracker() noexcept {
        return _cache_tracker;
    }
    const cache_tracker& get_cache_tracker() const noexcept {
        return _cache_tracker;
    }

    future<> write(const mutation&, compaction_group&, seastar::gate::holder cg_holder, db::timeout_clock::time_point timeout);

    future<std::optional<mutation>> read(const schema&, const primary_index&, const dht::decorated_key&, const query::partition_slice&);

    /// Create a mutation reader for a specific key
    mutation_reader make_reader(schema_ptr schema,
                                       const primary_index& index,
                                       reader_permit permit,
                                       const dht::partition_range& pr,
                                       const query::partition_slice& slice,
                                       tracing::trace_state_ptr trace_state = nullptr);

    void set_trigger_compaction_hook(std::function<void()> fn);
    void set_trigger_separator_flush_hook(std::function<void(segment_sequence)> fn);
};

} // namespace logstor
} // namespace replica
