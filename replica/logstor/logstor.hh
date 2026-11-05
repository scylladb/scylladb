/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/temporary_buffer.hh>
#include <map>
#include <seastar/core/scheduling.hh>
#include "db/cache_tracker.hh"
#include "readers/mutation_reader.hh"
#include "readers/mutation_reader_fwd.hh"
#include "replica/logstor/compaction.hh"
#include "readers/mutation_source.hh"
#include "types.hh"
#include "index.hh"
#include "segment_manager.hh"
#include "write_buffer.hh"
#include "cache.hh"
#include "mutation/mutation.hh"
#include "dht/decorated_key.hh"

class frozen_mutation;

namespace replica {

namespace logstor {

extern seastar::logger logstor_logger;

struct logstor_config {
    segment_manager_config segment_manager_cfg;
    seastar::scheduling_group flush_sg;
    size_t max_queued_write_bytes{0};
    size_t write_buffer_ring_size{5};
};

// Thrown when the record found at the indexed location holds a different key than the one that was looked up.
// This can happen if the hashes of the keys collide, or on corruption.
class key_mismatch_error : public std::runtime_error {
public:
    struct cache_location {};
    using location_variant = std::variant<record_location, cache_location>;

    key_mismatch_error(const partition_key& expected, const partition_key& actual, const location_variant& location);
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
    seastar::gate _async_gate;

public:

    logstor(logstor_config, ::cache_tracker& shared_cache_tracker);

    logstor(const logstor&) = delete;
    logstor& operator=(const logstor&) = delete;

    // Call it once, before start(). The catalog must live until start() returns.
    future<recovery_summary> do_recovery(recovery_catalog&);

    // Starts the store. Call it once, after do_recovery() succeeded.
    future<> start();
    future<> stop();

    size_t get_memory_usage() const;

    segment_manager& get_segment_manager() noexcept;
    const segment_manager& get_segment_manager() const noexcept;

    compaction_manager& get_compaction_manager() noexcept;
    const compaction_manager& get_compaction_manager() const noexcept;

    // Makes the primary index of a table.
    // `with_cache_tracker` is true when the node has the cache. It does not change for the life of
    // the index.
    // `cache_enabled` is the caching option of the table. The table can change it later with
    // primary_index::set_cache_enabled().
    std::unique_ptr<primary_index> make_primary_index(bool with_cache_tracker, bool cache_enabled);

    future<> flush_to_separator();

    future<> write(const mutation&, write_target target, db::timeout_clock::time_point timeout) noexcept;

    // Writes a frozen mutation without unfreezing it. s must be the schema version that the
    // mutation was frozen with.
    future<> write(const frozen_mutation&, const schema& s, write_target target, db::timeout_clock::time_point timeout) noexcept;

    // Creates a mutation reader for a partition range.
    // The reader fails with key_mismatch_error if the record or the cached partition at the indexed
    // location holds a different key.
    mutation_reader make_reader(schema_ptr schema,
                                const primary_index& index,
                                reader_permit permit,
                                const dht::partition_range& pr,
                                const query::partition_slice& slice,
                                tracing::trace_state_ptr trace_state = nullptr,
                                streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no,
                                mutation_reader::forwarding fwd_mr = mutation_reader::forwarding::no);

    // Debug introspection for SELECT * FROM MUTATION_FRAGMENTS(): returns one
    // mutation source per place the key's data currently lives in - the
    // in-memory logstor cache (if cached) and the log record - keyed by a name
    // identifying the source. Returns an empty map when the key is not in the
    // index. Neither source reads, populates or otherwise perturbs the cache.
    // The cached mutation is snapshotted here and charged to the given permit,
    // since the cache entry may be evicted before the source is read from.
    std::map<sstring, mutation_source> make_mutation_sources_for_dump(schema_ptr, const primary_index&, const dht::decorated_key&, reader_permit);

private:
    future<> do_write(const schema&, dht::decorated_key, api::timestamp_type, record_value,
            write_target target, db::timeout_clock::time_point timeout);
};

} // namespace logstor
} // namespace replica
