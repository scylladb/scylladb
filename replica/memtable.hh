/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <fmt/core.h>
#include "replica/database_fwd.hh"
#include "dht/decorated_key.hh"
#include "dht/ring_position.hh"
#include "schema/schema_fwd.hh"
#include "encoding_stats.hh"
#include "dirty_memory_manager.hh"
#include "db/commitlog/replay_position.hh"
#include "db/commitlog/rp_set.hh"
#include "utils/extremum_tracking.hh"
#include "mutation/mutation_cleaner.hh"
#include "utils/double-decker.hh"
#include "readers/empty_v2.hh"
#include "readers/mutation_source.hh"

class frozen_mutation;
class row_cache;

namespace bi = boost::intrusive;

namespace replica {

class memtable_entry {
    dht::decorated_key _key;
    partition_entry _pe;
    struct {
        bool _head : 1;
        bool _tail : 1;
        bool _train : 1;
    } _flags{};
public:
    bool is_head() const noexcept { return _flags._head; }
    void set_head(bool v) noexcept { _flags._head = v; }
    bool is_tail() const noexcept { return _flags._tail; }
    void set_tail(bool v) noexcept { _flags._tail = v; }
    bool with_train() const noexcept { return _flags._train; }
    void set_train(bool v) noexcept { _flags._train = v; }

    friend class memtable;

    memtable_entry(schema_ptr s, dht::decorated_key key, mutation_partition p)
        : _key(std::move(key))
        , _pe(*s, std::move(p))
    { }

    memtable_entry(memtable_entry&& o) noexcept;
    // Frees elements of the entry in batches.
    // Returns stop_iteration::yes iff there are no more elements to free.
    stop_iteration clear_gently() noexcept;
    const dht::decorated_key& key() const { return _key; }
    dht::decorated_key& key() { return _key; }
    const partition_entry& partition() const { return _pe; }
    partition_entry& partition() { return _pe; }
    const schema_ptr& schema() const { return _pe.get_schema(); }
    partition_snapshot_ptr snapshot(memtable& mtbl);

    // Makes the entry conform to given schema.
    // Must be called under allocating section of the region which owns the entry.
    void upgrade_schema(logalloc::region&, const schema_ptr&, mutation_cleaner&);

    size_t external_memory_usage_without_rows() const {
        return _key.key().external_memory_usage();
    }

    size_t object_memory_size(allocation_strategy& allocator);

    size_t size_in_allocator_without_rows(allocation_strategy& allocator) {
        return object_memory_size(allocator) + external_memory_usage_without_rows();
    }

    size_t size_in_allocator(allocation_strategy& allocator) {
        auto size = size_in_allocator_without_rows(allocator);
        for (auto&& v : _pe.versions()) {
            size += v.size_in_allocator(allocator);
        }
        return size;
    }

    friend dht::ring_position_view ring_position_view_to_compare(const memtable_entry& mt) { return mt._key; }
};

}

namespace replica {

// Statistics that need to be shared across all memtables for a single table
struct memtable_table_shared_data {
    logalloc::allocating_section read_section;
    logalloc::allocating_section allocating_section;
};

class dirty_memory_manager;

struct table_stats;

// Managed by lw_shared_ptr<>.
class memtable final
    : public enable_lw_shared_from_this<memtable>
    , public boost::intrusive::list_base_hook<boost::intrusive::link_mode<boost::intrusive::auto_unlink>>
    , private dirty_memory_manager_logalloc::size_tracked_region
    , public logalloc::region_listener {
public:
    using partitions_type = double_decker<int64_t, memtable_entry,
                            dht::raw_token_less_comparator, dht::ring_position_comparator,
                            16, bplus::key_search::linear>;
private:
    dirty_memory_manager& _dirty_mgr;
    mutation_cleaner _cleaner;
    memtable_list *_memtable_list;
    schema_ptr _schema;
    memtable_table_shared_data& _table_shared_data;
    partitions_type partitions;
    size_t nr_partitions = 0;
    db::replay_position _replay_position;
    db::rp_set _rp_set;
    // mutation source to which reads fall-back after mark_flushed()
    // so that memtable contents can be moved away while there are
    // still active readers. This is needed for this mutation_source
    // to be monotonic (not loose writes). Monotonicity of each
    // mutation_source is necessary for the combined mutation source to be
    // monotonic. That combined source in this case is cache + memtable.
    mutation_source_opt _underlying;
    uint64_t _flushed_memory = 0;
    bool _merging_into_cache = false;
    bool _merged_into_cache = false;
    replica::table_stats& _table_stats;

    class memtable_encoding_stats_collector : public encoding_stats_collector {
    private:
        min_max_tracker<api::timestamp_type> min_max_timestamp;
        min_tracker<api::timestamp_type> min_live_timestamp;
        min_tracker<api::timestamp_type> min_live_row_marker_timestamp;

        void update_timestamp(api::timestamp_type ts, is_live is_live) noexcept {
            if (ts == api::missing_timestamp) {
                return;
            }
            encoding_stats_collector::update_timestamp(ts);
            min_max_timestamp.update(ts);
            if (is_live) {
                min_live_timestamp.update(ts);
            }
        }

        void update_live_row_marker_timestamp(api::timestamp_type ts) noexcept {
            min_live_row_marker_timestamp.update(ts);
        }

    public:
        memtable_encoding_stats_collector() noexcept;
        void update(atomic_cell_view cell) noexcept;

        void update(tombstone tomb) noexcept;

        void update(const ::schema& s, const row& r, column_kind kind);
        void update(const range_tombstone& rt) noexcept;
        void update(const row_marker& marker) noexcept;
        void update(const ::schema& s, const deletable_row& dr);
        void update(const ::schema& s, const mutation_partition& mp);

        api::timestamp_type get_min_timestamp() const noexcept {
            return min_max_timestamp.min();
        }

        api::timestamp_type get_max_timestamp() const noexcept {
            return min_max_timestamp.max();
        }

        api::timestamp_type get_min_live_timestamp() const noexcept {
            return min_live_timestamp.get();
        }

        api::timestamp_type get_min_live_row_marker_timestamp() const noexcept {
            return min_live_row_marker_timestamp.get();
        }
    } _stats_collector;

    void update(db::rp_handle&&);
    friend class ::row_cache;
    friend class memtable_entry;
    friend class flush_reader;
    friend class flush_memory_accounter;
    friend class partition_snapshot_read_accounter;
private:
    std::ranges::subrange<partitions_type::const_iterator> slice(const dht::partition_range& r) const;
    partition_entry& find_or_create_partition(const dht::decorated_key& key);
    partition_entry& find_or_create_partition_slow(partition_key_view key);
    void upgrade_entry(memtable_entry&);
    void add_flushed_memory(uint64_t);
    void remove_flushed_memory(uint64_t);
    void clear() noexcept;
    uint64_t dirty_size() const;
public:
    explicit memtable(schema_ptr schema, dirty_memory_manager&,
            memtable_table_shared_data& shared_data,
            replica::table_stats& table_stats, memtable_list *memtable_list = nullptr,
            seastar::scheduling_group compaction_scheduling_group = seastar::current_scheduling_group());
    // Used for testing that want to control the flush process.
    explicit memtable(schema_ptr schema);
    ~memtable();
    // Clears this memtable gradually without consuming the whole CPU.
    // Never resolves with a failed future.
    future<> clear_gently() noexcept;
    schema_ptr schema() const noexcept { return _schema; }
    void set_schema(schema_ptr) noexcept;
    future<> apply(memtable&, reader_permit);
    // Applies mutation to this memtable.
    // The mutation is upgraded to current schema.
    void apply(const mutation& m, db::rp_handle&& = {});
    // The mutation is upgraded to current schema.
    void apply(const frozen_mutation& m, const schema_ptr& m_schema, db::rp_handle&& = {});
    void evict_entry(memtable_entry& e, mutation_cleaner& cleaner) noexcept;

    static memtable& from_region(logalloc::region& r) noexcept {
        return static_cast<memtable&>(r);
    }

    const logalloc::region& region() const noexcept {
        return *this;
    }

    logalloc::region& region() noexcept {
        return *this;
    }

    encoding_stats get_encoding_stats() const noexcept {
        return _stats_collector.get();
    }

    api::timestamp_type get_min_timestamp() const noexcept {
        return _stats_collector.get_min_timestamp();
    }

    api::timestamp_type get_max_timestamp() const noexcept {
        return _stats_collector.get_max_timestamp();
    }

    api::timestamp_type get_min_live_timestamp() const noexcept {
        return _stats_collector.get_min_live_timestamp();
    }

    api::timestamp_type get_min_live_row_marker_timestamp() const noexcept {
        return _stats_collector.get_min_live_row_marker_timestamp();
    }

    mutation_cleaner& cleaner() noexcept {
        return _cleaner;
    }

    bool contains_partition(const dht::decorated_key& key) const;
public:
    memtable_list* get_memtable_list() noexcept {
        return _memtable_list;
    }

    size_t partition_count() const noexcept { return nr_partitions; }
    logalloc::occupancy_stats occupancy() const noexcept;

    // Creates a reader of data in this memtable for given partition range.
    //
    // Live readers share ownership of the memtable instance, so caller
    // doesn't need to ensure that memtable remains live.
    //
    // The 'range' parameter must be live as long as the reader is being used
    //
    // Mutations returned by the reader will all have given schema.
    mutation_reader make_flat_reader(schema_ptr s,
                                             reader_permit permit,
                                             const dht::partition_range& range,
                                             const query::partition_slice& slice,
                                             tracing::trace_state_ptr trace_state_ptr = nullptr,
                                             streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no,
                                             mutation_reader::forwarding fwd_mr = mutation_reader::forwarding::yes) {
        if (auto reader_opt = make_flat_reader_opt(s, permit, range, slice, std::move(trace_state_ptr), fwd, fwd_mr)) {
            return std::move(*reader_opt);
        }
        [[unlikely]] return make_empty_flat_reader_v2(std::move(s), std::move(permit));
    }
    // Same as make_flat_reader, but returns an empty optional instead of a no-op reader when there is nothing to
    // read. This is an optimization.
    mutation_reader_opt make_flat_reader_opt(schema_ptr query_schema,
                                          reader_permit permit,
                                          const dht::partition_range& range,
                                          const query::partition_slice& slice,
                                          tracing::trace_state_ptr trace_state_ptr = nullptr,
                                          streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no,
                                          mutation_reader::forwarding fwd_mr = mutation_reader::forwarding::yes);

    mutation_reader make_flat_reader(schema_ptr s,
                                             reader_permit permit,
                                             const dht::partition_range& range = query::full_partition_range) {
        auto& full_slice = s->full_slice();
        return make_flat_reader(s, std::move(permit), range, full_slice);
    }

    mutation_reader make_flush_reader(schema_ptr, reader_permit permit);

    mutation_source as_data_source();

    bool empty() const noexcept { return partitions.empty(); }
    void mark_flushed(mutation_source) noexcept;
    bool is_merging_to_cache() const noexcept;
    bool is_flushed() const noexcept;
    void on_detach_from_region_group() noexcept;
    void revert_flushed_memory() noexcept;

    const db::replay_position& replay_position() const noexcept {
        return _replay_position;
    }
    /**
     * Returns the current rp_set, and resets the
     * stored one to empty. Only used for flushing
     * purposes, to one-shot report discarded rp:s
     * to commitlog
     */
    db::rp_set get_and_discard_rp_set() noexcept {
        return std::exchange(_rp_set, {});
    }
    friend class iterator_reader;

    dirty_memory_manager& get_dirty_memory_manager() noexcept {
        return _dirty_mgr;
    }

    // Implementation of region_listener.
    virtual void increase_usage(logalloc::region* r, ssize_t delta) override;
    virtual void decrease_evictable_usage(logalloc::region* r) override;
    virtual void decrease_usage(logalloc::region* r, ssize_t delta) override;
    virtual void add(logalloc::region* r) override;
    virtual void del(logalloc::region* r) override;
    virtual void moved(logalloc::region* old_address, logalloc::region* new_address) override;

    friend fmt::formatter<memtable>;
};

}

template <> struct fmt::formatter<replica::memtable_entry> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const replica::memtable_entry&, fmt::format_context& ctx) const -> decltype(ctx.out());
};

template <> struct fmt::formatter<replica::memtable> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(replica::memtable&, fmt::format_context& ctx) const -> decltype(ctx.out());
};
