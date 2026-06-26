/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <concepts>
#include <cstdint>
#include <optional>
#include <type_traits>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/future.hh>
#include <boost/intrusive/set.hpp>
#include "bytes.hh"
#include "utils/managed_bytes.hh"
#include "utils/fragment_range.hh"
#include "schema/schema_fwd.hh"
#include "system_keyspace.hh"
#include "sstables/shared_sstable.hh"
#include "sstables/types.hh"
#include "utils/assert.hh"
#include "utils/hash.hh"
#include "utils/updateable_value.hh"
#include "utils/pluggable.hh"
#include "db/large_data_cache_tracker.hh"

namespace sstables {
class sstable;
class key;
}

class partition_key_view;
class mutation_partition;

namespace db {

class system_keyspace;

using sstables::large_data_record;

struct lookup_key {
    bytes_view pk;
    managed_bytes_view ck;
    bytes_view column_name;
};

// Compile-time comparison depth, one per multiset.
//   partition → pk only
//   row       → pk + ck
//   cell      → pk + ck + column_name
enum class record_type { partition, row, collection };

template<record_type Depth>
struct record_compare {
private:
    static bytes_view get_pk(const large_data_record& r) noexcept { return bytes_view(r.partition_key.value); }
    static bytes_view get_pk(const lookup_key& k) noexcept { return k.pk; }
    static single_fragmented_view get_ck(const large_data_record& r) noexcept { return single_fragmented_view(bytes_view(r.clustering_key.value)); }
    static managed_bytes_view get_ck(const lookup_key& k) noexcept { return k.ck; }
    static bytes_view get_col(const large_data_record& r) noexcept { return bytes_view(r.column_name.value); }
    static bytes_view get_col(const lookup_key& k) noexcept { return k.column_name; }
public:
    template<typename L, typename R>
        requires (std::same_as<L, large_data_record> || std::same_as<R, large_data_record>)
    bool operator()(const L& a, const R& b) const noexcept {
        auto l_pk = get_pk(a), r_pk = get_pk(b);
        if (l_pk != r_pk) {
            return l_pk < r_pk;
        }
        if constexpr (Depth == record_type::partition) {
            return false;
        }
        if (auto c = compare_unsigned(get_ck(a), get_ck(b)); c != 0) {
            return c < 0;
        }
        if constexpr (Depth == record_type::row) {
            return false;
        }
        return get_col(a) < get_col(b);
    }
};

template<record_type Depth>
using record_set = boost::intrusive::multiset<large_data_record,
    boost::intrusive::member_hook<large_data_record,
        large_data_record::index_hook_type, &large_data_record::_index_hook>,
    boost::intrusive::compare<record_compare<Depth>>,
    boost::intrusive::constant_time_size<false>>;

// Per-table index over large_data_records from all live SSTables.
// Links directly into records stored in each SSTable's scylla_metadata
// via intrusive member hooks (auto_unlink).  Aggregation (max across
// SSTables for the same key) happens at lookup time via equal_range.
class large_data_record_index {
public:
    struct partition_entry {
        uint64_t partition_size = 0;
        uint64_t rows = 0;
    };

    void register_sstable(sstables::shared_sstable sst);

    void rebuild(const std::unordered_set<sstables::shared_sstable>& sstables);

    std::optional<partition_entry> lookup_partition(bytes_view pk_bytes) const;
    std::optional<uint64_t> lookup_row(bytes_view pk_bytes, managed_bytes_view ck_bytes) const;
    std::optional<uint64_t> lookup_collection(bytes_view pk_bytes,
            managed_bytes_view ck_bytes, bytes_view column_name) const;

private:
    record_set<record_type::partition> _partitions;
    record_set<record_type::row> _rows;
    record_set<record_type::collection> _collections;
};

// Soft limit violation categories.  Used as a set of bit flags so a single
// write can accumulate violations across all of its partitions, rows, cells,
// and collections (see the operators below).
enum class large_data_violation_type : uint8_t {
    none       = 0,
    row        = 1 << 0,
    cell       = 1 << 1,
    collection = 1 << 2,
    partition  = 1 << 3,
};

inline constexpr large_data_violation_type operator|(large_data_violation_type a, large_data_violation_type b) {
    return large_data_violation_type(std::underlying_type_t<large_data_violation_type>(a) | std::underlying_type_t<large_data_violation_type>(b));
}

inline constexpr void operator|=(large_data_violation_type& a, large_data_violation_type b) {
    a = a | b;
}

inline constexpr large_data_violation_type operator&(large_data_violation_type a, large_data_violation_type b) {
    return large_data_violation_type(std::underlying_type_t<large_data_violation_type>(a) & std::underlying_type_t<large_data_violation_type>(b));
}

// Translate a set of soft-limit violation categories into the client-facing CQL
// warning string, or an empty string when no categories are set.  The message
// lists the violated categories in partition, row, cell, collection order, e.g.
//   "Large data guardrail: Soft limit violation for partition, cell"
sstring large_data_soft_violation_warning(large_data_violation_type violations);

// Each replica::table holds a shared_ptr to either a real guardrail or a
// noop.  The guardrail owns the per-table large_data_record_index, so
// noop tables pay no index-maintenance cost.
//
// The guardrail provides two check methods:
//   check()              — replica-side, uses SSTable metadata index
//   check_coordinator()  — coordinator-side, checks mutation content directly
class large_data_guardrail_base {
public:
    virtual ~large_data_guardrail_base() = default;
    // Replica-side check: looks up partition/row/collection in the SSTable
    // metadata index and rejects or warns based on recorded on-disk sizes.
    virtual void check(const schema& s, const mutation_partition& mp,
            partition_key_view pk, db::large_data_violation_type* violations_out) const = 0;
    // Coordinator-side check: inspects the mutation content directly
    // (cell value sizes, row memory usage, collection element counts).
    // Hard limit violations throw; soft limit violations are logged.
    virtual void check_coordinator(const schema& s, const mutation_partition& mp,
            partition_key_view pk, db::large_data_violation_type* violations_out) const = 0;
    // Returns a tracker to pass to partition_entry::apply(), or nullptr
    // when no memtable-level tracking is needed.  The returned pointer
    // remains valid until the next call to get_memtable_cache_tracker() or
    // until this guardrail is destroyed.
    virtual large_data_cache_tracker* get_memtable_cache_tracker(const schema& s,
            partition_key_view pk) = 0;
    // Called after a memtable flush completes and new SSTables are registered.
    // Clears memtable-level caches whose data is now captured in the SSTable
    // index, preventing unbounded cache growth.
    virtual void on_flush() = 0;
    virtual void register_sstable(sstables::shared_sstable sst) = 0;
    virtual void rebuild(const std::unordered_set<sstables::shared_sstable>& sstables) = 0;
};

class noop_large_data_guardrail final : public large_data_guardrail_base {
public:
    static shared_ptr<large_data_guardrail_base> instance() {
        static thread_local auto inst = make_shared<noop_large_data_guardrail>();
        return inst;
    }
    void check(const schema&, const mutation_partition&,
            partition_key_view, db::large_data_violation_type*) const override {}
    void check_coordinator(const schema&, const mutation_partition&,
            partition_key_view, db::large_data_violation_type*) const override {}
    large_data_cache_tracker* get_memtable_cache_tracker(const schema&,
            partition_key_view) override { return nullptr; }
    void on_flush() override {}
    void register_sstable(sstables::shared_sstable) override {}
    void rebuild(const std::unordered_set<sstables::shared_sstable>&) override {}
};

class large_data_guardrail final : public large_data_guardrail_base {
public:
    explicit large_data_guardrail(guardrail_config cfg) noexcept
        : _cfg(std::move(cfg))
        , _tracker(_cfg, _memtable_row_cache, _memtable_collection_cache) {}

    // The memtable caches own managed_bytes keys allocated on the standard
    // heap (see with_standard_allocator() in the .cc).  Empty them under the
    // standard allocator before the maps themselves are destroyed.
    virtual ~large_data_guardrail() override;

    void check(const schema& s, const mutation_partition& mp,
            partition_key_view pk, db::large_data_violation_type* violations_out) const override;
    void check_coordinator(const schema& s, const mutation_partition& mp,
            partition_key_view pk, db::large_data_violation_type* violations_out) const override;
    large_data_cache_tracker* get_memtable_cache_tracker(const schema& s,
            partition_key_view pk) override;
    void on_flush() override;
    void register_sstable(sstables::shared_sstable sst) override;
    void rebuild(const std::unordered_set<sstables::shared_sstable>& sstables) override;

private:
    large_data_violation_type check_partition(const schema& s, bytes_view pk_bytes, partition_key_view pk) const;
    large_data_violation_type check_rows_and_collections(const schema& s, bytes_view pk_bytes, const mutation_partition& mp, partition_key_view pk) const;
    large_data_violation_type check_row_size(const schema& s, bytes_view pk_bytes, partition_key_view pk,
            bytes_view ck_bytes, const clustering_key_prefix* ck) const;
    large_data_violation_type check_collection_element_count(const schema& s, bytes_view pk_bytes, partition_key_view pk,
            const column_definition& cdef, bytes_view ck_bytes,
            const clustering_key_prefix* ck) const;
    // Empties the memtable caches, destroying their owning managed_bytes keys
    // under the standard allocator (see large_data_cache_tracker.hh).
    void clear_memtable_caches() noexcept;

    guardrail_config _cfg;
    large_data_record_index _index;
    mutable memtable_row_cache _memtable_row_cache;
    mutable memtable_collection_cache _memtable_collection_cache;
    large_data_cache_tracker _tracker;
};

class large_data_handler {
public:
    struct stats {
        int64_t partitions_bigger_than_threshold = 0; // number of large partition updates exceeding threshold_bytes
        int64_t rows_bigger_than_threshold = 0; // number of large row updates exceeding row_threshold_bytes
        int64_t cells_bigger_than_threshold = 0; // number of large cell updates exceeding cell_threshold_bytes
        int64_t collections_bigger_than_threshold = 0; // number of large collection updates exceeding collection_elements_count_threshold
    };

private:
    // Assuming:
    // * there is at most one log entry every 1MB
    // * the average latency of the log is 4ms (depends on the load)
    // * we aim to sustain 1GB/s of write bandwidth
    // We need a concurrency of:
    //  C = (1GB/s / 1MB) * 4ms = 1k/s * 4ms = 4
    // 16 should be enough for everybody.
    static constexpr size_t max_concurrency = 16;
    semaphore _sem{max_concurrency};

    // A convenience function for using the above semaphore. Unlike the global with_semaphore, this will not wait on the
    // future returned by func. The objective is for the future returned by func to run in parallel with whatever the
    // caller is doing, but limit how far behind we can get.
    template<typename Func>
    future<> with_sem(Func&& func) {
        return get_units(_sem, 1).then([func = std::forward<Func>(func)] (auto units) mutable {
            // Future is discarded purposefully, see method description.
            // FIXME: error handling.
            (void)func().finally([units = std::move(units)] {});
        });
    }

    bool _running = false;

protected:
    uint64_t _partition_threshold_bytes;
    uint64_t _row_threshold_bytes;
    uint64_t _cell_threshold_bytes;
    uint64_t _rows_count_threshold;
    uint64_t _collection_elements_count_threshold;

private:
    mutable large_data_handler::stats _stats;

protected:
    mutable utils::pluggable<db::system_keyspace> _sys_ks;

public:
    explicit large_data_handler(uint64_t partition_threshold_bytes, uint64_t row_threshold_bytes, uint64_t cell_threshold_bytes, uint64_t rows_count_threshold, uint64_t collection_elements_count_threshold);
    virtual ~large_data_handler() {}

    // Once large_data_handler is stopped no further updates will be accepted.
    bool running() const { return _running; }
    void start();
    future<> stop();

    future<bool> maybe_record_large_rows(const sstables::sstable& sst, const sstables::key& partition_key,
            const clustering_key_prefix* clustering_key, uint64_t row_size) {
        SCYLLA_ASSERT(running());
        if (row_size > _row_threshold_bytes) [[unlikely]] {
            ++_stats.rows_bigger_than_threshold;
            return with_sem([&sst, &partition_key, clustering_key, row_size, this] {
                return record_large_rows(sst, partition_key, clustering_key, row_size);
            }).then([] {
                return true;
            });
        }
        return make_ready_future<bool>(false);
    }

    struct above_threshold_result {
        bool size = false;
        bool elements = false;
    };
    future<above_threshold_result> maybe_record_large_partitions(const sstables::sstable& sst, const sstables::key& partition_key,
            uint64_t partition_size, uint64_t rows, uint64_t range_tombstones, uint64_t dead_rows);

    future<above_threshold_result> maybe_record_large_cells(const sstables::sstable& sst, const sstables::key& partition_key,
            const clustering_key_prefix* clustering_key, const column_definition& cdef, uint64_t cell_size, uint64_t collection_elements) {
        SCYLLA_ASSERT(running());
        above_threshold_result above_threshold{.size = cell_size > _cell_threshold_bytes, .elements = collection_elements > _collection_elements_count_threshold};
        _stats.cells_bigger_than_threshold += above_threshold.size;
        _stats.collections_bigger_than_threshold += above_threshold.elements;
        if (above_threshold.size || above_threshold.elements) [[unlikely]] {
            return with_sem([&sst, &partition_key, clustering_key, &cdef, cell_size, collection_elements, this] {
                return record_large_cells(sst, partition_key, clustering_key, cdef, cell_size, collection_elements);
            }).then([above_threshold] {
                return above_threshold;
            });
        }
        return make_ready_future<above_threshold_result>();
    }

    future<> maybe_delete_large_data_entries(sstables::shared_sstable sst);
    future<> maybe_update_large_data_entries_sstable_name(sstables::shared_sstable sst, sstring new_name);

    const large_data_handler::stats& stats() const { return _stats; }

    uint64_t get_partition_threshold_bytes() const noexcept {
        return _partition_threshold_bytes;
    }
    uint64_t get_row_threshold_bytes() const noexcept {
        return _row_threshold_bytes;
    }
    uint64_t get_cell_threshold_bytes() const noexcept {
        return _cell_threshold_bytes;
    }
    uint64_t get_rows_count_threshold() const noexcept {
        return _rows_count_threshold;
    }
    uint64_t get_collection_elements_count_threshold() const noexcept {
        return _collection_elements_count_threshold;
    }

    static sstring sst_filename(const sstables::sstable& sst);

    void plug_system_keyspace(db::system_keyspace& sys_ks) noexcept;
    future<> unplug_system_keyspace() noexcept;

protected:
    virtual future<> record_large_cells(const sstables::sstable& sst, const sstables::key& partition_key,
            const clustering_key_prefix* clustering_key, const column_definition& cdef, uint64_t cell_size, uint64_t collection_elements) const = 0;
    virtual future<> record_large_rows(const sstables::sstable& sst, const sstables::key& partition_key, const clustering_key_prefix* clustering_key, uint64_t row_size) const = 0;
    virtual future<> delete_large_data_entries(const schema& s, sstring sstable_name, std::string_view large_table_name) const = 0;
    virtual future<> update_large_data_entries_sstable_name(const schema& s, sstring old_name, sstring new_name, std::string_view large_table_name) const = 0;
    virtual future<> record_large_partitions(const sstables::sstable& sst, const sstables::key& partition_key, uint64_t partition_size, uint64_t rows, uint64_t range_tombstones, uint64_t dead_rows) const = 0;
};

class cql_table_large_data_handler : public large_data_handler {
    gms::feature_service& _feat;
    std::function<future<> (const sstables::sstable& sst, const sstables::key& partition_key,
            const clustering_key_prefix* clustering_key, const column_definition& cdef, uint64_t cell_size, uint64_t collection_elements)> _record_large_cells;
    std::function<future<> (const sstables::sstable& sst, const sstables::key& partition_key,
            uint64_t partition_size, uint64_t rows, uint64_t range_tombstones, uint64_t dead_rows)> _record_large_partitions;
    std::optional<std::any> _large_collection_detection_listener;
    std::optional<std::any> _range_tombstone_and_dead_rows_detection_listener;

    static constexpr uint64_t MB = 1024 * 1024;

    using threshold_updater = utils::transforming_value_updater<uint64_t, uint32_t>;
    threshold_updater _partition_threshold_mb_updater;
    threshold_updater _row_threshold_mb_updater;
    threshold_updater _cell_threshold_mb_updater;
    threshold_updater _rows_count_threshold_updater;
    threshold_updater _collection_elements_count_threshold_updater;
public:
    explicit cql_table_large_data_handler(gms::feature_service& feat,
            utils::updateable_value<uint32_t> partition_threshold_mb,
            utils::updateable_value<uint32_t> row_threshold_mb,
            utils::updateable_value<uint32_t> cell_threshold_mb,
            utils::updateable_value<uint32_t> rows_count_threshold,
            utils::updateable_value<uint32_t> collection_elements_count_threshold);

protected:
    virtual future<> record_large_partitions(const sstables::sstable& sst, const sstables::key& partition_key, uint64_t partition_size, uint64_t rows, uint64_t range_tombstones, uint64_t dead_rows) const override;
    virtual future<> delete_large_data_entries(const schema& s, sstring sstable_name, std::string_view large_table_name) const override;
    virtual future<> update_large_data_entries_sstable_name(const schema& s, sstring old_name, sstring new_name, std::string_view large_table_name) const override;
    virtual future<> record_large_cells(const sstables::sstable& sst, const sstables::key& partition_key,
            const clustering_key_prefix* clustering_key, const column_definition& cdef, uint64_t cell_size, uint64_t collection_elements) const override;
    virtual future<> record_large_rows(const sstables::sstable& sst, const sstables::key& partition_key, const clustering_key_prefix* clustering_key, uint64_t row_size) const override;

private:
    // Returns true if CQL writes to system.large_* tables should be skipped.
    // Once LARGE_DATA_VIRTUAL_TABLES is enabled, large data records are served
    // from SSTable metadata via virtual tables and the physical CQL tables are
    // dropped, so writing to them is both unnecessary and would fail.
    bool skip_cql_writes() const;

    future<> internal_record_large_cells(const sstables::sstable& sst, const sstables::key& partition_key,
            const clustering_key_prefix* clustering_key, const column_definition& cdef, uint64_t cell_size, uint64_t collection_elements) const;
    future<> internal_record_large_cells_and_collections(const sstables::sstable& sst, const sstables::key& partition_key,
            const clustering_key_prefix* clustering_key, const column_definition& cdef, uint64_t cell_size, uint64_t collection_elements) const;
    future<> internal_record_large_partitions(const sstables::sstable& sst, const sstables::key& partition_key, uint64_t partition_size, uint64_t rows) const;
    future<> internal_record_large_partitions_all_data(const sstables::sstable& sst, const sstables::key& partition_key, uint64_t partition_size, uint64_t rows,
            uint64_t dead_rows, uint64_t range_tombstones) const;

private:
    using row_reinsert_func = std::function<future<>(const cql3::untyped_result_set_row&)>;
    row_reinsert_func make_row_reinsert_func(std::string_view large_table_name, const sstring& ks_name, const sstring& cf_name, const sstring& new_name) const;

    template <typename... Args>
    future<> try_record(std::string_view large_table, const sstables::sstable& sst,  const sstables::key& partition_key, int64_t size,
            std::string_view size_desc, std::string_view desc, std::string_view extra_path, const std::vector<sstring> &extra_fields, Args&&... args) const;

    // Core INSERT helper used by both try_record (for new entries) and
    // update_large_data_entries_sstable_name (for re-inserting with a new sstable name).
    template <typename... Args>
    future<> do_insert_large_data_entry(std::string_view large_table,
            sstring ks_name, sstring cf_name, sstring sstable_name,
            int64_t size, sstring partition_key, db_clock::time_point compaction_time,
            const std::vector<sstring>& extra_fields, Args&&... args) const;
};

class nop_large_data_handler : public large_data_handler {
public:
    nop_large_data_handler();
    virtual future<> record_large_partitions(const sstables::sstable& sst, const sstables::key& partition_key, uint64_t partition_size, uint64_t rows, uint64_t range_tombstones, uint64_t dead_rows) const override {
        return make_ready_future<>();
    }

    virtual future<> delete_large_data_entries(const schema& s, sstring sstable_name, std::string_view large_table_name) const override {
        return make_ready_future<>();
    }

    virtual future<> update_large_data_entries_sstable_name(const schema& s, sstring old_name, sstring new_name, std::string_view large_table_name) const override {
        return make_ready_future<>();
    }

    virtual future<> record_large_cells(const sstables::sstable& sst, const sstables::key& partition_key,
        const clustering_key_prefix* clustering_key, const column_definition& cdef, uint64_t cell_size, uint64_t collection_elements) const override {
        return make_ready_future<>();
    }

    virtual future<> record_large_rows(const sstables::sstable& sst, const sstables::key& partition_key,
            const clustering_key_prefix* clustering_key, uint64_t row_size) const override {
        return make_ready_future<>();
    }
};

}
