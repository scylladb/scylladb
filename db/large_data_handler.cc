/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "utils/assert.hh"
#include <seastar/core/format.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/when_all.hh>
#include "utils/allocation_strategy.hh"
#include "db/system_keyspace.hh"
#include "gms/gossiper.hh"
#include "db/large_data_handler.hh"
#include "keys/keys.hh"
#include "mutation/mutation_partition.hh"
#include "mutation/atomic_cell.hh"
#include "mutation/mutation_partition_v2.hh"
#include "mutation/atomic_cell_or_collection.hh"
#include "mutation/collection_mutation.hh"
#include "replica/exceptions.hh"
#include "exceptions/exceptions.hh"
#include "sstables/sstables.hh"
#include "gms/feature_service.hh"
#include "cql3/untyped_result_set.hh"

static logging::logger large_data_logger("large_data");

namespace db {

template <typename Func>
static decltype(auto) with_standard_allocator(Func&& f) {
    return with_allocator(standard_allocator(), std::forward<Func>(f));
}

namespace {
constexpr uint64_t MB = 1024 * 1024;

enum class guardrail_source { replica, coordinator };

template <guardrail_source Source, typename ContextFn>
bool enforce_threshold(const schema& s, uint64_t value,
        uint64_t fail_threshold, uint64_t warn_threshold,
        std::string_view metric, ContextFn&& make_context) {
    if (fail_threshold > 0 && value > fail_threshold) [[unlikely]] {
        auto msg = seastar::format(
            "Large data guardrail: {} {} exceeds hard limit {} "
            "(keyspace={}, table={}, {})",
            metric, value, fail_threshold, s.ks_name(), s.cf_name(), make_context());
        if constexpr (Source == guardrail_source::replica) {
            throw replica::large_data_exception(s.ks_name(), s.cf_name(), std::move(msg));
        } else {
            throw exceptions::invalid_request_exception(std::move(msg));
        }
    }
    if (warn_threshold > 0 && value > warn_threshold) [[unlikely]] {
        large_data_logger.warn("Large data guardrail: {} {} exceeds soft limit {} "
            "(keyspace={}, table={}, {})",
            metric, value, warn_threshold, s.ks_name(), s.cf_name(), make_context());
        return true;
    }
    return false;
}
} // anonymous namespace

sstring large_data_soft_violation_warning(large_data_violation_type violations) {
    if (violations == large_data_violation_type::none) {
        return {};
    }
    // List categories in a stable partition, row, cell, collection order.
    static constexpr std::pair<large_data_violation_type, std::string_view> categories[] = {
        {large_data_violation_type::partition, "partition"},
        {large_data_violation_type::row, "row"},
        {large_data_violation_type::cell, "cell"},
        {large_data_violation_type::collection, "collection"},
    };
    std::vector<std::string_view> listed;
    for (const auto& [category, name] : categories) {
        if ((violations & category) != large_data_violation_type::none) {
            listed.push_back(name);
        }
    }
    return seastar::format("Large data guardrail: Soft limit violation for {}", fmt::join(listed, ", "));
}

nop_large_data_handler::nop_large_data_handler()
    : large_data_handler(std::numeric_limits<uint64_t>::max(), std::numeric_limits<uint64_t>::max(),
          std::numeric_limits<uint64_t>::max(), std::numeric_limits<uint64_t>::max(), std::numeric_limits<uint64_t>::max()) {
    // Don't require start() to be called on nop large_data_handler.
    start();
}

large_data_handler::large_data_handler(uint64_t partition_threshold_bytes, uint64_t row_threshold_bytes, uint64_t cell_threshold_bytes, uint64_t rows_count_threshold, uint64_t collection_elements_count_threshold)
        : _partition_threshold_bytes(partition_threshold_bytes)
        , _row_threshold_bytes(row_threshold_bytes)
        , _cell_threshold_bytes(cell_threshold_bytes)
        , _rows_count_threshold(rows_count_threshold)
        , _collection_elements_count_threshold(collection_elements_count_threshold)
        , _sys_ks("large_data_handler::system_keyspace")
{
    large_data_logger.debug("partition_threshold_bytes={} row_threshold_bytes={} cell_threshold_bytes={} rows_count_threshold={} collection_elements_count_threshold={}",
        partition_threshold_bytes, row_threshold_bytes, cell_threshold_bytes, rows_count_threshold, _collection_elements_count_threshold);
}

future<large_data_handler::above_threshold_result> large_data_handler::maybe_record_large_partitions(const sstables::sstable& sst, const sstables::key& key, uint64_t partition_size, uint64_t rows, uint64_t range_tombstones, uint64_t dead_rows) {
    SCYLLA_ASSERT(running());
    above_threshold_result above_threshold{.size = partition_size > _partition_threshold_bytes, .elements = rows > _rows_count_threshold};
    static_assert(std::is_same_v<decltype(above_threshold.size), bool>);
    _stats.partitions_bigger_than_threshold += above_threshold.size; // increment if true
    if (above_threshold.size || above_threshold.elements) [[unlikely]] {
        return with_sem([&sst, &key, partition_size, rows, range_tombstones, dead_rows, this] {
            return record_large_partitions(sst, key, partition_size, rows, range_tombstones, dead_rows);
        }).then([above_threshold] {
            return above_threshold;
        });
    }
    return make_ready_future<above_threshold_result>();
}

void large_data_handler::start() {
    _running = true;
}

future<> large_data_handler::stop() {
    if (running()) {
        _running = false;
        large_data_logger.info("Waiting for {} background handlers", max_concurrency - _sem.available_units());
        co_await _sem.wait(max_concurrency);
        co_await _sys_ks.close();
    }
}

void large_data_handler::plug_system_keyspace(db::system_keyspace& sys_ks) noexcept {
    _sys_ks.plug(sys_ks.shared_from_this());
}

future<> large_data_handler::unplug_system_keyspace() noexcept {
    co_await _sys_ks.unplug();
}

void large_data_record_index::register_sstable(sstables::shared_sstable sst) {
    auto& records_opt = sst->get_large_data_records();
    if (!records_opt) {
        return;
    }
    for (auto& rec : records_opt->elements) {
        switch (rec.type) {
        case sstables::large_data_type::partition_size:
        case sstables::large_data_type::rows_in_partition:
            _partitions.insert(rec);
            break;
        case sstables::large_data_type::row_size:
            _rows.insert(rec);
            break;
        case sstables::large_data_type::elements_in_collection:
            _collections.insert(rec);
            break;
        default:
            break;
        }
    }
}

void large_data_record_index::rebuild(
        const std::unordered_set<sstables::shared_sstable>& sstables) {
    _partitions.clear();
    _rows.clear();
    _collections.clear();
    for (const auto& sst : sstables) {
        register_sstable(sst);
    }
}

std::optional<large_data_record_index::partition_entry>
large_data_record_index::lookup_partition(bytes_view pk_bytes) const {
    lookup_key lk{pk_bytes, {}, {}};
    auto [begin, end] = _partitions.equal_range(lk, _partitions.key_comp());
    if (begin == end) {
        return std::nullopt;
    }
    partition_entry result;
    for (auto it = begin; it != end; ++it) {
        result.partition_size = std::max(result.partition_size, it->value);
        result.rows = std::max(result.rows, it->elements_count);
    }
    return result;
}

std::optional<uint64_t> large_data_record_index::lookup_row(bytes_view pk_bytes,
        managed_bytes_view ck_bytes) const {
    lookup_key lk{pk_bytes, ck_bytes, {}};
    auto [begin, end] = _rows.equal_range(lk, _rows.key_comp());
    if (begin == end) {
        return std::nullopt;
    }
    uint64_t result = 0;
    for (auto it = begin; it != end; ++it) {
        result = std::max(result, it->value);
    }
    return result;
}

std::optional<uint64_t> large_data_record_index::lookup_collection(bytes_view pk_bytes,
        managed_bytes_view ck_bytes, bytes_view column_name) const {
    lookup_key lk{pk_bytes, ck_bytes, column_name};
    auto [begin, end] = _collections.equal_range(lk, _collections.key_comp());
    if (begin == end) {
        return std::nullopt;
    }
    uint64_t result = 0;
    for (auto it = begin; it != end; ++it) {
        result = std::max(result, it->elements_count);
    }
    return result;
}

void large_data_guardrail::register_sstable(sstables::shared_sstable sst) {
    _index.register_sstable(std::move(sst));
}

void large_data_guardrail::clear_memtable_caches() noexcept {
    // Clearing destroys the owned managed_bytes keys; do it under the standard
    // allocator they were allocated from (see with_standard_allocator()).
    with_standard_allocator([&] {
        _memtable_row_cache.clear();
        _memtable_collection_cache.clear();
    });
}

void large_data_guardrail::on_flush() {
    clear_memtable_caches();
}

void large_data_guardrail::rebuild(const std::unordered_set<sstables::shared_sstable>& sstables) {
    _index.rebuild(sstables);
    clear_memtable_caches();
}

large_data_guardrail::~large_data_guardrail() {
    // Empty the caches before the maps themselves are destroyed, so the owned
    // managed_bytes keys are freed from the heap they were allocated on and the
    // maps are then destroyed empty (and so allocate/free nothing).
    clear_memtable_caches();
}

void large_data_guardrail::check(const schema& s, const mutation_partition& mp,
                                 partition_key_view pk, db::large_data_violation_type* violations_out) const {
    auto sst_key = sstables::key::from_partition_key(s, pk);
    auto pk_bytes = bytes_view(sst_key.get_bytes());
    auto violations = check_partition(s, pk_bytes, pk);
    violations |= check_rows_and_collections(s, pk_bytes, mp, pk);
    if (violations_out) {
        *violations_out = violations;
    }
}

large_data_violation_type large_data_guardrail::check_partition(const schema& s, bytes_view pk_bytes, partition_key_view pk) const {
    auto entry = _index.lookup_partition(pk_bytes);
    if (!entry) [[likely]] {
        return large_data_violation_type::none;
    }
    const bool track = _cfg.cql_warnings_enabled();
    large_data_violation_type violations = large_data_violation_type::none;
    auto context = [&] { return seastar::format("partition_key={}", pk); };
    if (enforce_threshold<guardrail_source::replica>(s, entry->partition_size,
        uint64_t(_cfg.partition_size_fail_threshold_mb()) * MB,
        uint64_t(_cfg.partition_size_warn_threshold_mb()) * MB,
        "partition size", context) && track) {
        violations |= large_data_violation_type::partition;
    }
    if (enforce_threshold<guardrail_source::replica>(s, entry->rows,
        uint64_t(_cfg.rows_count_fail_threshold()),
        uint64_t(_cfg.rows_count_warn_threshold()),
        "partition row count", context) && track) {
        violations |= large_data_violation_type::partition;
    }
    return violations;
}

large_data_violation_type large_data_guardrail::check_rows_and_collections(const schema& s, bytes_view pk_bytes,
        const mutation_partition& mp, partition_key_view pk) const {
    large_data_violation_type violations = large_data_violation_type::none;

    if (!mp.static_row().empty()) {
        violations |= check_row_size(s, pk_bytes, pk, bytes_view(), nullptr);
        mp.static_row().for_each_cell([&](column_id id, const atomic_cell_or_collection&) {
            violations |= check_collection_element_count(s, pk_bytes, pk, s.static_column_at(id), bytes_view(), nullptr);
        });
    }

    for (const auto& cr : mp.non_dummy_rows()) {
        auto ck_bytes = cr.key().view().representation().linearize();
        auto ck_bv = bytes_view(ck_bytes);
        violations |= check_row_size(s, pk_bytes, pk, ck_bv, &cr.key());
        cr.row().cells().for_each_cell([&](column_id id, const atomic_cell_or_collection&) {
            violations |= check_collection_element_count(s, pk_bytes, pk, s.regular_column_at(id), ck_bv, &cr.key());
        });
    }
    return violations;
}

large_data_violation_type large_data_guardrail::check_row_size(const schema& s, bytes_view pk_bytes, partition_key_view pk,
        bytes_view ck_bytes, const clustering_key_prefix* ck) const {
    const uint64_t fail = uint64_t(_cfg.row_size_fail_threshold_mb()) * MB;
    const uint64_t warn = uint64_t(_cfg.row_size_warn_threshold_mb()) * MB;
    auto row_size_opt = _index.lookup_row(pk_bytes, ck_bytes);
    uint64_t row_size = row_size_opt.value_or(0);
    if (ck) {
        auto it = _memtable_row_cache.find(row_key_view{pk_bytes, ck_bytes});
        if (it != _memtable_row_cache.end()) {
            row_size = std::max(row_size, it->second);
        }
    }
    if (row_size == 0) [[likely]] {
        return large_data_violation_type::none;
    }
    bool warned = enforce_threshold<guardrail_source::replica>(s, row_size,
        fail, warn, "row size",
        [&] { return seastar::format("clustering_key={}",
            ck ? seastar::format("{}", ck->with_schema(s)) : sstring()); });
    return (warned && _cfg.cql_warnings_enabled()) ? large_data_violation_type::row : large_data_violation_type::none;
}

large_data_violation_type large_data_guardrail::check_collection_element_count(const schema& s, bytes_view pk_bytes, partition_key_view pk,
        const column_definition& cdef, bytes_view ck_bytes,
        const clustering_key_prefix* ck) const {
    if (cdef.is_atomic()) {
        return large_data_violation_type::none;
    }
    auto col_bytes = to_bytes(cdef.name_as_text());
    auto count_opt = _index.lookup_collection(pk_bytes, ck_bytes, bytes_view(col_bytes));
    uint64_t count = count_opt.value_or(0);
    if (ck) {
        auto it = _memtable_collection_cache.find(
            collection_key_view{pk_bytes, ck_bytes, cdef.id});
        if (it != _memtable_collection_cache.end()) {
            count = std::max(count, it->second);
        }
    }
    if (count == 0) [[likely]] {
        return large_data_violation_type::none;
    }
    bool warned = enforce_threshold<guardrail_source::replica>(s, count,
        uint64_t(_cfg.collection_elements_fail_threshold()),
        uint64_t(_cfg.collection_elements_warn_threshold()),
        "collection element count",
        [&] { return seastar::format("column={}, clustering_key={}",
            cdef.name_as_text(),
            ck ? seastar::format("{}", ck->with_schema(s)) : sstring()); }) && _cfg.cql_warnings_enabled();
    return warned ? large_data_violation_type::collection : large_data_violation_type::none;
}

void large_data_guardrail::check_coordinator(const schema& s, const mutation_partition& mp,
        partition_key_view pk, db::large_data_violation_type* violations_out) const {
    const uint64_t cell_fail = uint64_t(_cfg.cell_size_fail_threshold_mb()) * MB;
    const uint64_t cell_warn = uint64_t(_cfg.cell_size_warn_threshold_mb()) * MB;
    const uint64_t row_fail = uint64_t(_cfg.row_size_fail_threshold_mb()) * MB;
    const uint64_t row_warn = uint64_t(_cfg.row_size_warn_threshold_mb()) * MB;
    const uint64_t coll_fail = uint64_t(_cfg.collection_elements_fail_threshold());
    const uint64_t coll_warn = uint64_t(_cfg.collection_elements_warn_threshold());

    // Only accumulate soft violations when CQL warnings are enabled by
    // configuration.  Hard limit enforcement and server-side soft limit logging
    // are unaffected.
    const bool track = _cfg.cql_warnings_enabled();
    large_data_violation_type violations = large_data_violation_type::none;
    auto note = [&] (large_data_violation_type category, bool warned) {
        if (track && warned) {
            violations |= category;
        }
    };

    auto check_cell = [&](const column_definition& cdef, const atomic_cell_or_collection& cell) {
        if (!cdef.is_atomic()) {
            return;
        }
        auto acv = cell.as_atomic_cell(cdef);
        if (!acv.is_live()) {
            return;
        }
        note(large_data_violation_type::cell, enforce_threshold<guardrail_source::coordinator>(s, acv.value_size(),
            cell_fail, cell_warn, "cell value size",
            [&] { return seastar::format("column={}", cdef.name_as_text()); }));
    };

    auto check_collection = [&](const column_definition& cdef, const atomic_cell_or_collection& cell) {
        if (cdef.is_atomic()) {
            return;
        }
        note(large_data_violation_type::collection, enforce_threshold<guardrail_source::coordinator>(s, cell.as_collection_mutation().size(),
            coll_fail, coll_warn, "collection element count",
            [&] { return seastar::format("column={}", cdef.name_as_text()); }));
    };

    if (!mp.static_row().empty()) {
        auto static_size = mp.static_row().external_memory_usage(s, column_kind::static_column);
        note(large_data_violation_type::row, enforce_threshold<guardrail_source::coordinator>(s, static_size,
            row_fail, row_warn, "static row size",
            [&] { return seastar::format("partition_key={}", pk); }));
        mp.static_row().for_each_cell([&](column_id id, const atomic_cell_or_collection& cell) {
            const column_definition& cdef = s.static_column_at(id);
            check_cell(cdef, cell);
            check_collection(cdef, cell);
        });
    }

    for (const rows_entry& e : mp.clustered_rows()) {
        if (e.dummy()) {
            continue;
        }
        note(large_data_violation_type::row, enforce_threshold<guardrail_source::coordinator>(s, e.memory_usage(s),
            row_fail, row_warn, "row size",
            [&] { return seastar::format("clustering_key={}", e.key().with_schema(s)); }));
        e.row().cells().for_each_cell([&](column_id id, const atomic_cell_or_collection& cell) {
            const column_definition& cdef = s.regular_column_at(id);
            check_cell(cdef, cell);
            check_collection(cdef, cell);
        });
    }

    if (violations_out) {
        *violations_out = violations;
    }
}

void large_data_cache_tracker::on_row_merged(const schema& s, const rows_entry& row) noexcept {
    try {
        constexpr uint64_t MB = 1024 * 1024;
        const uint64_t row_warn = uint64_t(_cfg.row_size_warn_threshold_mb()) * MB;
        const uint64_t row_fail = uint64_t(_cfg.row_size_fail_threshold_mb()) * MB;

        // The cache keys own a managed_bytes clustering key.  on_row_merged()
        // runs under the memtable's LSA allocator, so build the key and insert
        // (which copies it into the cache) under the standard allocator the
        // caches are torn down with (see with_standard_allocator()).
        with_standard_allocator([&] {
            auto ck_bytes_view = row.key().view().representation();

            if (row_warn > 0 || row_fail > 0) {
                size_t row_size = row.memory_usage(s);
                uint64_t threshold = row_warn > 0 ? row_warn : row_fail;
                if (row_size >= threshold) {
                    _row_cache.insert_or_assign(row_key{_pk_bytes, managed_bytes(ck_bytes_view)}, row_size);
                }
            }

            // Flush buffered collection observations from on_collection_merged().
            for (auto& pc : _pending_collections) {
                _collection_cache.insert_or_assign(
                    collection_key{_pk_bytes, managed_bytes(ck_bytes_view), pc.id}, pc.count);
            }
        });
        _pending_collections.clear();
    } catch (...) {
        // noexcept: a failure here only means the memtable-level cache is not
        // updated, so a large row may slip past the guardrail until the next
        // SSTable flush rebuilds the on-disk index.
        large_data_logger.warn("Failed to update memtable-level large-row cache; "
            "large rows may slip past the guardrail until the next SSTable flush: {}",
            std::current_exception());
        _pending_collections.clear();
    }
}

void large_data_cache_tracker::on_collection_merged(const column_definition& cdef,
        const atomic_cell_or_collection& merged_cell) noexcept {
    try {
        const uint64_t coll_warn = uint64_t(_cfg.collection_elements_warn_threshold());
        const uint64_t coll_fail = uint64_t(_cfg.collection_elements_fail_threshold());

        if (coll_warn == 0 && coll_fail == 0) {
            return;
        }

        auto cmv = merged_cell.as_collection_mutation();
        uint32_t count = cmv.size();
        uint64_t threshold = coll_warn > 0 ? coll_warn : coll_fail;
        if (count >= threshold) {
            _pending_collections.push_back({cdef.id, count});
        }
    } catch (...) {
        // noexcept: see on_row_merged().  A failure here only means a large
        // collection may slip past the guardrail until the next SSTable flush.
        large_data_logger.warn("Failed to record collection size for memtable-level "
            "large-collection cache; large collections may slip past the guardrail "
            "until the next SSTable flush: {}", std::current_exception());
    }
}

large_data_cache_tracker* large_data_guardrail::get_memtable_cache_tracker(const schema& s,
        partition_key_view pk) {
    constexpr uint64_t MB = 1024 * 1024;
    const uint64_t row_warn = uint64_t(_cfg.row_size_warn_threshold_mb()) * MB;
    const uint64_t row_fail = uint64_t(_cfg.row_size_fail_threshold_mb()) * MB;
    const uint64_t coll_warn = uint64_t(_cfg.collection_elements_warn_threshold());
    const uint64_t coll_fail = uint64_t(_cfg.collection_elements_fail_threshold());

    if (row_warn == 0 && row_fail == 0 && coll_warn == 0 && coll_fail == 0) {
        return nullptr;
    }

    auto sst_key = sstables::key::from_partition_key(s, pk);
    _tracker.set_partition_key(sst_key.get_bytes());
    return &_tracker;
}

sstring large_data_handler::sst_filename(const sstables::sstable& sst) {
    return sst.component_basename(sstables::component_type::Data);
}

future<> large_data_handler::maybe_delete_large_data_entries(sstables::shared_sstable sst) {
    SCYLLA_ASSERT(running());
    auto schema = sst->get_schema();
    auto filename = sst_filename(*sst);
    using ldt = sstables::large_data_type;
    auto above_threshold = [sst] (ldt type) -> bool {
        auto entry = sst->get_large_data_stat(type);
        return entry && entry->above_threshold;
    };

    future<> large_partitions = make_ready_future<>();
    if (above_threshold(ldt::partition_size) || above_threshold(ldt::rows_in_partition)) {
        large_partitions = with_sem([schema, filename, this] () mutable {
            return delete_large_data_entries(*schema, std::move(filename), db::system_keyspace::LARGE_PARTITIONS);
        });
    }
    future<> large_rows = make_ready_future<>();
    if (above_threshold(ldt::row_size)) {
        large_rows = with_sem([schema, filename, this] () mutable {
            return delete_large_data_entries(*schema, std::move(filename), db::system_keyspace::LARGE_ROWS);
        });
    }
    future<> large_cells = make_ready_future<>();
    if (above_threshold(ldt::cell_size) || above_threshold(ldt::elements_in_collection)) {
        large_cells = with_sem([schema, filename, this] () mutable {
            return delete_large_data_entries(*schema, std::move(filename), db::system_keyspace::LARGE_CELLS);
        });
    }
    return when_all(std::move(large_partitions), std::move(large_rows), std::move(large_cells)).discard_result();
}

future<> large_data_handler::maybe_update_large_data_entries_sstable_name(sstables::shared_sstable sst, sstring new_name) {
    SCYLLA_ASSERT(running());
    auto schema = sst->get_schema();
    auto old_name = sst_filename(*sst);
    using ldt = sstables::large_data_type;
    auto above_threshold = [sst] (ldt type) -> bool {
        auto entry = sst->get_large_data_stat(type);
        return entry && entry->above_threshold;
    };

    future<> large_partitions = make_ready_future<>();
    if (above_threshold(ldt::partition_size) || above_threshold(ldt::rows_in_partition)) {
        large_partitions = with_sem([schema, old_name, new_name, this] () mutable {
            return update_large_data_entries_sstable_name(*schema, std::move(old_name), std::move(new_name), db::system_keyspace::LARGE_PARTITIONS);
        });
    }
    future<> large_rows = make_ready_future<>();
    if (above_threshold(ldt::row_size)) {
        large_rows = with_sem([schema, old_name, new_name, this] () mutable {
            return update_large_data_entries_sstable_name(*schema, std::move(old_name), std::move(new_name), db::system_keyspace::LARGE_ROWS);
        });
    }
    future<> large_cells = make_ready_future<>();
    if (above_threshold(ldt::cell_size) || above_threshold(ldt::elements_in_collection)) {
        large_cells = with_sem([schema, old_name, new_name, this] () mutable {
            return update_large_data_entries_sstable_name(*schema, std::move(old_name), std::move(new_name), db::system_keyspace::LARGE_CELLS);
        });
    }
    return when_all(std::move(large_partitions), std::move(large_rows), std::move(large_cells)).discard_result();
}

bool cql_table_large_data_handler::skip_cql_writes() const {
    return bool(_feat.large_data_virtual_tables);
}

cql_table_large_data_handler::cql_table_large_data_handler(gms::feature_service& feat,
        utils::updateable_value<uint32_t> partition_threshold_mb,
        utils::updateable_value<uint32_t> row_threshold_mb,
        utils::updateable_value<uint32_t> cell_threshold_mb,
        utils::updateable_value<uint32_t> rows_count_threshold,
        utils::updateable_value<uint32_t> collection_elements_count_threshold)
    : large_data_handler(partition_threshold_mb() * MB, row_threshold_mb() * MB, cell_threshold_mb() * MB, rows_count_threshold(), collection_elements_count_threshold())
    , _feat(feat)
    , _record_large_cells([this] (const sstables::sstable& sst, const sstables::key& pk, const clustering_key_prefix* ck, const column_definition& cdef, uint64_t cell_size, uint64_t collection_elements) {
        return internal_record_large_cells(sst, pk, ck, cdef, cell_size, collection_elements);
    })
    , _record_large_partitions([this] (const sstables::sstable& sst, const sstables::key& pk, uint64_t partition_size, uint64_t rows, uint64_t range_tombstones, uint64_t dead_rows) {
        return internal_record_large_partitions(sst, pk, partition_size, rows);
    })
    , _large_collection_detection_listener(_feat.large_collection_detection.when_enabled([this] {
        large_data_logger.debug("Enabled large_collection detection");
        _record_large_cells = [this] (const sstables::sstable& sst, const sstables::key& pk, const clustering_key_prefix* ck, const column_definition& cdef, uint64_t cell_size, uint64_t collection_elements) {
            return internal_record_large_cells_and_collections(sst, pk, ck, cdef, cell_size, collection_elements);
        };
    }))
    , _range_tombstone_and_dead_rows_detection_listener(_feat.range_tombstone_and_dead_rows_detection.when_enabled([this] {
        large_data_logger.debug("Enabled detection or range tombstones and dead rows");
        _record_large_partitions = [this] (const sstables::sstable& sst, const sstables::key& pk, uint64_t partition_size, uint64_t rows, uint64_t range_tombstones, uint64_t dead_rows) {
            return internal_record_large_partitions_all_data(sst, pk, partition_size, rows, range_tombstones, dead_rows);
        };
    }))
    , _partition_threshold_mb_updater(_partition_threshold_bytes, std::move(partition_threshold_mb), [] (uint32_t threshold_mb) { return uint64_t(threshold_mb) * MB; })
    , _row_threshold_mb_updater(_row_threshold_bytes, std::move(row_threshold_mb), [] (uint32_t threshold_mb) { return uint64_t(threshold_mb) * MB; })
    , _cell_threshold_mb_updater(_cell_threshold_bytes, std::move(cell_threshold_mb), [] (uint32_t threshold_mb) { return uint64_t(threshold_mb) * MB; })
    , _rows_count_threshold_updater(_rows_count_threshold, std::move(rows_count_threshold))
    , _collection_elements_count_threshold_updater(_collection_elements_count_threshold, std::move(collection_elements_count_threshold))
{}

template <typename... Args>
future<> cql_table_large_data_handler::do_insert_large_data_entry(std::string_view large_table,
        sstring ks_name, sstring cf_name, sstring sstable_name,
        int64_t size, sstring partition_key, db_clock::time_point compaction_time,
        const std::vector<sstring>& extra_fields, Args&&... args) const {
    if (skip_cql_writes()) {
        co_return;
    }
    auto sys_ks = _sys_ks.get_permit();
    if (!sys_ks) {
        co_return;
    }

    sstring extra_fields_str;
    sstring extra_values;
    for (std::string_view field : extra_fields) {
        extra_fields_str += seastar::format(", {}", field);
        extra_values += ", ?";
    }
    const sstring req = seastar::format("INSERT INTO system.large_{}s (keyspace_name, table_name, sstable_name, {}_size, partition_key, compaction_time{}) VALUES (?, ?, ?, ?, ?, ?{}) USING TTL 2592000",
            large_table, large_table, extra_fields_str, extra_values);
    co_await sys_ks->execute_cql(req, ks_name, cf_name, sstable_name, size, partition_key, compaction_time, args...)
            .discard_result()
            .handle_exception([ks_name, cf_name, large_table = sstring(large_table), sstable_name] (std::exception_ptr ep) {
                large_data_logger.warn("Failed to add a record to system.large_{}s: ks = {}, table = {}, sst = {} exception = {}",
                        large_table, ks_name, cf_name, sstable_name, ep);
            });
}

template <typename... Args>
future<> cql_table_large_data_handler::try_record(std::string_view large_table, const sstables::sstable& sst,  const sstables::key& partition_key, int64_t size,
        std::string_view size_desc, std::string_view desc, std::string_view extra_path, const std::vector<sstring> &extra_fields, Args&&... args) const {
    const schema &s = *sst.get_schema();
    auto ks_name = s.ks_name();
    auto cf_name = s.cf_name();
    const auto sstable_name = large_data_handler::sst_filename(sst);
    std::string pk_str = key_to_str(partition_key.to_partition_key(s), s);
    auto timestamp = db_clock::now();
    large_data_logger.warn("Writing large {} {}/{}: {} ({}) to {}", desc, ks_name, cf_name, extra_path, size_desc, sstable_name);
    co_await do_insert_large_data_entry(large_table, std::move(ks_name), std::move(cf_name), std::move(sstable_name),
            size, std::move(pk_str), timestamp, extra_fields, std::forward<Args>(args)...);
}

future<> cql_table_large_data_handler::record_large_partitions(const sstables::sstable& sst, const sstables::key& key,
        uint64_t partition_size, uint64_t rows, uint64_t range_tombstones, uint64_t dead_rows) const {
    return _record_large_partitions(sst, key, partition_size, rows, range_tombstones, dead_rows);
}

future<> cql_table_large_data_handler::internal_record_large_partitions(const sstables::sstable& sst, const sstables::key& key,
        uint64_t partition_size, uint64_t rows) const {
    const sstring size_desc = seastar::format("{} bytes/{} rows", partition_size, rows);
    return try_record("partition", sst, key, int64_t(partition_size), size_desc, "partition", "", {"rows"}, data_value((int64_t)rows));
}

future<> cql_table_large_data_handler::internal_record_large_partitions_all_data(const sstables::sstable& sst, const sstables::key& key,
        uint64_t partition_size, uint64_t rows, uint64_t range_tombstones, uint64_t dead_rows) const {
    const sstring size_desc = seastar::format("{} bytes/{} rows", partition_size, rows);
    return try_record("partition", sst, key, int64_t(partition_size), size_desc, "partition", "", {"rows", "range_tombstones", "dead_rows"},
                data_value((int64_t)rows), data_value((int64_t)range_tombstones), data_value((int64_t)dead_rows));
}

future<> cql_table_large_data_handler::record_large_cells(const sstables::sstable& sst, const sstables::key& partition_key,
        const clustering_key_prefix* clustering_key, const column_definition& cdef, uint64_t cell_size, uint64_t collection_elements) const {
    return _record_large_cells(sst, partition_key, clustering_key, cdef, cell_size, collection_elements);
}

future<> cql_table_large_data_handler::internal_record_large_cells(const sstables::sstable& sst, const sstables::key& partition_key,
        const clustering_key_prefix* clustering_key, const column_definition& cdef, uint64_t cell_size, uint64_t collection_elements) const {
    auto column_name = cdef.name_as_text();
    std::string_view cell_type = cdef.is_atomic() ? "cell" : "collection";
    static const std::vector<sstring> extra_fields{"clustering_key", "column_name"};
    const sstring size_desc = seastar::format("{} bytes", cell_size);
    if (clustering_key) {
        const schema &s = *sst.get_schema();
        auto ck_str = key_to_str(*clustering_key, s);
        return try_record("cell", sst, partition_key, int64_t(cell_size), size_desc, cell_type, column_name, extra_fields, ck_str, column_name);
    } else {
        auto desc = seastar::format("static {}", cell_type);
        return try_record("cell", sst, partition_key, int64_t(cell_size), size_desc, desc, column_name, extra_fields, data_value::make_null(utf8_type), column_name);
    }
}

future<> cql_table_large_data_handler::internal_record_large_cells_and_collections(const sstables::sstable& sst, const sstables::key& partition_key,
        const clustering_key_prefix* clustering_key, const column_definition& cdef, uint64_t cell_size, uint64_t collection_elements) const {
    auto column_name = cdef.name_as_text();
    std::string_view cell_type = cdef.is_atomic() ? "cell" : "collection";
    const sstring size_desc = seastar::format("{} bytes", cell_size);
    static const std::vector<sstring> extra_fields{"clustering_key", "column_name", "collection_elements"};
    if (clustering_key) {
        const schema &s = *sst.get_schema();
        auto ck_str = key_to_str(*clustering_key, s);
        return try_record("cell", sst, partition_key, int64_t(cell_size), size_desc, cell_type, column_name, extra_fields, ck_str, column_name, data_value((int64_t)collection_elements));
    } else {
        auto desc = seastar::format("static {}", cell_type);
        return try_record("cell", sst, partition_key, int64_t(cell_size), size_desc, desc, column_name, extra_fields, data_value::make_null(utf8_type), column_name, data_value((int64_t)collection_elements));
    }
}

future<> cql_table_large_data_handler::record_large_rows(const sstables::sstable& sst, const sstables::key& partition_key,
        const clustering_key_prefix* clustering_key, uint64_t row_size) const {
    static const std::vector<sstring> extra_fields{"clustering_key"};
    const sstring size_desc = seastar::format("{} bytes", row_size);
    if (clustering_key) {
        const schema &s = *sst.get_schema();
        std::string ck_str = key_to_str(*clustering_key, s);
        return try_record("row", sst, partition_key, int64_t(row_size), size_desc, "row", "", extra_fields, ck_str);
    } else {
        return try_record("row", sst, partition_key, int64_t(row_size), size_desc, "static row", "", extra_fields, data_value::make_null(utf8_type));
    }
}

future<> cql_table_large_data_handler::delete_large_data_entries(const schema& s, sstring sstable_name, std::string_view large_table_name) const {
    if (skip_cql_writes()) {
        co_return;
    }
    auto sys_ks = _sys_ks.get_permit();
    SCYLLA_ASSERT(sys_ks);
    const sstring req =
            seastar::format("DELETE FROM system.{} WHERE keyspace_name = ? AND table_name = ? AND sstable_name = ?",
                    large_table_name);
    large_data_logger.debug("Dropping entries from {}: ks = {}, table = {}, sst = {}",
            large_table_name, s.ks_name(), s.cf_name(), sstable_name);
    co_await sys_ks->execute_cql(req, s.ks_name(), s.cf_name(), sstable_name)
            .discard_result()
            .handle_exception([&s, sstable_name, large_table_name] (std::exception_ptr ep) {
                large_data_logger.warn("Failed to drop entries from {}: ks = {}, table = {}, sst = {} exception = {}",
                        large_table_name, s.ks_name(), s.cf_name(), sstable_name, ep);
            });
}

cql_table_large_data_handler::row_reinsert_func cql_table_large_data_handler::make_row_reinsert_func(std::string_view large_table_name, const sstring& ks_name, const sstring& cf_name, const sstring& new_name) const {
    if (large_table_name == system_keyspace::LARGE_PARTITIONS) {
        return [this, ks_name, cf_name, new_name] (const cql3::untyped_result_set_row& row) {
            return do_insert_large_data_entry("partition", ks_name, cf_name, new_name,
                    row.get_as<int64_t>("partition_size"), row.get_as<sstring>("partition_key"), row.get_as<db_clock::time_point>("compaction_time"),
                    {"rows", "range_tombstones", "dead_rows"},
                    data_value(row.get_or<int64_t>("rows", 0)),
                    data_value(row.get_or<int64_t>("range_tombstones", 0)),
                    data_value(row.get_or<int64_t>("dead_rows", 0)));
        };
    } else if (large_table_name == system_keyspace::LARGE_ROWS) {
        return [this, ks_name, cf_name, new_name] (const cql3::untyped_result_set_row& row) {
            return do_insert_large_data_entry("row", ks_name, cf_name, new_name,
                    row.get_as<int64_t>("row_size"), row.get_as<sstring>("partition_key"), row.get_as<db_clock::time_point>("compaction_time"),
                    {"clustering_key"},
                    row.get_as<sstring>("clustering_key"));
        };
    } else {
        return [this, ks_name, cf_name, new_name] (const cql3::untyped_result_set_row& row) {
            return do_insert_large_data_entry("cell", ks_name, cf_name, new_name,
                    row.get_as<int64_t>("cell_size"), row.get_as<sstring>("partition_key"), row.get_as<db_clock::time_point>("compaction_time"),
                    {"clustering_key", "column_name", "collection_elements"},
                    row.get_as<sstring>("clustering_key"),
                    row.get_as<sstring>("column_name"),
                    data_value(row.get_or<int64_t>("collection_elements", 0)));
        };
    }
}

future<> cql_table_large_data_handler::update_large_data_entries_sstable_name(const schema& s, sstring old_name, sstring new_name, std::string_view large_table_name) const {
    if (skip_cql_writes()) {
        co_return;
    }
    auto sys_ks = _sys_ks.get_permit();
    SCYLLA_ASSERT(sys_ks);
    // sstable_name is a clustering key, so we can't update it in place.
    // Instead, select all rows with the old name, re-insert with the new name, then delete the old rows.
    const sstring select_req =
            seastar::format("SELECT * FROM system.{} WHERE keyspace_name = ? AND table_name = ? AND sstable_name = ?",
                    large_table_name);
    large_data_logger.debug("Updating sstable_name in {}: ks = {}, table = {}, old_sst = {} -> new_sst = {}",
            large_table_name, s.ks_name(), s.cf_name(), old_name, new_name);
    try {
        auto ks_name = s.ks_name();
        auto cf_name = s.cf_name();
        auto reinsert = make_row_reinsert_func(large_table_name, ks_name, cf_name, new_name);
        auto result = co_await sys_ks->execute_cql(select_req, ks_name, cf_name, old_name);
        for (auto& row : *result) {
            co_await reinsert(row);
        }
        // Delete old entries
        co_await delete_large_data_entries(s, std::move(old_name), large_table_name);
    } catch (...) {
        large_data_logger.warn("Failed to update sstable_name in {}: ks = {}, table = {}, old_sst = {}, new_sst = {}, exception = {}",
                large_table_name, s.ks_name(), s.cf_name(), old_name, new_name, std::current_exception());
    }
}

}
