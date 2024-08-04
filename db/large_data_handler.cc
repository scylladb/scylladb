/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "utils/assert.hh"
#include <seastar/core/print.hh>
#include <seastar/core/coroutine.hh>
#include "db/system_keyspace.hh"
#include "db/large_data_handler.hh"
#include "sstables/sstables.hh"
#include "gms/feature_service.hh"

static logging::logger large_data_logger("large_data");

namespace db {

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
{
    large_data_logger.debug("partition_threshold_bytes={} row_threshold_bytes={} cell_threshold_bytes={} rows_count_threshold={} collection_elements_count_threshold={}",
        partition_threshold_bytes, row_threshold_bytes, cell_threshold_bytes, rows_count_threshold, _collection_elements_count_threshold);
}

future<large_data_handler::partition_above_threshold> large_data_handler::maybe_record_large_partitions(const sstables::sstable& sst, const sstables::key& key, uint64_t partition_size, uint64_t rows, uint64_t range_tombstones, uint64_t dead_rows) {
    SCYLLA_ASSERT(running());
    partition_above_threshold above_threshold{partition_size > _partition_threshold_bytes, rows > _rows_count_threshold};
    static_assert(std::is_same_v<decltype(above_threshold.size), bool>);
    _stats.partitions_bigger_than_threshold += above_threshold.size; // increment if true
    if (above_threshold.size || above_threshold.rows) [[unlikely]] {
        return with_sem([&sst, &key, partition_size, rows, range_tombstones, dead_rows, this] {
            return record_large_partitions(sst, key, partition_size, rows, range_tombstones, dead_rows);
        }).then([above_threshold] {
            return above_threshold;
        });
    }
    return make_ready_future<partition_above_threshold>();
}

void large_data_handler::start() {
    _running = true;
}

future<> large_data_handler::stop() {
    if (running()) {
        _running = false;
        large_data_logger.info("Waiting for {} background handlers", max_concurrency - _sem.available_units());
        co_await _sem.wait(max_concurrency);
    }
}

void large_data_handler::plug_system_keyspace(db::system_keyspace& sys_ks) noexcept {
    _sys_ks = sys_ks.shared_from_this();
}

void large_data_handler::unplug_system_keyspace() noexcept {
    _sys_ks = nullptr;
}

template <typename T> static std::string key_to_str(const T& key, const schema& s) {
    return fmt::to_string(key.with_schema(s));
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
future<> cql_table_large_data_handler::try_record(std::string_view large_table, const sstables::sstable& sst,  const sstables::key& partition_key, int64_t size,
        std::string_view desc, std::string_view extra_path, const std::vector<sstring> &extra_fields, Args&&... args) const {
    if (!_sys_ks) {
        return make_ready_future<>();
    }

    sstring extra_fields_str;
    sstring extra_values;
    for (std::string_view field : extra_fields) {
        extra_fields_str += format(", {}", field);
        extra_values += ", ?";
    }
    const sstring req = format("INSERT INTO system.large_{}s (keyspace_name, table_name, sstable_name, {}_size, partition_key, compaction_time{}) VALUES (?, ?, ?, ?, ?, ?{}) USING TTL 2592000",
            large_table, large_table, extra_fields_str, extra_values);
    const schema &s = *sst.get_schema();
    auto ks_name = s.ks_name();
    auto cf_name = s.cf_name();
    const auto sstable_name = large_data_handler::sst_filename(sst);
    std::string pk_str = key_to_str(partition_key.to_partition_key(s), s);
    auto timestamp = db_clock::now();
    large_data_logger.warn("Writing large {} {}/{}: {} ({} bytes) to {}", desc, ks_name, cf_name, extra_path, size, sstable_name);
    return _sys_ks->execute_cql(req, ks_name, cf_name, sstable_name, size, pk_str, timestamp, args...)
            .discard_result()
            .handle_exception([ks_name, cf_name, large_table, sstable_name] (std::exception_ptr ep) {
                large_data_logger.warn("Failed to add a record to system.large_{}s: ks = {}, table = {}, sst = {} exception = {}",
                        large_table, ks_name, cf_name, sstable_name, ep);
            })
            .finally([ p = _sys_ks ] {});
}

future<> cql_table_large_data_handler::record_large_partitions(const sstables::sstable& sst, const sstables::key& key,
        uint64_t partition_size, uint64_t rows, uint64_t range_tombstones, uint64_t dead_rows) const {
    return _record_large_partitions(sst, key, partition_size, rows, range_tombstones, dead_rows);
}

future<> cql_table_large_data_handler::internal_record_large_partitions(const sstables::sstable& sst, const sstables::key& key,
        uint64_t partition_size, uint64_t rows) const {
    return try_record("partition", sst, key, int64_t(partition_size), "partition", "", {"rows"}, data_value((int64_t)rows));
}

future<> cql_table_large_data_handler::internal_record_large_partitions_all_data(const sstables::sstable& sst, const sstables::key& key,
        uint64_t partition_size, uint64_t rows, uint64_t range_tombstones, uint64_t dead_rows) const {
    return try_record("partition", sst, key, int64_t(partition_size), "partition", "", {"rows", "range_tombstones", "dead_rows"},
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
    if (clustering_key) {
        const schema &s = *sst.get_schema();
        auto ck_str = key_to_str(*clustering_key, s);
        return try_record("cell", sst, partition_key, int64_t(cell_size), cell_type, column_name, extra_fields, ck_str, column_name);
    } else {
        auto desc = format("static {}", cell_type);
        return try_record("cell", sst, partition_key, int64_t(cell_size), desc, column_name, extra_fields, data_value::make_null(utf8_type), column_name);
    }
}

future<> cql_table_large_data_handler::internal_record_large_cells_and_collections(const sstables::sstable& sst, const sstables::key& partition_key,
        const clustering_key_prefix* clustering_key, const column_definition& cdef, uint64_t cell_size, uint64_t collection_elements) const {
    auto column_name = cdef.name_as_text();
    std::string_view cell_type = cdef.is_atomic() ? "cell" : "collection";
    static const std::vector<sstring> extra_fields{"clustering_key", "column_name", "collection_elements"};
    if (clustering_key) {
        const schema &s = *sst.get_schema();
        auto ck_str = key_to_str(*clustering_key, s);
        return try_record("cell", sst, partition_key, int64_t(cell_size), cell_type, column_name, extra_fields, ck_str, column_name, data_value((int64_t)collection_elements));
    } else {
        auto desc = format("static {}", cell_type);
        return try_record("cell", sst, partition_key, int64_t(cell_size), desc, column_name, extra_fields, data_value::make_null(utf8_type), column_name, data_value((int64_t)collection_elements));
    }
}

future<> cql_table_large_data_handler::record_large_rows(const sstables::sstable& sst, const sstables::key& partition_key,
        const clustering_key_prefix* clustering_key, uint64_t row_size) const {
    static const std::vector<sstring> extra_fields{"clustering_key"};
    if (clustering_key) {
        const schema &s = *sst.get_schema();
        std::string ck_str = key_to_str(*clustering_key, s);
        return try_record("row", sst, partition_key, int64_t(row_size), "row", "", extra_fields, ck_str);
    } else {
        return try_record("row", sst, partition_key, int64_t(row_size), "static row", "", extra_fields, data_value::make_null(utf8_type));
    }
}

future<> cql_table_large_data_handler::delete_large_data_entries(const schema& s, sstring sstable_name, std::string_view large_table_name) const {
    SCYLLA_ASSERT(_sys_ks);
    const sstring req =
            format("DELETE FROM system.{} WHERE keyspace_name = ? AND table_name = ? AND sstable_name = ?",
                    large_table_name);
    large_data_logger.debug("Dropping entries from {}: ks = {}, table = {}, sst = {}",
            large_table_name, s.ks_name(), s.cf_name(), sstable_name);
    return _sys_ks->execute_cql(req, s.ks_name(), s.cf_name(), sstable_name)
            .discard_result()
            .handle_exception([&s, sstable_name, large_table_name] (std::exception_ptr ep) {
                large_data_logger.warn("Failed to drop entries from {}: ks = {}, table = {}, sst = {} exception = {}",
                        large_table_name, s.ks_name(), s.cf_name(), sstable_name, ep);
            })
            .finally([ p = _sys_ks ] {});
}
}
