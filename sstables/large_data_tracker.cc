/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "sstables/large_data_tracker.hh"
#include "sstables/sstables.hh"
#include "sstables/writer_impl.hh"
#include "db/large_data_handler.hh"
#include "keys/keys.hh"
#include "schema/schema.hh"

namespace sstables {

large_data_tracker::large_data_tracker(sstable& sst, const schema& s, uint32_t max_records_per_type)
        : _sst(sst)
        , _schema(s)
        , _max_records(max_records_per_type)
        , _partition_size_entry(
                    large_data_stats_entry{
                        .threshold = _sst.get_large_data_handler().get_partition_threshold_bytes(),
                    }
                )
        , _rows_in_partition_entry(
                    large_data_stats_entry{
                        .threshold = _sst.get_large_data_handler().get_rows_count_threshold(),
                    }
                )
        , _row_size_entry(
                    large_data_stats_entry{
                        .threshold = _sst.get_large_data_handler().get_row_threshold_bytes(),
                    }
                )
        , _cell_size_entry(
                    large_data_stats_entry{
                        .threshold = _sst.get_large_data_handler().get_cell_threshold_bytes(),
                    }
                )
        , _elements_in_collection_entry(
                    large_data_stats_entry{
                        .threshold = _sst.get_large_data_handler().get_collection_elements_count_threshold(),
                    }
                )
{
}

void large_data_tracker::maybe_record_large_partition(const sstables::key& partition_key,
                                           uint64_t partition_size, uint64_t rows, uint64_t range_rombstones, uint64_t dead_rows) {
    auto& size_entry = _partition_size_entry;
    auto& row_count_entry = _rows_in_partition_entry;
    size_entry.max_value = std::max(size_entry.max_value, partition_size);
    row_count_entry.max_value = std::max(row_count_entry.max_value, rows);
    auto ret = _sst.get_large_data_handler().maybe_record_large_partitions(_sst, partition_key, partition_size, rows, range_rombstones, dead_rows).get();
    size_entry.above_threshold += unsigned(bool(ret.size));
    row_count_entry.above_threshold += unsigned(bool(ret.elements));

    auto trace_log = [&] (large_data_type type) {
        sstable_writer_log.trace("Detected large partition: sstable={}, partition_key={}, record_type={}, partition_size={}, rows={}, range_tombstones={}, dead_rows={}",
            _sst.component_basename(component_type::Data),
            key_to_str(partition_key.to_partition_key(_schema), _schema),
            type,
            partition_size,
            rows,
            range_rombstones,
            dead_rows);
    };

    // Populate top-N large data records into separate per-type heaps.
    if (ret.size) {
        trace_log(large_data_type::partition_size);
        const auto& pk_bytes = partition_key.get_bytes();
        insert_into_ld_heap(_ld_partition_size_records, large_data_record{
            .type = large_data_type::partition_size,
            .partition_key = disk_string<uint32_t>{bytes(pk_bytes)},
            .clustering_key = disk_string<uint32_t>{bytes()},
            .column_name = disk_string<uint32_t>{bytes()},
            .value = partition_size,
            .elements_count = rows,
            .range_tombstones = range_rombstones,
            .dead_rows = dead_rows,
        });
    }
    if (ret.elements) {
        trace_log(large_data_type::rows_in_partition);
        const auto& pk_bytes = partition_key.get_bytes();
        insert_into_ld_heap(_ld_rows_in_partition_records, large_data_record{
            .type = large_data_type::rows_in_partition,
            .partition_key = disk_string<uint32_t>{bytes(pk_bytes)},
            .clustering_key = disk_string<uint32_t>{bytes()},
            .column_name = disk_string<uint32_t>{bytes()},
            .value = partition_size,
            .elements_count = rows,
            .range_tombstones = range_rombstones,
            .dead_rows = dead_rows,
        });
    }
}

void large_data_tracker::maybe_record_large_row(const sstables::key& partition_key,
        const clustering_key_prefix* clustering_key, uint64_t row_size) {
    auto& entry = _row_size_entry;
    if (entry.max_value < row_size) {
        entry.max_value = row_size;
    }
    if (_sst.get_large_data_handler().maybe_record_large_rows(_sst, partition_key, clustering_key, row_size).get()) {
        entry.above_threshold++;

        sstable_writer_log.trace("Detected large row: sstable={}, partition_key={}, clustering_key={}, record_type={}, row_size={}",
            _sst.component_basename(component_type::Data),
            key_to_str(partition_key.to_partition_key(_schema), _schema),
            clustering_key ? key_to_str(*clustering_key, _schema) : "",
            large_data_type::row_size,
            row_size);
        const auto& pk_bytes = partition_key.get_bytes();
        auto ck_bytes = clustering_key ? clustering_key->view().representation().linearize() : bytes();
        insert_into_ld_heap(_ld_row_size_records, large_data_record{
            .type = large_data_type::row_size,
            .partition_key = disk_string<uint32_t>{bytes(pk_bytes)},
            .clustering_key = disk_string<uint32_t>{std::move(ck_bytes)},
            .column_name = disk_string<uint32_t>{bytes()},
            .value = row_size,
        });
    };
}

void large_data_tracker::maybe_record_large_cell(const sstables::key& partition_key,
        const clustering_key_prefix* clustering_key, const column_definition& cdef, uint64_t cell_size, uint64_t collection_elements) {
    auto& cell_size_entry = _cell_size_entry;
    if (cell_size_entry.max_value < cell_size) {
        cell_size_entry.max_value = cell_size;
    }
    auto& collection_elements_entry = _elements_in_collection_entry;
    if (collection_elements_entry.max_value < collection_elements) {
        collection_elements_entry.max_value = collection_elements;
    }
    auto ret = _sst.get_large_data_handler().maybe_record_large_cells(_sst, partition_key, clustering_key, cdef, cell_size, collection_elements).get();
    if (ret.size) {
        cell_size_entry.above_threshold++;
    }
    if (ret.elements) {
        collection_elements_entry.above_threshold++;
    }

    auto trace_log = [&] (large_data_type type) {
        sstable_writer_log.trace("Detected large cell: sstable={}, partition_key={}, clustering_key={}, column={}, record_type={}, cell_size={}, collection_elements={}",
            _sst.component_basename(component_type::Data),
            key_to_str(partition_key.to_partition_key(_schema), _schema),
            clustering_key ? key_to_str(*clustering_key, _schema) : "",
            cdef.name_as_text(),
            type,
            cell_size,
            collection_elements);
    };

    // Populate top-N large data records into separate per-type heaps.
    if (ret.size) {
        trace_log(large_data_type::cell_size);
        const auto& pk_bytes = partition_key.get_bytes();
        auto ck_bytes = clustering_key ? clustering_key->view().representation().linearize() : bytes();
        auto col_name = cdef.name_as_text();
        insert_into_ld_heap(_ld_cell_size_records, large_data_record{
            .type = large_data_type::cell_size,
            .partition_key = disk_string<uint32_t>{bytes(pk_bytes)},
            .clustering_key = disk_string<uint32_t>{std::move(ck_bytes)},
            .column_name = disk_string<uint32_t>{to_bytes(col_name)},
            .value = cell_size,
            .elements_count = collection_elements,
        });
    }
    if (ret.elements) {
        trace_log(large_data_type::elements_in_collection);
        const auto& pk_bytes = partition_key.get_bytes();
        auto ck_bytes = clustering_key ? clustering_key->view().representation().linearize() : bytes();
        auto col_name = cdef.name_as_text();
        insert_into_ld_heap(_ld_elements_in_collection_records, large_data_record{
            .type = large_data_type::elements_in_collection,
            .partition_key = disk_string<uint32_t>{bytes(pk_bytes)},
            .clustering_key = disk_string<uint32_t>{std::move(ck_bytes)},
            .column_name = disk_string<uint32_t>{to_bytes(col_name)},
            .value = cell_size,
            .elements_count = collection_elements,
        });
    }
}

scylla_metadata::large_data_stats large_data_tracker::take_stats() {
    return scylla_metadata::large_data_stats{
        .map = {
            { large_data_type::partition_size, std::move(_partition_size_entry) },
            { large_data_type::rows_in_partition, std::move(_rows_in_partition_entry) },
            { large_data_type::row_size, std::move(_row_size_entry) },
            { large_data_type::cell_size, std::move(_cell_size_entry) },
            { large_data_type::elements_in_collection, std::move(_elements_in_collection_entry) },
        }
    };
}

std::optional<scylla_metadata::large_data_records> large_data_tracker::take_records() {
    // Drain all per-type min-heaps into a single large_data_records array.
    utils::chunked_vector<large_data_record> records;
    auto drain_size_heap = [&records](ld_size_heap& heap) {
        while (!heap.empty()) {
            records.push_back(std::move(const_cast<large_data_record&>(heap.top())));
            heap.pop();
        }
    };
    auto drain_elements_heap = [&records](ld_elements_heap& heap) {
        while (!heap.empty()) {
            records.push_back(std::move(const_cast<large_data_record&>(heap.top())));
            heap.pop();
        }
    };
    drain_size_heap(_ld_partition_size_records);
    drain_elements_heap(_ld_rows_in_partition_records);
    drain_size_heap(_ld_row_size_records);
    drain_size_heap(_ld_cell_size_records);
    drain_elements_heap(_ld_elements_in_collection_records);
    if (records.empty()) {
        return std::nullopt;
    }
    return scylla_metadata::large_data_records{.elements = std::move(records)};
}

} // namespace sstables
