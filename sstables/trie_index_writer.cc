/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "sstables/trie_index_writer.hh"
#include "sstables/sstables.hh"
#include "sstables/storage.hh"
#include "sstables/writer.hh"
#include "sstables/writer_impl.hh"

namespace sstables {

static uint32_t close_digest_writer(std::unique_ptr<file_writer>& w) {
    auto writer = std::move(w);
    writer->close();
    auto chksum_wr = static_cast<crc32_digest_file_writer*>(writer.get());
    return chksum_wr->full_checksum();
}

trie_index_writer::trie_index_writer(sstable& sst)
        : _sst(sst) {
    auto out = _sst._storage->make_data_or_index_sink(_sst, component_type::Rows).get();
    _rows_writer = std::make_unique<crc32_digest_file_writer>(std::move(out), _sst.sstable_buffer_size, component_name(_sst, component_type::Rows));
    _row_index_writer = trie::bti_row_index_writer(*_rows_writer);
    out = _sst._storage->make_data_or_index_sink(_sst, component_type::Partitions).get();
    _partitions_writer = std::make_unique<crc32_digest_file_writer>(std::move(out), _sst.sstable_buffer_size, component_name(_sst, component_type::Partitions));
    _partition_index_writer = trie::bti_partition_index_writer(_sst.get_version(), *_partitions_writer);
}

trie_index_writer::~trie_index_writer() {
    auto close_writer = [](auto& writer) {
        if (writer) {
            try {
                writer->close();
            } catch (...) {
                sstlog.error("writer failed to close file: {:t}", std::current_exception());
            }
        }
    };
    close_writer(_partitions_writer);
    close_writer(_rows_writer);
}

void trie_index_writer::start_partition(const dht::decorated_key& dk, uint64_t partition_position) {
    _current_dk = dk;
    _current_partition_position = partition_position;
}

void trie_index_writer::add_row_index_block(const schema& s, const clustering_info& first, const clustering_info& last,
        uint64_t offset_from_partition_start, const deletion_time& preceding_range_tombstone) {
    _row_index_writer->add(
        s,
        first,
        last,
        offset_from_partition_start,
        preceding_range_tombstone
    );
}

void trie_index_writer::end_partition(const schema& s, const utils::hashed_key& hash, uint64_t partition_end,
        const key& partition_key, const deletion_time& partition_tombstone) {
    auto partitions_db_payload = _row_index_writer->finish(
        _sst.get_version(),
        s,
        _current_partition_position,
        partition_end,
        partition_key,
        partition_tombstone
    );
    _partition_index_writer->add(
        s,
        *std::exchange(_current_dk, std::nullopt),
        hash,
        partitions_db_payload
    );
}

void trie_index_writer::finish(const key& first_key, const key& last_key) {
    _sst._partitions_db_footer = std::move(*_partition_index_writer).finish(
        first_key,
        last_key);
    _sst.get_components_digests().map[component_type::Partitions] = close_digest_writer(_partitions_writer);
    _sst.get_components_digests().map[component_type::Rows] = close_digest_writer(_rows_writer);
}

} // namespace sstables
