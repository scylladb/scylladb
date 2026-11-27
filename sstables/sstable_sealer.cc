/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "sstables/sstable_sealer.hh"
#include "sstables/sstables.hh"
#include "sstables/storage.hh"
#include "sstables/writer.hh"
#include "sstables/metadata_collector.hh"
#include "sstables/large_data_tracker.hh"
#include "dht/i_partitioner.hh"
#include "schema/schema.hh"

namespace sstables {

void sstable_sealer::open(const sstring& origin) {
    _sst.open_sstable(origin);
    _sst.create_data().get();
}

data_sink sstable_sealer::make_data_sink() {
    return _sst._storage->make_data_or_index_sink(_sst, component_type::Data).get();
}

std::unique_ptr<crc32_digest_file_writer> sstable_sealer::make_digest_data_writer() {
    return std::make_unique<crc32_digest_file_writer>(make_data_sink(), _sst.sstable_buffer_size, _sst.get_filename());
}

void sstable_sealer::set_owner_shard(shard_id shard) {
    _sst._shards = { shard };
}

void sstable_sealer::seal_statistics(serialization_header header, metadata_collector& collector, const encoding_stats& enc_stats) {
    _sst.set_first_and_last_keys();

    _sst._components->statistics.contents[metadata_type::Serialization] = std::make_unique<serialization_header>(std::move(header));
    sstables::seal_statistics(_sst.get_version(), _sst._components->statistics, collector,
        _sst._schema->get_partitioner().name(), _sst._schema->bloom_filter_fp_chance(),
        _sst._schema, _sst.get_first_decorated_key(), _sst.get_last_decorated_key(), enc_stats);
}

void sstable_sealer::write_data_digest(uint32_t full_checksum) {
    _sst.write_digest(full_checksum);
}

void sstable_sealer::write_statistics() {
    _sst.write_statistics();
}

void sstable_sealer::write_scylla_metadata(shard_id shard, run_id run, large_data_tracker& large_data, metadata_collector& collector,
        std::optional<scylla_metadata::parquet_data_file> parquet_data_file) {
    run_identifier identifier{run};
    std::optional<scylla_metadata::large_data_stats> ld_stats(large_data.take_stats());
    std::optional<scylla_metadata::ext_timestamp_stats> ts_stats(scylla_metadata::ext_timestamp_stats{
        .map = collector.get_ext_timestamp_stats()
    });
    std::optional<scylla_metadata::large_data_records> ld_records = large_data.take_records();
    _sst.write_scylla_metadata(shard, std::move(identifier), std::move(ld_stats), std::move(ts_stats), std::move(ld_records),
            std::move(parquet_data_file));
}

void sstable_sealer::seal(const sstable_writer_config& cfg) {
    if (!cfg.leave_unsealed) {
        _sst.seal_sstable(cfg.backup).get();
    }
}

} // namespace sstables
