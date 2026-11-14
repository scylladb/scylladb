/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <seastar/core/iostream.hh>
#include <seastar/core/sstring.hh>
#include "sstables/types.hh"
#include "utils/UUID.hh"

struct encoding_stats;

namespace sstables {

class sstable;
class metadata_collector;
class large_data_tracker;
struct sstable_writer_config;

// The steps that every sstable writer takes on the sstable itself, from
// opening it for writing to sealing it. The writer of a format adds its own
// components between these steps.
//
// All methods must be called in a seastar thread, in this order:
//  1. open(), first.
//  2. make_data_sink(), to write the Data file.
//  3. set_owner_shard().
//  ... the writer writes the partitions and finishes its index:
//      trie_index_writer::finish(), or the Summary of the legacy index ...
//  4. seal_statistics().
//  5. write_data_digest(), when the Data file is complete.
//  ... filter_writer::finish() ...
//  6. write_statistics().
//  7. write_scylla_metadata().
//  8. seal().
// The writer may write its own components between the steps, in the order
// that its format requires.
class sstable_sealer {
    sstable& _sst;
public:
    explicit sstable_sealer(sstable& sst) noexcept : _sst(sst) {}

    // Generates the TOC, opens the storage, and creates the Data file.
    // `origin` is the origin that Scylla.db records (sstable_writer_config::origin).
    void open(const sstring& origin);

    // Creates the sink of the Data file, below compression and checksums.
    // Encryption, if any, is applied by the sink.
    data_sink make_data_sink();

    // Records the shard that owns the sstable.
    void set_owner_shard(shard_id shard);

    // Loads the first and the last partition key from the finished index
    // (Partitions.db or Summary), and prepares Statistics.db with the
    // serialization header and the statistics of `collector`.
    // The index must be finished before this call.
    void seal_statistics(serialization_header header, metadata_collector& collector, const encoding_stats& enc_stats);

    // Writes Digest.crc32, with the CRC-32 of the whole Data file, and
    // records it as the digest of the Data component.
    void write_data_digest(uint32_t full_checksum);

    // Writes Statistics.db.
    void write_statistics();

    // Writes Scylla.db, with the large data statistics and records of
    // `large_data`, and the extended timestamp statistics of `collector`.
    // `large_data` must not be used after this call.
    // `parquet_data_file` is set exactly for a Parquet Data file.
    void write_scylla_metadata(shard_id shard, run_id run, large_data_tracker& large_data, metadata_collector& collector,
            std::optional<scylla_metadata::parquet_data_file> parquet_data_file = std::nullopt);

    // Writes the TOC and seals the sstable, unless the configuration asks
    // to leave it unsealed.
    void seal(const sstable_writer_config& cfg);
};

} // namespace sstables
