/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "sstables/writer_impl.hh"
#include "sstables/types.hh"
#include "encoding_stats.hh"

namespace sstables {
namespace mc {

// The mc serialization header for a schema. Exposed because `pq` needs it too:
// it writes mc-shaped index entries, and the index *parser* decides between the
// mc and legacy layouts by asking whether the column translation -- which comes
// from this header -- is empty. Without it a pq index parses as ka/la and every
// lookup misses.
serialization_header make_serialization_header(const schema& s,
    const encoding_stats& enc_stats,
    const sstable_writer_config& cfg);

std::unique_ptr<sstable_writer::writer_impl> make_writer(sstable& sst,
    const schema& s,
    uint64_t estimated_partitions,
    const sstable_writer_config& cfg,
    encoding_stats enc_stats,
    shard_id shard);

}
}
