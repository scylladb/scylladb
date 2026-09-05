/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "sstables/parquet/gain_estimator.hh"
#include "sstables/sstables.hh"
#include "schema/schema.hh"
#include "readers/mutation_reader.hh"
#include "dht/i_partitioner.hh"
#include "query/query-request.hh"

#include <seastar/util/log.hh>

#include <algorithm>
#include <ranges>

namespace sstables::parquet {

static seastar::logger elog("parquet_gain");

future<std::optional<double>> estimate_parquet_gain(schema_ptr s,
                                                    reader_permit permit,
                                                    const std::vector<sstables::shared_sstable>& inputs,
                                                    const pq_writer_config& cfg,
                                                    gain_sample_limits lim) {
    if (inputs.empty()) {
        co_return std::nullopt;
    }
    // Sample the largest input. It dominates the output, so it is the most
    // representative single file, and sampling one file rather than merging
    // several keeps this a plain sequential read.
    auto sst = *std::ranges::max_element(inputs, {},
            [] (const sstables::shared_sstable& x) { return x->ondisk_data_size(); });

    // On-disk, not data_size(): data_size() is the *uncompressed* length of the
    // data component, and comparing a compressed Parquet file against it would
    // report the native compressor's savings as ours.
    const uint64_t native_total = sst->ondisk_data_size();
    const uint64_t partitions_total = sst->get_estimated_key_count();
    if (!native_total || !partitions_total) {
        co_return std::nullopt;
    }

    fragment_shredder shredder(*s);
    uint64_t partitions = 0;
    uint64_t rows = 0;
    bool truncated = false;

    auto rd = sst->make_reader(s, std::move(permit), query::full_partition_range, s->full_slice());
    std::exception_ptr ex;
    try {
        co_await rd.consume_pausable([&] (mutation_fragment_v2&& mf) {
            switch (mf.mutation_fragment_kind()) {
            case mutation_fragment_v2::kind::partition_start:
                shredder.new_partition(mf.as_partition_start().key());
                shredder.set_partition_tombstone(mf.as_partition_start().partition_tombstone());
                ++partitions;
                break;
            case mutation_fragment_v2::kind::static_row:
                shredder.add_static_row(mf.as_static_row());
                break;
            case mutation_fragment_v2::kind::clustering_row:
                shredder.add_clustering_row(mf.as_clustering_row());
                ++rows;
                break;
            case mutation_fragment_v2::kind::range_tombstone_change:
                shredder.add_range_tombstone_change(mf.as_range_tombstone_change());
                break;
            case mutation_fragment_v2::kind::partition_end:
                shredder.end_partition();
                // Stop only here. A partition boundary is the one place where
                // abandoning the scan leaves the fragment stream well-formed and
                // the shredder's partition state closed.
                if (rows >= lim.max_rows || shredder.buffered_bytes() >= lim.max_bytes) {
                    truncated = true;
                    return stop_iteration::yes;
                }
                break;
            }
            return stop_iteration::no;
        });
    } catch (...) {
        ex = std::current_exception();
    }
    co_await rd.close();
    if (ex) {
        // A failed estimate is not a failed compaction: fall back to "unknown",
        // which keeps the data in the native format.
        elog.warn("gain estimate for {} failed, leaving format unchanged: {}",
                  sst->get_filename(), ex);
        co_return std::nullopt;
    }
    if (!rows || !partitions) {
        co_return std::nullopt;
    }

    const auto pq_bytes = shredder.to_parquet_for_storage(cfg).size();

    // Native bytes for the same rows. Scaling by the partition fraction is an
    // approximation in two ways -- get_estimated_key_count() is itself an
    // estimate, and partitions are not all the same size -- but the sample is
    // taken in token order, which is a hash order, so it is an unbiased one.
    // A full read needs no scaling at all, which is the common case for the small
    // sstables where the ratio matters least.
    const double frac = truncated
            ? std::min(1.0, double(partitions) / double(partitions_total))
            : 1.0;
    const double native_sample = double(native_total) * frac;
    if (native_sample < 1.0) {
        co_return std::nullopt;
    }

    const double gain = 1.0 - double(pq_bytes) / native_sample;
    elog.debug("gain estimate on {}: {} partitions ({:.1f} % of file), {} rows, "
               "parquet {} B vs native {:.0f} B -> {:.1f} % saved",
               sst->get_filename(), partitions, 100.0 * frac, rows,
               pq_bytes, native_sample, 100.0 * gain);
    co_return std::clamp(gain, -1.0, 1.0);
}

} // namespace sstables::parquet
