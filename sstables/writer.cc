/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "sstables.hh"
#include "sstable_writer.hh"
#include "writer.hh"
#include "mx/writer.hh"
#include "parquet/sstable_writer.hh"
#include "db/corrupt_data_handler.hh"

namespace sstables {

// The name predates the writers of other formats, and users may refer to it.
logging::logger sstable_writer_log("mc_writer");

uint64_t partition_split_size_threshold(uint64_t max_sstable_size) {
    // will allow size limit to be exceeded for 10%, so we won't perform unnecessary split
    // of a partition which crossed the limit by a small margin.
    const uint64_t max_size = std::numeric_limits<uint64_t>::max();
    if (max_sstable_size == max_size) {
        return max_size;
    }
    uint64_t threshold_goal = max_sstable_size * 1.1;
    // handle overflow.
    return threshold_goal < max_sstable_size ? max_size : threshold_goal;
}

deletion_time sstable_writer::writer_impl::to_deletion_time(tombstone t, const key& partition_key) {
    deletion_time dt;
    if (t) {
        bool capped;
        int32_t ldt = adjusted_local_deletion_time(t.deletion_time, capped);
        if (capped) {
            sstable_writer_log.warn("Capping tombstone local_deletion_time {} to max {}", t.deletion_time.time_since_epoch().count(), ldt);
            sstable_writer_log.warn("Capping tombstone in sstable = {}, partition_key = {}", _sst.get_filename(), partition_key.to_partition_key(_schema));
            _sst.get_stats().on_capped_tombstone_deletion_time();
        }
        dt.local_deletion_time = ldt;
        dt.marked_for_delete_at = t.timestamp;
    } else {
        // Default values for live, non-deleted rows.
        dt.local_deletion_time = no_deletion_time;
        dt.marked_for_delete_at = api::missing_timestamp;
    }
    return dt;
}

void sstable_writer::writer_impl::record_corrupt_row(clustering_row&& clustered_row, const key& partition_key) {
    auto& handler = _sst.get_corrupt_data_handler();

    const auto pk = partition_key.to_partition_key(_schema);
    const auto ck = clustered_row.key();

    db::corrupt_data_handler::entry_id corrupt_row_id;
    sstring result;
    try {
        corrupt_row_id = handler.record_corrupt_clustering_row(_schema, pk, std::move(clustered_row), "sstable-write", fmt::to_string(_sst.get_filename())).get();
        result = format("written corrupt row to {} with id {}", handler.storage_name(), corrupt_row_id);
    } catch (...) {
        result = format("failed to write corrupt row to {}: {:t}", handler.storage_name(), std::current_exception());
    }

    sstable_writer_log.error("found non-full clustering key {} in partition {} while writing sstable {} for non-compact table {}.{}; {}",
            ck,
            pk,
            _sst.get_filename(),
            _schema.ks_name(),
            _schema.cf_name(),
            result);
}

sstable_writer::sstable_writer(sstable& sst, const schema& s, uint64_t estimated_partitions,
        const sstable_writer_config& cfg, encoding_stats enc_stats, shard_id shard) {
    if (!is_writable(sst.get_version())) {
        on_internal_error(sstlog, format("writing sstables with too old format: {}", static_cast<int>(sst.get_version())));
    }
    if (has_parquet_data_file(sst.get_version())) {
        _impl = parquet::make_sstable_writer(sst, s, estimated_partitions, cfg, enc_stats, shard);
    } else {
        _impl = mc::make_writer(sst, s, estimated_partitions, cfg, enc_stats, shard);
    }
    if (cfg.replay_position) {
        _impl->_collector.set_replay_position(cfg.replay_position.value());
    }
    if (cfg.sstable_level) {
        _impl->_collector.set_sstable_level(cfg.sstable_level.value());
    }
    sst.get_stats().on_open_for_writing();
}

void sstable_writer::consume_new_partition(const dht::decorated_key& dk) {
    _impl->_validator(dk);
    _impl->_validator(mutation_fragment_v2::kind::partition_start, position_in_partition_view::for_partition_start(), {});
    _impl->_sst.get_stats().on_partition_write();
    return _impl->consume_new_partition(dk);
}

void sstable_writer::consume(tombstone t) {
    _impl->_sst.get_stats().on_tombstone_write();
    return _impl->consume(t);
}

stop_iteration sstable_writer::consume(static_row&& sr) {
    _impl->_validator(mutation_fragment_v2::kind::static_row, sr.position(), {});
    if (!sr.empty()) {
        _impl->_sst.get_stats().on_static_row_write();
    }
    return _impl->consume(std::move(sr));
}

stop_iteration sstable_writer::consume(clustering_row&& cr) {
    _impl->_validator(mutation_fragment_v2::kind::clustering_row, cr.position(), {});
    _impl->_sst.get_stats().on_row_write();
    return _impl->consume(std::move(cr));
}

stop_iteration sstable_writer::consume(range_tombstone_change&& rtc) {
    _impl->_validator(mutation_fragment_v2::kind::range_tombstone_change, rtc.position(), rtc.tombstone());
    _impl->_sst.get_stats().on_range_tombstone_write();
    return _impl->consume(std::move(rtc));
}

stop_iteration sstable_writer::consume_end_of_partition() {
    _impl->_validator.on_end_of_partition();
    return _impl->consume_end_of_partition();
}

void sstable_writer::consume_end_of_stream() {
    _impl->_validator.on_end_of_stream();
    if (_impl->_c_stats.capped_local_deletion_time) {
        _impl->_sst.get_stats().on_capped_local_deletion_time();
    }
    return _impl->consume_end_of_stream();
}

void sstable_writer::set_repaired_at(int64_t repaired_at) {
    _impl->set_repaired_at(repaired_at);
}

uint64_t sstable_writer::data_file_position_for_tests() const {
    return _impl->data_file_position_for_tests();
}

sstable_writer::sstable_writer(sstable_writer&& o) = default;
sstable_writer& sstable_writer::operator=(sstable_writer&& o) = default;
sstable_writer::~sstable_writer() {
    if (_impl) {
        _impl->_sst.get_stats().on_close_for_writing();
    }
}

} // namespace sstables
