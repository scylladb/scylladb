/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "core/print.hh"
#include "db/query_context.hh"
#include "db/system_keyspace.hh"
#include "db/large_partition_handler.hh"
#include "sstables/sstables.hh"

namespace db {

future<> large_partition_handler::maybe_update_large_partitions(const sstables::sstable& sst, const sstables::key& key, uint64_t partition_size) const {
    if (partition_size > _threshold_bytes) {
        ++_stats.partitions_bigger_than_threshold;

        const schema& s = *sst.get_schema();
        return update_large_partitions(s, sst.get_filename(), key, partition_size);
    }
    return make_ready_future<>();
}

future<> large_partition_handler::maybe_delete_large_partitions_entry(const sstables::sstable& sst) const {
    try {
        if (sst.data_size() > _threshold_bytes) {
            const schema& s = *sst.get_schema();
            return delete_large_partitions_entry(s, sst.get_filename());
        }
    } catch (...) {
        // no-op
    }

    return make_ready_future<>();
}

logging::logger cql_table_large_partition_handler::large_partition_logger("large_partition");

future<> cql_table_large_partition_handler::update_large_partitions(const schema& s, const sstring& sstable_name, const sstables::key& key, uint64_t partition_size) const {
    static const sstring req = sprint("INSERT INTO system.%s (keyspace_name, table_name, sstable_name, partition_size, partition_key, compaction_time) VALUES (?, ?, ?, ?, ?, ?) USING TTL 2592000",
            db::system_keyspace::LARGE_PARTITIONS);
    // avoid self-reporting
    if (s.ks_name() == "system" && s.cf_name() == db::system_keyspace::LARGE_PARTITIONS) {
        return make_ready_future<>();
    }
    auto ks_name = s.ks_name();
    auto cf_name = s.cf_name();
    std::ostringstream oss;
    oss << key.to_partition_key(s).with_schema(s);
    auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(db_clock::now().time_since_epoch()).count();
    auto key_str = oss.str();
    return db::execute_cql(req, ks_name, cf_name, sstable_name, int64_t(partition_size), key_str, timestamp)
    .then_wrapped([ks_name, cf_name, key_str, partition_size](auto&& f) {
        try {
            f.get();
            large_partition_logger.warn("Writing large row {}/{}:{} ({} bytes)", ks_name, cf_name, key_str, partition_size);
        } catch (...) {
            large_partition_logger.warn("Failed to update {}: {}", db::system_keyspace::LARGE_PARTITIONS, std::current_exception());
        }
    });
}

future<> cql_table_large_partition_handler::delete_large_partitions_entry(const schema& s, const sstring& sstable_name) const {
    static const sstring req = sprint("DELETE FROM system.%s WHERE keyspace_name = ? AND table_name = ? AND sstable_name = ?", db::system_keyspace::LARGE_PARTITIONS);
    return db::execute_cql(req, s.ks_name(), s.cf_name(), sstable_name).discard_result().handle_exception([](std::exception_ptr ep) {
            large_partition_logger.warn("Failed to drop entries from {}: {}", db::system_keyspace::LARGE_PARTITIONS, ep);
        });
}

}
