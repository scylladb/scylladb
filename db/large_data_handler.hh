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

#pragma once

#include <cstdint>
#include "schema.hh"

namespace sstables {
class sstable;
class key;
}

namespace db {

class large_data_handler {
public:
    struct stats {
        int64_t partitions_bigger_than_threshold = 0; // number of large partition updates exceeding threshold_bytes
    };

private:
    bool _stopped = false;
    uint64_t _partition_threshold_bytes;
    uint64_t _row_threshold_bytes;
    mutable large_data_handler::stats _stats;

public:
    explicit large_data_handler(uint64_t partition_threshold_bytes, uint64_t row_threshold_bytes)
        : _partition_threshold_bytes(partition_threshold_bytes)
        , _row_threshold_bytes(row_threshold_bytes) {}
    virtual ~large_data_handler() {}

    // Once large_data_handler is stopped it will ignore requests to update system.large_partitions. Any futures already
    // returned must be waited for by the caller.
    bool stopped() const { return _stopped; }
    void stop() {
        assert(!stopped());
        _stopped = true;
    }

    void maybe_log_large_row(const sstables::sstable& sst, const sstables::key& partition_key,
            const clustering_key_prefix* clustering_key, uint64_t row_size) const {
        if (__builtin_expect(row_size > _row_threshold_bytes, false)) {
            log_large_row(sst, partition_key, clustering_key, row_size);
        }
    }

    future<> maybe_update_large_partitions(const sstables::sstable& sst, const sstables::key& partition_key, uint64_t partition_size) const;

    future<> maybe_delete_large_partitions_entry(const schema& s, const sstring& filename, uint64_t data_size) const {
        if (!_stopped && __builtin_expect(data_size > _partition_threshold_bytes, false)) {
            return delete_large_partitions_entry(s, filename);
        }
        return make_ready_future<>();
    }

    const large_data_handler::stats& stats() const { return _stats; }

protected:
    virtual void log_large_row(const sstables::sstable& sst, const sstables::key& partition_key, const clustering_key_prefix* clustering_key, uint64_t row_size) const = 0;
    virtual future<> update_large_partitions(const schema& s, const sstring& sstable_name, const sstables::key& partition_key, uint64_t partition_size) const = 0;
    virtual future<> delete_large_partitions_entry(const schema& s, const sstring& sstable_name) const = 0;
};

class cql_table_large_data_handler : public large_data_handler {
protected:
    static logging::logger large_data_logger;

public:
    explicit cql_table_large_data_handler(uint64_t partition_threshold_bytes, uint64_t row_threshold_bytes)
        : large_data_handler(partition_threshold_bytes, row_threshold_bytes) {}

protected:
    virtual future<> update_large_partitions(const schema& s, const sstring& sstable_name, const sstables::key& partition_key, uint64_t partition_size) const override;
    virtual future<> delete_large_partitions_entry(const schema& s, const sstring& sstable_name) const override;
    virtual void log_large_row(const sstables::sstable& sst, const sstables::key& partition_key, const clustering_key_prefix* clustering_key, uint64_t row_size) const override;
};

class nop_large_data_handler : public large_data_handler {
public:
    nop_large_data_handler()
        : large_data_handler(std::numeric_limits<uint64_t>::max(), std::numeric_limits<uint64_t>::max()) {}
    virtual future<> update_large_partitions(const schema& s, const sstring& sstable_name, const sstables::key& partition_key, uint64_t partition_size) const override {
        return make_ready_future<>();
    }

    virtual future<> delete_large_partitions_entry(const schema& s, const sstring& sstable_name) const override {
        return make_ready_future<>();
    }

    virtual void log_large_row(const sstables::sstable& sst, const sstables::key& partition_key,
                               const clustering_key_prefix* clustering_key, uint64_t row_size) const override {}
};

}
