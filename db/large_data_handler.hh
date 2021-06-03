/*
 * Copyright (C) 2018-present ScyllaDB
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
#include "schema_fwd.hh"
#include "system_keyspace.hh"
#include "sstables/shared_sstable.hh"

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
    // Assuming:
    // * there is at most one log entry every 1MB
    // * the average latency of the log is 4ms (depends on the load)
    // * we aim to sustain 1GB/s of write bandwidth
    // We need a concurrency of:
    //  C = (1GB/s / 1MB) * 4ms = 1k/s * 4ms = 4
    // 16 should be enough for everybody.
    static constexpr size_t max_concurrency = 16;
    semaphore _sem{max_concurrency};

    // A convenience function for using the above semaphore. Unlike the global with_semaphore, this will not wait on the
    // future returned by func. The objective is for the future returned by func to run in parallel with whatever the
    // caller is doing, but limit how far behind we can get.
    template<typename Func>
    future<> with_sem(Func&& func) {
        return get_units(_sem, 1).then([func = std::forward<Func>(func)] (auto units) mutable {
            // Future is discarded purposefully, see method description.
            // FIXME: error handling.
            (void)func().finally([units = std::move(units)] {});
        });
    }

    bool _running = false;
    uint64_t _partition_threshold_bytes;
    uint64_t _row_threshold_bytes;
    uint64_t _cell_threshold_bytes;
    uint64_t _rows_count_threshold;
    mutable large_data_handler::stats _stats;

public:
    explicit large_data_handler(uint64_t partition_threshold_bytes, uint64_t row_threshold_bytes, uint64_t cell_threshold_bytes, uint64_t rows_count_threshold);
    virtual ~large_data_handler() {}

    // Once large_data_handler is stopped no further updates will be accepted.
    bool running() const { return _running; }
    void start();
    future<> stop();

    bool maybe_log_too_many_rows(const sstables::sstable& sst, const sstables::key& partition_key, uint64_t rows_count) {
        if (__builtin_expect(rows_count > _rows_count_threshold, false)) {
            log_too_many_rows(sst, partition_key, rows_count);
            return true;
        }
        return false;
    }

    future<bool> maybe_record_large_rows(const sstables::sstable& sst, const sstables::key& partition_key,
            const clustering_key_prefix* clustering_key, uint64_t row_size) {
        assert(running());
        if (__builtin_expect(row_size > _row_threshold_bytes, false)) {
            return with_sem([&sst, &partition_key, clustering_key, row_size, this] {
                return record_large_rows(sst, partition_key, clustering_key, row_size);
            }).then([] {
                return true;
            });
        }
        return make_ready_future<bool>(false);
    }

    future<bool> maybe_record_large_partitions(const sstables::sstable& sst, const sstables::key& partition_key, uint64_t partition_size);

    future<bool> maybe_record_large_cells(const sstables::sstable& sst, const sstables::key& partition_key,
            const clustering_key_prefix* clustering_key, const column_definition& cdef, uint64_t cell_size) {
        assert(running());
        if (__builtin_expect(cell_size > _cell_threshold_bytes, false)) {
            return with_sem([&sst, &partition_key, clustering_key, &cdef, cell_size, this] {
                return record_large_cells(sst, partition_key, clustering_key, cdef, cell_size);
            }).then([] {
                return true;
            });
        }
        return make_ready_future<bool>(false);
    }

    future<> maybe_delete_large_data_entries(sstables::shared_sstable sst);

    const large_data_handler::stats& stats() const { return _stats; }

    uint64_t get_partition_threshold_bytes() const noexcept {
        return _partition_threshold_bytes;
    }
    uint64_t get_row_threshold_bytes() const noexcept {
        return _row_threshold_bytes;
    }
    uint64_t get_cell_threshold_bytes() const noexcept {
        return _cell_threshold_bytes;
    }
    uint64_t get_rows_count_threshold() const noexcept {
        return _rows_count_threshold;
    }

protected:
    virtual void log_too_many_rows(const sstables::sstable& sst, const sstables::key& partition_key, uint64_t rows_count) const = 0;
    virtual future<> record_large_cells(const sstables::sstable& sst, const sstables::key& partition_key,
            const clustering_key_prefix* clustering_key, const column_definition& cdef, uint64_t cell_size) const = 0;
    virtual future<> record_large_rows(const sstables::sstable& sst, const sstables::key& partition_key, const clustering_key_prefix* clustering_key, uint64_t row_size) const = 0;
    virtual future<> delete_large_data_entries(const schema& s, sstring sstable_name, std::string_view large_table_name) const = 0;
    virtual future<> record_large_partitions(const sstables::sstable& sst, const sstables::key& partition_key, uint64_t partition_size) const = 0;
};

class cql_table_large_data_handler : public large_data_handler {
public:
    explicit cql_table_large_data_handler(uint64_t partition_threshold_bytes, uint64_t row_threshold_bytes, uint64_t cell_threshold_bytes, uint64_t rows_count_threshold)
        : large_data_handler(partition_threshold_bytes, row_threshold_bytes, cell_threshold_bytes, rows_count_threshold) {}

protected:
    virtual void log_too_many_rows(const sstables::sstable& sst, const sstables::key& partition_key, uint64_t rows_count) const override;
    virtual future<> record_large_partitions(const sstables::sstable& sst, const sstables::key& partition_key, uint64_t partition_size) const override;
    virtual future<> delete_large_data_entries(const schema& s, sstring sstable_name, std::string_view large_table_name) const override;
    virtual future<> record_large_cells(const sstables::sstable& sst, const sstables::key& partition_key,
            const clustering_key_prefix* clustering_key, const column_definition& cdef, uint64_t cell_size) const override;
    virtual future<> record_large_rows(const sstables::sstable& sst, const sstables::key& partition_key, const clustering_key_prefix* clustering_key, uint64_t row_size) const override;
};

class nop_large_data_handler : public large_data_handler {
public:
    nop_large_data_handler();
    virtual void log_too_many_rows(const sstables::sstable& sst, const sstables::key& partition_key, uint64_t rows_count) const override {
        return;
    }

    virtual future<> record_large_partitions(const sstables::sstable& sst, const sstables::key& partition_key, uint64_t partition_size) const override {
        return make_ready_future<>();
    }

    virtual future<> delete_large_data_entries(const schema& s, sstring sstable_name, std::string_view large_table_name) const override {
        return make_ready_future<>();
    }

    virtual future<> record_large_cells(const sstables::sstable& sst, const sstables::key& partition_key,
        const clustering_key_prefix* clustering_key, const column_definition& cdef, uint64_t cell_size) const override {
        return make_ready_future<>();
    }

    virtual future<> record_large_rows(const sstables::sstable& sst, const sstables::key& partition_key,
            const clustering_key_prefix* clustering_key, uint64_t row_size) const override {
        return make_ready_future<>();
    }
};

}
