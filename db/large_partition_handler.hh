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

class large_partition_handler {
public:
    virtual ~large_partition_handler() {}

    virtual future<> maybe_update_large_partitions(const sstables::sstable& sst, const sstables::key& partition_key, uint64_t partition_size) const = 0;
    virtual future<> maybe_delete_large_partitions_entry(const sstables::sstable& sst) const = 0;
};

class cql_table_large_partition_handler : public large_partition_handler {
protected:
    uint64_t _threshold_bytes;
    static logging::logger large_partition_logger;
public:
    explicit cql_table_large_partition_handler(uint64_t threshold_bytes) : _threshold_bytes(threshold_bytes) {}

    virtual future<> maybe_update_large_partitions(const sstables::sstable& sst, const sstables::key& partition_key, uint64_t partition_size) const override;
    virtual future<> maybe_delete_large_partitions_entry(const sstables::sstable& sst) const override;
protected:
    future<> update_large_partitions(const schema& s, const sstring& sstable_name, const sstables::key& partition_key, uint64_t partition_size) const;
    future<> delete_large_partitions_entry(const schema& s, const sstring& sstable_name) const;

};

class nop_large_partition_handler : public large_partition_handler {
public:
    virtual future<> maybe_update_large_partitions(const sstables::sstable& sst, const sstables::key& partition_key, uint64_t partition_size) const override {
        return make_ready_future<>();
    }
    virtual future<> maybe_delete_large_partitions_entry(const sstables::sstable& sst) const override {
        return make_ready_future<>();
    }
};

}
