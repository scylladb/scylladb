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

#include <optional>
#include <seastar/util/bool_class.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include "schema.hh"
#include "reader_permit.hh"
#include "dht/sharder.hh"
#include "dht/i_partitioner.hh"
#include "utils/phased_barrier.hh"
#include "flat_mutation_reader.hh"
#include "mutation_reader.hh"
#include "repair/hash.hh"

class repair_reader {
public:
    using is_local_reader = bool_class<class is_local_reader_tag>;

private:
    schema_ptr _schema;
    reader_permit _permit;
    dht::partition_range _range;
    // Used to find the range that repair master will work on
    dht::selective_token_range_sharder _sharder;
    // Seed for the repair row hashing
    uint64_t _seed;
    // Pin the table while the reader is alive.
    // Only needed for local readers, the multishard reader takes care
    // of pinning tables on used shards.
    std::optional<utils::phased_barrier::operation> _local_read_op;
    // Local reader or multishard reader to read the range
    flat_mutation_reader _reader;
    std::optional<evictable_reader_handle> _reader_handle;
    // Current partition read from disk
    lw_shared_ptr<const decorated_key_with_hash> _current_dk;
    uint64_t _reads_issued = 0;
    uint64_t _reads_finished = 0;

public:
    repair_reader(
        seastar::sharded<database>& db,
        column_family& cf,
        schema_ptr s,
        reader_permit permit,
        dht::token_range range,
        const dht::sharder& remote_sharder,
        unsigned remote_shard,
        uint64_t seed,
        is_local_reader local_reader);

    future<mutation_fragment_opt> read_mutation_fragment();

    void on_end_of_stream();

    lw_shared_ptr<const decorated_key_with_hash>& get_current_dk() {
        return _current_dk;
    }

    void set_current_dk(const dht::decorated_key& key);

    void clear_current_dk();

    void check_current_dk();

    void pause();
};
