/*
 * Copyright (C) 2015 ScyllaDB
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

#include "query-request.hh"
#include "query-result.hh"
#include "mutation_reader.hh"
#include "frozen_mutation.hh"

class reconcilable_result;
class frozen_reconcilable_result;

// Can be read by other cores after publishing.
struct partition {
    uint32_t _row_count;
    frozen_mutation _m; // FIXME: We don't need cf UUID, which frozen_mutation includes.

    partition(uint32_t row_count, frozen_mutation m)
        : _row_count(row_count)
        , _m(std::move(m))
    { }

    uint32_t row_count() const {
        return _row_count;
    }

    const frozen_mutation& mut() const {
        return _m;
    }

    frozen_mutation& mut() {
        return _m;
    }


    bool operator==(const partition& other) const {
        return _row_count == other._row_count && _m.representation() == other._m.representation();
    }

    bool operator!=(const partition& other) const {
        return !(*this == other);
    }
};

// The partitions held by this object are ordered according to dht::decorated_key ordering and non-overlapping.
// Each mutation must have different key.
//
// Can be read by other cores after publishing.
class reconcilable_result {
    uint32_t _row_count;
    std::vector<partition> _partitions;
public:
    ~reconcilable_result();
    reconcilable_result();
    reconcilable_result(reconcilable_result&&) = default;
    reconcilable_result& operator=(reconcilable_result&&) = default;
    reconcilable_result(uint32_t row_count, std::vector<partition> partitions);

    const std::vector<partition>& partitions() const;
    std::vector<partition>& partitions();

    uint32_t row_count() const {
        return _row_count;
    }

    bool operator==(const reconcilable_result& other) const;
    bool operator!=(const reconcilable_result& other) const;

    struct printer {
        const reconcilable_result& self;
        schema_ptr schema;
        friend std::ostream& operator<<(std::ostream&, const printer&);
    };

    printer pretty_printer(schema_ptr) const;
};

query::result to_data_query_result(const reconcilable_result&, schema_ptr, const query::partition_slice&);

// Performs a query on given data source returning data in reconcilable form.
//
// Reads at most row_limit rows. If less rows are returned, the data source
// didn't have more live data satisfying the query.
//
// Any cells which have expired according to query_time are returned as
// deleted cells and do not count towards live data. The mutations are
// compact, meaning that any cell which is covered by higher-level tombstone
// is absent in the results.
//
// 'source' doesn't have to survive deferring.
future<reconcilable_result> mutation_query(
    schema_ptr,
    const mutation_source& source,
    const query::partition_range& range,
    const query::partition_slice& slice,
    uint32_t row_limit,
    gc_clock::time_point query_time);


class querying_reader {
    schema_ptr _schema;
    const query::partition_range& _range;
    const query::partition_slice& _slice;
    uint32_t _requested_limit;
    gc_clock::time_point _query_time;
    uint32_t _limit;
    const mutation_source& _source;
    std::function<void(uint32_t, mutation&&)> _consumer;
    std::experimental::optional<mutation_reader> _reader;
public:
    querying_reader(schema_ptr s,
                    const mutation_source& source,
                    const query::partition_range& range,
                    const query::partition_slice& slice,
                    uint32_t row_limit,
                    gc_clock::time_point query_time,
                    std::function<void(uint32_t, mutation&&)> consumer);

    future<> read();
};
