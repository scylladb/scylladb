/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "query-request.hh"
#include "query-result.hh"
#include "mutation/frozen_mutation.hh"
#include "db/timeout_clock.hh"
#include "mutation/mutation.hh"
#include "utils/chunked_vector.hh"
#include "mutation/range_tombstone_assembler.hh"

class reconcilable_result;
class frozen_reconcilable_result;
class mutation_source;

// Can be read by other cores after publishing.
struct partition {
    uint32_t _row_count_low_bits;
    frozen_mutation _m; // FIXME: We don't need cf UUID, which frozen_mutation includes.
    uint32_t _row_count_high_bits;
    partition(uint32_t row_count_low_bits, frozen_mutation m, uint32_t row_count_high_bits)
        : _row_count_low_bits(row_count_low_bits)
        , _m(std::move(m))
        , _row_count_high_bits(row_count_high_bits)
    { }

    partition(uint64_t row_count, frozen_mutation m)
        : _row_count_low_bits(static_cast<uint32_t>(row_count))
        , _m(std::move(m))
        , _row_count_high_bits(static_cast<uint32_t>(row_count >> 32))
    { }

    uint32_t row_count_low_bits() const {
        return _row_count_low_bits;
    }

    uint32_t row_count_high_bits() const {
        return _row_count_high_bits;
    }
    
    uint64_t row_count() const {
        return (static_cast<uint64_t>(_row_count_high_bits) << 32) | _row_count_low_bits;
    }

    const frozen_mutation& mut() const {
        return _m;
    }

    frozen_mutation& mut() {
        return _m;
    }


    bool operator==(const partition& other) const {
        return row_count() == other.row_count() && _m.representation() == other._m.representation();
    }
};

// The partitions held by this object are ordered according to dht::decorated_key ordering and non-overlapping.
// Each mutation must have different key.
//
// Can be read by other cores after publishing.
class reconcilable_result {
    uint32_t _row_count_low_bits;
    query::short_read _short_read;
    query::result_memory_tracker _memory_tracker;
    utils::chunked_vector<partition> _partitions;
    uint32_t _row_count_high_bits;
public:
    ~reconcilable_result();
    reconcilable_result();
    reconcilable_result(reconcilable_result&&) = default;
    reconcilable_result& operator=(reconcilable_result&&) = default;
    reconcilable_result(uint32_t row_count_low_bits, utils::chunked_vector<partition> partitions, query::short_read short_read,
                        uint32_t row_count_high_bits, query::result_memory_tracker memory_tracker = { });
    reconcilable_result(uint64_t row_count, utils::chunked_vector<partition> partitions, query::short_read short_read,
                        query::result_memory_tracker memory_tracker = { });

    const utils::chunked_vector<partition>& partitions() const;
    utils::chunked_vector<partition>& partitions();

    uint32_t row_count_low_bits() const {
        return _row_count_low_bits;
    }

    uint32_t row_count_high_bits() const {
        return _row_count_high_bits;
    }

    uint64_t row_count() const {
        return (static_cast<uint64_t>(_row_count_high_bits) << 32) | _row_count_low_bits;
    }

    query::short_read is_short_read() const {
        return _short_read;
    }

    size_t memory_usage() const {
        return _memory_tracker.used_memory();
    }

    bool operator==(const reconcilable_result& other) const;

    // other must be disjoint with this
    // does not merge or update memory trackers
    void merge_disjoint(schema_ptr schema, const reconcilable_result& other);

    struct printer {
        const reconcilable_result& self;
        schema_ptr schema;
        friend std::ostream& operator<<(std::ostream&, const printer&);
    };

    printer pretty_printer(schema_ptr) const;
};

// Reverse reconcilable_result by reversing mutations for all partitions.
future<foreign_ptr<lw_shared_ptr<reconcilable_result>>> reversed(foreign_ptr<lw_shared_ptr<reconcilable_result>> result);

template <>
struct fmt::formatter<reconcilable_result::printer> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const reconcilable_result::printer&, fmt::format_context& ctx) const
        -> decltype(ctx.out());
};


class reconcilable_result_builder {
    schema_ptr _query_schema;
    const query::partition_slice& _slice;

    bool _return_static_content_on_partition_with_no_rows{};
    bool _static_row_is_alive{};
    uint64_t _total_live_rows = 0;
    query::result_memory_accounter _memory_accounter;
    stop_iteration _stop;
    std::optional<streamed_mutation_freezer> _mutation_consumer;
    range_tombstone_assembler _rt_assembler;

    uint64_t _live_rows{};
    // make this the last member so it is destroyed first. #7240
    utils::chunked_vector<partition> _result;
    size_t _used_at_entry;

private:
    stop_iteration consume(range_tombstone&& rt);

public:
    // Expects reversed schema and reversed slice when building results for reverse query.
    reconcilable_result_builder(const schema& query_schema, const query::partition_slice& slice,
                                query::result_memory_accounter&& accounter) noexcept
        : _query_schema(query_schema.shared_from_this()), _slice(slice)
        , _memory_accounter(std::move(accounter))
    { }

    void consume_new_partition(const dht::decorated_key& dk);
    void consume(tombstone t);
    stop_iteration consume(static_row&& sr, tombstone, bool is_alive);
    stop_iteration consume(clustering_row&& cr, row_tombstone, bool is_alive);
    stop_iteration consume(range_tombstone_change&& rtc);
    stop_iteration consume_end_of_partition();
    reconcilable_result consume_end_of_stream();
};

future<query::result> to_data_query_result(
        const reconcilable_result&,
        schema_ptr,
        const query::partition_slice&,
        uint64_t row_limit,
        uint32_t partition_limit,
        query::result_options opts = query::result_options::only_result());

// Query the content of the mutation.
//
// The mutation is destroyed in the process, see `mutation::consume()`.
query::result query_mutation(
        mutation&& m,
        const query::partition_slice& slice,
        uint64_t row_limit = query::max_rows,
        gc_clock::time_point now = gc_clock::now(),
        query::result_options opts = query::result_options::only_result());

// Performs a query for counter updates.
future<mutation_opt> counter_write_query(schema_ptr, const mutation_source&, reader_permit permit,
                                         const dht::decorated_key& dk,
                                         const query::partition_slice& slice,
                                         tracing::trace_state_ptr trace_ptr);

