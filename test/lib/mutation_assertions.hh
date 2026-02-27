/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <boost/test/unit_test.hpp>

#include "mutation/mutation.hh"
#include "mutation/mutation_fragment_stream_validator.hh"
#include "utils/log.hh"
#include "mutation/mutation_partition_v2.hh"
#include "dht/i_partitioner.hh"

extern logging::logger testlog;

class mutation_partition_assertion {
    schema_ptr _schema;
    mutation_partition _m;
private:
    static mutation_partition compacted(const schema& s, const mutation_partition& m) {
        mutation_partition res(s, m);
        auto key = dht::decorate_key(s, partition_key::make_empty());
        res.compact_for_compaction(s, always_gc, key, gc_clock::time_point::min(), tombstone_gc_state(nullptr));
        return res;
    }
public:
    mutation_partition_assertion(schema_ptr s, mutation_partition&& m)
        : _schema(s)
        , _m(std::move(m))
    { }

    mutation_partition_assertion(schema_ptr s, const mutation_partition& m)
        : _schema(s)
        , _m(*s, m)
    { }

    // If ck_ranges is passed, verifies only that information relevant for ck_ranges matches.
    mutation_partition_assertion& is_equal_to(const mutation_partition& other,
            const std::optional<query::clustering_row_ranges>& ck_ranges = {}) {
        return is_equal_to(*_schema, other, ck_ranges);
    }

    // If ck_ranges is passed, verifies only that information relevant for ck_ranges matches.
    mutation_partition_assertion& is_equal_to(const schema& s, const mutation_partition& other,
            const std::optional<query::clustering_row_ranges>& ck_ranges = {});

    mutation_partition_assertion& is_equal_to_compacted(const schema& s,
                                                        const mutation_partition& other,
                                                        const std::optional<query::clustering_row_ranges>& ck_ranges = {}) {
        mutation_partition_assertion(s.shared_from_this(), compacted(s, _m))
            .is_equal_to(compacted(s, other), ck_ranges);
        return *this;
    }

    mutation_partition_assertion& is_equal_to_compacted(const mutation_partition& other,
                                                        const std::optional<query::clustering_row_ranges>& ck_ranges = {}) {
        return is_equal_to_compacted(*_schema, other, ck_ranges);
    }

    mutation_partition_assertion& is_not_equal_to(const mutation_partition& other) {
        return is_not_equal_to(*_schema, other);
    }

    mutation_partition_assertion& is_not_equal_to(const schema& s, const mutation_partition& other);

    mutation_partition_assertion& has_same_continuity(const mutation_partition& other);

    mutation_partition_assertion& is_continuous(const position_range& r, is_continuous cont = is_continuous::yes);
};

inline
mutation_partition_assertion assert_that(schema_ptr s, const mutation_partition& mp) {
    return {std::move(s), mp};
}

inline
mutation_partition_assertion assert_that(schema_ptr s, const mutation_partition_v2& mp) {
    return {s, mp.as_mutation_partition(*s)};
}

class mutation_assertion {
    mutation _m;
public:
    mutation_assertion(mutation m)
        : _m(std::move(m))
    { }

    // If ck_ranges is passed, verifies only that information relevant for ck_ranges matches.
    mutation_assertion& is_equal_to(const mutation& other, const std::optional<query::clustering_row_ranges>& ck_ranges = {}) ;

    mutation_assertion& is_equal_to_compacted(const mutation& other, const std::optional<query::clustering_row_ranges>& ck_ranges = {});

    mutation_assertion& is_not_equal_to(const mutation& other);

    mutation_assertion& has_schema(schema_ptr s);

    mutation_assertion& has_same_continuity(const mutation& other);

    mutation_assertion& is_continuous(const position_range& r, is_continuous cont = is_continuous::yes);

    // Verifies that mutation data remains unchanged when upgraded to the new schema
    void is_upgrade_equivalent(schema_ptr new_schema);
};

inline
mutation_assertion assert_that(mutation m) {
    return { std::move(m) };
}

class mutation_opt_assertions {
    mutation_opt _mo;
public:
    mutation_opt_assertions(mutation_opt mo) : _mo(std::move(mo)) {}

    mutation_assertion has_mutation();

    void has_no_mutation();
};

inline
mutation_opt_assertions assert_that(mutation_opt mo) {
    return { std::move(mo) };
}

class validating_consumer {
    mutation_fragment_stream_validator _validator;

public:
    explicit validating_consumer(const schema& s) : _validator(s) { }

    void consume_new_partition(const dht::decorated_key& dk) {
        testlog.debug("consume new partition: {}", dk);
        BOOST_REQUIRE(_validator(mutation_fragment_v2::kind::partition_start, position_in_partition_view::for_partition_start(), {}));
    }
    void consume(tombstone) { }
    stop_iteration consume(static_row&& sr) {
        testlog.debug("consume static_row");
        BOOST_REQUIRE(_validator(mutation_fragment_v2::kind::static_row, sr.position(), {}));
        return stop_iteration::no;
    }
    stop_iteration consume(clustering_row&& cr) {
        testlog.debug("consume clustering_row: {}", cr.key());
        BOOST_REQUIRE(_validator(mutation_fragment_v2::kind::clustering_row, cr.position(), {}));
        return stop_iteration::no;
    }
    stop_iteration consume(range_tombstone_change&& rtc) {
        testlog.debug("consume range_tombstone_change: {} {}", rtc.position(), rtc.tombstone());
        BOOST_REQUIRE(_validator(mutation_fragment_v2::kind::range_tombstone_change, rtc.position(), rtc.tombstone()));
        return stop_iteration::no;
    }
    stop_iteration consume_end_of_partition() {
        testlog.debug("consume end of partition");
        BOOST_REQUIRE(_validator(mutation_fragment_v2::kind::partition_end, position_in_partition_view::for_partition_end(), {}));
        return stop_iteration::no;
    }
    void consume_end_of_stream() {
        testlog.debug("consume end of stream");
        BOOST_REQUIRE(_validator.on_end_of_stream());
    }
};

