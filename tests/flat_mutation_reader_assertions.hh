/*
 * Copyright (C) 2017 ScyllaDB
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

#include <boost/test/unit_test.hpp>
#include "flat_mutation_reader.hh"

// Intended to be called in a seastar thread
class flat_reader_assertions {
    flat_mutation_reader _reader;
    dht::partition_range _pr;
private:
    mutation_fragment_opt read_next() {
        return _reader().get0();
    }
public:
    flat_reader_assertions(flat_mutation_reader reader)
        : _reader(std::move(reader))
    { }

    flat_reader_assertions& produces_partition_start(const dht::decorated_key& dk,
                                                     stdx::optional<tombstone> tomb = stdx::nullopt) {
        BOOST_TEST_MESSAGE(sprint("Expecting partition start with key %s", dk));
        auto mfopt = read_next();
        if (!mfopt) {
            BOOST_FAIL(sprint("Expected: partition start with key %s, got end of stream", dk));
        }
        if (!mfopt->is_partition_start()) {
            BOOST_FAIL(sprint("Expected: partition start with key %s, got: %s", dk, *mfopt));
        }
        if (!mfopt->as_partition_start().key().equal(*_reader.schema(), dk)) {
            BOOST_FAIL(sprint("Expected: partition start with key %s, got: %s", dk, *mfopt));
        }
        if (tomb && mfopt->as_partition_start().partition_tombstone() != *tomb) {
            BOOST_FAIL(sprint("Expected: partition start with tombstone %s, got: %s", *tomb, *mfopt));
        }
        return *this;
    }

    flat_reader_assertions& produces_static_row() {
        BOOST_TEST_MESSAGE(sprint("Expecting static row"));
        auto mfopt = read_next();
        if (!mfopt) {
            BOOST_FAIL("Expected static row, got end of stream");
        }
        if (!mfopt->is_static_row()) {
            BOOST_FAIL(sprint("Expected static row, got: %s", *mfopt));
        }
        return *this;
    }

    flat_reader_assertions& produces_row_with_key(const clustering_key& ck) {
        BOOST_TEST_MESSAGE(sprint("Expect %s", ck));
        auto mfopt = read_next();
        if (!mfopt) {
            BOOST_FAIL(sprint("Expected row with key %s, but got end of stream", ck));
        }
        if (!mfopt->is_clustering_row()) {
            BOOST_FAIL(sprint("Expected row with key %s, but got %s", ck, *mfopt));
        }
        auto& actual = mfopt->as_clustering_row().key();
        if (!actual.equal(*_reader.schema(), ck)) {
            BOOST_FAIL(sprint("Expected row with key %s, but key is %s", ck, actual));
        }
        return *this;
    }

    flat_reader_assertions& produces_partition_end() {
        BOOST_TEST_MESSAGE("Expecting partition end");
        auto mfopt = read_next();
        if (!mfopt) {
            BOOST_FAIL(sprint("Expected partition end but got end of stream"));
        }
        if (!mfopt->is_end_of_partition()) {
            BOOST_FAIL(sprint("Expected partition end but got %s", *mfopt));
        }
        return *this;
    }

    flat_reader_assertions& produces_end_of_stream() {
        BOOST_TEST_MESSAGE("Expecting end of stream");
        auto mfopt = read_next();
        if (bool(mfopt)) {
            BOOST_FAIL(sprint("Expected end of stream, got %s", *mfopt));
        }
        return *this;
    }

    flat_reader_assertions& produces(mutation_fragment::kind k, std::vector<int> ck_elements) {
        std::vector<bytes> ck_bytes;
        for (auto&& e : ck_elements) {
            ck_bytes.emplace_back(int32_type->decompose(e));
        }
        auto ck = clustering_key_prefix::from_exploded(*_reader.schema(), std::move(ck_bytes));

        auto mfopt = read_next();
        if (!mfopt) {
            BOOST_FAIL(sprint("Expected mutation fragment %s, got end of stream", ck));
        }
        if (mfopt->mutation_fragment_kind() != k) {
            BOOST_FAIL(sprint("Expected mutation fragment kind %s, got: %s", k, mfopt->mutation_fragment_kind()));
        }
        clustering_key::equality ck_eq(*_reader.schema());
        if (!ck_eq(mfopt->key(), ck)) {
            BOOST_FAIL(sprint("Expected key %s, got: %s", ck, mfopt->key()));
        }
        return *this;
    }

    flat_reader_assertions& produces_partition(const mutation& m) {
        return produces(m);
    }

    flat_reader_assertions& produces(const mutation& m) {
        BOOST_TEST_MESSAGE(sprint("Expecting a partition with key %s", m));
        produces_partition_start(m.decorated_key(), m.partition().partition_tombstone());
        auto produced_m = mutation(m.decorated_key(), m.schema());
        produced_m.partition().apply(m.partition().partition_tombstone());
        auto mfopt = read_next();
        while (mfopt && !mfopt->is_end_of_partition()) {
            produced_m.apply(*mfopt);
            mfopt = read_next();
        }
        if (!mfopt) {
            BOOST_FAIL("Expected a partition end before the end of stream");
        }
        if (m != produced_m) {
            BOOST_FAIL(sprint("Expected: partition %s, got: %s", m, produced_m));
        }
        return *this;
    }

    flat_reader_assertions& produces(const dht::decorated_key& dk) {
        produces_partition_start(dk);
        next_partition();
        return *this;
    }

    template<typename Range>
    flat_reader_assertions& produces(const Range& range) {
        for (auto&& m : range) {
            produces(m);
        }
        return *this;
    }

    void has_monotonic_positions() {
        position_in_partition::less_compare less(*_reader.schema());
        mutation_fragment_opt previous_fragment;
        mutation_fragment_opt previous_partition;
        bool inside_partition = false;
        for (;;) {
            auto mfo = read_next();
            if (!mfo) {
                break;
            }
            if (mfo->is_partition_start()) {
                BOOST_REQUIRE(!inside_partition);
                auto& dk = mfo->as_partition_start().key();
                if (previous_partition && !previous_partition->as_partition_start().key().less_compare(*_reader.schema(), dk)) {
                    BOOST_FAIL(sprint("previous partition had greater key: prev=%s, current=%s", *previous_partition, *mfo));
                }
                previous_partition = std::move(mfo);
                previous_fragment = stdx::nullopt;
                inside_partition = true;
            } else if (mfo->is_end_of_partition()) {
                BOOST_REQUIRE(inside_partition);
                inside_partition = false;
            } else {
                BOOST_REQUIRE(inside_partition);
                if (previous_fragment) {
                    if (!less(previous_fragment->position(), mfo->position())) {
                        BOOST_FAIL(sprint("previous fragment has greater position: prev=%s, current=%s", *previous_fragment, *mfo));
                    }
                }
                previous_fragment = std::move(mfo);
            }
        }
        BOOST_REQUIRE(!inside_partition);
    }

    flat_reader_assertions& fast_forward_to(const dht::partition_range& pr) {
        _pr = pr;
        _reader.fast_forward_to(_pr).get();
        return *this;
    }

    flat_reader_assertions& next_partition() {
        _reader.next_partition();
        return *this;
    }

    flat_reader_assertions& fast_forward_to(position_range pr) {
        _reader.fast_forward_to(std::move(pr)).get();
        return *this;
    }
};

inline
flat_reader_assertions assert_that(flat_mutation_reader r) {
    return { std::move(r) };
}
