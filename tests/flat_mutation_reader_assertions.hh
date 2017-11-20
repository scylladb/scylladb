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
private:
    mutation_fragment_opt read_next() {
        return _reader().get0();
    }
public:
    flat_reader_assertions(flat_mutation_reader reader)
        : _reader(std::move(reader))
    { }

    flat_reader_assertions& produces_partition_start(const dht::decorated_key& dk) {
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

    flat_reader_assertions& fast_forward_to(const dht::partition_range& pr) {
        _reader.fast_forward_to(pr);
        return *this;
    }

    flat_reader_assertions& next_partition() {
        _reader.next_partition();
        return *this;
    }

    flat_reader_assertions& fast_forward_to(position_range pr) {
        _reader.fast_forward_to(std::move(pr));
        return *this;
    }
};

inline
flat_reader_assertions assert_that(flat_mutation_reader r) {
    return { std::move(r) };
}
