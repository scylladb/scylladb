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

#include <boost/test/unit_test.hpp>
#include "mutation_reader.hh"
#include "mutation_assertions.hh"

// Intended to be called in a seastar thread
class reader_assertions {
    mutation_reader _reader;
    dht::partition_range _pr;
public:
    reader_assertions(mutation_reader reader)
        : _reader(std::move(reader))
    { }

    reader_assertions& produces(const dht::decorated_key& dk) {
        BOOST_TEST_MESSAGE(sprint("Expecting key %s", dk));
        _reader().then([&] (auto sm) {
            if (!sm) {
                BOOST_FAIL(sprint("Expected: %s, got end of stream", dk));
            }
            if (!sm->decorated_key().equal(*sm->schema(), dk)) {
                BOOST_FAIL(sprint("Expected: %s, got: %s", dk, sm->decorated_key()));
            }
        }).get0();
        return *this;
    }

    reader_assertions& produces(mutation m) {
        BOOST_TEST_MESSAGE(sprint("Expecting %s", m));
        _reader().then([] (auto sm) {
            return mutation_from_streamed_mutation(std::move(sm));
        }).then([this, m = std::move(m)] (mutation_opt&& mo) mutable {
            BOOST_REQUIRE(bool(mo));
            assert_that(*mo).is_equal_to(m);
        }).get0();
        return *this;
    }

    mutation_assertion next_mutation() {
        return _reader().then([] (auto sm) {
            return mutation_from_streamed_mutation(std::move(sm));
        }).then([] (mutation_opt&& mo) mutable {
            BOOST_REQUIRE(bool(mo));
            return mutation_assertion(std::move(*mo));
        }).get0();
    }

    template<typename RangeOfMutations>
    reader_assertions& produces(const RangeOfMutations& mutations) {
        for (auto&& m : mutations) {
            produces(m);
        }
        return *this;
    }

    reader_assertions& produces_end_of_stream() {
        BOOST_TEST_MESSAGE("Expecting end of stream");
        _reader().then([] (auto sm) {
            return mutation_from_streamed_mutation(std::move(sm));
        }).then([this] (mutation_opt&& mo) mutable {
            if (bool(mo)) {
                BOOST_FAIL(sprint("Expected end of stream, got %s", *mo));
            }
        }).get0();
        return *this;
    }

    reader_assertions& produces_eos_or_empty_mutation() {
        BOOST_TEST_MESSAGE("Expecting eos or empty mutation");
        auto sm = _reader().get0();
        mutation_opt mo = mutation_from_streamed_mutation(std::move(sm)).get0();
        if (mo) {
            if (!mo->partition().empty()) {
                BOOST_FAIL(sprint("Mutation is not empty: %s", *mo));
            }
        }
        return *this;
    }

    reader_assertions& fast_forward_to(const dht::partition_range& pr) {
        _pr = pr;
        _reader.fast_forward_to(_pr).get0();
        return *this;
    }
};

inline
reader_assertions assert_that(mutation_reader r) {
    return { std::move(r) };
}
