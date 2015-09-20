/*
 * Copyright 2015 Cloudius Systems
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
public:
    reader_assertions(mutation_reader reader)
        : _reader(std::move(reader))
    { }

    reader_assertions& produces(mutation m) {
        _reader().then([this, m = std::move(m)] (mutation_opt&& mo) mutable {
            BOOST_REQUIRE(bool(mo));
            assert_that(*mo).is_equal_to(m);
        }).get0();
        return *this;
    }

    template<typename RangeOfMutations>
    reader_assertions& produces(const RangeOfMutations& mutations) {
        for (auto&& m : mutations) {
            produces(m);
        }
        return *this;
    }

    reader_assertions& produces_end_of_stream() {
        _reader().then([this] (mutation_opt&& mo) mutable {
            if (bool(mo)) {
                BOOST_FAIL(sprint("Expected end of stream, got %s", *mo));
            }
        }).get0();
        return *this;
    }
};

inline
reader_assertions assert_that(mutation_reader r) {
    return { std::move(r) };
}
