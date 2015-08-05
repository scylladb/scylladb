/*
 * Copyright 2015 Cloudius Systems
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
