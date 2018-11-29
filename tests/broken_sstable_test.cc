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

#include <boost/test/unit_test.hpp>

#include "tests/test-utils.hh"
#include "sstable_test.hh"

using namespace sstables;

static future<> broken_sst(sstring dir, unsigned long generation) {
    // Using an empty schema for this function, which is only about loading
    // a malformed component and checking that it fails.
    auto s = make_lw_shared(schema({}, "ks", "cf", {}, {}, {}, {}, utf8_type));
    auto sst = std::make_unique<sstable>(s, dir, generation, la, big);
    auto fut = sst->load();
    return std::move(fut).then_wrapped([sst = std::move(sst)] (future<> f) mutable {
        try {
            f.get();
            BOOST_FAIL("expecting exception");
        } catch (malformed_sstable_exception& e) {
            // ok
        }
        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(empty_toc) {
    return broken_sst("tests/sstables/badtoc", 1);
}

SEASTAR_TEST_CASE(alien_toc) {
    return broken_sst("tests/sstables/badtoc", 2);
}

SEASTAR_TEST_CASE(truncated_toc) {
    return broken_sst("tests/sstables/badtoc", 3);
}

SEASTAR_TEST_CASE(wrong_format_toc) {
    return broken_sst("tests/sstables/badtoc", 4);
}

SEASTAR_TEST_CASE(compression_truncated) {
    return broken_sst("tests/sstables/badcompression", 1);
}

SEASTAR_TEST_CASE(compression_bytes_flipped) {
    return broken_sst("tests/sstables/badcompression", 2);
}
