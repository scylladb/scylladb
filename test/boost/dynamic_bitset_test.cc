/*
 * Copyright 2015-present ScyllaDB
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

#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>
#include <boost/algorithm/cxx11/any_of.hpp>
#include <boost/range/irange.hpp>
#include <vector>
#include <random>

#include "utils/dynamic_bitset.hh"

BOOST_AUTO_TEST_CASE(test_set_clear_test) {
    utils::dynamic_bitset bits(178);
    for (size_t i = 0; i < 178; i++) {
        BOOST_REQUIRE(!bits.test(i));
    }

    for (size_t i = 0; i < 178; i += 2) {
        bits.set(i);
    }

    for (size_t i = 0; i < 178; i++) {
        if (i % 2) {
            BOOST_REQUIRE(!bits.test(i));
        } else {
            BOOST_REQUIRE(bits.test(i));
        }
    }

    for (size_t i = 0; i < 178; i += 4) {
        bits.clear(i);
    }

    for (size_t i = 0; i < 178; i++) {
        if (i % 2 || i % 4 == 0) {
            BOOST_REQUIRE(!bits.test(i));
        } else {
            BOOST_REQUIRE(bits.test(i));
        }
    }
}

BOOST_AUTO_TEST_CASE(test_find_first_next) {
    utils::dynamic_bitset bits(178);
    for (size_t i = 0; i < 178; i++) {
        BOOST_REQUIRE(!bits.test(i));
    }
    BOOST_REQUIRE_EQUAL(bits.find_first_set(), utils::dynamic_bitset::npos);

    for (size_t i = 0; i < 178; i += 2) {
        bits.set(i);
    }

    size_t i = bits.find_first_set();
    BOOST_REQUIRE_EQUAL(i, 0);
    do {
        auto j = bits.find_next_set(i);
        BOOST_REQUIRE_EQUAL(i + 2, j);
        i = j;
    } while (i < 176);
    BOOST_REQUIRE_EQUAL(bits.find_next_set(i), utils::dynamic_bitset::npos);

    for (size_t i = 0; i < 178; i += 4) {
        bits.clear(i);
    }

    i = bits.find_first_set();
    BOOST_REQUIRE_EQUAL(i, 2);
    do {
        auto j = bits.find_next_set(i);
        BOOST_REQUIRE_EQUAL(i + 4, j);
        i = j;
    } while (i < 174);
    BOOST_REQUIRE_EQUAL(bits.find_next_set(i), utils::dynamic_bitset::npos);

}

BOOST_AUTO_TEST_CASE(test_find_last_prev) {
    utils::dynamic_bitset bits(178);
    for (size_t i = 0; i < 178; i++) {
        BOOST_REQUIRE(!bits.test(i));
    }
    BOOST_REQUIRE_EQUAL(bits.find_last_set(), utils::dynamic_bitset::npos);

    for (size_t i = 0; i < 178; i += 2) {
        bits.set(i);
    }

    size_t i = bits.find_last_set();
    BOOST_REQUIRE_EQUAL(i, 176);

    for (size_t i = 0; i < 178; i += 4) {
        bits.clear(i);
    }

    i = bits.find_last_set();
    BOOST_REQUIRE_EQUAL(i, 174);
}

static void test_random_ops(size_t size, std::default_random_engine& re ) {
    // BOOST_REQUIRE and friends are very slow, just use regular throws instead.
    auto require = [] (bool b) {
        if (!b) {
            throw 0;
        }
    };
    auto require_equal = [&] (const auto& a, const auto& b) {
        require(a == b);
    };

    utils::dynamic_bitset db{size};
    std::vector<bool> bv(size, false);
    std::uniform_int_distribution<size_t> global_op_dist(0, size-1);
    std::uniform_int_distribution<size_t> bit_dist(0, size-1);
    std::uniform_int_distribution<int> global_op_selection_dist(0, 1);
    std::uniform_int_distribution<int> single_op_selection_dist(0, 5);
    auto is_set = [&] (size_t i) -> bool {
        return bv[i];
    };
    size_t limit = std::log(size) * 1000;
    for (size_t i = 0; i != limit; ++i) {
        if (global_op_dist(re) == 0) {
            // perform a global operation
            switch (global_op_selection_dist(re)) {
            case 0:
                for (size_t j = 0; j != size; ++j) {
                    db.clear(j);
                    bv[j] = false;
                }
                break;
            case 1:
                for (size_t j = 0; j != size; ++j) {
                    db.set(j);
                    bv[j] = true;
                }
                break;
            }
        } else {
            // perform a single-bit operation
            switch (single_op_selection_dist(re)) {
            case 0: {
                auto bit = bit_dist(re);
                db.set(bit);
                bv[bit] = true;
                break;
            }
            case 1: {
                auto bit = bit_dist(re);
                db.clear(bit);
                bv[bit] = false;
                break;
            }
            case 2: {
                auto bit = bit_dist(re);
                bool dbb = db.test(bit);
                bool bvb = bv[bit];
                require_equal(dbb, bvb);
                break;
            }
            case 3: {
                auto bit = bit_dist(re);
                auto next = db.find_next_set(bit);
                if (next == db.npos) {
                    require(!boost::algorithm::any_of(boost::irange<size_t>(bit+1, size), is_set));
                } else {
                    require(!boost::algorithm::any_of(boost::irange<size_t>(bit+1, next), is_set));
                    require(is_set(next));
                }
                break;            }
            case 4: {
                auto next = db.find_first_set();
                if (next == db.npos) {
                    require(!boost::algorithm::any_of(boost::irange<size_t>(0, size), is_set));
                } else {
                    require(!boost::algorithm::any_of(boost::irange<size_t>(0, next), is_set));
                    require(is_set(next));
                }
                break;
            }
            case 5: {
                auto next = db.find_last_set();
                if (next == db.npos) {
                    require(!boost::algorithm::any_of(boost::irange<size_t>(0, size), is_set));
                } else {
                    require(!boost::algorithm::any_of(boost::irange<size_t>(next + 1, size), is_set));
                    require(is_set(next));
                }
                break;
            }
            }
        }
    }
}


BOOST_AUTO_TEST_CASE(test_random_operations) {
    std::random_device rd;
    std::default_random_engine re(rd());
    for (auto size : { 1, 63, 64, 65, 2000, 4096-65, 4096-64, 4096-63, 4096-1, 4096, 4096+1, 262144-1, 262144, 262144+1}) {
        BOOST_CHECK_NO_THROW(test_random_ops(size, re));
    }
}
