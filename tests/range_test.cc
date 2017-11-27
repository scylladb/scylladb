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

#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>
#include "boost/icl/interval.hpp"
#include "boost/icl/interval_map.hpp"
#include <unordered_set>

#include "query-request.hh"
#include "schema_builder.hh"

#include "locator/token_metadata.hh"

// yuck, but what can one do?  needed for BOOST_REQUIRE_EQUAL
namespace std {

ostream&
operator<<(ostream& os, const stdx::nullopt_t&) {
    return os << "{}";
}

}

static
bool includes_token(const schema& s, const dht::partition_range& r, const dht::token& tok) {
    dht::ring_position_comparator cmp(s);

    return !r.before(dht::ring_position(tok, dht::ring_position::token_bound::end), cmp)
        && !r.after(dht::ring_position(tok, dht::ring_position::token_bound::start), cmp);
}

BOOST_AUTO_TEST_CASE(test_range_with_positions_within_the_same_token) {
    auto s = schema_builder("ks", "cf")
        .with_column("key", bytes_type, column_kind::partition_key)
        .with_column("v", bytes_type)
        .build();

    dht::token tok = dht::global_partitioner().get_random_token();

    auto key1 = dht::decorated_key{tok,
                                   partition_key::from_single_value(*s, bytes_type->decompose(data_value(bytes("key1"))))};

    auto key2 = dht::decorated_key{tok,
                                   partition_key::from_single_value(*s, bytes_type->decompose(data_value(bytes("key2"))))};

    {
        auto r = dht::partition_range::make(
            dht::ring_position(key1),
            dht::ring_position(key2));

        BOOST_REQUIRE(includes_token(*s, r, tok));
        BOOST_REQUIRE(r.contains(key1, dht::ring_position_comparator(*s)));
        BOOST_REQUIRE(r.contains(key2, dht::ring_position_comparator(*s)));
    }

    {
        auto r = dht::partition_range::make(
            {dht::ring_position(key1), false},
            {dht::ring_position(key2), false});

        BOOST_REQUIRE(includes_token(*s, r, tok));
        BOOST_REQUIRE(!r.contains(key1, dht::ring_position_comparator(*s)));
        BOOST_REQUIRE(!r.contains(key2, dht::ring_position_comparator(*s)));
    }

    {
        auto r = dht::partition_range::make(
            {dht::ring_position::starting_at(tok), false},
            {dht::ring_position(key2), true});

        BOOST_REQUIRE(includes_token(*s, r, tok));
        BOOST_REQUIRE(r.contains(key1, dht::ring_position_comparator(*s)));
        BOOST_REQUIRE(r.contains(key2, dht::ring_position_comparator(*s)));
    }

    {
        auto r = dht::partition_range::make(
            {dht::ring_position::starting_at(tok), true},
            {dht::ring_position::ending_at(tok), true});

        BOOST_REQUIRE(includes_token(*s, r, tok));
        BOOST_REQUIRE(r.contains(key1, dht::ring_position_comparator(*s)));
        BOOST_REQUIRE(r.contains(key2, dht::ring_position_comparator(*s)));
    }

}

BOOST_AUTO_TEST_CASE(test_range_contains) {
    auto cmp = [] (int i1, int i2) -> int { return i1 - i2; };

    auto check_contains = [&] (range<int> enclosing, range<int> enclosed) {
        BOOST_REQUIRE(enclosing.contains(enclosed, cmp));
        BOOST_REQUIRE(!enclosed.contains(enclosing, cmp));
    };

    BOOST_REQUIRE(range<int>({}, {}).contains(range<int>({}, {}), cmp));
    check_contains(range<int>({}, {}), range<int>({1}, {2}));
    check_contains(range<int>({}, {}), range<int>({}, {2}));
    check_contains(range<int>({}, {}), range<int>({1}, {}));
    check_contains(range<int>({}, {}), range<int>({2}, {1}));

    BOOST_REQUIRE(range<int>({}, {3}).contains(range<int>({}, {3}), cmp));
    BOOST_REQUIRE(range<int>({3}, {}).contains(range<int>({3}, {}), cmp));
    BOOST_REQUIRE(range<int>({}, {{3, false}}).contains(range<int>({}, {{3, false}}), cmp));
    BOOST_REQUIRE(range<int>({{3, false}}, {}).contains(range<int>({{3, false}}, {}), cmp));
    BOOST_REQUIRE(range<int>({1}, {3}).contains(range<int>({1}, {3}), cmp));
    BOOST_REQUIRE(range<int>({3}, {1}).contains(range<int>({3}, {1}), cmp));

    check_contains(range<int>({}, {3}), range<int>({}, {2}));
    check_contains(range<int>({}, {3}), range<int>({2}, {{3, false}}));
    BOOST_REQUIRE(!range<int>({}, {3}).contains(range<int>({}, {4}), cmp));
    BOOST_REQUIRE(!range<int>({}, {3}).contains(range<int>({2}, {{4, false}}), cmp));
    BOOST_REQUIRE(!range<int>({}, {3}).contains(range<int>({}, {}), cmp));

    check_contains(range<int>({3}, {}), range<int>({4}, {}));
    check_contains(range<int>({3}, {}), range<int>({{3, false}}, {}));
    check_contains(range<int>({3}, {}), range<int>({3}, {4}));
    check_contains(range<int>({3}, {}), range<int>({4}, {5}));
    BOOST_REQUIRE(!range<int>({3}, {}).contains(range<int>({2}, {4}), cmp));
    BOOST_REQUIRE(!range<int>({3}, {}).contains(range<int>({}, {}), cmp));

    check_contains(range<int>({}, {{3, false}}), range<int>({}, {2}));
    BOOST_REQUIRE(!range<int>({}, {{3, false}}).contains(range<int>({}, {3}), cmp));
    BOOST_REQUIRE(!range<int>({}, {{3, false}}).contains(range<int>({}, {4}), cmp));

    check_contains(range<int>({1}, {3}), range<int>({1}, {2}));
    check_contains(range<int>({1}, {3}), range<int>({1}, {1}));
    BOOST_REQUIRE(!range<int>({1}, {3}).contains(range<int>({2}, {4}), cmp));
    BOOST_REQUIRE(!range<int>({1}, {3}).contains(range<int>({0}, {1}), cmp));
    BOOST_REQUIRE(!range<int>({1}, {3}).contains(range<int>({0}, {4}), cmp));

    check_contains(range<int>({3}, {1}), range<int>({0}, {1}));
    check_contains(range<int>({3}, {1}), range<int>({3}, {4}));
    check_contains(range<int>({3}, {1}), range<int>({}, {1}));
    check_contains(range<int>({3}, {1}), range<int>({}, {{1, false}}));
    check_contains(range<int>({3}, {1}), range<int>({3}, {}));
    check_contains(range<int>({3}, {1}), range<int>({{3, false}}, {}));
    check_contains(range<int>({3}, {1}), range<int>({{3, false}}, {{1, false}}));
    check_contains(range<int>({3}, {1}), range<int>({{3, false}}, {1}));
    check_contains(range<int>({3}, {1}), range<int>({3}, {{1, false}}));
    BOOST_REQUIRE(!range<int>({3}, {1}).contains(range<int>({2}, {2}), cmp));
    BOOST_REQUIRE(!range<int>({3}, {1}).contains(range<int>({2}, {{3, false}}), cmp));
    BOOST_REQUIRE(!range<int>({3}, {1}).contains(range<int>({{1, false}}, {{3, false}}), cmp));
    BOOST_REQUIRE(!range<int>({3}, {1}).contains(range<int>({{1, false}}, {3}), cmp));
}

BOOST_AUTO_TEST_CASE(test_range_subtract) {
    auto cmp = [] (int i1, int i2) -> int { return i1 - i2; };
    using r = range<int>;
    using vec = std::vector<r>;

    BOOST_REQUIRE_EQUAL(r({2}, {4}).subtract(r({0}, {1}), cmp), vec({r({2}, {4})}));
    BOOST_REQUIRE_EQUAL(r({2}, {4}).subtract(r({}, {1}), cmp), vec({r({2}, {4})}));
    BOOST_REQUIRE_EQUAL(r({2}, {4}).subtract(r({}, {2}), cmp), vec({r({{2, false}}, {4})}));
    BOOST_REQUIRE_EQUAL(r({2}, {4}).subtract(r({}, {3}), cmp), vec({r({{3, false}}, {4})}));
    BOOST_REQUIRE_EQUAL(r({2}, {4}).subtract(r({}, {4}), cmp), vec());
    BOOST_REQUIRE_EQUAL(r({2}, {4}).subtract(r({1}, {4}), cmp), vec());
    BOOST_REQUIRE_EQUAL(r({2}, {4}).subtract(r({1}, {3}), cmp), vec({r({{3, false}}, {4})}));
    BOOST_REQUIRE_EQUAL(r({2}, {4}).subtract(r({1}, {{3, false}}), cmp), vec({r({3}, {4})}));
    BOOST_REQUIRE_EQUAL(r({2}, {4}).subtract(r({2}, {4}), cmp), vec());
    BOOST_REQUIRE_EQUAL(r({2}, {4}).subtract(r({}, {{4, false}}), cmp), vec({r({4}, {4})}));
    BOOST_REQUIRE_EQUAL(r({2}, {4}).subtract(r({}, {}), cmp), vec());
    BOOST_REQUIRE_EQUAL(r({2}, {4}).subtract(r({{2, false}}, {}), cmp), vec({r({2}, {2})}));
    BOOST_REQUIRE_EQUAL(r({2}, {4}).subtract(r({{2, false}}, {4}), cmp), vec({r({2}, {2})}));
    BOOST_REQUIRE_EQUAL(r({2}, {4}).subtract(r({3}, {5}), cmp), vec({r({2}, {{3, false}})}));
    BOOST_REQUIRE_EQUAL(r({2}, {4}).subtract(r({3}, {1}), cmp), vec({r({2}, {{3, false}})}));
    BOOST_REQUIRE_EQUAL(r({2}, {4}).subtract(r({5}, {1}), cmp), vec({r({2}, {4})}));
    BOOST_REQUIRE_EQUAL(r({2}, {4}).subtract(r::make_singular(3), cmp), vec({r({2}, {{3, false}}), r({{3, false}}, {4})}));
    BOOST_REQUIRE_EQUAL(r({2}, {4}).subtract(r::make_singular(4), cmp), vec({r({2}, {{4, false}})}));
    BOOST_REQUIRE_EQUAL(r({2}, {4}).subtract(r::make_singular(5), cmp), vec({r({2}, {4})}));

    BOOST_REQUIRE_EQUAL(r({}, {4}).subtract(r({3}, {5}), cmp), vec({r({}, {{3, false}})}));
    BOOST_REQUIRE_EQUAL(r({}, {4}).subtract(r({5}, {}), cmp), vec({r({}, {4})}));
    BOOST_REQUIRE_EQUAL(r({}, {4}).subtract(r({5}, {6}), cmp), vec({r({}, {4})}));
    BOOST_REQUIRE_EQUAL(r({}, {4}).subtract(r({5}, {2}), cmp), vec({r({{2, false}}, {4})}));
    BOOST_REQUIRE_EQUAL(r({4}, {}).subtract(r({3}, {5}), cmp), vec({r({{5, false}}, {})}));
    BOOST_REQUIRE_EQUAL(r({4}, {}).subtract(r({1}, {3}), cmp), vec({r({4}, {})}));
    BOOST_REQUIRE_EQUAL(r({4}, {}).subtract(r({}, {3}), cmp), vec({r({4}, {})}));
    BOOST_REQUIRE_EQUAL(r({4}, {}).subtract(r({7}, {5}), cmp), vec({r({{5, false}}, {{7, false}})}));

    BOOST_REQUIRE_EQUAL(r({5}, {1}).subtract(r({6}, {}), cmp), vec({r({}, {1}), r({5}, {{6, false}})}));
    BOOST_REQUIRE_EQUAL(r({5}, {1}).subtract(r({6}, {1}), cmp), vec({r({5}, {{6, false}})}));
    BOOST_REQUIRE_EQUAL(r({5}, {1}).subtract(r({6}, {2}), cmp), vec({r({5}, {{6, false}})}));

    // FIXME: Also accept adjacent ranges merged
    BOOST_REQUIRE_EQUAL(r({5}, {1}).subtract(r({4}, {7}), cmp), vec({r({}, {1}), r({{7, false}}, {})}));

    // FIXME: Also accept adjacent ranges merged
    BOOST_REQUIRE_EQUAL(r({5}, {1}).subtract(r({6}, {7}), cmp), vec({r({}, {1}), r({5}, {{6, false}}), r({{7, false}}, {})}));

    BOOST_REQUIRE_EQUAL(r({8}, {3}).subtract(r({2}, {1}), cmp), vec({r({{1, false}}, {{2, false}})}));
    BOOST_REQUIRE_EQUAL(r({8}, {3}).subtract(r({10}, {9}), cmp), vec({r({{9, false}}, {{10, false}})}));

    BOOST_REQUIRE_EQUAL(r({5}, {1}).subtract(r({6}, {0}), cmp), vec({r({{0, false}}, {1}), r({5}, {{6, false}})}));
    BOOST_REQUIRE_EQUAL(r({5}, {1}).subtract(r({}, {0}), cmp), vec({r({{0, false}}, {1}), r({5}, {})}));
}

struct unsigned_comparator {
    int operator()(unsigned u1, unsigned u2) const {
        return (u1 > u2 ? 1 : (u1 == u2 ? 0 : -1));
    }
};

BOOST_AUTO_TEST_CASE(range_overlap_tests) {
    unsigned min = 0;
    unsigned max = std::numeric_limits<unsigned>::max();

    auto range0 = range<unsigned>::make(max, max);
    auto range1 = range<unsigned>::make(min, max);
    BOOST_REQUIRE(range0.overlaps(range1, unsigned_comparator()) == true);
    BOOST_REQUIRE(range1.overlaps(range1, unsigned_comparator()) == true);
    BOOST_REQUIRE(range1.overlaps(range0, unsigned_comparator()) == true);

    auto range2 = range<unsigned>::make(1, max);
    BOOST_REQUIRE(range1.overlaps(range2, unsigned_comparator()) == true);

    auto range3 = range<unsigned>::make(min, max-2);
    BOOST_REQUIRE(range2.overlaps(range3, unsigned_comparator()) == true);

    auto range4 = range<unsigned>::make(2, 10);
    auto range5 = range<unsigned>::make(12, 20);
    auto range6 = range<unsigned>::make(22, 40);
    BOOST_REQUIRE(range4.overlaps(range5, unsigned_comparator()) == false);
    BOOST_REQUIRE(range5.overlaps(range4, unsigned_comparator()) == false);
    BOOST_REQUIRE(range4.overlaps(range6, unsigned_comparator()) == false);

    auto range7 = range<unsigned>::make(2, 10);
    auto range8 = range<unsigned>::make(10, 20);
    auto range9 = range<unsigned>::make(min, 100);
    BOOST_REQUIRE(range7.overlaps(range8, unsigned_comparator()) == true);
    BOOST_REQUIRE(range6.overlaps(range8, unsigned_comparator()) == false);
    BOOST_REQUIRE(range8.overlaps(range9, unsigned_comparator()) == true);

    // wrap around checks
    auto range10 = range<unsigned>::make(25, 15);
    BOOST_REQUIRE(range9.overlaps(range10, unsigned_comparator()) == true);
    BOOST_REQUIRE(range10.overlaps(range<unsigned>({20}, {18}), unsigned_comparator()) == true);
    auto range11 = range<unsigned>({}, {2});
    BOOST_REQUIRE(range11.overlaps(range10, unsigned_comparator()) == true);
    auto range12 = range<unsigned>::make(18, 20);
    BOOST_REQUIRE(range12.overlaps(range10, unsigned_comparator()) == false);

    BOOST_REQUIRE(range<unsigned>({1}, {{2, false}}).overlaps(range<unsigned>({2}, {3}), unsigned_comparator()) == false);

    // open and infinite bound checks
    BOOST_REQUIRE(range<unsigned>({1}, {}).overlaps(range<unsigned>({2}, {3}), unsigned_comparator()) == true);
    BOOST_REQUIRE(range<unsigned>({5}, {}).overlaps(range<unsigned>({2}, {3}), unsigned_comparator()) == false);
    BOOST_REQUIRE(range<unsigned>({}, {{3, false}}).overlaps(range<unsigned>({2}, {3}), unsigned_comparator()) == true);
    BOOST_REQUIRE(range<unsigned>({}, {{2, false}}).overlaps(range<unsigned>({2}, {3}), unsigned_comparator()) == false);
    BOOST_REQUIRE(range<unsigned>({}, {2}).overlaps(range<unsigned>({2}, {}), unsigned_comparator()) == true);
    BOOST_REQUIRE(range<unsigned>({}, {2}).overlaps(range<unsigned>({3}, {4}), unsigned_comparator()) == false);

    // [3,4] and [4,5]
    BOOST_REQUIRE(range<unsigned>({3}, {4}).overlaps(range<unsigned>({4}, {5}), unsigned_comparator()) == true);
    // [3,4) and [4,5]
    BOOST_REQUIRE(range<unsigned>({3}, {{4, false}}).overlaps(range<unsigned>({4}, {5}), unsigned_comparator()) == false);
    // [3,4] and (4,5]
    BOOST_REQUIRE(range<unsigned>({3}, {4}).overlaps(range<unsigned>({{4, false}}, {5}), unsigned_comparator()) == false);
    // [3,4) and (4,5]
    BOOST_REQUIRE(range<unsigned>({3}, {{4, false}}).overlaps(range<unsigned>({{4, false}}, {5}), unsigned_comparator()) == false);
}

auto get_item(std::string left, std::string right, std::string val) {
    using value_type = std::unordered_set<std::string>;
    auto l = dht::global_partitioner().from_sstring(left);
    auto r = dht::global_partitioner().from_sstring(right);
    auto rg = range<dht::token>({{l, false}}, {r});
    value_type v{val};
    return std::make_pair(locator::token_metadata::range_to_interval(rg), v);
}

BOOST_AUTO_TEST_CASE(test_range_interval_map) {
    using value_type = std::unordered_set<std::string>;
    using token = dht::token;
    boost::icl::interval_map<token, value_type> mymap;

    mymap += get_item("1", "5", "A");
    mymap += get_item("5", "8", "B");
    mymap += get_item("1", "3", "C");
    mymap += get_item("3", "8", "D");

    std::cout << "my map: " << "\n";
    for (auto x : mymap) {
        std::cout << x.first << " -> ";
        for (auto s : x.second) {
            std::cout << s << ", ";
        }
        std::cout << "\n";
    }

    auto search_item = [&mymap] (std::string val) {
        auto tok = dht::global_partitioner().from_sstring(val);
        auto search = range<token>(tok);
        auto it = mymap.find(locator::token_metadata::range_to_interval(search));
        if (it != mymap.end()) {
            std::cout << "Found OK:" << " token = " << tok << " in range: " << it->first << "\n";
            return true;
        } else {
            std::cout << "Found NO:" << " token = " << tok << "\n";
            return false;
        }
    };

    BOOST_REQUIRE(search_item("0") == false);
    BOOST_REQUIRE(search_item("1") == false);
    BOOST_REQUIRE(search_item("2") == true);
    BOOST_REQUIRE(search_item("3") == true);
    BOOST_REQUIRE(search_item("4") == true);
    BOOST_REQUIRE(search_item("5") == true);
    BOOST_REQUIRE(search_item("6") == true);
    BOOST_REQUIRE(search_item("7") == true);
    BOOST_REQUIRE(search_item("8") == true);
    BOOST_REQUIRE(search_item("9") == false);
}

BOOST_AUTO_TEST_CASE(test_split_after) {
    using b = range_bound<unsigned>;
    using wr = wrapping_range<unsigned>;
    using nwr = nonwrapping_range<unsigned>;
    auto cmp = unsigned_comparator();

    auto nwr1 = nwr(b(5), b(8));
    BOOST_REQUIRE_EQUAL(nwr1.split_after(2, cmp), nwr1);
    BOOST_REQUIRE_EQUAL(nwr1.split_after(5, cmp), nwr(b(5, false), b(8)));
    BOOST_REQUIRE_EQUAL(nwr1.split_after(6, cmp), nwr(b(6, false), b(8)));
    BOOST_REQUIRE_EQUAL(nwr1.split_after(8, cmp), stdx::nullopt);
    BOOST_REQUIRE_EQUAL(nwr1.split_after(9, cmp), stdx::nullopt);
    auto nwr2 = nwr(b(5, false), b(8, false));
    BOOST_REQUIRE_EQUAL(nwr2.split_after(2, cmp), nwr2);
    BOOST_REQUIRE_EQUAL(nwr2.split_after(5, cmp), nwr(b(5, false), b(8, false)));
    BOOST_REQUIRE_EQUAL(nwr2.split_after(6, cmp), nwr(b(6, false), b(8, false)));
    BOOST_REQUIRE_EQUAL(nwr2.split_after(8, cmp), stdx::nullopt);
    BOOST_REQUIRE_EQUAL(nwr2.split_after(9, cmp), stdx::nullopt);
    auto nwr3 = nwr(b(5, false), stdx::nullopt);
    BOOST_REQUIRE_EQUAL(nwr3.split_after(2, cmp), nwr3);
    BOOST_REQUIRE_EQUAL(nwr3.split_after(5, cmp), nwr3);
    BOOST_REQUIRE_EQUAL(nwr3.split_after(6, cmp), nwr(b(6, false), stdx::nullopt));
    auto nwr4 = nwr(stdx::nullopt, b(5, false));
    BOOST_REQUIRE_EQUAL(nwr4.split_after(2, cmp), nwr(b(2, false), b(5, false)));
    BOOST_REQUIRE_EQUAL(nwr4.split_after(5, cmp), stdx::nullopt);
    BOOST_REQUIRE_EQUAL(nwr4.split_after(6, cmp), stdx::nullopt);
    auto nwr5 = nwr(stdx::nullopt, stdx::nullopt);
    BOOST_REQUIRE_EQUAL(nwr5.split_after(2, cmp), nwr(b(2, false), stdx::nullopt));

    auto wr1 = wr(b(5), b(8));
    BOOST_REQUIRE_EQUAL(wr1.split_after(2, cmp), wr1);
    BOOST_REQUIRE_EQUAL(wr1.split_after(5, cmp), wr(b(5, false), b(8)));
    BOOST_REQUIRE_EQUAL(wr1.split_after(6, cmp), wr(b(6, false), b(8)));
    BOOST_REQUIRE_EQUAL(wr1.split_after(8, cmp), stdx::nullopt);
    BOOST_REQUIRE_EQUAL(wr1.split_after(9, cmp), stdx::nullopt);
    auto wr2 = wr(b(5, false), b(8, false));
    BOOST_REQUIRE_EQUAL(wr2.split_after(2, cmp), wr2);
    BOOST_REQUIRE_EQUAL(wr2.split_after(5, cmp), wr(b(5, false), b(8, false)));
    BOOST_REQUIRE_EQUAL(wr2.split_after(6, cmp), wr(b(6, false), b(8, false)));
    BOOST_REQUIRE_EQUAL(wr2.split_after(8, cmp), stdx::nullopt);
    BOOST_REQUIRE_EQUAL(wr2.split_after(9, cmp), stdx::nullopt);
    auto wr3 = wr(b(5, false), stdx::nullopt);
    BOOST_REQUIRE_EQUAL(wr3.split_after(2, cmp), wr3);
    BOOST_REQUIRE_EQUAL(wr3.split_after(5, cmp), wr3);
    BOOST_REQUIRE_EQUAL(wr3.split_after(6, cmp), wr(b(6, false), stdx::nullopt));
    auto wr4 = wr(stdx::nullopt, b(5, false));
    BOOST_REQUIRE_EQUAL(wr4.split_after(2, cmp), wr(b(2, false), b(5, false)));
    BOOST_REQUIRE_EQUAL(wr4.split_after(5, cmp), stdx::nullopt);
    BOOST_REQUIRE_EQUAL(wr4.split_after(6, cmp), stdx::nullopt);
    auto wr5 = wr(stdx::nullopt, stdx::nullopt);
    BOOST_REQUIRE_EQUAL(wr5.split_after(2, cmp), wr(b(2, false), stdx::nullopt));
    auto wr6 = wr(b(8), b(5));
    BOOST_REQUIRE_EQUAL(wr6.split_after(2, cmp), wr(b(2, false), b(5)));
    BOOST_REQUIRE_EQUAL(wr6.split_after(5, cmp), stdx::nullopt);
    BOOST_REQUIRE_EQUAL(wr6.split_after(6, cmp), stdx::nullopt);
    BOOST_REQUIRE_EQUAL(wr6.split_after(8, cmp), wr(b(8, false), b(5)));
    BOOST_REQUIRE_EQUAL(wr6.split_after(9, cmp), wr(b(9, false), b(5)));
}

BOOST_AUTO_TEST_CASE(test_intersection) {
    using b = range_bound<unsigned>;
    using nwr = nonwrapping_range<unsigned>;
    auto cmp = unsigned_comparator();

    auto r1 = nwr(b(5), b(10));
    auto r2 = nwr(b(1), b(4));
    BOOST_REQUIRE_EQUAL(r1.intersection(r2, cmp), stdx::nullopt);
    auto r3 = nwr(b(1), b(5));
    BOOST_REQUIRE_EQUAL(r1.intersection(r3, cmp), nwr(b(5), b(5)));
    auto r4 = nwr(b(2), b(7));
    BOOST_REQUIRE_EQUAL(r1.intersection(r4, cmp), nwr(b(5), b(7)));
    auto r5 = nwr(b(5), b(8));
    BOOST_REQUIRE_EQUAL(r1.intersection(r5, cmp), nwr(b(5), b(8)));
    auto r6 = nwr(b(6), b(8));
    BOOST_REQUIRE_EQUAL(r1.intersection(r6, cmp), nwr(b(6), b(8)));
    auto r7 = nwr(b(7), b(10));
    BOOST_REQUIRE_EQUAL(r1.intersection(r7, cmp), nwr(b(7), b(10)));
    auto r8 = nwr(b(8), b(11));
    BOOST_REQUIRE_EQUAL(r1.intersection(r8, cmp), nwr(b(8), b(10)));
    auto r9 = nwr(b(10), b(12));
    BOOST_REQUIRE_EQUAL(r1.intersection(r9, cmp), nwr(b(10), b(10)));
    auto r10 = nwr(b(12), b(20));
    BOOST_REQUIRE_EQUAL(r1.intersection(r10, cmp), stdx::nullopt);
    auto r11 = nwr(b(1), b(20));
    BOOST_REQUIRE_EQUAL(r1.intersection(r11, cmp), nwr(b(5), b(10)));

    auto r12 = nwr(b(1), b(3, false));
    BOOST_REQUIRE_EQUAL(r12.intersection(nwr(b(3, false), b(5)), cmp), stdx::nullopt);
    BOOST_REQUIRE_EQUAL(r12.intersection(nwr(b(3, false), b(5)), cmp), stdx::nullopt);
    BOOST_REQUIRE_EQUAL(r12.intersection(nwr(b(2), { }), cmp), nwr(b(2), b(3, false)));
    BOOST_REQUIRE_EQUAL(r12.intersection(nwr({ }, b(2)), cmp), nwr(b(1), b(2)));

}
