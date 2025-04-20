/*
 * Copyright (C) 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>
#include "boost/icl/interval.hpp"
#include "boost/icl/interval_map.hpp"
#include <fmt/ranges.h>
#include <unordered_set>

#include "schema/schema_builder.hh"

#include "locator/token_metadata.hh"

#include "test/lib/test_utils.hh"

using ring_position = dht::ring_position;

static
bool includes_token(const schema& s, const dht::partition_range& r, const dht::token& tok) {
    dht::ring_position_comparator cmp(s);

    return !r.before(dht::ring_position(tok, dht::ring_position::token_bound::end), cmp)
        && !r.after(dht::ring_position(tok, dht::ring_position::token_bound::start), cmp);
}

BOOST_AUTO_TEST_CASE(test_range_with_positions_within_the_same_token) {
    using bound = dht::partition_range::bound;
    auto s = schema_builder("ks", "cf")
        .with_column("key", bytes_type, column_kind::partition_key)
        .with_column("v", bytes_type)
        .build();

    dht::token tok = dht::token::get_random_token();

    auto key1 = dht::decorated_key{tok,
                                   partition_key::from_single_value(*s, bytes_type->decompose(data_value(bytes("key1"))))};

    auto key2 = dht::decorated_key{tok,
                                   partition_key::from_single_value(*s, bytes_type->decompose(data_value(bytes("key2"))))};

    {
        auto r = dht::partition_range(
            {bound(dht::ring_position(key1))},
            {bound(dht::ring_position(key2))});

        BOOST_REQUIRE(includes_token(*s, r, tok));
        BOOST_REQUIRE(r.contains(key1, dht::ring_position_comparator(*s)));
        BOOST_REQUIRE(r.contains(key2, dht::ring_position_comparator(*s)));
    }

    {
        auto r = dht::partition_range(
            {bound(dht::ring_position(key1), false)},
            {bound(dht::ring_position(key2), false)});

        BOOST_REQUIRE(includes_token(*s, r, tok));
        BOOST_REQUIRE(!r.contains(key1, dht::ring_position_comparator(*s)));
        BOOST_REQUIRE(!r.contains(key2, dht::ring_position_comparator(*s)));
    }

    {
        auto r = dht::partition_range(
            {bound(dht::ring_position::starting_at(tok), false)},
            {bound(dht::ring_position(key2), true)});

        BOOST_REQUIRE(includes_token(*s, r, tok));
        BOOST_REQUIRE(r.contains(key1, dht::ring_position_comparator(*s)));
        BOOST_REQUIRE(r.contains(key2, dht::ring_position_comparator(*s)));
    }

    {
        auto r = dht::partition_range(
            {bound(dht::ring_position::starting_at(tok), true)},
            {bound(dht::ring_position::ending_at(tok), true)});

        BOOST_REQUIRE(includes_token(*s, r, tok));
        BOOST_REQUIRE(r.contains(key1, dht::ring_position_comparator(*s)));
        BOOST_REQUIRE(r.contains(key2, dht::ring_position_comparator(*s)));
    }
}

BOOST_AUTO_TEST_CASE(test_range_with_equal_value_but_opposite_inclusiveness_is_a_full_wrap_around) {
}

BOOST_AUTO_TEST_CASE(test_range_contains) {
    auto cmp = [] (int i1, int i2) -> std::strong_ordering { return i1 <=> i2; };

    auto check_contains = [&] (interval<int> enclosing, interval<int> enclosed) {
        BOOST_REQUIRE(enclosing.contains(enclosed, cmp));
        BOOST_REQUIRE(!enclosed.contains(enclosing, cmp));
    };

    BOOST_REQUIRE(interval<int>({}, {}).contains(interval<int>({}, {}), cmp));
    check_contains(interval<int>({}, {}), interval<int>({1}, {2}));
    check_contains(interval<int>({}, {}), interval<int>({}, {2}));
    check_contains(interval<int>({}, {}), interval<int>({1}, {}));
    check_contains(interval<int>({}, {}), interval<int>({2}, {1}));

    BOOST_REQUIRE(interval<int>({}, {3}).contains(interval<int>({}, {3}), cmp));
    BOOST_REQUIRE(interval<int>({3}, {}).contains(interval<int>({3}, {}), cmp));
    BOOST_REQUIRE(interval<int>({}, {{3, false}}).contains(interval<int>({}, {{3, false}}), cmp));
    BOOST_REQUIRE(interval<int>({{3, false}}, {}).contains(interval<int>({{3, false}}, {}), cmp));
    BOOST_REQUIRE(interval<int>({1}, {3}).contains(interval<int>({1}, {3}), cmp));
    BOOST_REQUIRE(interval<int>({3}, {1}).contains(interval<int>({3}, {1}), cmp));

    check_contains(interval<int>({}, {3}), interval<int>({}, {2}));
    check_contains(interval<int>({}, {3}), interval<int>({2}, {{3, false}}));
    BOOST_REQUIRE(!interval<int>({}, {3}).contains(interval<int>({}, {4}), cmp));
    BOOST_REQUIRE(!interval<int>({}, {3}).contains(interval<int>({2}, {{4, false}}), cmp));
    BOOST_REQUIRE(!interval<int>({}, {3}).contains(interval<int>({}, {}), cmp));

    check_contains(interval<int>({3}, {}), interval<int>({4}, {}));
    check_contains(interval<int>({3}, {}), interval<int>({{3, false}}, {}));
    check_contains(interval<int>({3}, {}), interval<int>({3}, {4}));
    check_contains(interval<int>({3}, {}), interval<int>({4}, {5}));
    BOOST_REQUIRE(!interval<int>({3}, {}).contains(interval<int>({2}, {4}), cmp));
    BOOST_REQUIRE(!interval<int>({3}, {}).contains(interval<int>({}, {}), cmp));

    check_contains(interval<int>({}, {{3, false}}), interval<int>({}, {2}));
    BOOST_REQUIRE(!interval<int>({}, {{3, false}}).contains(interval<int>({}, {3}), cmp));
    BOOST_REQUIRE(!interval<int>({}, {{3, false}}).contains(interval<int>({}, {4}), cmp));

    check_contains(interval<int>({1}, {3}), interval<int>({1}, {2}));
    check_contains(interval<int>({1}, {3}), interval<int>({1}, {1}));
    BOOST_REQUIRE(!interval<int>({1}, {3}).contains(interval<int>({2}, {4}), cmp));
    BOOST_REQUIRE(!interval<int>({1}, {3}).contains(interval<int>({0}, {1}), cmp));
    BOOST_REQUIRE(!interval<int>({1}, {3}).contains(interval<int>({0}, {4}), cmp));
}

BOOST_AUTO_TEST_CASE(test_range_subtract) {
    auto cmp = [] (int i1, int i2) -> std::strong_ordering { return i1 <=> i2; };
    using r = interval<int>;
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
    BOOST_REQUIRE_EQUAL(r({2}, {4}).subtract(r::make_singular(3), cmp), vec({r({2}, {{3, false}}), r({{3, false}}, {4})}));
    BOOST_REQUIRE_EQUAL(r({2}, {4}).subtract(r::make_singular(4), cmp), vec({r({2}, {{4, false}})}));
    BOOST_REQUIRE_EQUAL(r({2}, {4}).subtract(r::make_singular(5), cmp), vec({r({2}, {4})}));

    BOOST_REQUIRE_EQUAL(r({}, {4}).subtract(r({3}, {5}), cmp), vec({r({}, {{3, false}})}));
    BOOST_REQUIRE_EQUAL(r({}, {4}).subtract(r({5}, {}), cmp), vec({r({}, {4})}));
    BOOST_REQUIRE_EQUAL(r({}, {4}).subtract(r({5}, {6}), cmp), vec({r({}, {4})}));
    BOOST_REQUIRE_EQUAL(r({4}, {}).subtract(r({3}, {5}), cmp), vec({r({{5, false}}, {})}));
    BOOST_REQUIRE_EQUAL(r({4}, {}).subtract(r({1}, {3}), cmp), vec({r({4}, {})}));
    BOOST_REQUIRE_EQUAL(r({4}, {}).subtract(r({}, {3}), cmp), vec({r({4}, {})}));

    BOOST_REQUIRE_EQUAL(r({5}, {1}).subtract(r({6}, {}), cmp), vec({r({}, {1}), r({5}, {{6, false}})}));
    BOOST_REQUIRE_EQUAL(r({5}, {1}).subtract(r({6}, {1}), cmp), vec({r({5}, {{6, false}})}));
    BOOST_REQUIRE_EQUAL(r({5}, {1}).subtract(r({6}, {2}), cmp), vec({r({5}, {{6, false}})}));

    // FIXME: Also accept adjacent ranges merged
    BOOST_REQUIRE_EQUAL(r({5}, {1}).subtract(r({4}, {7}), cmp), vec({r({}, {1}), r({{7, false}}, {})}));

    // FIXME: Also accept adjacent ranges merged
    BOOST_REQUIRE_EQUAL(r({5}, {1}).subtract(r({6}, {7}), cmp), vec({r({}, {1}), r({5}, {{6, false}}), r({{7, false}}, {})}));

    BOOST_REQUIRE_EQUAL(r({5}, {1}).subtract(r({}, {0}), cmp), vec({r({{0, false}}, {1}), r({5}, {})}));
}

struct unsigned_comparator {
    std::strong_ordering operator()(unsigned u1, unsigned u2) const {
        return u1 <=> u2;
    }
};

BOOST_AUTO_TEST_CASE(range_overlap_tests) {
    unsigned min = 0;
    unsigned max = std::numeric_limits<unsigned>::max();

    auto range0 = interval<unsigned>({max}, {max});
    auto range1 = interval<unsigned>({min}, {max});
    BOOST_REQUIRE(range0.overlaps(range1, unsigned_comparator()) == true);
    BOOST_REQUIRE(range1.overlaps(range1, unsigned_comparator()) == true);
    BOOST_REQUIRE(range1.overlaps(range0, unsigned_comparator()) == true);

    auto range2 = interval<unsigned>({1}, {max});
    BOOST_REQUIRE(range1.overlaps(range2, unsigned_comparator()) == true);

    auto range3 = interval<unsigned>({min}, {max-2});
    BOOST_REQUIRE(range2.overlaps(range3, unsigned_comparator()) == true);

    auto range4 = interval<unsigned>({2}, {10});
    auto range5 = interval<unsigned>({12}, {20});
    auto range6 = interval<unsigned>({22}, {40});
    BOOST_REQUIRE(range4.overlaps(range5, unsigned_comparator()) == false);
    BOOST_REQUIRE(range5.overlaps(range4, unsigned_comparator()) == false);
    BOOST_REQUIRE(range4.overlaps(range6, unsigned_comparator()) == false);

    auto range7 = interval<unsigned>({2}, {10});
    auto range8 = interval<unsigned>({10}, {20});
    auto range9 = interval<unsigned>({min}, {100});
    BOOST_REQUIRE(range7.overlaps(range8, unsigned_comparator()) == true);
    BOOST_REQUIRE(range6.overlaps(range8, unsigned_comparator()) == false);
    BOOST_REQUIRE(range8.overlaps(range9, unsigned_comparator()) == true);

    BOOST_REQUIRE(interval<unsigned>({1}, {{2, false}}).overlaps(interval<unsigned>({2}, {3}), unsigned_comparator()) == false);

    // open and infinite bound checks
    BOOST_REQUIRE(interval<unsigned>({1}, {}).overlaps(interval<unsigned>({2}, {3}), unsigned_comparator()) == true);
    BOOST_REQUIRE(interval<unsigned>({5}, {}).overlaps(interval<unsigned>({2}, {3}), unsigned_comparator()) == false);
    BOOST_REQUIRE(interval<unsigned>({}, {{3, false}}).overlaps(interval<unsigned>({2}, {3}), unsigned_comparator()) == true);
    BOOST_REQUIRE(interval<unsigned>({}, {{2, false}}).overlaps(interval<unsigned>({2}, {3}), unsigned_comparator()) == false);
    BOOST_REQUIRE(interval<unsigned>({}, {2}).overlaps(interval<unsigned>({2}, {}), unsigned_comparator()) == true);
    BOOST_REQUIRE(interval<unsigned>({}, {2}).overlaps(interval<unsigned>({3}, {4}), unsigned_comparator()) == false);

    // [3,4] and [4,5]
    BOOST_REQUIRE(interval<unsigned>({3}, {4}).overlaps(interval<unsigned>({4}, {5}), unsigned_comparator()) == true);
    // [3,4) and [4,5]
    BOOST_REQUIRE(interval<unsigned>({3}, {{4, false}}).overlaps(interval<unsigned>({4}, {5}), unsigned_comparator()) == false);
    // [3,4] and (4,5]
    BOOST_REQUIRE(interval<unsigned>({3}, {4}).overlaps(interval<unsigned>({{4, false}}, {5}), unsigned_comparator()) == false);
    // [3,4) and (4,5]
    BOOST_REQUIRE(interval<unsigned>({3}, {{4, false}}).overlaps(interval<unsigned>({{4, false}}, {5}), unsigned_comparator()) == false);
}

auto get_item(std::string left, std::string right, std::string val) {
    using value_type = std::unordered_set<std::string>;
    auto l = dht::token::from_sstring(left);
    auto r = dht::token::from_sstring(right);
    auto rg = dht::token_range({{l, false}}, {r});
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
        auto tok = dht::token::from_sstring(val);
        auto search = dht::token_range(tok);
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

BOOST_AUTO_TEST_CASE(range_deoverlap_tests) {
    using ranges = std::vector<interval<unsigned>>;

    {
        ranges rs = { interval<unsigned>({1}, {4}), interval<unsigned>({2}, {5}) };
        auto deoverlapped = interval<unsigned>::deoverlap(std::move(rs), unsigned_comparator());
        BOOST_REQUIRE_EQUAL(1, deoverlapped.size());
        BOOST_REQUIRE_EQUAL(interval<unsigned>({1}, {5}), deoverlapped[0]);
    }

    {
        ranges rs = { interval<unsigned>({1}, {4}), interval<unsigned>({4}, {5}) };
        auto deoverlapped = interval<unsigned>::deoverlap(std::move(rs), unsigned_comparator());
        BOOST_REQUIRE_EQUAL(1, deoverlapped.size());
        BOOST_REQUIRE_EQUAL(interval<unsigned>({1}, {5}), deoverlapped[0]);
    }

    {
        ranges rs = { interval<unsigned>({2}, {4}), interval<unsigned>({1}, {3}) };
        auto deoverlapped = interval<unsigned>::deoverlap(std::move(rs), unsigned_comparator());
        BOOST_REQUIRE_EQUAL(1, deoverlapped.size());
        BOOST_REQUIRE_EQUAL(interval<unsigned>({1}, {4}), deoverlapped[0]);
    }

    {
        ranges rs = { interval<unsigned>({1}, {4}), interval<unsigned>({0}, {5}), interval<unsigned>({7}, {12}), interval<unsigned>({8}, {10}) };
        auto deoverlapped = interval<unsigned>::deoverlap(std::move(rs), unsigned_comparator());
        BOOST_REQUIRE_EQUAL(2, deoverlapped.size());
        BOOST_REQUIRE_EQUAL(interval<unsigned>({0}, {5}), deoverlapped[0]);
        BOOST_REQUIRE_EQUAL(interval<unsigned>({7}, {12}), deoverlapped[1]);
    }

    {
        ranges rs = { interval<unsigned>({1}, {4}), interval<unsigned>({2}, { }) };
        auto deoverlapped = interval<unsigned>::deoverlap(std::move(rs), unsigned_comparator());
        BOOST_REQUIRE_EQUAL(1, deoverlapped.size());
        BOOST_REQUIRE_EQUAL(interval<unsigned>({1}, { }), deoverlapped[0]);
    }

    {
        ranges rs = { interval<unsigned>({ }, {4}), interval<unsigned>({3}, {5}) };
        auto deoverlapped = interval<unsigned>::deoverlap(std::move(rs), unsigned_comparator());
        BOOST_REQUIRE_EQUAL(1, deoverlapped.size());
        BOOST_REQUIRE_EQUAL(interval<unsigned>({ }, {5}), deoverlapped[0]);
    }

    {
        ranges rs = { interval<unsigned>({14}, { }), interval<unsigned>({2}, { }) };
        auto deoverlapped = interval<unsigned>::deoverlap(std::move(rs), unsigned_comparator());
        BOOST_REQUIRE_EQUAL(1, deoverlapped.size());
        BOOST_REQUIRE_EQUAL(interval<unsigned>({2}, { }), deoverlapped[0]);
    }

    {
        ranges rs = { interval<unsigned>({2}, { }), interval<unsigned>({12}, { }) };
        auto deoverlapped = interval<unsigned>::deoverlap(std::move(rs), unsigned_comparator());
        BOOST_REQUIRE_EQUAL(1, deoverlapped.size());
        BOOST_REQUIRE_EQUAL(interval<unsigned>({2}, { }), deoverlapped[0]);
    }

    {
        ranges rs = { interval<unsigned>({3}, {{4, false}}), interval<unsigned>({{4, false}}, {5}) };
        auto deoverlapped = interval<unsigned>::deoverlap(std::move(rs), unsigned_comparator());
        BOOST_REQUIRE_EQUAL(2, deoverlapped.size());
        BOOST_REQUIRE_EQUAL(interval<unsigned>({3}, {{4, false}}), deoverlapped[0]);
        BOOST_REQUIRE_EQUAL(interval<unsigned>({{4, false}}, {5}), deoverlapped[1]);
    }

    {
        ranges rs = { interval<unsigned>({3}, {{4, false}}), interval<unsigned>({4}, {5}) };
        auto deoverlapped = interval<unsigned>::deoverlap(std::move(rs), unsigned_comparator());
        BOOST_REQUIRE_EQUAL(2, deoverlapped.size());
        BOOST_REQUIRE_EQUAL(interval<unsigned>({3}, {{4, false}}), deoverlapped[0]);
        BOOST_REQUIRE_EQUAL(interval<unsigned>({4}, {5}), deoverlapped[1]);
    }

    {
        ranges rs = { interval<unsigned>({3}, {4}), interval<unsigned>({{4, false}}, {5}) };
        auto deoverlapped = interval<unsigned>::deoverlap(std::move(rs), unsigned_comparator());
        BOOST_REQUIRE_EQUAL(2, deoverlapped.size());
        BOOST_REQUIRE_EQUAL(interval<unsigned>({3}, {4}), deoverlapped[0]);
        BOOST_REQUIRE_EQUAL(interval<unsigned>({{4, false}}, {5}), deoverlapped[1]);
    }
}
