/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#define BOOST_TEST_MODULE interval_index

#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <limits>
#include <random>
#include <stdexcept>
#include <vector>

#include "utils/interval_index.hh"

using key_type = uint64_t;
static constexpr key_type max_key = std::numeric_limits<key_type>::max();

// The behavior the index is expected to have, computed the obvious way.
class reference {
public:
    struct entry {
        key_type start;
        key_type end;
        int value;
        auto operator<=>(const entry&) const = default;
    };
private:
    std::vector<entry> _entries;
public:
    void insert(key_type start, key_type end, int value) {
        _entries.push_back({start, end, value});
    }
    bool erase(key_type start, key_type end, int value) {
        auto it = std::ranges::find(_entries, entry{start, end, value});
        if (it == _entries.end()) {
            return false;
        }
        _entries.erase(it);
        return true;
    }
    size_t size() const { return _entries.size(); }
    std::vector<entry> overlapping(key_type low, key_type high) const {
        std::vector<entry> ret;
        for (auto& e : _entries) {
            if (e.start <= high && e.end >= low) {
                ret.push_back(e);
            }
        }
        std::ranges::sort(ret);
        return ret;
    }
    std::vector<entry> all() const {
        auto ret = _entries;
        std::ranges::sort(ret);
        return ret;
    }
    // The smallest position after pos at which the set of covering intervals
    // differs from the one at pos, if any.
    std::optional<key_type> change_at(key_type pos) const {
        std::optional<key_type> ret;
        auto candidate = [&] (key_type k) {
            if (k > pos && (!ret || k < *ret)) {
                ret = k;
            }
        };
        for (auto& e : _entries) {
            candidate(e.start);
            if (e.end != max_key) {
                candidate(e.end + 1);
            }
        }
        return ret;
    }
};

template <size_t block_size>
static std::vector<reference::entry> overlapping(const utils::interval_index<key_type, int, block_size>& index,
        key_type low, key_type high) {
    std::vector<reference::entry> ret;
    index.for_each_overlapping(low, high, [&] (key_type start, key_type end, const int& value) {
        ret.push_back({start, end, value});
    });
    std::ranges::sort(ret);
    return ret;
}

template <size_t block_size>
static std::vector<reference::entry> all(const utils::interval_index<key_type, int, block_size>& index) {
    std::vector<reference::entry> ret;
    index.for_each([&] (key_type start, key_type end, const int& value) {
        ret.push_back({start, end, value});
    });
    std::ranges::sort(ret);
    return ret;
}

BOOST_AUTO_TEST_CASE(test_empty) {
    utils::interval_index<key_type, int> index;
    BOOST_REQUIRE(index.empty());
    BOOST_REQUIRE_EQUAL(index.size(), 0u);
    BOOST_REQUIRE(index.invariants_hold());
    BOOST_REQUIRE(overlapping(index, 0, max_key).empty());
    BOOST_REQUIRE(!index.erase(1, 2, 3));

    auto c = index.make_cursor();
    c.seek(17);
    BOOST_REQUIRE(!c.covered());
    BOOST_REQUIRE(!c.change_at());
    c.advance_to(100);
    BOOST_REQUIRE(!c.covered());
}

BOOST_AUTO_TEST_CASE(test_single_entry) {
    utils::interval_index<key_type, int> index;
    index.insert(10, 20, 7);
    BOOST_REQUIRE_EQUAL(index.size(), 1u);
    BOOST_REQUIRE(index.invariants_hold());

    BOOST_REQUIRE(overlapping(index, 0, 9).empty());
    BOOST_REQUIRE(overlapping(index, 21, max_key).empty());
    for (key_type p : {key_type(10), key_type(15), key_type(20)}) {
        auto r = overlapping(index, p, p);
        BOOST_REQUIRE_EQUAL(r.size(), 1u);
        BOOST_REQUIRE(r[0] == (reference::entry{10, 20, 7}));
    }
    BOOST_REQUIRE_EQUAL(overlapping(index, 0, max_key).size(), 1u);
    // An empty query range yields nothing.
    BOOST_REQUIRE(overlapping(index, 20, 10).empty());

    BOOST_REQUIRE(!index.erase(10, 20, 8));
    BOOST_REQUIRE(!index.erase(10, 21, 7));
    BOOST_REQUIRE(index.erase(10, 20, 7));
    BOOST_REQUIRE(index.empty());
    BOOST_REQUIRE(index.invariants_hold());
}

BOOST_AUTO_TEST_CASE(test_extreme_keys) {
    utils::interval_index<key_type, int> index;
    index.insert(0, 0, 1);
    index.insert(0, max_key, 2);
    index.insert(max_key, max_key, 3);
    BOOST_REQUIRE(index.invariants_hold());

    BOOST_REQUIRE_EQUAL(overlapping(index, 0, 0).size(), 2u);
    BOOST_REQUIRE_EQUAL(overlapping(index, max_key, max_key).size(), 2u);
    BOOST_REQUIRE_EQUAL(overlapping(index, 1, max_key - 1).size(), 1u);
    BOOST_REQUIRE_EQUAL(overlapping(index, 0, max_key).size(), 3u);

    auto c = index.make_cursor();
    c.seek(max_key);
    BOOST_REQUIRE_EQUAL(std::ranges::distance(c.covering()), 2);
    // Nothing changes after the last position.
    BOOST_REQUIRE(!c.change_at());
}

// Entries sharing a start, in numbers which spill over block boundaries, are
// the case where an entry may not belong to the block its start would suggest.
BOOST_AUTO_TEST_CASE(test_equal_starts_across_blocks) {
    constexpr size_t block_size = 4;
    utils::interval_index<key_type, int, block_size> index;
    reference ref;
    // Insert with decreasing ends, so that each entry belongs before all the
    // ones already inserted.
    for (int i = 40; i > 0; i--) {
        index.insert(5, 5 + i, i);
        ref.insert(5, 5 + i, i);
        BOOST_REQUIRE(index.invariants_hold());
    }
    BOOST_REQUIRE(all(index) == ref.all());
    for (key_type p : {key_type(4), key_type(5), key_type(6), key_type(20), key_type(45), key_type(46)}) {
        BOOST_REQUIRE(overlapping(index, p, p) == ref.overlapping(p, p));
    }
    for (int i = 1; i <= 40; i++) {
        BOOST_REQUIRE(index.erase(5, 5 + i, i));
        BOOST_REQUIRE(ref.erase(5, 5 + i, i));
        BOOST_REQUIRE(index.invariants_hold());
        BOOST_REQUIRE(all(index) == ref.all());
    }
    BOOST_REQUIRE(index.empty());
}

BOOST_AUTO_TEST_CASE(test_duplicate_entries) {
    utils::interval_index<key_type, int> index;
    for (int i = 0; i < 10; i++) {
        index.insert(1, 2, 3);
    }
    BOOST_REQUIRE_EQUAL(index.size(), 10u);
    for (int i = 0; i < 10; i++) {
        BOOST_REQUIRE_EQUAL(overlapping(index, 1, 1).size(), size_t(10 - i));
        BOOST_REQUIRE(index.erase(1, 2, 3));
        BOOST_REQUIRE(index.invariants_hold());
    }
    BOOST_REQUIRE(!index.erase(1, 2, 3));
}

BOOST_AUTO_TEST_CASE(test_copy_is_independent) {
    utils::interval_index<key_type, int> index;
    for (int i = 0; i < 100; i++) {
        index.insert(i * 10, i * 10 + 25, i);
    }
    auto copy = index;
    BOOST_REQUIRE(copy.invariants_hold());
    BOOST_REQUIRE(all(copy) == all(index));

    for (int i = 0; i < 50; i++) {
        BOOST_REQUIRE(copy.erase(i * 10, i * 10 + 25, i));
    }
    BOOST_REQUIRE_EQUAL(copy.size(), 50u);
    BOOST_REQUIRE_EQUAL(index.size(), 100u);
    BOOST_REQUIRE(copy.invariants_hold());
    BOOST_REQUIRE(index.invariants_hold());
    BOOST_REQUIRE_EQUAL(overlapping(index, 0, 25).size(), 3u);
    BOOST_REQUIRE(overlapping(copy, 0, 25).empty());
}

// A few intervals spanning most of the key space, which is what the block
// summaries are meant to cope with.
BOOST_AUTO_TEST_CASE(test_wide_intervals) {
    constexpr size_t block_size = 8;
    utils::interval_index<key_type, int, block_size> index;
    reference ref;
    int value = 0;
    for (int i = 0; i < 200; i++) {
        // Every eighth interval spans nearly everything, so that every block
        // holds one.
        key_type start = i * 1000;
        key_type end = i % 8 == 0 ? 1000000 : start + 500;
        index.insert(start, end, value);
        ref.insert(start, end, value);
        value++;
    }
    BOOST_REQUIRE(index.invariants_hold());
    for (key_type p = 0; p <= 200000; p += 137) {
        BOOST_REQUIRE(overlapping(index, p, p) == ref.overlapping(p, p));
    }
    BOOST_REQUIRE(overlapping(index, 999999, 1000000) == ref.overlapping(999999, 1000000));
}

// Compares the cursor against the reference at an increasing sequence of
// positions, which needn't be the positions at which the covering set changes:
// the cursor may be advanced by more than that, skipping over intervals.
template <size_t block_size>
static void check_positions(const utils::interval_index<key_type, int, block_size>& index, const reference& ref,
        const std::vector<key_type>& positions, bool use_seek) {
    auto c = index.make_cursor();
    for (auto pos : positions) {
        if (use_seek) {
            c.seek(pos);
        } else if (pos == positions.front()) {
            c.seek(pos);
        } else {
            c.advance_to(pos);
        }
        auto covering = std::vector<int>(c.covering().begin(), c.covering().end());
        std::ranges::sort(covering);
        std::vector<int> expected;
        for (auto& e : ref.overlapping(pos, pos)) {
            expected.push_back(e.value);
        }
        std::ranges::sort(expected);
        BOOST_REQUIRE_EQUAL(c.position(), pos);
        BOOST_REQUIRE(covering == expected);
        BOOST_REQUIRE(c.change_at() == ref.change_at(pos));
        // Whatever the cursor reports as the next change must be after the
        // position, or the range the caller derives from it is empty or
        // inverted.
        if (auto change = c.change_at()) {
            BOOST_REQUIRE_GT(*change, pos);
        }
    }
}

// Compares a sweep of the whole key space against the reference, at every
// position at which the covering set is expected to change.
template <size_t block_size>
static void check_sweep(const utils::interval_index<key_type, int, block_size>& index, const reference& ref) {
    auto c = index.make_cursor();
    c.seek(0);
    key_type pos = 0;
    while (true) {
        auto covering = std::vector<int>(c.covering().begin(), c.covering().end());
        std::ranges::sort(covering);
        auto expected_entries = ref.overlapping(pos, pos);
        std::vector<int> expected;
        for (auto& e : expected_entries) {
            expected.push_back(e.value);
        }
        std::ranges::sort(expected);
        BOOST_REQUIRE_EQUAL(c.position(), pos);
        BOOST_REQUIRE(covering == expected);
        BOOST_REQUIRE(c.covered() == !covering.empty());

        auto next = c.change_at();
        BOOST_REQUIRE(next == ref.change_at(pos));
        if (!next) {
            break;
        }
        // Alternate between advancing and seeking, both of which must land on
        // the same state.
        pos = *next;
        if (pos % 2) {
            c.advance_to(pos);
        } else {
            c.seek(pos);
        }
    }
}

BOOST_AUTO_TEST_CASE(test_sweep) {
    utils::interval_index<key_type, int, 4> index;
    reference ref;
    auto add = [&] (key_type start, key_type end, int value) {
        index.insert(start, end, value);
        ref.insert(start, end, value);
    };
    // Nested, overlapping, touching, disjoint and repeated intervals.
    add(10, 20, 1);
    add(10, 20, 2);
    add(12, 14, 3);
    add(20, 21, 4);
    add(22, 30, 5);
    add(0, 100, 6);
    add(50, 50, 7);
    add(51, 60, 8);
    BOOST_REQUIRE(index.invariants_hold());
    check_sweep(index, ref);
}

// Advancing past the end of an interval must not leave it in the covering set.
// A sweep opens an interval exactly when its start is reached, so only a jump
// can present the cursor with an interval which is already over.
BOOST_AUTO_TEST_CASE(test_advance_past_intervals) {
    utils::interval_index<key_type, int, 4> index;
    reference ref;
    auto add = [&] (key_type start, key_type end, int value) {
        index.insert(start, end, value);
        ref.insert(start, end, value);
    };
    add(10, 20, 1);
    add(15, 16, 2);
    add(30, 40, 3);
    add(35, 100, 4);
    add(50, 60, 5);

    // 25 is past [10,20] and [15,16], which both start before it.
    check_positions(index, ref, {0, 25}, false);
    // 45 is past [30,40], and inside [35,100].
    check_positions(index, ref, {0, 45}, false);
    // Straight to a position past everything but [35,100].
    check_positions(index, ref, {0, 70}, false);
    // Past everything.
    check_positions(index, ref, {0, 1000}, false);
    // The same by seeking rather than advancing.
    check_positions(index, ref, {0, 25, 45, 70, 1000}, true);
    // Advancing in one step to a position covered by nothing, then on.
    check_positions(index, ref, {25, 45, 70}, false);
}

BOOST_AUTO_TEST_CASE(test_sweep_of_empty_index) {
    utils::interval_index<key_type, int> index;
    reference ref;
    check_sweep(index, ref);
}

// Random operations against the reference, over a key space small enough for
// intervals to overlap heavily and to share bounds.
template <size_t block_size>
static void random_operations(unsigned seed, key_type key_space, unsigned iterations) {
    std::mt19937 rng(seed);
    utils::interval_index<key_type, int, block_size> index;
    reference ref;
    std::vector<reference::entry> live;
    int value = 0;

    auto random_key = [&] { return std::uniform_int_distribution<key_type>(0, key_space)(rng); };

    for (unsigned i = 0; i < iterations; i++) {
        auto op = std::uniform_int_distribution<int>(0, 9)(rng);
        if (op < 6 || live.empty()) {
            auto a = random_key();
            auto b = random_key();
            auto e = reference::entry{std::min(a, b), std::max(a, b), value++};
            index.insert(e.start, e.end, e.value);
            ref.insert(e.start, e.end, e.value);
            live.push_back(e);
        } else if (op < 9) {
            auto& e = live[std::uniform_int_distribution<size_t>(0, live.size() - 1)(rng)];
            BOOST_REQUIRE(index.erase(e.start, e.end, e.value));
            BOOST_REQUIRE(ref.erase(e.start, e.end, e.value));
            e = live.back();
            live.pop_back();
        } else {
            // Erasing something which isn't there must not disturb the index.
            auto a = random_key();
            BOOST_REQUIRE(!index.erase(a, a, -1));
        }
        BOOST_REQUIRE(index.invariants_hold());
        BOOST_REQUIRE_EQUAL(index.size(), ref.size());
        BOOST_REQUIRE(all(index) == ref.all());

        // Point and range queries, including ones outside the key space.
        for (int q = 0; q < 4; q++) {
            auto a = random_key();
            auto b = random_key();
            BOOST_REQUIRE(overlapping(index, a, a) == ref.overlapping(a, a));
            BOOST_REQUIRE(overlapping(index, std::min(a, b), std::max(a, b))
                    == ref.overlapping(std::min(a, b), std::max(a, b)));
        }
        BOOST_REQUIRE(overlapping(index, 0, max_key) == ref.overlapping(0, max_key));
        // A query entirely above the key space, when there is room for one:
        // if the key space is the whole of the key type there is none, and
        // key_space + 1 would wrap round to 0.
        if (key_space < max_key) {
            BOOST_REQUIRE(overlapping(index, key_space + 1, max_key) == ref.overlapping(key_space + 1, max_key));
        }
    }
    check_sweep(index, ref);

    // Positions which skip over intervals, which a sweep never does. They
    // must increase, as the cursor is only ever advanced forward.
    for (unsigned attempt = 0; attempt < 8; attempt++) {
        std::vector<key_type> positions;
        for (int i = 0; i < 12; i++) {
            positions.push_back(random_key());
        }
        positions.push_back(0);
        positions.push_back(key_space);
        positions.push_back(max_key);
        std::ranges::sort(positions);
        positions.erase(std::unique(positions.begin(), positions.end()), positions.end());
        check_positions(index, ref, positions, attempt % 2);
    }
}

BOOST_AUTO_TEST_CASE(test_random_operations_dense_keys) {
    // A key space this small makes shared bounds and duplicate intervals common.
    random_operations<4>(1, 20, 300);
    random_operations<32>(2, 20, 300);
}

BOOST_AUTO_TEST_CASE(test_random_operations_sparse_keys) {
    random_operations<4>(3, 1000000, 200);
    random_operations<8>(4, 1000000, 200);
    random_operations<64>(5, 1000000, 200);
}

BOOST_AUTO_TEST_CASE(test_random_operations_wide_key_space) {
    random_operations<32>(6, max_key, 200);
}

// The bound the block summaries are there for: a block is never looked inside
// without yielding a match, save for the one block the queried position falls
// in.
BOOST_AUTO_TEST_CASE(test_blocks_are_not_examined_in_vain) {
    constexpr size_t block_size = 32;
    utils::interval_index<key_type, int, block_size> index;
    std::mt19937 rng(7);
    // Narrow intervals spread over the key space, plus a few wide ones.
    for (int i = 0; i < 4000; i++) {
        key_type start = std::uniform_int_distribution<key_type>(0, 1000000)(rng);
        key_type end = start + (i % 500 == 0 ? 900000 : 100);
        index.insert(start, end, i);
    }
    BOOST_REQUIRE(index.invariants_hold());
    BOOST_REQUIRE_GT(index.block_count(), 100u);
    for (key_type p = 0; p <= 1000000; p += 9973) {
        size_t matches = 0;
        index.for_each_overlapping(p, p, [&] (key_type, key_type, const int&) { matches++; });
        BOOST_REQUIRE_LE(index.blocks_examined(p, p), matches + 1);
        // Same for a range query.
        size_t range_matches = 0;
        index.for_each_overlapping(p, p + 50000, [&] (key_type, key_type, const int&) { range_matches++; });
        BOOST_REQUIRE_LE(index.blocks_examined(p, p + 50000), range_matches + 1);
    }
    // The index must be far more compact than one interval per block.
    BOOST_REQUIRE_LE(index.block_count(), index.size() / (block_size / 4));
}

// Exception safety.
//
// Each operation is run over and over, with a different one of the points at
// which it can fail failing each time, and the index is checked after every
// failure.
//
// The failure points are the construction and the copying of a value. The
// index also allocates, but only through chunked_vector, which takes its
// chunks from malloc() directly, so an allocation failure cannot be injected
// without overriding malloc() for the whole test, which isn't worth it. What
// is covered is therefore every operation which handles a value: the
// construction of the argument of insert() and of erase(), the copying of
// every value when the index is copied, and the copying of the values a
// cursor opens.

namespace injection {

// Failure points left to let pass before failing one; negative disables
// injection. Injecting disarms the counter, so that the unwinding which
// follows a failure doesn't run into another one.
long countdown = -1;

static bool fail_now() noexcept {
    if (countdown < 0) {
        return false;
    }
    if (countdown == 0) {
        countdown = -1;
        return true;
    }
    countdown--;
    return false;
}

// Whether the last armed run injected a failure. False once the counter has
// outrun the operation's failure points, which is what ends the walk.
static bool injected() noexcept {
    return countdown < 0;
}

}

// A value which fails to be constructed and to be copied, but not to be
// moved, which the index requires of it. Comparing must not fail either, as
// erase() is noexcept.
class throwing_value {
    int _v = 0;
public:
    explicit throwing_value(int v) : _v(v) {
        if (injection::fail_now()) {
            throw std::runtime_error("injected value construction failure");
        }
    }
    throwing_value(const throwing_value& o) : _v(o._v) {
        if (injection::fail_now()) {
            throw std::runtime_error("injected value copy failure");
        }
    }
    throwing_value(throwing_value&&) noexcept = default;
    throwing_value& operator=(const throwing_value&) = default;
    throwing_value& operator=(throwing_value&&) noexcept = default;
    ~throwing_value() = default;
    int value() const noexcept { return _v; }
    bool operator==(const throwing_value&) const noexcept = default;
};

// A small block size, so that blocks split and merge often.
using throwing_index = utils::interval_index<key_type, throwing_value, 4>;

// The queries copy each value they are handed, so that they have a failure
// point of their own: a query must be unaffected by a callback which throws.
static std::vector<reference::entry> all(const throwing_index& index) {
    std::vector<reference::entry> ret;
    index.for_each([&] (key_type start, key_type end, const throwing_value& value) {
        ret.push_back({start, end, throwing_value(value).value()});
    });
    std::ranges::sort(ret);
    return ret;
}

static std::vector<reference::entry> overlapping(const throwing_index& index, key_type low, key_type high) {
    std::vector<reference::entry> ret;
    index.for_each_overlapping(low, high, [&] (key_type start, key_type end, const throwing_value& value) {
        ret.push_back({start, end, throwing_value(value).value()});
    });
    std::ranges::sort(ret);
    return ret;
}

// Runs op() once for each of its failure points, failing that one, and calls
// check() after each failure. Returns once the operation has run through with
// no failure injected, i.e. once every failure point has been covered.
template <typename Op, typename Check>
static void with_each_failure(const throwing_index& index, Op&& op, Check&& check) {
    for (long k = 0; ; k++) {
        injection::countdown = k;
        bool failed = false;
        try {
            op();
        } catch (const std::runtime_error&) {
            failed = true;
        }
        bool injected = injection::injected();
        injection::countdown = -1;
        // Whatever failed, the index must still be a well formed index.
        BOOST_REQUIRE(index.invariants_hold());
        BOOST_REQUIRE_EQUAL(failed, injected);
        if (!injected) {
            return;
        }
        check();
        // An operation handling this many values isn't one of ours.
        BOOST_REQUIRE_LT(k, 10000);
    }
}

// insert() and erase(), with the construction of the value they are given
// failing in turn. Neither may leave a trace of the failed operation.
BOOST_AUTO_TEST_CASE(test_insert_and_erase_are_exception_safe) {
    throwing_index index;
    reference ref;
    std::mt19937 rng(11);
    auto random_key = [&] { return std::uniform_int_distribution<key_type>(0, 40)(rng); };
    std::vector<reference::entry> live;
    int value = 0;

    // The index is unchanged, and holds what the reference says it does.
    auto check = [&] {
        BOOST_REQUIRE_EQUAL(index.size(), ref.size());
        BOOST_REQUIRE(all(index) == ref.all());
    };

    for (int i = 0; i < 300; i++) {
        if (std::uniform_int_distribution<int>(0, 9)(rng) < 6 || live.empty()) {
            auto a = random_key();
            auto b = random_key();
            auto e = reference::entry{std::min(a, b), std::max(a, b), value++};
            with_each_failure(index, [&] { index.insert(e.start, e.end, throwing_value(e.value)); }, check);
            ref.insert(e.start, e.end, e.value);
            live.push_back(e);
        } else {
            auto& e = live[std::uniform_int_distribution<size_t>(0, live.size() - 1)(rng)];
            bool erased = false;
            with_each_failure(index, [&] { erased = index.erase(e.start, e.end, throwing_value(e.value)); }, check);
            BOOST_REQUIRE(erased);
            BOOST_REQUIRE(ref.erase(e.start, e.end, e.value));
            e = live.back();
            live.pop_back();
        }
        check();
    }

    // Erasing something which isn't there, and erasing the index empty.
    with_each_failure(index, [&] { BOOST_REQUIRE(!index.erase(41, 41, throwing_value(-1))); }, check);
    while (!live.empty()) {
        auto e = live.back();
        live.pop_back();
        with_each_failure(index, [&] { index.erase(e.start, e.end, throwing_value(e.value)); }, check);
        BOOST_REQUIRE(ref.erase(e.start, e.end, e.value));
        check();
    }
    BOOST_REQUIRE(index.empty());
}

// The reading operations, plus copying and clearing, with every value copy
// they make failing in turn. None of them may change the index.
BOOST_AUTO_TEST_CASE(test_queries_and_copies_are_exception_safe) {
    constexpr key_type key_space = 40;
    throwing_index index;
    reference ref;
    std::mt19937 rng(12);
    auto random_key = [&] { return std::uniform_int_distribution<key_type>(0, key_space)(rng); };
    for (int i = 0; i < 40; i++) {
        auto a = random_key();
        auto b = random_key();
        index.insert(std::min(a, b), std::max(a, b), throwing_value(i));
        ref.insert(std::min(a, b), std::max(a, b), i);
    }
    BOOST_REQUIRE_GT(index.block_count(), 4u);

    auto check = [&] {
        BOOST_REQUIRE_EQUAL(index.size(), ref.size());
        BOOST_REQUIRE(all(index) == ref.all());
    };

    // for_each().
    std::vector<reference::entry> got;
    with_each_failure(index, [&] { got = all(index); }, check);
    BOOST_REQUIRE(got == ref.all());

    // for_each_overlapping(), point and range.
    for (key_type p = 0; p <= key_space; p++) {
        with_each_failure(index, [&] { got = overlapping(index, p, p); }, check);
        BOOST_REQUIRE(got == ref.overlapping(p, p));
        with_each_failure(index, [&] { got = overlapping(index, p, p + 7); }, check);
        BOOST_REQUIRE(got == ref.overlapping(p, p + 7));
    }

    // Copying. The copy is a full one, so a failure has to unwind an
    // arbitrary number of already copied values.
    std::vector<reference::entry> copied;
    with_each_failure(index, [&] {
        throwing_index copy(index);
        copied = all(copy);
    }, check);
    BOOST_REQUIRE(copied == ref.all());

    // A sweep with a cursor, which copies each value it opens. The cursor is
    // made afresh on each attempt, so a failed advance_to() is not resumed.
    std::vector<std::vector<int>> covering;
    auto covering_at = [&] (key_type pos) {
        std::vector<int> ret;
        for (auto& e : ref.overlapping(pos, pos)) {
            ret.push_back(e.value);
        }
        std::ranges::sort(ret);
        return ret;
    };
    auto cursor_covering = [] (const throwing_index::cursor& c) {
        std::vector<int> ret;
        for (const throwing_value& v : c.covering()) {
            ret.push_back(v.value());
        }
        std::ranges::sort(ret);
        return ret;
    };
    with_each_failure(index, [&] {
        covering.clear();
        auto c = index.make_cursor();
        c.seek(0);
        for (key_type p = 0; p <= key_space; p++) {
            c.advance_to(p);
            covering.push_back(cursor_covering(c));
        }
    }, check);
    for (key_type p = 0; p <= key_space; p++) {
        BOOST_REQUIRE(covering[p] == covering_at(p));
    }

    // A failed advance_to() leaves the cursor short of the position, and
    // repeating it finishes the move.
    for (long k = 0; ; k++) {
        auto c = index.make_cursor();
        c.seek(0);
        injection::countdown = k;
        bool failed = false;
        try {
            c.advance_to(key_space);
        } catch (const std::runtime_error&) {
            failed = true;
        }
        bool injected = injection::injected();
        injection::countdown = -1;
        BOOST_REQUIRE_EQUAL(failed, injected);
        if (!injected) {
            break;
        }
        BOOST_REQUIRE_LE(c.position(), key_space);
        c.advance_to(key_space);
        BOOST_REQUIRE_EQUAL(c.position(), key_space);
        BOOST_REQUIRE(cursor_covering(c) == covering_at(key_space));
    }

    // Nothing to inject into clear(), which doesn't touch a value; it is here
    // for the check that follows it.
    with_each_failure(index, [&] { index.clear(); }, check);
    BOOST_REQUIRE(index.empty());
    BOOST_REQUIRE(index.invariants_hold());
}
