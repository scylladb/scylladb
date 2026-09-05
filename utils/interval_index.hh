/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <algorithm>
#include <array>
#include <bit>
#include <concepts>
#include <cstdint>
#include <limits>
#include <optional>
#include <ranges>
#include <utility>
#include <vector>

#include <boost/container/static_vector.hpp>

#include "utils/assert.hh"
#include "utils/chunked_vector.hh"

namespace utils {

// An index of closed intervals [start, end] over an integral Key, each
// associated with a Value, which answers
//
//   * overlap queries: the values of all the intervals overlapping a query
//     interval, see for_each_overlapping()
//   * an ordered sweep: for successive positions, the values of the intervals
//     covering the position, and the position at which that set changes, see
//     cursor
//
// The same interval, and the same (interval, value) pair, may be inserted more
// than once; erase() then removes one of them.
//
// Entries are held in blocks of block_size entries, ordered by (start, end)
// throughout: every entry of a block precedes every entry of the following
// block, and the entries of a block are ordered among themselves. Each block
// is summarized by the smallest start and the largest end in it, held in two
// arrays parallel to the block array. A query reads the summaries and looks
// inside a block only when its summary says the block can hold a match.
//
// The summaries make a query output-sensitive. Consider a query for the
// intervals covering position p, and let b be the last block whose smallest
// start is <= p. Every entry of every block before b starts at or before p, so
// if such a block's largest end is >= p, then the entry with that end covers
// p: a block is never looked inside without yielding a match. Only b itself
// can be looked inside in vain. So a query costs
//
//   O(size() / block_size + matches)
//
// where the first term is a scan of the summary array, of one Key per
// block_size entries.
//
// Note that the summary of a block is not affected by the intervals in any
// other block, which is what keeps the bound above from degrading in the
// presence of intervals spanning much of the key space. Such an interval
// belongs to the single block its start falls in, and raises the largest end
// of that block alone.
//
// A block holds its entries inline, so an entry costs 2*sizeof(Key) +
// sizeof(Value) and no allocation is ever made within a block. Copying the
// index copies the blocks, without allocating per entry.
//
// Within a block the starts, the ends and the values are held in separate
// arrays, so that scanning a block for matches reads only the starts and the
// ends, in loops of a constant number of iterations which compile into vector
// compares.
template <std::integral Key, typename Value, size_t block_size = 32>
class interval_index {
    static_assert(block_size >= 2 && block_size <= 64);

    // A bit per entry of a block.
    using mask_type = uint64_t;

    struct block {
        // The slots from values.size() on are unused. They are kept
        // initialized, so that a scan may read the whole array and mask off
        // the unused slots afterwards, rather than having to stop at
        // values.size().
        std::array<Key, block_size> starts{};
        std::array<Key, block_size> ends{};
        boost::container::static_vector<Value, block_size> values;

        unsigned size() const noexcept { return values.size(); }
        bool full() const noexcept { return size() == block_size; }
    };

    // Parallel to _blocks. Held apart from the blocks so that a query scans
    // them without reading the blocks themselves.
    //
    // Chunked, as a block is large enough (776 bytes for 32 entries of a
    // 64 bit key and a pointer) for a contiguous block array to outgrow the
    // largest allocation the pools serve, and so to need the large allocation
    // pool, at a few thousand entries.
    utils::chunked_vector<Key> _min_start;
    utils::chunked_vector<Key> _max_end;
    utils::chunked_vector<block> _blocks;
    size_t _size = 0;

private:
    static constexpr mask_type size_mask(unsigned n) noexcept {
        return n == 64 ? ~mask_type(0) : (mask_type(1) << n) - 1;
    }

    // Entries are ordered by (start, end). The value takes no part in the
    // ordering, so equal intervals may be held in any order.
    static bool entry_less(Key s1, Key e1, Key s2, Key e2) noexcept {
        return s1 != s2 ? s1 < s2 : e1 < e2;
    }

    static bool block_starts_after(const block& b, Key start, Key end) noexcept {
        return entry_less(start, end, b.starts[0], b.ends[0]);
    }

    static bool block_ends_before(const block& b, Key start, Key end) noexcept {
        auto last = b.size() - 1;
        return entry_less(b.starts[last], b.ends[last], start, end);
    }

    // The entries of b which end at or after `low`.
    static mask_type ends_at_or_after(const block& b, Key low) noexcept {
        mask_type m = 0;
        for (unsigned i = 0; i < block_size; i++) {
            m |= mask_type(b.ends[i] >= low) << i;
        }
        return m & size_mask(b.size());
    }

    // The entries of b which start at or before `high`.
    static mask_type starts_at_or_before(const block& b, Key high) noexcept {
        mask_type m = 0;
        for (unsigned i = 0; i < block_size; i++) {
            m |= mask_type(b.starts[i] <= high) << i;
        }
        return m & size_mask(b.size());
    }

    // One past the last block which may hold an interval starting at or before
    // `high`.
    size_t blocks_starting_at_or_before(Key high) const noexcept {
        return std::upper_bound(_min_start.begin(), _min_start.end(), high) - _min_start.begin();
    }

    void update_summary(size_t i) noexcept {
        const block& b = _blocks[i];
        _min_start[i] = b.starts[0];
        _max_end[i] = *std::max_element(b.ends.begin(), b.ends.begin() + b.size());
    }

    void insert_at(size_t i, unsigned j, Key start, Key end, Value value) {
        block& b = _blocks[i];
        auto n = b.size();
        std::move_backward(b.starts.begin() + j, b.starts.begin() + n, b.starts.begin() + n + 1);
        std::move_backward(b.ends.begin() + j, b.ends.begin() + n, b.ends.begin() + n + 1);
        b.starts[j] = start;
        b.ends[j] = end;
        b.values.insert(b.values.begin() + j, std::move(value));
        _min_start[i] = b.starts[0];
        _max_end[i] = std::max(_max_end[i], end);
        _size++;
    }

    void erase_at(size_t i, unsigned j) {
        block& b = _blocks[i];
        auto n = b.size();
        std::move(b.starts.begin() + j + 1, b.starts.begin() + n, b.starts.begin() + j);
        std::move(b.ends.begin() + j + 1, b.ends.begin() + n, b.ends.begin() + j);
        // Keep the vacated slot from matching a scan.
        b.starts[n - 1] = Key{};
        b.ends[n - 1] = Key{};
        b.values.erase(b.values.begin() + j);
        _size--;
        if (b.size() == 0) {
            _blocks.erase(_blocks.begin() + i);
            _min_start.erase(_min_start.begin() + i);
            _max_end.erase(_max_end.begin() + i);
            return;
        }
        update_summary(i);
        maybe_merge(i);
    }

    // Moves the upper half of the entries of block i into a new block after it.
    void split(size_t i) {
        block upper;
        block& lower = _blocks[i];
        auto n = lower.size();
        auto keep = n / 2;
        std::move(lower.starts.begin() + keep, lower.starts.begin() + n, upper.starts.begin());
        std::move(lower.ends.begin() + keep, lower.ends.begin() + n, upper.ends.begin());
        for (auto j = keep; j < n; j++) {
            upper.values.push_back(std::move(lower.values[j]));
            lower.starts[j] = Key{};
            lower.ends[j] = Key{};
        }
        lower.values.erase(lower.values.begin() + keep, lower.values.end());
        _blocks.insert(_blocks.begin() + i + 1, std::move(upper));
        _min_start.insert(_min_start.begin() + i + 1, Key{});
        _max_end.insert(_max_end.begin() + i + 1, Key{});
        update_summary(i);
        update_summary(i + 1);
    }

    // Merges block i with the following one if their entries fit in a single
    // block, so that a series of erases doesn't leave the index with many
    // near-empty blocks.
    void maybe_merge(size_t i) {
        if (i + 1 == _blocks.size()) {
            if (i == 0) {
                return;
            }
            i--;
        }
        block& lower = _blocks[i];
        block& upper = _blocks[i + 1];
        auto n = lower.size();
        auto m = upper.size();
        if (n + m > block_size / 2) {
            return;
        }
        std::move(upper.starts.begin(), upper.starts.begin() + m, lower.starts.begin() + n);
        std::move(upper.ends.begin(), upper.ends.begin() + m, lower.ends.begin() + n);
        for (unsigned j = 0; j < m; j++) {
            lower.values.push_back(std::move(upper.values[j]));
        }
        _blocks.erase(_blocks.begin() + i + 1);
        _min_start.erase(_min_start.begin() + i + 1);
        _max_end.erase(_max_end.begin() + i + 1);
        update_summary(i);
    }

public:
    interval_index() = default;

    size_t size() const noexcept { return _size; }
    bool empty() const noexcept { return _size == 0; }
    size_t block_count() const noexcept { return _blocks.size(); }

    void clear() noexcept {
        _blocks.clear();
        _min_start.clear();
        _max_end.clear();
        _size = 0;
    }

    // Adds the interval [start, end], which must not be empty. The same
    // (interval, value) may be added more than once.
    void insert(Key start, Key end, Value value) {
        if (_blocks.empty()) {
            _blocks.emplace_back();
            _min_start.push_back(start);
            _max_end.push_back(end);
            insert_at(0, 0, start, end, std::move(value));
            return;
        }
        // The last block whose first entry precedes the new one, or the first
        // block if the new entry precedes them all. The following block starts
        // after the new entry and the preceding ones end before it, so the
        // index stays ordered wherever within this block the entry goes.
        size_t i = std::upper_bound(_blocks.begin(), _blocks.end(), start,
                [end] (Key start, const block& b) { return block_starts_after(b, start, end); }) - _blocks.begin();
        i = i > 0 ? i - 1 : 0;
        const block& b = _blocks[i];
        SCYLLA_ASSERT(!b.full());
        unsigned j = 0;
        while (j < b.size() && !entry_less(start, end, b.starts[j], b.ends[j])) {
            j++;
        }
        insert_at(i, j, start, end, std::move(value));
        if (_blocks[i].full()) {
            split(i);
        }
    }

    // Removes one entry equal to (start, end, value), and returns whether one
    // was found.
    bool erase(Key start, Key end, const Value& value) {
        // The entries equal to (start, end) form a run of the index, so they
        // are held by consecutive blocks: the ones which neither end before
        // the run nor start after it.
        size_t i = std::lower_bound(_blocks.begin(), _blocks.end(), start,
                [end] (const block& b, Key start) { return block_ends_before(b, start, end); }) - _blocks.begin();
        for (; i < _blocks.size() && !block_starts_after(_blocks[i], start, end); i++) {
            const block& b = _blocks[i];
            for (unsigned j = 0; j < b.size(); j++) {
                if (b.starts[j] == start && b.ends[j] == end && b.values[j] == value) {
                    erase_at(i, j);
                    return true;
                }
            }
        }
        return false;
    }

private:
    // Calls on_block(block index) for each block looked inside, and
    // f(start, end, value) for each interval overlapping [low, high].
    template <typename Fn, typename BlockFn>
    void for_each_overlapping_block(Key low, Key high, Fn&& f, BlockFn&& on_block) const {
        if (low > high) {
            return;
        }
        auto last = blocks_starting_at_or_before(high);
        for (size_t i = 0; i < last; i++) {
            if (_max_end[i] < low) {
                continue;
            }
            on_block(i);
            const block& b = _blocks[i];
            auto m = ends_at_or_after(b, low);
            // Every entry of a block other than the last one starts at or
            // before `high`, as the following block's smallest start does.
            if (i + 1 == last) {
                m &= starts_at_or_before(b, high);
            }
            while (m) {
                auto j = std::countr_zero(m);
                m &= m - 1;
                f(b.starts[j], b.ends[j], b.values[j]);
            }
        }
    }

public:
    // Calls f(start, end, value) for every interval overlapping [low, high].
    template <typename Fn>
    requires std::invocable<Fn, Key, Key, const Value&>
    void for_each_overlapping(Key low, Key high, Fn&& f) const {
        for_each_overlapping_block(low, high, f, [] (size_t) {});
    }

    // The number of blocks for_each_overlapping() would look inside; for
    // tests, which check that it doesn't exceed the number of matches by more
    // than one.
    size_t blocks_examined(Key low, Key high) const {
        size_t n = 0;
        for_each_overlapping_block(low, high, [] (Key, Key, const Value&) {}, [&] (size_t) { n++; });
        return n;
    }

    // Calls f(start, end, value) for every interval, in (start, end) order.
    template <typename Fn>
    requires std::invocable<Fn, Key, Key, const Value&>
    void for_each(Fn&& f) const {
        for (const block& b : _blocks) {
            for (unsigned j = 0; j < b.size(); j++) {
                f(b.starts[j], b.ends[j], b.values[j]);
            }
        }
    }

    // Sweeps the index in key order, reporting for successive positions the
    // intervals covering the position, and the position at which that set
    // changes.
    //
    // A cursor is invalidated by any change to the index; seek() makes it
    // usable again.
    class cursor {
        const interval_index* _index;
        // The next entry to open, i.e. the first entry starting after the
        // current position.
        size_t _block = 0;
        unsigned _entry = 0;
        // The intervals covering the current position, as (end, value) pairs
        // ordered as a heap on the end, so that the interval which closes
        // first is at the front.
        std::vector<std::pair<Key, Value>> _open;
        Key _pos{};

    private:
        struct closes_later {
            bool operator()(const std::pair<Key, Value>& a, const std::pair<Key, Value>& b) const noexcept {
                return a.first > b.first;
            }
        };

        bool at_end() const noexcept {
            return _block == _index->_blocks.size();
        }

        void step() noexcept {
            if (++_entry == _index->_blocks[_block].size()) {
                _block++;
                _entry = 0;
            }
        }

        // Opens the intervals which start at or before pos. Those among them
        // which end before pos are skipped: they don't cover pos, and can't
        // cover a later position either, as the cursor only moves forward.
        // Advancing by more than the position at which the covering set
        // changes is what makes them possible.
        void open_up_to(Key pos) {
            while (!at_end()) {
                const block& b = _index->_blocks[_block];
                if (b.starts[_entry] > pos) {
                    return;
                }
                if (b.ends[_entry] >= pos) {
                    _open.emplace_back(b.ends[_entry], b.values[_entry]);
                    std::push_heap(_open.begin(), _open.end(), closes_later());
                }
                step();
            }
        }

        void close_before(Key pos) {
            while (!_open.empty() && _open.front().first < pos) {
                std::pop_heap(_open.begin(), _open.end(), closes_later());
                _open.pop_back();
            }
        }

    public:
        explicit cursor(const interval_index& index) noexcept : _index(&index) {}

        // Moves to pos, which must not precede the current position, but may
        // be anywhere at or after it: advancing by more than change_at() is
        // allowed and skips over the intervals in between.
        // Costs O(intervals opened + intervals closed).
        void advance_to(Key pos) {
            close_before(pos);
            open_up_to(pos);
            _pos = pos;
        }

        // Moves to pos, which may precede the current position, at the cost of
        // a query.
        void seek(Key pos) {
            _open.clear();
            _index->for_each_overlapping(pos, pos, [this] (Key start, Key end, const Value& value) {
                _open.emplace_back(end, value);
            });
            std::make_heap(_open.begin(), _open.end(), closes_later());
            // The first entry starting after pos.
            _block = _index->blocks_starting_at_or_before(pos);
            _block = _block > 0 ? _block - 1 : 0;
            _entry = 0;
            while (!at_end() && _index->_blocks[_block].starts[_entry] <= pos) {
                step();
            }
            _pos = pos;
        }

        Key position() const noexcept { return _pos; }

        // The values of the intervals covering the current position, in
        // unspecified order.
        auto covering() const noexcept {
            return _open | std::views::transform([] (const std::pair<Key, Value>& p) -> const Value& { return p.second; });
        }

        bool covered() const noexcept { return !_open.empty(); }

        // The smallest position after the current one at which covering()
        // changes, if there is one. covering() holds throughout
        // [position(), change_at()).
        std::optional<Key> change_at() const noexcept {
            std::optional<Key> next;
            // An interval opens.
            if (!at_end()) {
                next = _index->_blocks[_block].starts[_entry];
            }
            // An interval closes.
            if (!_open.empty() && _open.front().first != std::numeric_limits<Key>::max()) {
                auto closes = _open.front().first + 1;
                next = next ? std::min(*next, closes) : closes;
            }
            return next;
        }
    };

    cursor make_cursor() const noexcept { return cursor(*this); }

    // Checks the invariants; for tests.
    bool invariants_hold() const noexcept {
        if (_blocks.size() != _min_start.size() || _blocks.size() != _max_end.size()) {
            return false;
        }
        size_t n = 0;
        std::optional<Key> prev_start, prev_end;
        for (size_t i = 0; i < _blocks.size(); i++) {
            const block& b = _blocks[i];
            if (b.size() == 0 || b.size() >= block_size) {
                return false;
            }
            n += b.size();
            for (unsigned j = 0; j < b.size(); j++) {
                if (b.starts[j] > b.ends[j]) {
                    return false;
                }
                if (prev_start && entry_less(b.starts[j], b.ends[j], *prev_start, *prev_end)) {
                    return false;
                }
                prev_start = b.starts[j];
                prev_end = b.ends[j];
            }
            if (_min_start[i] != b.starts[0]) {
                return false;
            }
            if (_max_end[i] != *std::max_element(b.ends.begin(), b.ends.begin() + b.size())) {
                return false;
            }
            // The unused slots must not match a scan of the whole array.
            for (unsigned j = b.size(); j < block_size; j++) {
                if (b.starts[j] != Key{} || b.ends[j] != Key{}) {
                    return false;
                }
            }
        }
        return n == _size;
    }
};

} // namespace utils
