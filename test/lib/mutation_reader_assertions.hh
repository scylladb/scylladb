/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "utils/assert.hh"
#include <boost/test/unit_test.hpp>
#include <seastar/util/backtrace.hh>
#include "readers/mutation_reader.hh"
#include "mutation_assertions.hh"
#include "schema/schema.hh"
#include "test/lib/log.hh"

inline bool trim_range_tombstone(const schema& s, range_tombstone& rt, const query::clustering_row_ranges& ck_ranges) {
    if (ck_ranges.empty()) {
        return true;
    }
    position_in_partition::less_compare less(s);
    bool relevant = false;
    for (auto& range : ck_ranges) {
        relevant |= rt.trim(s, position_in_partition::for_range_start(range), position_in_partition::for_range_end(range));
    }
    return relevant;
}

static inline void match_compacted_mutation(const mutation_opt& mo, const mutation& m, gc_clock::time_point query_time,
                                            const std::optional<query::clustering_row_ranges>& ck_ranges = {}) {
    // If the passed in mutation is empty, allow for the reader to produce an empty or no partition.
    if (m.partition().empty() && !mo) {
        return;
    }
    BOOST_REQUIRE(bool(mo));
    memory::scoped_critical_alloc_section dfg;
    mutation got = *mo;
    got.partition().compact_for_compaction(*m.schema(), always_gc, got.decorated_key(), query_time, tombstone_gc_state(nullptr));
    assert_that(got).is_equal_to(m, ck_ranges);
}

// Intended to be called in a seastar thread
class flat_reader_assertions_v2 {
    mutation_reader _reader;
    dht::partition_range _pr;
    bool _ignore_deletion_time = false;
    bool _exact = false; // Don't ignore irrelevant fragments
    tombstone _rt;
private:
    mutation_fragment_v2* peek_next() {
        while (auto next = _reader.peek().get()) {
            // There is no difference between an empty row and a row that doesn't exist.
            // While readers that emit spurious empty rows may be wasteful, it is not
            // incorrect to do so, so let's ignore them.
            if (next->is_clustering_row() && next->as_clustering_row().empty()) {
                testlog.trace("Received empty clustered row: {}", mutation_fragment_v2::printer(*_reader.schema(), *next));
                _reader().get();
                continue;
            }
            // silently ignore rtcs that don't change anything
            if (next->is_range_tombstone_change()) {
                auto rtc_mf = std::move(*_reader().get());
                auto tomb = rtc_mf.as_range_tombstone_change().tombstone();
                auto cmp = position_in_partition::tri_compare(*_reader.schema());
                // squash rtcs with the same pos
                while (auto next_maybe_rtc = _reader.peek().get()) {
                    if (next_maybe_rtc->is_range_tombstone_change() && cmp(next_maybe_rtc->position(), rtc_mf.position()) == 0) {
                        testlog.trace("Squashing {} with {}", next_maybe_rtc->as_range_tombstone_change().tombstone(), tomb);
                        tomb = next_maybe_rtc->as_range_tombstone_change().tombstone();
                        _reader().get();
                    } else {
                        break;
                    }
                }
                rtc_mf.mutate_as_range_tombstone_change(*_reader.schema(), [tomb] (range_tombstone_change& rtc) { rtc.set_tombstone(tomb); });
                if (tomb == _rt) {
                    testlog.trace("Received spurious rtcs, equivalent to: {}", mutation_fragment_v2::printer(*_reader.schema(), rtc_mf));
                    continue;
                }
                _reader.unpop_mutation_fragment(std::move(rtc_mf));
                next = _reader.peek().get();
            }
            return next;
        }
        return nullptr;
    }
    mutation_fragment_v2_opt read_next() {
        if (!_exact) {
            peek_next();
        }
        auto next = _reader().get();
        if (next) {
            testlog.trace("read_next(): {}", mutation_fragment_v2::printer(*_reader.schema(), *next));
        } else {
            testlog.trace("read_next(): null");
        }
        return next;
    }
    range_tombstone_change maybe_drop_deletion_time(const range_tombstone_change& rt) const {
        if (!_ignore_deletion_time) {
            return rt;
        } else {
            return {rt.position(), {rt.tombstone().timestamp, {}}};
        }
    }
    void reset_rt() {
        _rt = {};
    }
    void apply_rtc(const range_tombstone_change& rtc) {
        _rt = rtc.tombstone();
    }
public:
    flat_reader_assertions_v2(mutation_reader reader)
            : _reader(std::move(reader))
    { }

    ~flat_reader_assertions_v2() {
        _reader.close().get();
    }

    flat_reader_assertions_v2(const flat_reader_assertions_v2&) = delete;
    flat_reader_assertions_v2(flat_reader_assertions_v2&&) = default;

    flat_reader_assertions_v2& operator=(flat_reader_assertions_v2&& o) {
        if (this != &o) {
            _reader.close().get();
            _reader = std::move(o._reader);
            _pr = std::move(o._pr);
            _ignore_deletion_time = std::move(o._ignore_deletion_time);
            _rt = std::move(o._rt);
        }
        return *this;
    }

    flat_reader_assertions_v2&& ignore_deletion_time(bool ignore = true) {
        _ignore_deletion_time = ignore;
        return std::move(*this);
    }

    flat_reader_assertions_v2&& exact(bool exact = true) {
        _exact = exact;
        return std::move(*this);
    }

    flat_reader_assertions_v2& produces_partition_start(const dht::decorated_key& dk,
                                                     std::optional<tombstone> tomb = std::nullopt) {
        testlog.trace("Expecting partition start with key {}", dk);
        auto mfopt = read_next();
        if (!mfopt) {
            BOOST_FAIL(format("Expected: partition start with key {}, got end of stream", dk));
        }
        if (!mfopt->is_partition_start()) {
            BOOST_FAIL(format("Expected: partition start with key {}, got: {}", dk, mutation_fragment_v2::printer(*_reader.schema(), *mfopt)));
        }
        if (!mfopt->as_partition_start().key().equal(*_reader.schema(), dk)) {
            BOOST_FAIL(format("Expected: partition start with key {}, got: {}", dk, mutation_fragment_v2::printer(*_reader.schema(), *mfopt)));
        }
        if (tomb && mfopt->as_partition_start().partition_tombstone() != *tomb) {
            BOOST_FAIL(format("Expected: partition start with tombstone {}, got: {}", *tomb, mutation_fragment_v2::printer(*_reader.schema(), *mfopt)));
        }
        reset_rt();
        return *this;
    }

    flat_reader_assertions_v2& produces_static_row() {
        testlog.trace("Expecting static row");
        auto mfopt = read_next();
        if (!mfopt) {
            BOOST_FAIL("Expected static row, got end of stream");
        }
        if (!mfopt->is_static_row()) {
            BOOST_FAIL(format("Expected static row, got: {}", mutation_fragment_v2::printer(*_reader.schema(), *mfopt)));
        }
        return *this;
    }

    flat_reader_assertions_v2& produces_row_with_key(const clustering_key& ck, std::optional<tombstone> active_range_tombstone = std::nullopt) {
        testlog.trace("Expect {}", ck);
        if (active_range_tombstone) {
            testlog.trace("(with active range tombstone: {})", *active_range_tombstone);
        }
        auto mfopt = read_next();
        if (!mfopt) {
            BOOST_FAIL(format("Expected row with key {}, but got end of stream", ck));
        }
        if (!mfopt->is_clustering_row()) {
            BOOST_FAIL(format("Expected row with key {}, but got {}", ck, mutation_fragment_v2::printer(*_reader.schema(), *mfopt)));
        }
        auto& actual = mfopt->as_clustering_row().key();
        if (!actual.equal(*_reader.schema(), ck)) {
            BOOST_FAIL(format("Expected row with key {}, but key is {}", ck, actual));
        }
        if (active_range_tombstone) {
            BOOST_REQUIRE_EQUAL(*active_range_tombstone, _rt);
        }
        return *this;
    }

    struct expected_column {
        column_id id;
        const sstring& name;
        bytes value;
        expected_column(const column_definition* cdef, bytes value)
                : id(cdef->id)
                , name(cdef->name_as_text())
                , value(std::move(value))
        { }
    };

    flat_reader_assertions_v2& produces_static_row(const std::vector<expected_column>& columns) {
        testlog.trace("Expecting static row");
        auto mfopt = read_next();
        if (!mfopt) {
            BOOST_FAIL("Expected static row, got end of stream");
        }
        if (!mfopt->is_static_row()) {
            BOOST_FAIL(format("Expected static row, got: {}", mutation_fragment_v2::printer(*_reader.schema(), *mfopt)));
        }
        auto& cells = mfopt->as_static_row().cells();
        if (cells.size() != columns.size()) {
            BOOST_FAIL(format("Expected static row with {} columns, but has {}", columns.size(), cells.size()));
        }
        for (size_t i = 0; i < columns.size(); ++i) {
            const atomic_cell_or_collection* cell = cells.find_cell(columns[i].id);
            if (!cell) {
                BOOST_FAIL(format("Expected static row with column {}, but it is not present", columns[i].name));
            }
            auto& cdef = _reader.schema()->static_column_at(columns[i].id);
            auto cmp = compare_unsigned(columns[i].value, cell->as_atomic_cell(cdef).value().linearize());
            if (cmp != 0) {
                BOOST_FAIL(format("Expected static row with column {} having value {}, but it has value {}",
                                  columns[i].name,
                                  columns[i].value,
                                  cell->as_atomic_cell(cdef).value()));
            }
        }
        return *this;
    }

    flat_reader_assertions_v2& produces_row(const clustering_key& ck, const std::vector<expected_column>& columns) {
        testlog.trace("Expect {}", ck);
        auto mfopt = read_next();
        if (!mfopt) {
            BOOST_FAIL(format("Expected row with key {}, but got end of stream", ck));
        }
        if (!mfopt->is_clustering_row()) {
            BOOST_FAIL(format("Expected row with key {}, but got {}", ck, mutation_fragment_v2::printer(*_reader.schema(), *mfopt)));
        }
        auto& actual = mfopt->as_clustering_row().key();
        if (!actual.equal(*_reader.schema(), ck)) {
            BOOST_FAIL(format("Expected row with key {}, but key is {}", ck, actual));
        }
        auto& cells = mfopt->as_clustering_row().cells();
        if (cells.size() != columns.size()) {
            BOOST_FAIL(format("Expected row with {} columns, but has {}", columns.size(), cells.size()));
        }
        for (size_t i = 0; i < columns.size(); ++i) {
            const atomic_cell_or_collection* cell = cells.find_cell(columns[i].id);
            if (!cell) {
                BOOST_FAIL(format("Expected row with column {}, but it is not present", columns[i].name));
            }
            auto& cdef = _reader.schema()->regular_column_at(columns[i].id);
            SCYLLA_ASSERT (!cdef.is_multi_cell());
            auto cmp = compare_unsigned(columns[i].value, cell->as_atomic_cell(cdef).value().linearize());
            if (cmp != 0) {
                BOOST_FAIL(format("Expected row with column {} having value {}, but it has value {}",
                                  columns[i].name,
                                  columns[i].value,
                                  cell->as_atomic_cell(cdef).value().linearize()));
            }
        }
        return *this;
    }

    using assert_function = noncopyable_function<void(const column_definition&, const atomic_cell_or_collection*)>;

    flat_reader_assertions_v2& produces_row(const clustering_key& ck,
                                         const std::vector<column_id>& column_ids,
                                         const std::vector<assert_function>& column_assert) {
        testlog.trace("Expect {}", ck);
        auto mfopt = read_next();
        if (!mfopt) {
            BOOST_FAIL(format("Expected row with key {}, but got end of stream", ck));
        }
        if (!mfopt->is_clustering_row()) {
            BOOST_FAIL(format("Expected row with key {}, but got {}", ck, mutation_fragment_v2::printer(*_reader.schema(), *mfopt)));
        }
        auto& actual = mfopt->as_clustering_row().key();
        if (!actual.equal(*_reader.schema(), ck)) {
            BOOST_FAIL(format("Expected row with key {}, but key is {}", ck, actual));
        }
        auto& cells = mfopt->as_clustering_row().cells();
        if (cells.size() != column_ids.size()) {
            BOOST_FAIL(format("Expected row with {} columns, but has {}", column_ids.size(), cells.size()));
        }
        for (size_t i = 0; i < column_ids.size(); ++i) {
            const atomic_cell_or_collection* cell = cells.find_cell(column_ids[i]);
            if (!cell) {
                BOOST_FAIL(format("Expected row with column {:d}, but it is not present", column_ids[i]));
            }
            auto& cdef = _reader.schema()->regular_column_at(column_ids[i]);
            column_assert[i](cdef, cell);
        }
        return *this;
    }

    flat_reader_assertions_v2& may_produce_tombstones(position_range range) {
        testlog.trace("Expect possible range tombstone changes in {}", range);
        while (auto next = peek_next()) {
            if (!next->is_range_tombstone_change()) {
                break;
            }
            auto rtc = maybe_drop_deletion_time(next->as_range_tombstone_change());
            if (!interval<position_in_partition>{range.start(), range.end()}.contains(rtc.position(), position_in_partition::tri_compare{*_reader.schema()})) {
                testlog.trace("{} is out of range {}", mutation_fragment_v2::printer(*_reader.schema(), *next), range);
                break;
            }
            testlog.trace("Received: {}", rtc);
            apply_rtc(rtc);
            _reader().get();
        }
        return *this;
    }

    flat_reader_assertions_v2& produces_range_tombstone_change(const range_tombstone_change& rtc_) {
        auto rtc = maybe_drop_deletion_time(rtc_);
        testlog.trace("Expect {}", rtc);
        auto mfopt = read_next();
        if (!mfopt) {
            BOOST_FAIL(format("Expected {}, but got end of stream", rtc));
        }
        if (!mfopt->is_range_tombstone_change()) {
            BOOST_FAIL(format("Expected {}, but got {}", rtc, mutation_fragment_v2::printer(*_reader.schema(), *mfopt)));
        }
        auto read_rtc = maybe_drop_deletion_time(mfopt->as_range_tombstone_change());
        if (!rtc.equal(*_reader.schema(), read_rtc)) {
            BOOST_FAIL(format("Read {} does not match expected {}", read_rtc, rtc));
        }
        apply_rtc(rtc);
        return *this;
    }

    flat_reader_assertions_v2& produces_partition_end() {
        testlog.trace("Expecting partition end");
        BOOST_REQUIRE(!_rt);
        auto mfopt = read_next();
        if (!mfopt) {
            BOOST_FAIL(format("Expected partition end but got end of stream"));
        }
        if (!mfopt->is_end_of_partition()) {
            BOOST_FAIL(format("Expected partition end but got {}", mutation_fragment_v2::printer(*_reader.schema(), *mfopt)));
        }
        return *this;
    }

    flat_reader_assertions_v2& produces(const schema& s, const mutation_fragment_v2& mf) {
        if (mf.is_range_tombstone_change()) {
            return produces_range_tombstone_change(mf.as_range_tombstone_change());
        }

        auto mfopt = read_next();
        if (!mfopt) {
            BOOST_FAIL(format("Expected {}, but got end of stream", mutation_fragment_v2::printer(*_reader.schema(), mf)));
        }
        if (!mfopt->equal(s, mf)) {
            BOOST_FAIL(format("Expected {}, but got {}", mutation_fragment_v2::printer(*_reader.schema(), mf), mutation_fragment_v2::printer(*_reader.schema(), *mfopt)));
        }
        return *this;
    }

    flat_reader_assertions_v2& produces_end_of_stream() {
        testlog.trace("Expecting end of stream");
        auto mfopt = read_next();
        if (bool(mfopt)) {
            BOOST_FAIL(format("Expected end of stream, got {}", mutation_fragment_v2::printer(*_reader.schema(), *mfopt)));
        }
        reset_rt();
        return *this;
    }

    flat_reader_assertions_v2& produces(mutation_fragment_v2::kind k, std::vector<int> ck_elements, bool make_full_key = false) {
        testlog.trace("Expect {} {{{}}}", k, fmt::join(ck_elements, ", "));
        std::vector<bytes> ck_bytes;
        for (auto&& e : ck_elements) {
            ck_bytes.emplace_back(int32_type->decompose(e));
        }
        auto ck = clustering_key_prefix::from_exploded(*_reader.schema(), std::move(ck_bytes));
        if (make_full_key) {
            clustering_key::make_full(*_reader.schema(), ck);
        }

        auto mfopt = read_next();
        if (!mfopt) {
            BOOST_FAIL(format("Expected mutation fragment {}, got end of stream", ck));
        }
        if (mfopt->mutation_fragment_kind() != k) {
            BOOST_FAIL(format("Expected mutation fragment kind {}, got: {}", k, mfopt->mutation_fragment_kind()));
        }
        clustering_key::equality ck_eq(*_reader.schema());
        if (!ck_eq(mfopt->key(), ck)) {
            BOOST_FAIL(format("Expected key {}, got: {}", ck, mfopt->key()));
        }
        if (mfopt->is_range_tombstone_change()) {
            apply_rtc(maybe_drop_deletion_time(mfopt->as_range_tombstone_change()));
        }
        testlog.trace("Received {}", mutation_fragment_v2::printer(*_reader.schema(), *mfopt));
        return *this;
    }

    flat_reader_assertions_v2& produces_partition(const mutation& m) {
        return produces(m);
    }

    flat_reader_assertions_v2& produces(const mutation& m, const std::optional<query::clustering_row_ranges>& ck_ranges = {}) {
        auto mo = read_mutation_from_mutation_reader(_reader).get();
        if (!mo) {
            BOOST_FAIL(format("Expected {}, but got end of stream, at: {}", m, seastar::current_backtrace()));
        }
        memory::scoped_critical_alloc_section dfg;
        assert_that(*mo).is_equal_to_compacted(m, ck_ranges);
        return *this;
    }

    flat_reader_assertions_v2& produces(const dht::decorated_key& dk) {
        produces_partition_start(dk);
        next_partition();
        return *this;
    }

    template<typename Range>
    flat_reader_assertions_v2& produces(const Range& range) {
        for (auto&& m : range) {
            produces(m);
        }
        return *this;
    }

    flat_reader_assertions_v2& produces_eos_or_empty_mutation() {
        testlog.trace("Expecting eos or empty mutation");
        auto mo = read_mutation_from_mutation_reader(_reader).get();
        if (mo) {
            if (!mo->partition().empty()) {
                BOOST_FAIL(format("Mutation is not empty: {}", *mo));
            }
        }
        return *this;
    }

    void has_monotonic_positions() {
        position_in_partition::less_compare less(*_reader.schema());
        mutation_fragment_v2_opt previous_fragment;
        mutation_fragment_v2_opt previous_partition;
        bool inside_partition = false;
        for (;;) {
            auto mfo = read_next();
            if (!mfo) {
                break;
            }
            if (mfo->is_partition_start()) {
                BOOST_REQUIRE(!inside_partition);
                auto& dk = mfo->as_partition_start().key();
                if (previous_partition && !previous_partition->as_partition_start().key().less_compare(*_reader.schema(), dk)) {
                    BOOST_FAIL(format("previous partition had greater or equal key: prev={}, current={}",
                                      mutation_fragment_v2::printer(*_reader.schema(), *previous_partition), mutation_fragment_v2::printer(*_reader.schema(), *mfo)));
                }
                previous_partition = std::move(mfo);
                previous_fragment = std::nullopt;
                inside_partition = true;
            } else if (mfo->is_end_of_partition()) {
                BOOST_REQUIRE(inside_partition);
                inside_partition = false;
            } else {
                BOOST_REQUIRE(inside_partition);
                if (previous_fragment) {
                    if (less(mfo->position(), previous_fragment->position())) {
                        BOOST_FAIL(format("previous fragment is not strictly before: prev={}, current={}",
                                          mutation_fragment_v2::printer(*_reader.schema(), *previous_fragment), mutation_fragment_v2::printer(*_reader.schema(), *mfo)));
                    }
                }
                previous_fragment = std::move(mfo);
            }
        }
        BOOST_REQUIRE(!inside_partition);
    }

    flat_reader_assertions_v2& fast_forward_to(const dht::partition_range& pr) {
        testlog.trace("Fast forward to partition range: {}", pr);
        _pr = pr;
        _reader.fast_forward_to(_pr).get();
        return *this;
    }

    flat_reader_assertions_v2& next_partition() {
        testlog.trace("Skip to next partition");
        _reader.next_partition().get();
        reset_rt();
        return *this;
    }

    flat_reader_assertions_v2& fast_forward_to(position_range pr) {
        testlog.trace("Fast forward to clustering range: {}", pr);
        _reader.fast_forward_to(std::move(pr)).get();
        return *this;
    }

    flat_reader_assertions_v2& fast_forward_to(const clustering_key& ck1, const clustering_key& ck2) {
        testlog.trace("Fast forward to clustering range: [{}, {})", ck1, ck2);
        return fast_forward_to(position_range{
                position_in_partition(position_in_partition::clustering_row_tag_t(), ck1),
                position_in_partition(position_in_partition::clustering_row_tag_t(), ck2)
        });
    }

    flat_reader_assertions_v2& produces_compacted(const mutation& m, gc_clock::time_point query_time,
                                               const std::optional<query::clustering_row_ranges>& ck_ranges = {}) {
        match_compacted_mutation(read_mutation_from_mutation_reader(_reader).get(), m, query_time, ck_ranges);
        return *this;
    }

    mutation_assertion next_mutation() {
        auto mo = read_mutation_from_mutation_reader(_reader).get();
        BOOST_REQUIRE(bool(mo));
        return mutation_assertion(std::move(*mo));
    }

    future<> fill_buffer() {
        return _reader.fill_buffer();
    }

    bool is_buffer_full() const {
        return _reader.is_buffer_full();
    }

    void set_max_buffer_size(size_t size) {
        _reader.set_max_buffer_size(size);
    }
};

inline
flat_reader_assertions_v2 assert_that(mutation_reader r) {
    return { std::move(r) };
}
