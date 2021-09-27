/*
 * Copyright (C) 2017-present ScyllaDB
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
#include <seastar/util/backtrace.hh>
#include "flat_mutation_reader_v2.hh"
#include "mutation_assertions.hh"
#include "schema.hh"
#include "test/lib/log.hh"

// Intended to be called in a seastar thread
class flat_reader_assertions {
    flat_mutation_reader _reader;
    dht::partition_range _pr;
    range_tombstone_list _tombstones;
private:
    mutation_fragment_opt read_next() {
        return _reader().get0();
    }

    static bool are_tombstones_mergeable(const schema& s, const range_tombstone& a, const range_tombstone& b) {
        const auto range_a = position_range(position_in_partition(a.position()), position_in_partition(a.end_position()));
        const auto tri_cmp = position_in_partition::tri_compare(s);
        return (a.tomb <=> b.tomb) == 0 && (range_a.overlaps(s, b.position(), b.end_position()) ||
                tri_cmp(a.end_position(), b.position()) == 0 ||
                tri_cmp(b.end_position(), a.position()) == 0);
    }

public:
    flat_reader_assertions(flat_mutation_reader reader)
        : _reader(std::move(reader))
        , _tombstones(*_reader.schema())
    { }

    ~flat_reader_assertions() {
        _reader.close().get();
    }

    flat_reader_assertions(const flat_reader_assertions&) = delete;
    flat_reader_assertions(flat_reader_assertions&&) = default;

    flat_reader_assertions& operator=(flat_reader_assertions&& o) {
        if (this != &o) {
            _reader.close().get();
            _reader = std::move(o._reader);
            _pr = std::move(o._pr);
            _tombstones = std::move(o._tombstones);
        }
        return *this;
    }

    flat_reader_assertions& produces_partition_start(const dht::decorated_key& dk,
                                                     std::optional<tombstone> tomb = std::nullopt) {
        testlog.trace("Expecting partition start with key {}", dk);
        auto mfopt = read_next();
        if (!mfopt) {
            BOOST_FAIL(format("Expected: partition start with key {}, got end of stream", dk));
        }
        if (!mfopt->is_partition_start()) {
            BOOST_FAIL(format("Expected: partition start with key {}, got: {}", dk, mutation_fragment::printer(*_reader.schema(), *mfopt)));
        }
        if (!mfopt->as_partition_start().key().equal(*_reader.schema(), dk)) {
            BOOST_FAIL(format("Expected: partition start with key {}, got: {}", dk, mutation_fragment::printer(*_reader.schema(), *mfopt)));
        }
        if (tomb && mfopt->as_partition_start().partition_tombstone() != *tomb) {
            BOOST_FAIL(format("Expected: partition start with tombstone {}, got: {}", *tomb, mutation_fragment::printer(*_reader.schema(), *mfopt)));
        }
        _tombstones.clear();
        return *this;
    }

    flat_reader_assertions& produces_static_row() {
        testlog.trace("Expecting static row");
        auto mfopt = read_next();
        if (!mfopt) {
            BOOST_FAIL("Expected static row, got end of stream");
        }
        if (!mfopt->is_static_row()) {
            BOOST_FAIL(format("Expected static row, got: {}", mutation_fragment::printer(*_reader.schema(), *mfopt)));
        }
        return *this;
    }

    flat_reader_assertions& produces_row_with_key(const clustering_key& ck, std::optional<api::timestamp_type> active_range_tombstone = std::nullopt) {
        testlog.trace("Expect {}", ck);
        auto mfopt = read_next();
        if (!mfopt) {
            BOOST_FAIL(format("Expected row with key {}, but got end of stream", ck));
        }
        if (!mfopt->is_clustering_row()) {
            BOOST_FAIL(format("Expected row with key {}, but got {}", ck, mutation_fragment::printer(*_reader.schema(), *mfopt)));
        }
        auto& actual = mfopt->as_clustering_row().key();
        if (!actual.equal(*_reader.schema(), ck)) {
            BOOST_FAIL(format("Expected row with key {}, but key is {}", ck, actual));
        }
        if (active_range_tombstone) {
            BOOST_CHECK_EQUAL(*active_range_tombstone, _tombstones.search_tombstone_covering(*_reader.schema(), ck).timestamp);
        }
        return *this;
    }

    flat_reader_assertions& may_produce_tombstones(position_range range) {
        while (mutation_fragment* next = _reader.peek().get0()) {
            if (next->is_range_tombstone()) {
                if (!range.overlaps(*_reader.schema(), next->as_range_tombstone().position(), next->as_range_tombstone().end_position())) {
                    break;
                }
                testlog.trace("Received range tombstone: {}", mutation_fragment::printer(*_reader.schema(), *next));
                range = position_range(position_in_partition(next->position()), range.end());
                _tombstones.apply(*_reader.schema(), _reader().get0()->as_range_tombstone());
            } else if (next->is_clustering_row() && next->as_clustering_row().empty()) {
                if (!range.contains(*_reader.schema(), next->position())) {
                    break;
                }
                // There is no difference between an empty row and a row that doesn't exist.
                // While readers that emit spurious empty rows may be wasteful, it is not
                // incorrect to do so, so let's ignore them.
                testlog.trace("Received empty clustered row: {}", mutation_fragment::printer(*_reader.schema(), *next));
                range = position_range(position_in_partition(next->position()), range.end());
                _reader().get();
            } else {
                break;
            }
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

    flat_reader_assertions& produces_static_row(const std::vector<expected_column>& columns) {
        testlog.trace("Expecting static row");
        auto mfopt = read_next();
        if (!mfopt) {
            BOOST_FAIL("Expected static row, got end of stream");
        }
        if (!mfopt->is_static_row()) {
            BOOST_FAIL(format("Expected static row, got: {}", mutation_fragment::printer(*_reader.schema(), *mfopt)));
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

    flat_reader_assertions& produces_row(const clustering_key& ck, const std::vector<expected_column>& columns) {
        testlog.trace("Expect {}", ck);
        auto mfopt = read_next();
        if (!mfopt) {
            BOOST_FAIL(format("Expected row with key {}, but got end of stream", ck));
        }
        if (!mfopt->is_clustering_row()) {
            BOOST_FAIL(format("Expected row with key {}, but got {}", ck, mutation_fragment::printer(*_reader.schema(), *mfopt)));
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
            assert (!cdef.is_multi_cell());
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

    flat_reader_assertions& produces_row(const clustering_key& ck,
                                         const std::vector<column_id>& column_ids,
                                         const std::vector<assert_function>& column_assert) {
        testlog.trace("Expect {}", ck);
        auto mfopt = read_next();
        if (!mfopt) {
            BOOST_FAIL(format("Expected row with key {}, but got end of stream", ck));
        }
        if (!mfopt->is_clustering_row()) {
            BOOST_FAIL(format("Expected row with key {}, but got {}", ck, mutation_fragment::printer(*_reader.schema(), *mfopt)));
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

    // If ck_ranges is passed, verifies only that information relevant for ck_ranges matches.
    flat_reader_assertions& produces_range_tombstone(const range_tombstone& rt, const query::clustering_row_ranges& ck_ranges = {}) {
        testlog.trace("Expect {}", rt);
        auto mfo = read_next();
        if (!mfo) {
            BOOST_FAIL(format("Expected range tombstone {}, but got end of stream", rt));
        }
        if (!mfo->is_range_tombstone()) {
            BOOST_FAIL(format("Expected range tombstone {}, but got {}", rt, mutation_fragment::printer(*_reader.schema(), *mfo)));
        }
        const schema& s = *_reader.schema();
        range_tombstone_list actual_list(s);
        actual_list.apply(s, mfo->as_range_tombstone());
        _tombstones.apply(s, mfo->as_range_tombstone());
        position_in_partition::equal_compare eq(s);
        while (mutation_fragment* next = _reader.peek().get0()) {
            if (!next->is_range_tombstone() || !are_tombstones_mergeable(s, actual_list.begin()->tombstone(), next->as_range_tombstone())) {
                break;
            }
            auto rt = _reader().get0()->as_range_tombstone();
            actual_list.apply(s, rt);
            assert(actual_list.size() == 1);
            _tombstones.apply(s, rt);
        }
        {
            range_tombstone_list expected_list(s);
            expected_list.apply(s, rt);
            actual_list.trim(s, ck_ranges);
            expected_list.trim(s, ck_ranges);
            if (!actual_list.equal(s, expected_list)) {
                BOOST_FAIL(format("Expected {}, but got {}", expected_list, actual_list));
            }
        }
        return *this;
    }

    flat_reader_assertions& produces_partition_end() {
        testlog.trace("Expecting partition end");
        auto mfopt = read_next();
        if (!mfopt) {
            BOOST_FAIL(format("Expected partition end but got end of stream"));
        }
        if (!mfopt->is_end_of_partition()) {
            BOOST_FAIL(format("Expected partition end but got {}", mutation_fragment::printer(*_reader.schema(), *mfopt)));
        }
        _tombstones.clear();
        return *this;
    }

    flat_reader_assertions& produces(const schema& s, const mutation_fragment& mf) {
        testlog.trace("Expect {}", mutation_fragment::printer(s, mf));
        auto mfopt = read_next();
        if (!mfopt) {
            BOOST_FAIL(format("Expected {}, but got end of stream", mutation_fragment::printer(*_reader.schema(), mf)));
        }
        if (!mfopt->equal(s, mf)) {
            BOOST_FAIL(format("Expected {}, but got {}", mutation_fragment::printer(*_reader.schema(), mf), mutation_fragment::printer(*_reader.schema(), *mfopt)));
        }
        if (mf.is_range_tombstone()) {
            _tombstones.apply(*_reader.schema(), mf.as_range_tombstone());
        }
        return *this;
    }

    flat_reader_assertions& produces_end_of_stream() {
        testlog.trace("Expecting end of stream");
        auto mfopt = read_next();
        if (bool(mfopt)) {
            BOOST_FAIL(format("Expected end of stream, got {}", mutation_fragment::printer(*_reader.schema(), *mfopt)));
        }
        return *this;
    }

    flat_reader_assertions& produces(mutation_fragment::kind k, std::vector<int> ck_elements, bool make_full_key = false) {
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
        if (mfopt->is_range_tombstone()) {
            _tombstones.apply(*_reader.schema(), mfopt->as_range_tombstone());
        }
        return *this;
    }

    flat_reader_assertions& produces_partition(const mutation& m) {
        return produces(m);
    }

    flat_reader_assertions& produces(const mutation& m, const std::optional<query::clustering_row_ranges>& ck_ranges = {}) {
        auto mo = read_mutation_from_flat_mutation_reader(_reader).get0();
        if (!mo) {
            BOOST_FAIL(format("Expected {}, but got end of stream, at: {}", m, seastar::current_backtrace()));
        }
        memory::scoped_critical_alloc_section dfg;
        assert_that(*mo).is_equal_to(m, ck_ranges);
        return *this;
    }

    flat_reader_assertions& produces(const dht::decorated_key& dk) {
        produces_partition_start(dk);
        next_partition();
        return *this;
    }

    template<typename Range>
    flat_reader_assertions& produces(const Range& range) {
        for (auto&& m : range) {
            produces(m);
        }
        return *this;
    }

    flat_reader_assertions& produces_eos_or_empty_mutation() {
        testlog.trace("Expecting eos or empty mutation");
        auto mo = read_mutation_from_flat_mutation_reader(_reader).get0();
        if (mo) {
            if (!mo->partition().empty()) {
                BOOST_FAIL(format("Mutation is not empty: {}", *mo));
            }
        }
        return *this;
    }

    void has_monotonic_positions() {
        position_in_partition::less_compare less(*_reader.schema());
        mutation_fragment_opt previous_fragment;
        mutation_fragment_opt previous_partition;
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
                                      mutation_fragment::printer(*_reader.schema(), *previous_partition), mutation_fragment::printer(*_reader.schema(), *mfo)));
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
                        BOOST_FAIL(format("previous fragment has greater position: prev={}, current={}",
                                          mutation_fragment::printer(*_reader.schema(), *previous_fragment), mutation_fragment::printer(*_reader.schema(), *mfo)));
                    }
                }
                previous_fragment = std::move(mfo);
            }
        }
        BOOST_REQUIRE(!inside_partition);
    }

    flat_reader_assertions& fast_forward_to(const dht::partition_range& pr) {
        testlog.trace("Fast forward to partition range: {}", pr);
        _pr = pr;
        _reader.fast_forward_to(_pr).get();
        return *this;
    }

    flat_reader_assertions& next_partition() {
        testlog.trace("Skip to next partition");
        _reader.next_partition().get();
        return *this;
    }

    flat_reader_assertions& fast_forward_to(position_range pr) {
        testlog.trace("Fast forward to clustering range: {}", pr);
        _reader.fast_forward_to(std::move(pr)).get();
        return *this;
    }

    flat_reader_assertions& fast_forward_to(const clustering_key& ck1, const clustering_key& ck2) {
        testlog.trace("Fast forward to clustering range: [{}, {})", ck1, ck2);
        return fast_forward_to(position_range{
            position_in_partition(position_in_partition::clustering_row_tag_t(), ck1),
            position_in_partition(position_in_partition::clustering_row_tag_t(), ck2)
        });
    }

    flat_reader_assertions& produces_compacted(const mutation& m, gc_clock::time_point query_time,
            const std::optional<query::clustering_row_ranges>& ck_ranges = {}) {
        auto mo = read_mutation_from_flat_mutation_reader(_reader).get0();
        // If the passed in mutation is empty, allow for the reader to produce an empty or no partition.
        if (m.partition().empty() && !mo) {
            return *this;
        }
        BOOST_REQUIRE(bool(mo));
        memory::scoped_critical_alloc_section dfg;
        mutation got = *mo;
        got.partition().compact_for_compaction(*m.schema(), always_gc, query_time);
        assert_that(got).is_equal_to(m, ck_ranges);
        return *this;
    }

    mutation_assertion next_mutation() {
        auto mo = read_mutation_from_flat_mutation_reader(_reader).get0();
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
flat_reader_assertions assert_that(flat_mutation_reader r) {
    return { std::move(r) };
}

// Intended to be called in a seastar thread
class flat_reader_assertions_v2 {
    flat_mutation_reader_v2 _reader;
    dht::partition_range _pr;
private:
    mutation_fragment_v2_opt read_next() {
        return _reader().get0();
    }
public:
    flat_reader_assertions_v2(flat_mutation_reader_v2 reader)
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
        }
        return *this;
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

    flat_reader_assertions_v2& produces_row_with_key(const clustering_key& ck) {
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
            assert (!cdef.is_multi_cell());
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

    flat_reader_assertions_v2& produces_range_tombstone_change(const range_tombstone_change& rt) {
        testlog.trace("Expect {}", rt);
        auto mfo = read_next();
        if (!mfo) {
            BOOST_FAIL(format("Expected range tombstone {}, but got end of stream", rt));
        }
        if (!mfo->is_range_tombstone_change()) {
            BOOST_FAIL(format("Expected range tombstone change {}, but got {}", rt, mutation_fragment_v2::printer(*_reader.schema(), *mfo)));
        }
        if (!mfo->as_range_tombstone_change().equal(*_reader.schema(), rt)) {
            BOOST_FAIL(format("Expected {}, but got {}", rt, mutation_fragment_v2::printer(*_reader.schema(), *mfo)));
        }
        return *this;
    }

    flat_reader_assertions_v2& produces_partition_end() {
        testlog.trace("Expecting partition end");
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
        return *this;
    }

    flat_reader_assertions_v2& produces(mutation_fragment_v2::kind k, std::vector<int> ck_elements, bool make_full_key = false) {
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
        return *this;
    }

    flat_reader_assertions_v2& produces_partition(const mutation& m) {
        return produces(m);
    }

    flat_reader_assertions_v2& produces(const mutation& m, const std::optional<query::clustering_row_ranges>& ck_ranges = {}) {
        auto mo = read_mutation_from_flat_mutation_reader(_reader).get0();
        if (!mo) {
            BOOST_FAIL(format("Expected {}, but got end of stream, at: {}", m, seastar::current_backtrace()));
        }
        memory::scoped_critical_alloc_section dfg;
        assert_that(*mo).is_equal_to(m, ck_ranges);
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
        auto mo = read_mutation_from_flat_mutation_reader(_reader).get0();
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
                    if (!less(previous_fragment->position(), mfo->position())) {
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
        auto mo = read_mutation_from_flat_mutation_reader(_reader).get0();
        // If the passed in mutation is empty, allow for the reader to produce an empty or no partition.
        if (m.partition().empty() && !mo) {
            return *this;
        }
        BOOST_REQUIRE(bool(mo));
        memory::scoped_critical_alloc_section dfg;
        mutation got = *mo;
        got.partition().compact_for_compaction(*m.schema(), always_gc, query_time);
        assert_that(got).is_equal_to(m, ck_ranges);
        return *this;
    }

    mutation_assertion next_mutation() {
        auto mo = read_mutation_from_flat_mutation_reader(_reader).get0();
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
flat_reader_assertions_v2 assert_that(flat_mutation_reader_v2 r) {
    return { std::move(r) };
}
