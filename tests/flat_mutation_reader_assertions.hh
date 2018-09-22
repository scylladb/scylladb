/*
 * Copyright (C) 2017 ScyllaDB
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
#include "flat_mutation_reader.hh"
#include "mutation_assertions.hh"
#include "schema.hh"

// Intended to be called in a seastar thread
class flat_reader_assertions {
    flat_mutation_reader _reader;
    dht::partition_range _pr;
private:
    mutation_fragment_opt read_next() {
        return _reader(db::no_timeout).get0();
    }
public:
    flat_reader_assertions(flat_mutation_reader reader)
        : _reader(std::move(reader))
    { }

    flat_reader_assertions& produces_partition_start(const dht::decorated_key& dk,
                                                     stdx::optional<tombstone> tomb = stdx::nullopt) {
        BOOST_TEST_MESSAGE(sprint("Expecting partition start with key %s", dk));
        auto mfopt = read_next();
        if (!mfopt) {
            BOOST_FAIL(sprint("Expected: partition start with key %s, got end of stream", dk));
        }
        if (!mfopt->is_partition_start()) {
            BOOST_FAIL(sprint("Expected: partition start with key %s, got: %s", dk, *mfopt));
        }
        if (!mfopt->as_partition_start().key().equal(*_reader.schema(), dk)) {
            BOOST_FAIL(sprint("Expected: partition start with key %s, got: %s", dk, *mfopt));
        }
        if (tomb && mfopt->as_partition_start().partition_tombstone() != *tomb) {
            BOOST_FAIL(sprint("Expected: partition start with tombstone %s, got: %s", *tomb, *mfopt));
        }
        return *this;
    }

    flat_reader_assertions& produces_static_row() {
        BOOST_TEST_MESSAGE(sprint("Expecting static row"));
        auto mfopt = read_next();
        if (!mfopt) {
            BOOST_FAIL("Expected static row, got end of stream");
        }
        if (!mfopt->is_static_row()) {
            BOOST_FAIL(sprint("Expected static row, got: %s", *mfopt));
        }
        return *this;
    }

    flat_reader_assertions& produces_row_with_key(const clustering_key& ck) {
        BOOST_TEST_MESSAGE(sprint("Expect %s", ck));
        auto mfopt = read_next();
        if (!mfopt) {
            BOOST_FAIL(sprint("Expected row with key %s, but got end of stream", ck));
        }
        if (!mfopt->is_clustering_row()) {
            BOOST_FAIL(sprint("Expected row with key %s, but got %s", ck, *mfopt));
        }
        auto& actual = mfopt->as_clustering_row().key();
        if (!actual.equal(*_reader.schema(), ck)) {
            BOOST_FAIL(sprint("Expected row with key %s, but key is %s", ck, actual));
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
        BOOST_TEST_MESSAGE(sprint("Expecting static row"));
        auto mfopt = read_next();
        if (!mfopt) {
            BOOST_FAIL("Expected static row, got end of stream");
        }
        if (!mfopt->is_static_row()) {
            BOOST_FAIL(sprint("Expected static row, got: %s", *mfopt));
        }
        auto& cells = mfopt->as_static_row().cells();
        if (cells.size() != columns.size()) {
            BOOST_FAIL(sprint("Expected static row with %s columns, but has %s", columns.size(), cells.size()));
        }
        for (size_t i = 0; i < columns.size(); ++i) {
            const atomic_cell_or_collection* cell = cells.find_cell(columns[i].id);
            if (!cell) {
                BOOST_FAIL(sprint("Expected static row with column %s, but it is not present", columns[i].name));
            }
            auto& cdef = _reader.schema()->static_column_at(columns[i].id);
            auto cmp = compare_unsigned(columns[i].value, cell->as_atomic_cell(cdef).value().linearize());
            if (cmp != 0) {
                BOOST_FAIL(sprint("Expected static row with column %s having value %s, but it has value %s",
                                  columns[i].name,
                                  columns[i].value,
                                  cell->as_atomic_cell(cdef).value()));
            }
        }
        return *this;
    }

    flat_reader_assertions& produces_row(const clustering_key& ck, const std::vector<expected_column>& columns) {
        BOOST_TEST_MESSAGE(sprint("Expect %s", ck));
        auto mfopt = read_next();
        if (!mfopt) {
            BOOST_FAIL(sprint("Expected row with key %s, but got end of stream", ck));
        }
        if (!mfopt->is_clustering_row()) {
            BOOST_FAIL(sprint("Expected row with key %s, but got %s", ck, *mfopt));
        }
        auto& actual = mfopt->as_clustering_row().key();
        if (!actual.equal(*_reader.schema(), ck)) {
            BOOST_FAIL(sprint("Expected row with key %s, but key is %s", ck, actual));
        }
        auto& cells = mfopt->as_clustering_row().cells();
        if (cells.size() != columns.size()) {
            BOOST_FAIL(sprint("Expected row with %s columns, but has %s", columns.size(), cells.size()));
        }
        for (size_t i = 0; i < columns.size(); ++i) {
            const atomic_cell_or_collection* cell = cells.find_cell(columns[i].id);
            if (!cell) {
                BOOST_FAIL(sprint("Expected row with column %s, but it is not present", columns[i].name));
            }
            auto& cdef = _reader.schema()->regular_column_at(columns[i].id);
            assert (!cdef.is_multi_cell());
            auto cmp = compare_unsigned(columns[i].value, cell->as_atomic_cell(cdef).value().linearize());
            if (cmp != 0) {
                BOOST_FAIL(sprint("Expected row with column %s having value %s, but it has value %s",
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
        BOOST_TEST_MESSAGE(sprint("Expect %s", ck));
        auto mfopt = read_next();
        if (!mfopt) {
            BOOST_FAIL(sprint("Expected row with key %s, but got end of stream", ck));
        }
        if (!mfopt->is_clustering_row()) {
            BOOST_FAIL(sprint("Expected row with key %s, but got %s", ck, *mfopt));
        }
        auto& actual = mfopt->as_clustering_row().key();
        if (!actual.equal(*_reader.schema(), ck)) {
            BOOST_FAIL(sprint("Expected row with key %s, but key is %s", ck, actual));
        }
        auto& cells = mfopt->as_clustering_row().cells();
        if (cells.size() != column_ids.size()) {
            BOOST_FAIL(sprint("Expected row with %s columns, but has %s", column_ids.size(), cells.size()));
        }
        for (size_t i = 0; i < column_ids.size(); ++i) {
            const atomic_cell_or_collection* cell = cells.find_cell(column_ids[i]);
            if (!cell) {
                BOOST_FAIL(sprint("Expected row with column %d, but it is not present", column_ids[i]));
            }
            auto& cdef = _reader.schema()->regular_column_at(column_ids[i]);
            column_assert[i](cdef, cell);
        }
        return *this;
    }

    // If ck_ranges is passed, verifies only that information relevant for ck_ranges matches.
    flat_reader_assertions& produces_range_tombstone(const range_tombstone& rt, const query::clustering_row_ranges& ck_ranges = {}) {
        BOOST_TEST_MESSAGE(sprint("Expect %s", rt));
        auto mfo = read_next();
        if (!mfo) {
            BOOST_FAIL(sprint("Expected range tombstone %s, but got end of stream", rt));
        }
        if (!mfo->is_range_tombstone()) {
            BOOST_FAIL(sprint("Expected range tombstone %s, but got %s", rt, *mfo));
        }
        const schema& s = *_reader.schema();
        range_tombstone_list actual_list(s);
        position_in_partition::equal_compare eq(s);
        while (mutation_fragment* next = _reader.peek(db::no_timeout).get0()) {
            if (!next->is_range_tombstone() || !eq(next->position(), mfo->position())) {
                break;
            }
            actual_list.apply(s, _reader(db::no_timeout).get0()->as_range_tombstone());
        }
        actual_list.apply(s, mfo->as_range_tombstone());
        {
            range_tombstone_list expected_list(s);
            expected_list.apply(s, rt);
            actual_list.trim(s, ck_ranges);
            expected_list.trim(s, ck_ranges);
            if (!actual_list.equal(s, expected_list)) {
                BOOST_FAIL(sprint("Expected %s, but got %s", expected_list, actual_list));
            }
        }
        return *this;
    }

    flat_reader_assertions& produces_partition_end() {
        BOOST_TEST_MESSAGE("Expecting partition end");
        auto mfopt = read_next();
        if (!mfopt) {
            BOOST_FAIL(sprint("Expected partition end but got end of stream"));
        }
        if (!mfopt->is_end_of_partition()) {
            BOOST_FAIL(sprint("Expected partition end but got %s", *mfopt));
        }
        return *this;
    }

    flat_reader_assertions& produces(const schema& s, const mutation_fragment& mf) {
        auto mfopt = read_next();
        if (!mfopt) {
            BOOST_FAIL(sprint("Expected %s, but got end of stream", mf));
        }
        if (!mfopt->equal(s, mf)) {
            BOOST_FAIL(sprint("Expected %s, but got %s", mf, *mfopt));
        }
        return *this;
    }

    flat_reader_assertions& produces_end_of_stream() {
        BOOST_TEST_MESSAGE("Expecting end of stream");
        auto mfopt = read_next();
        if (bool(mfopt)) {
            BOOST_FAIL(sprint("Expected end of stream, got %s", *mfopt));
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
            BOOST_FAIL(sprint("Expected mutation fragment %s, got end of stream", ck));
        }
        if (mfopt->mutation_fragment_kind() != k) {
            BOOST_FAIL(sprint("Expected mutation fragment kind %s, got: %s", k, mfopt->mutation_fragment_kind()));
        }
        clustering_key::equality ck_eq(*_reader.schema());
        if (!ck_eq(mfopt->key(), ck)) {
            BOOST_FAIL(sprint("Expected key %s, got: %s", ck, mfopt->key()));
        }
        return *this;
    }

    flat_reader_assertions& produces_partition(const mutation& m) {
        return produces(m);
    }

    flat_reader_assertions& produces(const mutation& m, const stdx::optional<query::clustering_row_ranges>& ck_ranges = {}) {
        auto mo = read_mutation_from_flat_mutation_reader(_reader, db::no_timeout).get0();
        if (!mo) {
            BOOST_FAIL(sprint("Expected %s, but got end of stream, at: %s", m, seastar::current_backtrace()));
        }
        memory::disable_failure_guard dfg;
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
        BOOST_TEST_MESSAGE("Expecting eos or empty mutation");
        auto mo = read_mutation_from_flat_mutation_reader(_reader, db::no_timeout).get0();
        if (mo) {
            if (!mo->partition().empty()) {
                BOOST_FAIL(sprint("Mutation is not empty: %s", *mo));
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
                    BOOST_FAIL(sprint("previous partition had greater key: prev=%s, current=%s", *previous_partition, *mfo));
                }
                previous_partition = std::move(mfo);
                previous_fragment = stdx::nullopt;
                inside_partition = true;
            } else if (mfo->is_end_of_partition()) {
                BOOST_REQUIRE(inside_partition);
                inside_partition = false;
            } else {
                BOOST_REQUIRE(inside_partition);
                if (previous_fragment) {
                    if (!less(previous_fragment->position(), mfo->position())) {
                        BOOST_FAIL(sprint("previous fragment has greater position: prev=%s, current=%s", *previous_fragment, *mfo));
                    }
                }
                previous_fragment = std::move(mfo);
            }
        }
        BOOST_REQUIRE(!inside_partition);
    }

    flat_reader_assertions& fast_forward_to(const dht::partition_range& pr) {
        _pr = pr;
        _reader.fast_forward_to(_pr, db::no_timeout).get();
        return *this;
    }

    flat_reader_assertions& next_partition() {
        _reader.next_partition();
        return *this;
    }

    flat_reader_assertions& fast_forward_to(position_range pr) {
        _reader.fast_forward_to(std::move(pr), db::no_timeout).get();
        return *this;
    }

    flat_reader_assertions& fast_forward_to(const clustering_key& ck1, const clustering_key& ck2) {
        return fast_forward_to(position_range{
            position_in_partition(position_in_partition::clustering_row_tag_t(), ck1),
            position_in_partition(position_in_partition::clustering_row_tag_t(), ck2)
        });
    }

    flat_reader_assertions& produces_compacted(const mutation& m, const stdx::optional<query::clustering_row_ranges>& ck_ranges = {}) {
        auto mo = read_mutation_from_flat_mutation_reader(_reader, db::no_timeout).get0();
        BOOST_REQUIRE(bool(mo));
        memory::disable_failure_guard dfg;
        mutation got = *mo;
        got.partition().compact_for_compaction(*m.schema(), always_gc, gc_clock::now());
        assert_that(got).is_equal_to(m, ck_ranges);
        return *this;
    }

    mutation_assertion next_mutation() {
        auto mo = read_mutation_from_flat_mutation_reader(_reader, db::no_timeout).get0();
        BOOST_REQUIRE(bool(mo));
        return mutation_assertion(std::move(*mo));
    }

    future<> fill_buffer() {
        return _reader.fill_buffer(db::no_timeout);
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
