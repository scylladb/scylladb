/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/testing/test_case.hh>
#include "sstables/mx/reader.hh"
#include "test/lib/mutation_reader_assertions.hh"
#include "test/lib/simple_schema.hh"
#include "test/lib/sstable_utils.hh"
#include "test/lib/nondeterministic_choice_stack.hh"
#include "test/lib/sstable_test_env.hh"
#include "partition_slice_builder.hh"

// Return the positions of all clustering key blocks (known to the index)
// in the Data file.
std::vector<uint64_t> get_ck_positions(shared_sstable sst, reader_permit permit) {
    std::vector<uint64_t> result;
    auto abstract_r = sst->make_index_reader(permit, nullptr, use_caching::no, false);
    auto& r = dynamic_cast<index_reader&>(*abstract_r);
    r.advance_to(dht::partition_range::make_open_ended_both_sides()).get();
    r.read_partition_data().get();
    auto close_ir = deferred_close(r);

    while (!r.eof()) {
        auto partition_pos = r.data_file_positions().start;
        sstables::clustered_index_cursor* cur = r.current_clustered_cursor();
        while (auto ei_opt = cur->next_entry().get()) {
            result.push_back(ei_opt.value().offset + partition_pos);
        }
        r.advance_to_next_partition().get();
    }
    return result;
}

// Return the positions of all partitions in the Data file.
std::vector<uint64_t> get_partition_positions(shared_sstable sst, reader_permit permit) {
    std::vector<uint64_t> result;
    {
        auto ir = sst->make_index_reader(permit, nullptr, use_caching::no, false);
        ir->advance_to(dht::partition_range::make_open_ended_both_sides()).get();
        ir->read_partition_data().get();
        auto close_ir = deferred_close(*ir);
        while (!ir->eof()) {
            result.push_back(ir->data_file_positions().start);
            ir->advance_to_next_partition().get();
        }
    }
    return result;
}

// An in-memory implementation of a sstable index,
// which can be off by one partition on queries,
// and might not be able to provide the partition key for the target partition.
//
// (I.e. it behaves like index which by default only stores key prefixes
// instead of whole keys).
//
// It is nondeterministic whether the index query resutls are exact or inexact,
// and whether the partition key is known or not.
// This allows for checking if the reader correctly handles either case.
struct inexact_partition_index : abstract_index_reader {
    nondeterministic_choice_stack& _ncs;
    std::vector<uint64_t> _positions;
    std::span<const uint64_t> _pk_positions;
    std::span<const uint64_t> _ck_positions;
    std::span<const dht::decorated_key> _pks;
    std::span<const clustering_key> _cks;
    schema_ptr _s;

    int _lower = 0;
    int _upper = 0;

    inexact_partition_index(
        nondeterministic_choice_stack& ncs,
        std::span<const uint64_t> pk_positions,
        std::span<const uint64_t> ck_positions,
        std::span<const dht::decorated_key> pks,
        std::span<const clustering_key> cks,
        schema_ptr s)
    : _ncs(ncs)
    , _pk_positions(pk_positions)
    , _ck_positions(ck_positions)
    , _pks(pks)
    , _cks(cks)
    , _s(s) {
        _positions.insert(_positions.end(), ck_positions.begin(), ck_positions.end());
        _positions.insert(_positions.end(), pk_positions.begin(), pk_positions.end());
        std::ranges::sort(_positions);
        _upper = _positions.size() - 1;
    }

    future<> close() noexcept override {
        return make_ready_future<>();
    }
    data_file_positions_range data_file_positions() const override {
        return {_positions[_lower], _positions[_upper]};
    }
    future<std::optional<uint64_t>> last_block_offset() override {
        abort();
    }
    future<bool> advance_lower_and_check_if_present(dht::ring_position_view rpv) override {
        auto cmp = dht::ring_position_less_comparator(*_s);
        auto pk_idx = std::ranges::lower_bound(_pks, rpv, cmp) - _pks.begin();
        if (dht::ring_position_comparator(*_s)(_pks[pk_idx], rpv) != 0) {
            if (pk_idx != 0) {
                if (_ncs.choose_bool()) {
                    // The index *might* conservatively return a partition immediately before the target.
                    pk_idx -= 1;
                }
            }
        }
        _lower = pk_idx_to_pos_idx(pk_idx);
        testlog.trace("<inexact_partition_index/advance_lower_and_check_if_present: _lower={}, _upper={}, pk_idx={}", _lower, _upper, pk_idx);
        return make_ready_future<bool>(true);
    }
    future<> advance_upper_past(position_in_partition_view pos) override {
        auto cmp = position_in_partition::less_compare(*_s);
        auto pk_idx = pos_idx_to_pk_idx(_lower);
        _upper = pk_idx_to_pos_idx(pk_idx);
        size_t ck_idx = std::ranges::upper_bound(_cks, pos, cmp) - _cks.begin();
        _upper = (ck_idx == _cks.size()) ? _upper + 1 : _upper + ck_idx;
        testlog.trace("inexact_partition_index/advance_upper_past: _lower={}, _upper={}, pk_idx={}", _lower, _upper, pk_idx);
        return make_ready_future<>();
    }
    future<> advance_to_next_partition() override {
        auto pk_idx = pos_idx_to_pk_idx(_lower);
        testlog.trace(">inexact_partition_index/advance_to_next_partition: _lower={}, _upper={}, pk_idx={}", _lower, _upper, pk_idx);
        pk_idx += 1;
        _lower = pk_idx_to_pos_idx(pk_idx);
        testlog.trace("<inexact_partition_index/advance_to_next_partition: _lower={}, _upper={}, pk_idx={}", _lower, _upper, pk_idx);
        return make_ready_future<>();
    }
    indexable_element element_kind() const override {
        if (eof()) {
            return indexable_element::partition;
        }
        if (_positions[_lower] == *std::ranges::lower_bound(_pk_positions, _positions[_lower])) {
            return indexable_element::partition;
        } else {
            return indexable_element::cell;
        }
    }
    uint64_t pk_idx_to_pos_idx(uint64_t pk_idx) {
        return std::ranges::lower_bound(_positions, _pk_positions[pk_idx]) - _positions.begin();
    }
    uint64_t pos_idx_to_pk_idx(uint64_t pos_idx) {
        return std::ranges::upper_bound(_pk_positions, _positions[pos_idx]) - _pk_positions.begin() - 1;
    }
    future<> advance_past_definitely_present_partition(const dht::decorated_key& dk) override {
        auto cmp = dht::ring_position_less_comparator(*_s);
        size_t pk_idx = std::ranges::lower_bound(_pks, dk, cmp) - _pks.begin() + 1;
        _lower = pk_idx_to_pos_idx(pk_idx);
        testlog.trace("inexact_partition_index/advance_past_definitely_present_partition: _lower={}, _upper={}, pk_idx={}", _lower, _upper, pk_idx);
        return make_ready_future<>();
    }
    future<> advance_to_definitely_present_partition(const dht::decorated_key& dk) override {
        auto cmp = dht::ring_position_less_comparator(*_s);
        size_t pk_idx = std::ranges::lower_bound(_pks, dk, cmp) - _pks.begin();
        _lower = pk_idx_to_pos_idx(pk_idx);
        testlog.trace("inexact_partition_index/advance_to_definitely_present_partition: _lower={}, _upper={}, pk_idx={}", _lower, _upper, pk_idx);
        return make_ready_future<>();
    }
    future<> advance_to(position_in_partition_view pos) override {
        auto cmp = position_in_partition::less_compare(*_s);
        auto pk_idx = pos_idx_to_pk_idx(_lower);
        testlog.trace(">inexact_partition_index/advance_to(pipv={}): _lower={}, _upper={}, pk_idx={}", pos, _lower, _upper, pk_idx);
        size_t ck_idx = std::max<int>(0, std::ranges::upper_bound(_cks, pos, cmp) - _cks.begin() - 1);
        _lower = pk_idx * (_cks.size() + 1) + ck_idx;
        testlog.trace("<inexact_partition_index/advance_to(pipv={}): _lower={}, _upper={}, pk_idx={}", pos, _lower, _upper, pk_idx);
        return make_ready_future<>();
    }
    std::optional<sstables::deletion_time> partition_tombstone() override {
        return sstables::deletion_time {
            std::numeric_limits<int32_t>::max(),
            std::numeric_limits<int64_t>::min(),
        };
    }
    std::optional<partition_key> get_partition_key() override {
        if (_ncs.choose_bool()) {
            // The index *might* have the PK present.
            return _pks[pos_idx_to_pk_idx(_lower)].key();
        }
        return std::nullopt;
    }
    bool partition_data_ready() const override {
        return true;
    }
    future<> read_partition_data() override {
        return make_ready_future<>();
    }
    future<> advance_reverse(position_in_partition_view pos) override {
        abort();
    }
    future<> advance_to(const dht::partition_range& pr) override {
        auto cmp = dht::ring_position_less_comparator(*_s);
        if (pr.start()) {
            auto rpv = dht::ring_position_view::for_range_start(pr);
            auto pk_idx = std::ranges::lower_bound(_pks, rpv, cmp) - _pks.begin();
            if (pk_idx != 0) {
                if (_ncs.choose_bool()) {
                    // The index *might* conservatively return a partition immediately before the target.
                    pk_idx -= 1;
                }
            }
            _lower = pk_idx_to_pos_idx(pk_idx);
        } else {
            _lower = 0;
        }
        if (pr.end()) {
            auto rpv = dht::ring_position_view::for_range_end(pr);
            size_t pk_idx = std::ranges::upper_bound(_pks, rpv, cmp) - _pks.begin();
            if (pk_idx != _pks.size()) {
                if (_ncs.choose_bool()) {
                    // The index *might* conservatively return a partition immediately after the target.
                    pk_idx += 1;
                }
            }
            _upper = pk_idx_to_pos_idx(pk_idx);
        } else {
            _upper = _positions.size() - 1;
        }
        testlog.trace("inexact_partition_index/advance_to(pr={}), _lower={}, _upper={}", pr, _lower, _upper);
        return make_ready_future<>();
    }
    future<> advance_reverse_to_next_partition() override {
        abort();
    }
    std::optional<open_rt_marker> end_open_marker() const override {
        return std::nullopt;
    }
    std::optional<open_rt_marker> reverse_end_open_marker() const override {
        abort();
    }
    future<> prefetch_lower_bound(position_in_partition_view pos) override {
        return make_ready_future<>();
    }
    bool eof() const override {
        return _lower == int(_positions.size() - 1);
    }
};

// Test design:
// - Write an sstable.
// - Using a key universe which includes the written keys and some extra "in-between" keys,
//   nondeterministically:
//   - Construct a reader (using a nondeterministically-inexact index) on *some* partition range,
//     check that it outputs the right mutation fragments.
//   - Optionally, forward it to *some* range after the first one, check that the reader
//     outputs the right mutation fragments.
SEASTAR_TEST_CASE(test_inexact_partition_index_range_query) {
    return sstables::test_env::do_with_async([] (sstables::test_env& env) {
        // Force a clustering block for every row,
        // so that the non-full slice induces parser skips to the middle of the partition.
        env.manager().set_promoted_index_block_size(1);
        auto row_value = std::string(1024, 'a');

        simple_schema table;
        auto permit = env.make_reader_permit();

        // Having more keys makes the test stronger but increases
        // the number of test paths exponentially.
        //
        // 3 keys make the test too long for CI.
        constexpr bool hard_mode = false;

        // Number of all possible partition keys.
        size_t n_all_pks;
        // Indices of present partitions in the list of possible keys.
        std::vector<int> present_pk_indices;

        if (hard_mode) {
            n_all_pks = 7;
            present_pk_indices = std::vector<int>{1, 3, 5};
        } else {
            n_all_pks = 3;
            present_pk_indices = std::vector<int>{0, 2};
        }

        // Generate the list of all possible partition keys.
        std::vector<dht::decorated_key> all_dks;
        for (size_t i = 0; i < n_all_pks; ++i) {
            auto pk = partition_key::from_single_value(*table.schema(), serialized(format("pk{:010d}", i)));
            all_dks.push_back(dht::decorate_key(*table.schema(), pk));
        }
        std::ranges::sort(all_dks, dht::ring_position_less_comparator(*table.schema()));

        // Choose the written partition keys.
        std::vector<dht::decorated_key> present_dks;
        for (const auto& i : present_pk_indices) {
            present_dks.push_back(all_dks[i]);
        }

        // Generate the present mutations.
        utils::chunked_vector<mutation> muts;
        auto cks = table.make_ckeys(3);
        for (const auto& i : present_pk_indices) {
            mutation m(table.schema(), all_dks[i]);
            for (const auto& ck : cks) {
                table.add_row(m, ck, row_value);
            }
            muts.push_back(std::move(m));
        }

        // Generate the sstable.
        auto sst = make_sstable_containing(env.make_sstable(table.schema()), muts);

        // Use the index to find key positions.
        std::vector<uint64_t> partition_positions = get_partition_positions(sst, permit);
        std::vector<uint64_t> ck_positions = get_ck_positions(sst, permit);
        partition_positions.push_back(sst->data_size());
        {
            testlog.debug("Sstable initialized with:");
            size_t i = 0;
            size_t j = 0;
            for (const auto& dk : present_pk_indices) {
                testlog.debug("{}: {} (dk={})", dk, partition_positions[i], all_dks[dk]);
                while (j < ck_positions.size() && ck_positions[j] < partition_positions[i + 1]) {
                    testlog.debug(" ck at {}", ck_positions[j]);
                    ++j;
                }
                ++i;
            }
            testlog.debug("eof at {}", partition_positions.back());
        }

        nondeterministic_choice_stack ncs;
        int test_cases = 0;
        do {
            testlog.debug("Starting test case {}", test_cases);
            ++test_cases;

            bool do_slice = ncs.choose_bool();
            auto slice = do_slice
                ? partition_slice_builder(*table.schema())
                    .with_range(query::clustering_range::make_singular(cks[1]))
                    .build()
                : table.schema()->full_slice();
            auto range = do_slice
                ? query::clustering_range::make_singular(cks[1])
                : query::clustering_range::make_open_ended_both_sides();

            dht::partition_range pr;

            std::optional<mutation_reader_assertions> reader;
            constexpr int max_forwards = 1;

            // Special value -1 means -inf
            // Special value all_dks.size() means +inf
            int last_dk = -1;
            bool last_inclusive = false;

            for (int range_index = 0; range_index <= max_forwards; ++range_index) {
                // Choose some partition range after the last one
                auto start_dk = last_dk;
                if (last_inclusive) {
                    start_dk += 1;
                }
                if (start_dk >= int(all_dks.size())) {
                    break;
                }
                start_dk += ncs.choose_up_to(all_dks.size() - 1 - start_dk);
                bool start_inclusive;
                if (start_dk == -1 || (start_dk == last_dk && last_inclusive)) {
                    start_inclusive = true;
                } else {
                    start_inclusive = ncs.choose_bool();
                }
                int end_dk = start_dk;
                if (!start_inclusive || start_dk == -1) {
                    end_dk += 1;
                }
                end_dk += ncs.choose_up_to(all_dks.size() - end_dk);
                bool end_inclusive;
                if (end_dk == int(all_dks.size()) || (end_dk == start_dk)) {
                    end_inclusive = true;
                } else {
                    end_inclusive = ncs.choose_bool();
                }
                if (start_dk == -1) {
                    if (end_dk == int(all_dks.size())) {
                        pr = dht::partition_range::make_open_ended_both_sides();
                    } else {
                        pr = dht::partition_range::make_ending_with(dht::partition_range::bound(all_dks[end_dk], end_inclusive));
                    }
                } else if (end_dk == int(all_dks.size())) {
                    pr = dht::partition_range::make_starting_with(dht::partition_range::bound(all_dks[start_dk], start_inclusive));
                } else {
                    pr = dht::partition_range::make(
                        dht::partition_range::bound(all_dks[start_dk], start_inclusive),
                        dht::partition_range::bound(all_dks[end_dk], end_inclusive)
                    );
                }

                // If not created, create the reader on the chosen partition range,
                // otherwise forward to it.
                if (reader) {
                    testlog.debug("Forward to {}{}, {}{} (pr={})",
                        start_inclusive ? '[' : '(',
                        start_dk,
                        end_dk,
                        end_inclusive ? ']' : ')',
                        pr
                    );
                    reader->fast_forward_to(pr);
                } else {
                    testlog.debug("Create for {}{}, {}{} (pr={})",
                        start_inclusive ? '[' : '(',
                        start_dk,
                        end_dk,
                        end_inclusive ? ']' : ')',
                        pr
                    );
                    reader = assert_that(sstables::mx::make_reader_with_index_reader(
                        sst,
                        table.schema(),
                        permit,
                        pr,
                        slice,
                        nullptr,
                        streamed_mutation::forwarding::no,
                        mutation_reader::forwarding::yes,
                        default_read_monitor(),
                        sstables::integrity_check::no,
                        std::make_unique<inexact_partition_index>(ncs, partition_positions, ck_positions, present_dks, cks, table.schema())
                    ));
                }
                // Check that the reader produces correct output for the chosen partition range.
                for (int i = start_dk + !start_inclusive; i < end_dk + end_inclusive; ++i) {
                    if (auto it = std::ranges::find(present_pk_indices, i); it != present_pk_indices.end()) {
                        testlog.debug("Check produces {} (dk={})", i, muts[it - present_pk_indices.begin()].decorated_key());
                        reader->produces(muts[it - present_pk_indices.begin()].sliced({range}));
                    }
                }
                testlog.debug("Check produces end of stream");
                reader->produces_end_of_stream();
                last_dk = end_dk;
                last_inclusive = end_inclusive;
            }
        } while (ncs.rewind());
    });
}

// Test that the reader can deal with a single-partition query
// for which the index returns a false positive.
SEASTAR_TEST_CASE(test_inexact_partition_index_singular_query) {
    return sstables::test_env::do_with_async([] (sstables::test_env& env) {
        simple_schema table;
        auto permit = env.make_reader_permit();

        // Number of all possible partition keys.
        size_t n_all_pks = 3;
        // Indices of present partitions in the list of possible keys.
        std::vector<int> present_pk_indices{0, 2};

        // Generate the list of all possible partition keys.
        std::vector<dht::decorated_key> all_dks;
        for (size_t i = 0; i < n_all_pks; ++i) {
            auto pk = partition_key::from_single_value(*table.schema(), serialized(format("pk{:010d}", i)));
            all_dks.push_back(dht::decorate_key(*table.schema(), pk));
        }
        std::ranges::sort(all_dks, dht::ring_position_less_comparator(*table.schema()));

        // Choose the written partition keys.
        std::vector<dht::decorated_key> present_dks;
        for (const auto& i : present_pk_indices) {
            present_dks.push_back(all_dks[i]);
        }

        // Generate the present mutations.
        utils::chunked_vector<mutation> muts;
        for (const auto& i : present_pk_indices) {
            mutation m(table.schema(), all_dks[i]);
            muts.push_back(std::move(m));
        }

        // Generate the sstable.
        auto sst = make_sstable_containing(env.make_sstable(table.schema()), muts);

        // Use the index to find key positions.
        std::vector<uint64_t> partition_positions = get_partition_positions(sst, permit);
        partition_positions.push_back(sst->data_size());
        {
            testlog.debug("Sstable initialized with:");
            size_t i = 0;
            for (const auto& dk : present_pk_indices) {
                testlog.debug("{}: {} (dk={})", dk, partition_positions[i], all_dks[dk]);
                ++i;
            }
            testlog.debug("eof at {}", partition_positions.back());
        }

        nondeterministic_choice_stack ncs;
        do {
            auto slice = table.schema()->full_slice();
            auto pr = dht::partition_range::make_singular(all_dks[1]);

            testlog.debug("Create for pr={}", pr);
            mutation_reader_assertions reader = assert_that(sstables::mx::make_reader_with_index_reader(
                sst,
                table.schema(),
                permit,
                pr,
                slice,
                nullptr,
                streamed_mutation::forwarding::no,
                mutation_reader::forwarding::yes,
                default_read_monitor(),
                sstables::integrity_check::no,
                std::make_unique<inexact_partition_index>(
                    ncs,
                    partition_positions,
                    std::span<uint64_t>(),
                    present_dks,
                    std::span<clustering_key>(),
                    table.schema())
            ));
            testlog.debug("Check that the reader produces nothing");
            reader.produces_end_of_stream();
        } while (ncs.rewind());
    });
}
