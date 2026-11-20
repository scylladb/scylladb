/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <map>

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include "mutation/atomic_cell_or_collection_block.hh"

using block = atomic_cell_or_collection_block;
using builder = atomic_cell_or_collection_block_builder;
using block_ptr = block::ptr;
constexpr unsigned nr_cells = atomic_cell_or_collection_block_nr_cells;

static managed_bytes make_cell(size_t size, int seed) {
    managed_bytes b(managed_bytes::initialized_later(), size);
    for (size_t i = 0; i < size; ++i) {
        b[i] = bytes::value_type(seed * 31 + i);
    }
    return b;
}

static bytes linearized_cell(managed_bytes_view v) {
    return v.linearize();
}

// slot -> (cell, hash)
using cells_map = std::map<unsigned, std::pair<bytes, cell_hash_opt>>;

static cells_map contents(const block& b) {
    cells_map ret;
    b.for_each_cell([&] (const block::cell_entry& e) {
        BOOST_REQUIRE(b.contains(e.slot));
        BOOST_REQUIRE_EQUAL(linearized_cell(e.cell), linearized_cell(b.cell(e.slot)));
        BOOST_REQUIRE_EQUAL((e.external_storage != nullptr), b.is_external(e.slot));
        ret.emplace(e.slot, std::pair(linearized_cell(e.cell), e.hash));
    });
    return ret;
}

static void check_contents(const block& b, const cells_map& expected) {
    auto actual = contents(b);
    BOOST_REQUIRE_EQUAL(actual.size(), expected.size());
    BOOST_REQUIRE_EQUAL(b.nr_present(), expected.size());
    size_t expected_data_size = 0;
    for (auto& [slot, cell_and_hash] : expected) {
        expected_data_size += cell_and_hash.first.size();
    }
    BOOST_REQUIRE_EQUAL(b.representation_independent_data_size(), expected_data_size);
    for (auto& [slot, cell_and_hash] : expected) {
        BOOST_REQUIRE(actual.contains(slot));
        BOOST_REQUIRE_EQUAL(actual[slot].first, cell_and_hash.first);
        BOOST_REQUIRE_EQUAL(bool(actual[slot].second), bool(cell_and_hash.second));
        if (cell_and_hash.second) {
            BOOST_REQUIRE_EQUAL(actual[slot].second->hash, cell_and_hash.second->hash);
        }
    }
}

static block_ptr build_block(std::vector<std::pair<unsigned, size_t>> slots_and_sizes, cells_map& expected) {
    // The builder requires slots in increasing order.
    std::ranges::sort(slots_and_sizes);
    builder bld;
    for (auto [slot, size] : slots_and_sizes) {
        auto cell = make_cell(size, slot);
        auto hash = slot % 2 ? cell_hash_opt(cell_hash{slot + 1}) : cell_hash_opt();
        expected.emplace(slot, std::pair(linearized_cell(managed_bytes_view(cell)), hash));
        bld.add_copy(slot, managed_bytes_view(cell), hash);
    }
    return bld.build();
}

SEASTAR_THREAD_TEST_CASE(test_round_trip) {
    for (unsigned present = 1; present < (1u << nr_cells); ++present) {
        std::vector<std::pair<unsigned, size_t>> cells;
        for (unsigned slot = 0; slot < nr_cells; ++slot) {
            if (present & (1u << slot)) {
                cells.emplace_back(slot, 1 + slot * 37 % 50);
            }
        }
        cells_map expected;
        auto b = build_block(cells, expected);
        BOOST_REQUIRE_EQUAL(b->present(), present);
        BOOST_REQUIRE_EQUAL(b->external(), 0);
        check_contents(*b, expected);
    }
}

SEASTAR_THREAD_TEST_CASE(test_budget_spill) {
    // All cells together exceed the budget; the largest ones must go external.
    std::vector<std::pair<unsigned, size_t>> cells;
    size_t total = 0;
    for (unsigned slot = 0; slot < nr_cells; ++slot) {
        size_t size = builder::inline_budget / nr_cells * 2 + slot;
        cells.emplace_back(slot, size);
        total += size;
    }
    cells_map expected;
    auto b = build_block(cells, expected);
    check_contents(*b, expected);
    // Spilling starts with the largest, i.e. the highest slots here.
    size_t inline_size = total;
    block::mask_type expected_external = 0;
    for (int slot = nr_cells - 1; inline_size > builder::inline_budget; --slot) {
        inline_size -= cells[slot].second;
        expected_external |= bitmask::bit<block::mask_type>(slot);
    }
    BOOST_REQUIRE_EQUAL(b->external(), expected_external);

    // A single cell larger than the budget.
    cells_map expected2;
    auto b2 = build_block({{3, builder::inline_budget + 1}, {5, 10}}, expected2);
    BOOST_REQUIRE_EQUAL(b2->external(), bitmask::bit<block::mask_type>(3));
    check_contents(*b2, expected2);
}

SEASTAR_THREAD_TEST_CASE(test_layout_is_canonical) {
    // The same cells, provided in different ways, must produce the same layout.
    std::vector<size_t> sizes = {10, builder::inline_budget, 3000, 7, 2000, 1, 5000, 100};
    std::vector<managed_bytes> sources;
    for (unsigned slot = 0; slot < nr_cells; ++slot) {
        sources.push_back(make_cell(sizes[slot], slot));
    }
    builder copied;
    builder borrowed;
    builder stolen;
    builder owned;
    std::vector<managed_bytes> stealable = sources;
    for (unsigned slot = 0; slot < nr_cells; ++slot) {
        copied.add_copy(slot, managed_bytes_view(sources[slot]), {});
        borrowed.add_borrowed(slot, managed_bytes_view(sources[slot]), {});
        stolen.add_borrowed(slot, managed_bytes_view(stealable[slot]), {}, &stealable[slot]);
        owned.add_owned(slot, managed_bytes(sources[slot]), {});
    }
    auto b1 = copied.build();
    auto b2 = borrowed.build();
    auto b3 = stolen.build();
    auto b4 = owned.build();
    for (auto* b : {b2.get(), b3.get(), b4.get()}) {
        BOOST_REQUIRE_EQUAL(b->external(), b1->external());
        BOOST_REQUIRE_EQUAL(b->storage_size(), b1->storage_size());
        BOOST_REQUIRE_EQUAL(b->memory_usage(), b1->memory_usage());
        for (unsigned slot = 0; slot < nr_cells; ++slot) {
            BOOST_REQUIRE_EQUAL(linearized_cell(b->cell(slot)), linearized_cell(managed_bytes_view(sources[slot])));
        }
    }
    // Stealing moved from the sources of external cells only.
    for (unsigned slot = 0; slot < nr_cells; ++slot) {
        BOOST_REQUIRE_EQUAL(stealable[slot].empty(), b3->is_external(slot));
    }
    stolen.roll_back(std::move(b3));
    for (unsigned slot = 0; slot < nr_cells; ++slot) {
        BOOST_REQUIRE(stealable[slot] == sources[slot]);
    }
}

SEASTAR_THREAD_TEST_CASE(test_in_place_modification) {
    cells_map expected;
    auto b = build_block({{0, 20}, {2, builder::inline_budget + 100}, {7, 30}}, expected);
    b->hash(2) = cell_hash{42};
    expected[2].second = cell_hash{42};
    for (unsigned slot : {0, 2, 7}) {
        auto v = b->mutable_cell(slot);
        v.current_fragment()[0] = 'x';
        expected[slot].first[0] = 'x';
    }
    check_contents(*b, expected);
}
