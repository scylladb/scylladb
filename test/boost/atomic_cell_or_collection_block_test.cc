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
#include "utils/logalloc.hh"

using block = atomic_cell_or_collection_block;
using builder = atomic_cell_or_collection_block_builder;
using tree = atomic_cell_or_collection_block_tree;
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
        expected_external |= mask_bit<block::mask_type>(slot);
    }
    BOOST_REQUIRE_EQUAL(b->external(), expected_external);

    // A single cell larger than the budget.
    cells_map expected2;
    auto b2 = build_block({{3, builder::inline_budget + 1}, {5, 10}}, expected2);
    BOOST_REQUIRE_EQUAL(b2->external(), mask_bit<block::mask_type>(3));
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

// Reference model for tree tests: block index -> contents.
using tree_model = std::map<tree::block_index_type, cells_map>;

static block_ptr make_block_for(tree::block_index_type index, cells_map& expected) {
    // Make some blocks have external cells.
    size_t big = index % 3 == 0 ? builder::inline_budget + index % 100 : 10;
    return build_block({{index % nr_cells, 5 + index % 17}, {(index + 3) % nr_cells, big}}, expected);
}

static void check_tree(const tree& t, const tree_model& model) {
    for (auto& [index, cells] : model) {
        auto* b = t.find_block(index);
        BOOST_REQUIRE(b);
        check_contents(*b, cells);
    }
    auto it = model.begin();
    t.for_each_block([&] (tree::block_index_type index, const block& b) {
        BOOST_REQUIRE(it != model.end());
        BOOST_REQUIRE_EQUAL(index, it->first);
        check_contents(b, it->second);
        ++it;
    });
    BOOST_REQUIRE(it == model.end());
    size_t nr_cells_in_model = 0;
    for (auto& [index, cells] : model) {
        nr_cells_in_model += cells.size();
    }
    BOOST_REQUIRE_EQUAL(t.size(), nr_cells_in_model);
    if (model.empty()) {
        BOOST_REQUIRE(t.empty());
        BOOST_REQUIRE_EQUAL(t.height(), 0);
    } else {
        // The height is the smallest that holds the largest block index.
        unsigned expected_height = 0;
        for (uint64_t capacity = 1; model.rbegin()->first >= capacity; capacity <<= atomic_cell_or_collection_block_vector_index_bits) {
            ++expected_height;
        }
        BOOST_REQUIRE_EQUAL(t.height(), expected_height);
    }
}

static std::vector<tree::block_index_type> interesting_indexes() {
    return {0, 1, 5, 63, 64, 65, 127, 4095, 4096, 4097, 300000, 2, 1000, 64 * 64 * 64 * 2 + 1};
}

static void populate(tree& t, tree_model& model) {
    for (auto index : interesting_indexes()) {
        cells_map expected;
        auto b = make_block_for(index, expected);
        t.set_block(index, std::move(b));
        model[index] = std::move(expected);
        check_tree(t, model);
        BOOST_REQUIRE(!t.find_block(index + 1000000));
    }
}

SEASTAR_THREAD_TEST_CASE(test_tree_insert_find_remove) {
    tree t;
    tree_model model;
    populate(t, model);

    // Replace an existing block.
    {
        cells_map expected;
        auto b = build_block({{1, 3}}, expected);
        t.set_block(64, std::move(b));
        model[64] = std::move(expected);
        check_tree(t, model);
    }

    // Remove in an order different from insertion; the height must go back
    // down as high blocks are removed.
    auto indexes = interesting_indexes();
    std::ranges::sort(indexes, std::greater<>());
    for (auto index : indexes) {
        t.remove_block(index);
        model.erase(index);
        check_tree(t, model);
        if (!model.empty()) {
            auto max_index = model.rbegin()->first;
            unsigned expected_height = 0;
            for (uint64_t capacity = 1; max_index >= capacity; capacity <<= atomic_cell_or_collection_block_vector_index_bits) {
                ++expected_height;
            }
            BOOST_REQUIRE_EQUAL(t.height(), expected_height);
        }
    }
    BOOST_REQUIRE(t.empty());
}

SEASTAR_THREAD_TEST_CASE(test_tree_rebuild_blocks) {
    tree t;
    tree_model model;
    populate(t, model);

    // Remove every other block, replace the rest.
    bool remove = false;
    t.rebuild_blocks([&] (tree::block_index_type index, block_ptr& b) {
        if (remove) {
            b.reset();
            model.erase(index);
        } else {
            cells_map expected;
            b = build_block({{index % nr_cells, 1 + index % 5}}, expected);
            model[index] = std::move(expected);
        }
        remove = !remove;
    });
    check_tree(t, model);
}

SEASTAR_THREAD_TEST_CASE(test_tree_move_and_clone) {
    tree t;
    tree_model model;
    populate(t, model);

    auto t2 = std::move(t);
    BOOST_REQUIRE(t.empty());
    check_tree(t2, model);

    auto t3 = t2.clone([] (const block& b) {
        builder bld;
        b.for_each_cell([&] (const block::cell_entry& e) {
            bld.add_borrowed(e.slot, e.cell, e.hash);
        });
        return bld.build();
    });
    check_tree(t3, model);
    BOOST_REQUIRE_EQUAL(t3.memory_usage(), t2.memory_usage());
}

SEASTAR_THREAD_TEST_CASE(test_lsa_migration) {
    logalloc::region region;
    with_allocator(region.allocator(), [&] {
        tree_model model;
        {
            tree t;
            populate(t, model);
            // Also move the tree around, so that its root slot changes.
            std::vector<tree> trees;
            trees.push_back(std::move(t));
            for (int i = 0; i < 3; ++i) {
                region.full_compaction();
                check_tree(trees.back(), model);
                trees.push_back(std::move(trees.back()));
            }
            // In-place modifications survive compaction.
            auto* b = trees.back().find_block(0);
            b->hash(b->present() & 1 ? 0 : std::countr_zero(b->present())) = cell_hash{7};
            auto first_slot = std::countr_zero(b->present());
            model[0][first_slot].second = cell_hash{7};
            region.full_compaction();
            check_tree(trees.back(), model);
        }
        BOOST_REQUIRE_EQUAL(region.occupancy().used_space(), 0);
    });
}
