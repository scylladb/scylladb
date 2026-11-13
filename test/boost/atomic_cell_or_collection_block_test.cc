/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <map>
#include <stdexcept>

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
        BOOST_REQUIRE_EQUAL(e.hash != nullptr, e.cell.size() >= block::hashed_cell_min_size);
        BOOST_REQUIRE_EQUAL(e.hash != nullptr, b.hash(e.slot) != nullptr);
        ret.emplace(e.slot, std::pair(linearized_cell(e.cell), e.hash_value()));
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
        // Cells too small for a hash slot drop their hash.
        expected.emplace(slot, std::pair(linearized_cell(managed_bytes_view(cell)), size >= block::hashed_cell_min_size ? hash : cell_hash_opt()));
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

SEASTAR_THREAD_TEST_CASE(test_any_slot_order) {
    // The builder accepts slots in any order, with the same result.
    std::vector<size_t> sizes = {10, builder::inline_budget, 3000, 7, 2000, 1, 5000, 100};
    std::vector<managed_bytes> cells;
    for (unsigned slot = 0; slot < nr_cells; ++slot) {
        cells.push_back(make_cell(sizes[slot % sizes.size()], slot));
    }
    builder in_order;
    builder reversed;
    for (unsigned slot = 0; slot < nr_cells; ++slot) {
        in_order.add_copy(slot, managed_bytes_view(cells[slot]), cell_hash{slot + 1});
        auto reversed_slot = nr_cells - 1 - slot;
        reversed.add_copy(reversed_slot, managed_bytes_view(cells[reversed_slot]), cell_hash{reversed_slot + 1});
    }
    auto b1 = in_order.build();
    auto b2 = reversed.build();
    BOOST_REQUIRE_EQUAL(b1->external(), b2->external());
    BOOST_REQUIRE_EQUAL(b1->storage_size(), b2->storage_size());
    check_contents(*b2, contents(*b1));
}

SEASTAR_THREAD_TEST_CASE(test_in_place_modification) {
    cells_map expected;
    auto b = build_block({{0, 20}, {2, builder::inline_budget + 100}, {7, 30}}, expected);
    BOOST_REQUIRE(!b->hash(0));
    *b->hash(2) = cell_hash{42};
    expected[2].second = cell_hash{42};
    for (unsigned slot : {0, 2, 7}) {
        auto v = b->mutable_cell(slot);
        v.current_fragment()[0] = 'x';
        expected[slot].first[0] = 'x';
    }
    check_contents(*b, expected);
}

SEASTAR_THREAD_TEST_CASE(test_hash_slots_only_for_large_cells) {
    constexpr auto min = block::hashed_cell_min_size;
    for (unsigned big : {0u, 1u, 3u, nr_cells}) {
        cells_map expected;
        std::vector<std::pair<unsigned, size_t>> cells;
        for (unsigned slot = 0; slot < nr_cells; ++slot) {
            cells.emplace_back(slot, slot < big ? min + slot : min - 1 - slot);
        }
        auto b = build_block(cells, expected);
        BOOST_REQUIRE_EQUAL(std::popcount(b->hashed()), big);
        size_t inline_size = 0;
        for (auto& [slot, size] : cells) {
            inline_size += size;
        }
        BOOST_REQUIRE_EQUAL(b->storage_size(), block::storage_size_for(nr_cells, 0, big, inline_size));
        check_contents(*b, expected);
        auto c = b->clone();
        check_contents(*c, expected);
        BOOST_REQUIRE_EQUAL(c->storage_size(), b->storage_size());
    }
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
    // The iterator visits the same blocks as for_each_block().
    BOOST_REQUIRE(std::ranges::equal(t.block_range(), model, [&] (const tree::block_entry& e, const auto& index_and_cells) {
        check_contents(e.block, index_and_cells.second);
        return e.index == index_and_cells.first;
    }));
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

    // Take turns keeping, removing, replacing and releasing blocks; also try to
    // release one into a function that throws without taking it.
    tree released;
    tree_model released_model;
    unsigned turn = 0;
    t.rebuild_blocks([&] (tree::block_index_type index, tree::block_being_rebuilt& b) {
        switch (turn++ % 5) {
        case 0:
            break;
        case 1:
            b.remove();
            model.erase(index);
            break;
        case 2: {
            cells_map expected;
            b.replace(build_block({{index % nr_cells, 1 + index % 5}}, expected));
            model[index] = std::move(expected);
            break;
        }
        case 3:
            b.release_to([&] (block_ptr&& block) {
                released.set_block(index, std::move(block));
            });
            released_model[index] = std::move(model[index]);
            model.erase(index);
            break;
        case 4:
            BOOST_REQUIRE_THROW(b.release_to([] (block_ptr&&) {
                throw std::runtime_error("not taken");
            }), std::runtime_error);
            check_contents(*b, model[index]);
            break;
        }
    });
    check_tree(t, model);
    check_tree(released, released_model);
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
            bld.add_borrowed(e.slot, e.cell, e.hash_value());
        });
        return bld.build();
    });
    check_tree(t3, model);
    // The clone's vectors have no spare capacity.
    BOOST_REQUIRE_LE(t3.memory_usage(), t2.memory_usage());
}

static block_ptr clone_block(const block& b) {
    builder bld;
    b.for_each_cell([&] (const block::cell_entry& e) {
        bld.add_borrowed(e.slot, e.cell, e.hash_value());
    });
    return bld.build();
}

static std::vector<tree::block_index_type> sorted_interesting_indexes() {
    auto indexes = interesting_indexes();
    std::ranges::sort(indexes);
    return indexes;
}

// Loads `indexes` (in the given, non-decreasing, order) into a tree with a loader,
// calling between_blocks() after each block is added to the loader.
static tree load(const std::vector<tree::block_index_type>& indexes, tree_model& model, std::invocable<tree::loader&> auto&& between_blocks) {
    tree::loader loader;
    for (auto index : indexes) {
        BOOST_REQUIRE(loader.can_set_block(index));
        cells_map expected;
        loader.set_block(index, make_block_for(index, expected));
        model[index] = std::move(expected);
        between_blocks(loader);
        // Only the blocks of the open vector of blocks can be found.
        for (auto& [i, cells] : model) {
            auto* b = loader.find_block(i);
            BOOST_REQUIRE_EQUAL(bool(b), i / atomic_cell_or_collection_block_vector_fanout == index / atomic_cell_or_collection_block_vector_fanout);
            if (b) {
                check_contents(*b, cells);
            }
            BOOST_REQUIRE_EQUAL(loader.can_set_block(i), i / atomic_cell_or_collection_block_vector_fanout >= index / atomic_cell_or_collection_block_vector_fanout);
        }
        BOOST_REQUIRE(!loader.find_block(index + 1000000));
    }
    tree t;
    loader.load_into(t);
    BOOST_REQUIRE(loader.empty());
    return t;
}

SEASTAR_THREAD_TEST_CASE(test_tree_loader) {
    auto no_op = [] (tree::loader&) {};
    auto check_exactly_sized = [] (const tree& t) {
        // Every vector is exactly sized, as in a clone.
        BOOST_REQUIRE_EQUAL(t.memory_usage(), t.clone(clone_block).memory_usage());
    };
    // Single block 0: no vectors.
    {
        tree_model model;
        auto t = load({0}, model, no_op);
        check_tree(t, model);
        BOOST_REQUIRE_EQUAL(t.height(), 0);
    }
    // Single blocks elsewhere, where the root has a single child.
    for (tree::block_index_type index : {1u, 64u, 4096u, 300000u}) {
        tree_model model;
        auto t = load({index}, model, no_op);
        check_tree(t, model);
        check_exactly_sized(t);
    }
    // A single block, replaced while it is the only one.
    {
        tree_model model;
        auto t = load({3, 3}, model, no_op);
        check_tree(t, model);
        check_exactly_sized(t);
    }
    // A single block replaced, then another added.
    {
        tree_model model;
        auto t = load({3, 3, 70}, model, no_op);
        check_tree(t, model);
        check_exactly_sized(t);
    }
    // Block 0 and a block which needs a higher tree: block 0 is in a chain.
    {
        tree_model model;
        auto t = load({0, 4096}, model, no_op);
        check_tree(t, model);
        check_exactly_sized(t);
    }
    // Over several levels.
    {
        tree_model model;
        auto t = load(sorted_interesting_indexes(), model, no_op);
        check_tree(t, model);
        check_exactly_sized(t);
        // The loaded tree is a regular one.
        cells_map expected;
        t.set_block(66, make_block_for(66, expected));
        model[66] = std::move(expected);
        check_tree(t, model);
    }
    // Filling vectors of blocks, with the blocks of the open vector replaced.
    {
        std::vector<tree::block_index_type> indexes;
        for (tree::block_index_type i = 0; i < 150; ++i) {
            indexes.push_back(i);
        }
        indexes.push_back(130);
        indexes.push_back(149);
        indexes.push_back(5000);
        tree_model model;
        auto t = load(indexes, model, no_op);
        check_tree(t, model);
        check_exactly_sized(t);
    }
    // Blocks of a loader which is destroyed are freed.
    {
        logalloc::region region;
        with_allocator(region.allocator(), [&] {
            {
                tree::loader loader;
                for (auto index : sorted_interesting_indexes()) {
                    cells_map expected;
                    loader.set_block(index, make_block_for(index, expected));
                }
            }
            BOOST_REQUIRE_EQUAL(region.occupancy().used_space(), 0);
        });
    }
}

SEASTAR_THREAD_TEST_CASE(test_tree_loader_lsa_migration) {
    logalloc::region region;
    with_allocator(region.allocator(), [&] {
        {
            tree_model model;
            auto t = load(sorted_interesting_indexes(), model, [&] (tree::loader&) {
                region.full_compaction();
            });
            region.full_compaction();
            check_tree(t, model);
        }
        BOOST_REQUIRE_EQUAL(region.occupancy().used_space(), 0);
    });
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
            BOOST_REQUIRE(b->hashed());
            auto hashed_slot = std::countr_zero(b->hashed());
            *b->hash(hashed_slot) = cell_hash{7};
            model[0][hashed_slot].second = cell_hash{7};
            region.full_compaction();
            check_tree(trees.back(), model);
        }
        BOOST_REQUIRE_EQUAL(region.occupancy().used_space(), 0);
    });
}

// LSA migration of blocks mixing hashed and unhashed inline cells.
SEASTAR_THREAD_TEST_CASE(test_lsa_migration_partial_hashes) {
    constexpr auto min = block::hashed_cell_min_size;
    logalloc::region region;
    with_allocator(region.allocator(), [&] {
        {
            tree t;
            tree_model model;
            for (unsigned big : {1u, 3u}) {
                std::vector<std::pair<unsigned, size_t>> cells;
                for (unsigned slot = 0; slot < nr_cells; ++slot) {
                    cells.emplace_back(slot, slot < big ? min + slot : min - 1 - slot);
                }
                t.set_block(big, build_block(cells, model[big]));
            }
            region.full_compaction();
            check_tree(t, model);
        }
        BOOST_REQUIRE_EQUAL(region.occupancy().used_space(), 0);
    });
}
