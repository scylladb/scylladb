/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "mutation/atomic_cell_or_collection_block.hh"

#include <cstring>
#include <memory>

#include "utils/assert.hh"

static_assert(std::has_single_bit(atomic_cell_or_collection_block_nr_cells));
static_assert(sizeof(atomic_cell_or_collection_block) == 16);
static_assert(sizeof(atomic_cell_or_collection_block) % atomic_cell_or_collection_block::external_cells_alignment == 0);
static_assert(alignof(atomic_cell_or_collection_block) >= atomic_cell_or_collection_block::external_cells_alignment);
static_assert(sizeof(managed_bytes) % alignof(cell_hash_opt) == 0);
static_assert(sizeof(cell_hash_opt) % alignof(atomic_cell_or_collection_block::offset_type) == 0);
static_assert(std::is_trivially_copyable_v<cell_hash_opt>);

atomic_cell_or_collection_block::atomic_cell_or_collection_block(atomic_cell_or_collection_block&& o) noexcept
    : atomic_cell_or_collection_block_tree_node_header{o._backref}
    , _present(o._present)
    , _external(o._external)
    , _inline_size(o._inline_size)
{
    std::uninitialized_move_n(o.external_cells(), nr_external(), external_cells());
    // hashes, offsets and data are contiguous and trivially copyable
    std::memcpy(hashes(), o.hashes(), nr_present() * sizeof(cell_hash_opt) + (nr_internal() + 1) * sizeof(offset_type) + _inline_size);
    relocated_to(this);
}

atomic_cell_or_collection_block::~atomic_cell_or_collection_block() {
    std::destroy_n(external_cells(), nr_external());
}

atomic_cell_or_collection_block::ptr atomic_cell_or_collection_block::clone() const {
    // Copy the external cells before allocating the block, so that the block is
    // fully constructed as soon as it is allocated.
    std::array<managed_bytes, nr_cells> external_copies;
    auto ext = external_cells();
    for (unsigned i = 0; i < nr_external(); ++i) {
        external_copies[i] = managed_bytes(ext[i]);
    }
    auto size = storage_size();
    void* storage = current_allocator().alloc<atomic_cell_or_collection_block>(size);
    // Nothrow from here on.
    auto* block = new (storage) atomic_cell_or_collection_block(_present, _external, _inline_size);
    std::uninitialized_move_n(external_copies.begin(), nr_external(), block->external_cells());
    std::memcpy(block->hashes(), hashes(), nr_present() * sizeof(cell_hash_opt) + (nr_internal() + 1) * sizeof(offset_type) + _inline_size);
    return ptr(block);
}

size_t atomic_cell_or_collection_block::representation_independent_data_size() const noexcept {
    // The inline cells are stored back to back.
    size_t size = _inline_size;
    auto ext = external_cells();
    for (unsigned i = 0; i < nr_external(); ++i) {
        size += ext[i].size();
    }
    return size;
}

size_t atomic_cell_or_collection_block::memory_usage() const noexcept {
    size_t usage = storage_size();
    auto ext = external_cells();
    for (unsigned i = 0; i < nr_external(); ++i) {
        usage += ext[i].external_memory_usage();
    }
    return usage;
}

managed_bytes_mutable_view atomic_cell_or_collection_block::mutable_cell(unsigned slot) noexcept {
    unsigned rank = bitmask::rank(_present, slot);
    unsigned external_rank = bitmask::rank(_external, slot);
    if (is_external(slot)) {
        return managed_bytes_mutable_view(external_cells()[external_rank]);
    }
    auto internal_rank = rank - external_rank;
    auto offs = offsets();
    return managed_bytes_mutable_view(bytes_mutable_view(data() + offs[internal_rank], offs[internal_rank + 1] - offs[internal_rank]));
}
