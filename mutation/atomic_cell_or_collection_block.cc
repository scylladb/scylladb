/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "mutation/atomic_cell_or_collection_block.hh"

#include <cstring>
#include <limits>
#include <memory>

#include "utils/assert.hh"

static_assert(std::has_single_bit(atomic_cell_or_collection_block_nr_cells));
static_assert(sizeof(atomic_cell_or_collection_block) == 16);
static_assert(sizeof(atomic_cell_or_collection_block) % atomic_cell_or_collection_block::external_cells_alignment == 0);
static_assert(alignof(atomic_cell_or_collection_block) >= atomic_cell_or_collection_block::external_cells_alignment);
static_assert(sizeof(managed_bytes) % alignof(cell_hash_opt) == 0);
static_assert(sizeof(cell_hash_opt) % alignof(atomic_cell_or_collection_block::offset_type) == 0);
static_assert(std::is_trivially_copyable_v<cell_hash_opt>);

static void copy_view_to(bytes::value_type* dst, managed_bytes_view src) noexcept {
    while (!src.empty()) {
        auto frag = src.current_fragment();
        std::memcpy(dst, frag.data(), frag.size());
        dst += frag.size();
        src.remove_current();
    }
}

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

static_assert(atomic_cell_or_collection_block_builder::inline_budget < std::numeric_limits<atomic_cell_or_collection_block::offset_type>::max());

atomic_cell_or_collection_block_builder::entry&
atomic_cell_or_collection_block_builder::new_entry(unsigned slot, size_t size, cell_hash_opt hash) {
    SCYLLA_ASSERT(slot < atomic_cell_or_collection_block_nr_cells);
    SCYLLA_ASSERT(!(_slots & bitmask::bit<mask_type>(slot)));
    auto& e = *new (&_entries[slot].e) entry();
    e.size = size;
    e.hash = hash;
    _slots |= bitmask::bit<mask_type>(slot);
    return e;
}

managed_bytes_mutable_view atomic_cell_or_collection_block_builder::reserve(entry& e, size_t size) {
    if (size <= _scratch.size() - _scratch_size) {
        e.kind = source_kind::scratch;
        e.scratch_offset = _scratch_size;
        _scratch_size += size;
        return managed_bytes_mutable_view(bytes_mutable_view(_scratch.data() + e.scratch_offset, size));
    }
    // A cell this large will likely be external anyway.
    e.owned = managed_bytes(managed_bytes::initialized_later(), size);
    e.kind = source_kind::owned;
    return managed_bytes_mutable_view(e.owned);
}

void atomic_cell_or_collection_block_builder::add_copy(unsigned slot, managed_bytes_view cell, cell_hash_opt hash) {
    if (cell.empty()) {
        return;
    }
    auto& e = new_entry(slot, cell.size(), hash);
    try {
        auto out = reserve(e, cell.size());
        copy_fragmented_view(out, cell);
    } catch (...) {
        std::destroy_at(&e);
        _slots &= ~bitmask::bit<mask_type>(slot);
        throw;
    }
}

void atomic_cell_or_collection_block_builder::add_borrowed(unsigned slot, managed_bytes_view cell, cell_hash_opt hash, managed_bytes* stealable) noexcept {
    if (cell.empty()) {
        return;
    }
    auto& e = new_entry(slot, cell.size(), hash);
    e.kind = source_kind::borrowed;
    e.borrowed = cell;
    e.stealable = stealable;
}

void atomic_cell_or_collection_block_builder::add_owned(unsigned slot, managed_bytes&& cell, cell_hash_opt hash) noexcept {
    if (cell.empty()) {
        return;
    }
    auto& e = new_entry(slot, cell.size(), hash);
    e.kind = source_kind::owned;
    e.owned = std::move(cell);
}

managed_bytes_view atomic_cell_or_collection_block_builder::entry_view(const entry& e) const noexcept {
    switch (e.kind) {
    case source_kind::scratch:
        return managed_bytes_view(bytes_view(_scratch.data() + e.scratch_offset, e.size));
    case source_kind::borrowed:
        return e.borrowed;
    case source_kind::owned:
        return managed_bytes_view(e.owned);
    }
    std::abort();
}

atomic_cell_or_collection_block_builder::block_ptr atomic_cell_or_collection_block_builder::build() {
    SCYLLA_ASSERT(_slots);

    // Decide which cells are external. Depends only on the cell sizes. Cells are
    // visited in slot order, so ties are broken by the lowest slot.
    size_t inline_size = 0;
    bitmask::for_each_set(_slots, [&] (unsigned slot) {
        inline_size += entry_of(slot).size;
    });
    mask_type external = 0;
    while (inline_size > inline_budget) {
        unsigned largest = 0;
        size_t largest_size = 0;
        bitmask::for_each_set(mask_type(_slots & ~external), [&] (unsigned slot) {
            if (entry_of(slot).size > largest_size) {
                largest = slot;
                largest_size = entry_of(slot).size;
            }
        });
        inline_size -= largest_size;
        external |= bitmask::bit<mask_type>(largest);
    }

    // External cells which we can neither steal nor already own are copied into
    // managed_bytes of their own. This and the block allocation are the only
    // steps which may throw.
    bitmask::for_each_set(external, [&] (unsigned slot) {
        auto& e = entry_of(slot);
        if (e.kind != source_kind::owned && !e.stealable) {
            e.owned = managed_bytes(entry_view(e));
            e.kind = source_kind::owned;
        }
    });

    auto size = atomic_cell_or_collection_block::storage_size_for(std::popcount(_slots), std::popcount(external), inline_size);
    void* storage = current_allocator().alloc<atomic_cell_or_collection_block>(size);

    // Nothrow from here on.
    auto* block = new (storage) atomic_cell_or_collection_block(_slots, external, inline_size);
    auto ext = block->external_cells();
    auto hashes = block->hashes();
    auto offsets = block->offsets();
    auto data = block->data();
    unsigned external_rank = 0;
    unsigned internal_rank = 0;
    offsets[0] = 0;
    bitmask::for_each_set_with_rank(_slots, [&] (unsigned slot, unsigned rank) {
        auto& e = entry_of(slot);
        new (&hashes[rank]) cell_hash_opt(e.hash);
        if (external & bitmask::bit<mask_type>(slot)) {
            if (e.kind == source_kind::owned) {
                new (&ext[external_rank]) managed_bytes(std::move(e.owned));
            } else {
                new (&ext[external_rank]) managed_bytes(std::move(*e.stealable));
            }
            ++external_rank;
        } else {
            auto start = offsets[internal_rank];
            copy_view_to(data + start, entry_view(e));
            offsets[++internal_rank] = start + e.size;
        }
    });
    return block_ptr(block);
}

void atomic_cell_or_collection_block_builder::roll_back(block_ptr block) noexcept {
    // build() moved the external cells from the stealable sources of the entries
    // it didn't own (copying the others into owned ones first).
    auto ext = block->external_cells();
    bitmask::for_each_set_with_rank(block->external(), [&] (unsigned slot, unsigned external_rank) {
        auto& e = entry_of(slot);
        if (e.kind != source_kind::owned) {
            *e.stealable = std::move(ext[external_rank]);
        }
    });
}

void atomic_cell_or_collection_block_builder::clear() noexcept {
    bitmask::for_each_set(_slots, [&] (unsigned slot) {
        std::destroy_at(&entry_of(slot));
    });
    _slots = 0;
    _scratch_size = 0;
}
