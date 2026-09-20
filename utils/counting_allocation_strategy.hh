/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <seastar/core/on_internal_error.hh>
#include <seastar/util/log.hh>
#include <fmt/format.h>

#include "utils/allocation_strategy.hh"

// Forwards every allocation to another strategy and tracks how much it has handed out. Install it
// with with_allocator() around the work to be counted; this picks up what a data structure
// allocates internally, which no sum over its elements can see. The size counted is what the
// allocator set aside, so its rounding is included.
//
// Everything counted must also be freed through it, so the with_allocator() scope has to cover the
// allocations to count and nothing else: anything called inside it that allocates for its own
// purposes must install the delegate again. A free below zero means that was not done.
class counting_allocation_strategy final : public allocation_strategy {
    allocation_strategy& _delegate;
    seastar::logger& _logger;
    size_t _allocated_memory = 0;

    void account_free(const void* obj) noexcept {
        size_t size = _delegate.object_memory_size_in_allocator(obj);
        if (size > _allocated_memory) [[unlikely]] {
            // Wrapping would leave a count the size of the address space for callers to act on.
            seastar::on_internal_error_noexcept(_logger, fmt::format(
                    "counting_allocation_strategy: freeing {} bytes with only {} accounted for", size, _allocated_memory));
            _allocated_memory = 0;
            return;
        }
        _allocated_memory -= size;
    }

public:
    counting_allocation_strategy(allocation_strategy& delegate, seastar::logger& logger) noexcept
        : _delegate(delegate)
        , _logger(logger)
    {
        _preferred_max_contiguous_allocation = delegate.preferred_max_contiguous_allocation();
    }

    counting_allocation_strategy(const counting_allocation_strategy&) = delete;
    counting_allocation_strategy& operator=(const counting_allocation_strategy&) = delete;
    counting_allocation_strategy(counting_allocation_strategy&&) = delete;
    counting_allocation_strategy& operator=(counting_allocation_strategy&&) = delete;

    void* alloc(migrate_fn mf, size_t size, size_t alignment) override {
        void* obj = _delegate.alloc(mf, size, alignment);
        _allocated_memory += _delegate.object_memory_size_in_allocator(obj);
        return obj;
    }

    void free(void* obj, size_t size) override {
        account_free(obj);
        _delegate.free(obj, size);
    }

    void free(void* obj) override {
        account_free(obj);
        _delegate.free(obj);
    }

    size_t object_memory_size_in_allocator(const void* obj) const noexcept override {
        return _delegate.object_memory_size_in_allocator(obj);
    }

    uintptr_t reserve(size_t memory) override {
        return _delegate.reserve(memory);
    }

    void unreserve(uintptr_t opaque) noexcept override {
        _delegate.unreserve(opaque);
    }

    size_t allocated_memory() const noexcept { return _allocated_memory; }
};
