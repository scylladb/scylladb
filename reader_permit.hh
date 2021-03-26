/*
 * Copyright (C) 2019-present ScyllaDB
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

#include <seastar/util/optimized_optional.hh>
#include "seastarx.hh"

#include "db/timeout_clock.hh"
#include "schema_fwd.hh"

namespace seastar {
    class file;
} // namespace seastar

struct reader_resources {
    int count = 0;
    ssize_t memory = 0;

    static reader_resources with_memory(ssize_t memory) { return reader_resources(0, memory); }

    reader_resources() = default;

    reader_resources(int count, ssize_t memory)
        : count(count)
        , memory(memory) {
    }

    bool operator>=(const reader_resources& other) const {
        return count >= other.count && memory >= other.memory;
    }

    reader_resources operator-(const reader_resources& other) const {
        return reader_resources{count - other.count, memory - other.memory};
    }

    reader_resources& operator-=(const reader_resources& other) {
        count -= other.count;
        memory -= other.memory;
        return *this;
    }

    reader_resources operator+(const reader_resources& other) const {
        return reader_resources{count + other.count, memory + other.memory};
    }

    reader_resources& operator+=(const reader_resources& other) {
        count += other.count;
        memory += other.memory;
        return *this;
    }

    explicit operator bool() const {
        return count > 0 || memory > 0;
    }
};

inline bool operator==(const reader_resources& a, const reader_resources& b) {
    return a.count == b.count && a.memory == b.memory;
}

class reader_concurrency_semaphore;

/// A permit for a specific read.
///
/// Used to track the read's resource consumption and wait for admission to read
/// from the disk.
/// Use `consume_memory()` to register memory usage. Use `wait_admission()` to
/// wait for admission, before reading from the disk. Both methods return a
/// `resource_units` RAII object that should be held onto while the respective
/// resources are in use.
class reader_permit {
    friend class reader_concurrency_semaphore;

public:
    class resource_units;
    class used_guard;
    class blocked_guard;

    enum class state {
        waiting, // waiting for admission
        active_unused,
        active_used,
        active_blocked,
        inactive,
        evicted,
    };

    class impl;

private:
    shared_ptr<impl> _impl;

private:
    reader_permit() = default;
    reader_permit(shared_ptr<impl>);
    explicit reader_permit(reader_concurrency_semaphore& semaphore, const schema* const schema, std::string_view op_name,
            reader_resources base_resources);
    explicit reader_permit(reader_concurrency_semaphore& semaphore, const schema* const schema, sstring&& op_name,
            reader_resources base_resources);

    void on_waiting();
    void on_admission();

    void mark_used() noexcept;

    void mark_unused() noexcept;

    void mark_blocked() noexcept;

    void mark_unblocked() noexcept;

    operator bool() const { return bool(_impl); }

    friend class optimized_optional<reader_permit>;

public:
    ~reader_permit();

    reader_permit(const reader_permit&) = default;
    reader_permit(reader_permit&&) = default;

    reader_permit& operator=(const reader_permit&) = default;
    reader_permit& operator=(reader_permit&&) = default;

    bool operator==(const reader_permit& o) const {
        return _impl == o._impl;
    }

    reader_concurrency_semaphore& semaphore();

    future<resource_units> wait_admission(size_t memory, db::timeout_clock::time_point timeout);

    future<> maybe_wait_readmission(db::timeout_clock::time_point timeout);

    void consume(reader_resources res);

    void signal(reader_resources res);

    resource_units consume_memory(size_t memory = 0);

    resource_units consume_resources(reader_resources res);

    reader_resources consumed_resources() const;

    reader_resources base_resources() const;

    sstring description() const;
};

using reader_permit_opt = optimized_optional<reader_permit>;

class reader_permit::resource_units {
    reader_permit _permit;
    reader_resources _resources;

    friend class reader_permit;
    friend class reader_concurrency_semaphore;
private:
    resource_units(reader_permit permit, reader_resources res) noexcept;
public:
    resource_units(const resource_units&) = delete;
    resource_units(resource_units&&) noexcept;
    ~resource_units();
    resource_units& operator=(const resource_units&) = delete;
    resource_units& operator=(resource_units&&) noexcept;
    void add(resource_units&& o);
    void reset(reader_resources res = {});
    reader_permit permit() const { return _permit; }
    reader_resources resources() const { return _resources; }
};

/// Mark a permit as used.
///
/// Conceptually, a permit is considered used, when at least one reader
/// associated with it has an ongoing foreground operation initiated by
/// its consumer. E.g. a pending `fill_buffer()` call.
/// This class is an RAII used marker meant to be used by keeping it alive
/// until the reader is used.
class reader_permit::used_guard {
    reader_permit_opt _permit;
public:
    explicit used_guard(reader_permit permit) noexcept : _permit(std::move(permit)) {
        _permit->mark_used();
    }
    used_guard(used_guard&&) noexcept = default;
    used_guard(const used_guard&) = delete;
    ~used_guard() {
        if (_permit) {
            _permit->mark_unused();
        }
    }
    used_guard& operator=(used_guard&&) = delete;
    used_guard& operator=(const used_guard&) = delete;
};

/// Mark a permit as blocked.
///
/// Conceptually, a permit is considered blocked, when at least one reader
/// associated with it is waiting on I/O or a remote shard as part of a
/// foreground operation initiated by its consumer. E.g. an sstable reader
/// waiting on a disk read as part of its `fill_buffer()` call.
/// This class is an RAII block marker meant to be used by keeping it alive
/// until said block resolves.
class reader_permit::blocked_guard {
    reader_permit_opt _permit;
public:
    explicit blocked_guard(reader_permit permit) noexcept : _permit(std::move(permit)) {
        _permit->mark_blocked();
    }
    blocked_guard(blocked_guard&&) noexcept = default;
    blocked_guard(const blocked_guard&) = delete;
    ~blocked_guard() {
        if (_permit) {
            _permit->mark_unblocked();
        }
    }
    blocked_guard& operator=(blocked_guard&&) = delete;
    blocked_guard& operator=(const blocked_guard&) = delete;
};

template <typename Char>
temporary_buffer<Char> make_tracked_temporary_buffer(temporary_buffer<Char> buf, reader_permit& permit) {
    return temporary_buffer<Char>(buf.get_write(), buf.size(),
            make_deleter(buf.release(), [units = permit.consume_memory(buf.size())] () mutable { units.reset(); }));
}

file make_tracked_file(file f, reader_permit p);

template <typename T>
class tracking_allocator {
public:
    using value_type = T;
    using propagate_on_container_move_assignment = std::true_type;
    using is_always_equal = std::false_type;

private:
    reader_permit _permit;
    std::allocator<T> _alloc;

public:
    tracking_allocator(reader_permit permit) noexcept : _permit(std::move(permit)) { }

    T* allocate(size_t n) {
        auto p = _alloc.allocate(n);
        _permit.consume(reader_resources::with_memory(n * sizeof(T)));
        return p;
    }
    void deallocate(T* p, size_t n) {
        _alloc.deallocate(p, n);
        if (n) {
            _permit.signal(reader_resources::with_memory(n * sizeof(T)));
        }
    }

    template <typename U>
    friend bool operator==(const tracking_allocator<U>& a, const tracking_allocator<U>& b);
};

template <typename T>
bool operator==(const tracking_allocator<T>& a, const tracking_allocator<T>& b) {
    return a._semaphore == b._semaphore;
}
