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

/*
 * Copyright (C) 2017 ScyllaDB
 */

#pragma once

#include <map>
#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/semaphore.hh>
#include "db/timeout_clock.hh"

using namespace seastar;

/// Specific semaphore for controlling reader concurrency
///
/// Before creating a reader one should obtain a permit by calling
/// `wait_admission()`. This permit can then be used for tracking the
/// reader's memory consumption via `reader_resource_tracker`.
/// The permit should be held onto for the lifetime of the reader
/// and/or any buffer its tracking.
/// Reader concurrency is dual limited by count and memory.
/// The semaphore can be configured with the desired limits on
/// construction. New readers will only be admitted when there is both
/// enough count and memory units available. Readers are admitted in
/// FIFO order.
/// It's possible to specify the maximum allowed number of waiting
/// readers by the `max_queue_length` constructor parameter. When the
/// number waiting readers would be equal or greater than this number
/// (when calling `wait_admission()`) an exception will be thrown.
/// The type of the exception and optionally some additional code
/// that should be executed when this happens can be customized by the
/// `raise_queue_overloaded_exception` constructor parameter. This
/// function will be called every time the queue limit is surpassed.
/// It is expected to return an `std::exception_ptr` that will be
/// injected into the future.
class reader_concurrency_semaphore {
public:
    struct resources {
        int count = 0;
        ssize_t memory = 0;

        resources() = default;

        resources(int count, ssize_t memory)
            : count(count)
            , memory(memory) {
        }

        bool operator>=(const resources& other) const {
            return count >= other.count && memory >= other.memory;
        }

        resources& operator-=(const resources& other) {
            count -= other.count;
            memory -= other.memory;
            return *this;
        }

        resources& operator+=(const resources& other) {
            count += other.count;
            memory += other.memory;
            return *this;
        }

        explicit operator bool() const {
            return count >= 0 && memory >= 0;
        }
    };

    class reader_permit {
        reader_concurrency_semaphore& _semaphore;
        const resources _base_cost;
    public:
        reader_permit(reader_concurrency_semaphore& semaphore, resources base_cost)
            : _semaphore(semaphore)
            , _base_cost(base_cost) {
        }

        ~reader_permit() {
            _semaphore.signal(_base_cost);
        }

        reader_permit(const reader_permit&) = delete;
        reader_permit& operator=(const reader_permit&) = delete;

        reader_permit(reader_permit&& other) = delete;
        reader_permit& operator=(reader_permit&& other) = delete;

        void consume_memory(size_t memory) {
            _semaphore.consume_memory(memory);
        }

        void signal_memory(size_t memory) {
            _semaphore.signal_memory(memory);
        }
    };

    class inactive_read {
    public:
        virtual void evict() = 0;
        virtual ~inactive_read() = default;
    };

    class inactive_read_handle {
        uint64_t _id = 0;

        friend class reader_concurrency_semaphore;

        inactive_read_handle() = default;
        explicit inactive_read_handle(uint64_t id)
            : _id(id) {
        }
    public:
        explicit operator bool() const {
            return bool(_id);
        }
    };

private:
    struct entry {
        promise<lw_shared_ptr<reader_permit>> pr;
        resources res;
        entry(promise<lw_shared_ptr<reader_permit>>&& pr, resources r) : pr(std::move(pr)), res(r) {}
    };
    struct expiry_handler {
        void operator()(entry& e) noexcept {
            e.pr.set_exception(semaphore_timed_out());
        }
    };

private:
    resources _resources;

    expiring_fifo<entry, expiry_handler, db::timeout_clock> _wait_list;

    size_t _max_queue_length = std::numeric_limits<size_t>::max();
    std::function<std::exception_ptr()> _make_queue_overloaded_exception;
    uint64_t _next_id = 1;
    std::map<uint64_t, std::unique_ptr<inactive_read>> _inactive_reads;

private:
    static std::exception_ptr default_make_queue_overloaded_exception() {
        return std::make_exception_ptr(std::runtime_error("restricted mutation reader queue overload"));
    }

    bool has_available_units(const resources& r) const {
        return bool(_resources) && _resources >= r;
    }

    bool may_proceed(const resources& r) const {
        return has_available_units(r) && _wait_list.empty();
    }

    void consume_memory(size_t memory) {
        _resources.memory -= memory;
    }

    void signal(const resources& r);

    void signal_memory(size_t memory) {
        signal(resources(0, static_cast<ssize_t>(memory)));
    }
public:
    struct no_limits { };

    reader_concurrency_semaphore(int count,
            ssize_t memory,
            size_t max_queue_length = std::numeric_limits<size_t>::max(),
            std::function<std::exception_ptr()> raise_queue_overloaded_exception = default_make_queue_overloaded_exception)
        : _resources(count, memory)
        , _max_queue_length(max_queue_length)
        , _make_queue_overloaded_exception(raise_queue_overloaded_exception) {
    }

    /// Create a semaphore with practically unlimited count and memory.
    ///
    /// And conversely, no queue limit either.
    explicit reader_concurrency_semaphore(no_limits)
        : reader_concurrency_semaphore(std::numeric_limits<int>::max(), std::numeric_limits<ssize_t>::max()) {
    }

    reader_concurrency_semaphore(const reader_concurrency_semaphore&) = delete;
    reader_concurrency_semaphore& operator=(const reader_concurrency_semaphore&) = delete;

    reader_concurrency_semaphore(reader_concurrency_semaphore&&) = delete;
    reader_concurrency_semaphore& operator=(reader_concurrency_semaphore&&) = delete;

    /// Register an inactive read.
    ///
    /// The semaphore will evict this read when there is a shortage of
    /// permits. This might be immediate, during this register call.
    /// Clients can use the returned handle to unregister the read, when it
    /// stops being inactive and hence evictable.
    ///
    /// An inactive read is an object implementing the `inactive_read`
    /// interface.
    /// The semaphore takes ownership of the created object and destroys it if
    /// it is evicted.
    inactive_read_handle register_inactive_read(std::unique_ptr<inactive_read> ir);

    /// Unregister the previously registered inactive read.
    ///
    /// If the read was not evicted, the inactive read object, passed in to the
    /// register call, will be returned. Otherwise a nullptr is returned.
    std::unique_ptr<inactive_read> unregister_inactive_read(inactive_read_handle irh);

    /// Try to evict an inactive read.
    ///
    /// Return true if an inactive read was evicted and false otherwise
    /// (if there was no reader to evict).
    bool try_evict_one_inactive_read();

    void clear_inactive_reads() {
        _inactive_reads.clear();
    }

    future<lw_shared_ptr<reader_permit>> wait_admission(size_t memory, db::timeout_clock::time_point timeout = db::no_timeout);

    /// Consume the specific amount of resources without waiting.
    lw_shared_ptr<reader_permit> consume_resources(resources r);

    const resources available_resources() const {
        return _resources;
    }

    size_t waiters() const {
        return _wait_list.size();
    }
};

class reader_resource_tracker {
    lw_shared_ptr<reader_concurrency_semaphore::reader_permit> _permit;
public:
    reader_resource_tracker() = default;
    explicit reader_resource_tracker(lw_shared_ptr<reader_concurrency_semaphore::reader_permit> permit)
        : _permit(std::move(permit)) {
    }

    bool operator==(const reader_resource_tracker& other) const {
        return _permit == other._permit;
    }

    file track(file f) const;

    lw_shared_ptr<reader_concurrency_semaphore::reader_permit> get_permit() const {
        return _permit;
    }
};

inline reader_resource_tracker no_resource_tracking() {
    return {};
}
