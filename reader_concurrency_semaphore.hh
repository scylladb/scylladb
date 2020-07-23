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
#include <seastar/core/future.hh>
#include "reader_permit.hh"

using namespace seastar;

/// Specific semaphore for controlling reader concurrency
///
/// Use `make_permit()` to create a permit to track the resource consumption
/// of a specific read. The permit should be created before the read is even
/// started so it is available to track resource consumption from the start.
/// Reader concurrency is dual limited by count and memory.
/// The semaphore can be configured with the desired limits on
/// construction. New readers will only be admitted when there is both
/// enough count and memory units available. Readers are admitted in
/// FIFO order.
/// Semaphore's `name` must be provided in ctor and its only purpose is
/// to increase readability of exceptions: both timeout exceptions and
/// queue overflow exceptions (read below) include this `name` in messages.
/// It's also possible to specify the maximum allowed number of waiting
/// readers by the `max_queue_length` constructor parameter. When the
/// number of waiting readers becomes equal or greater than
/// `max_queue_length` (upon calling `wait_admission()`) an exception of
/// type `std::runtime_error` is thrown. Optionally, some additional
/// code can be executed just before throwing (`prethrow_action` 
/// constructor parameter).
class reader_concurrency_semaphore {
public:
    using resources = reader_resources;

    friend class reader_permit;

    class inactive_read {
    public:
        virtual void evict() = 0;
        virtual ~inactive_read() = default;
    };

    class inactive_read_handle {
        uint64_t _id = 0;

        friend class reader_concurrency_semaphore;

        explicit inactive_read_handle(uint64_t id)
            : _id(id) {
        }
    public:
        inactive_read_handle() = default;
        inactive_read_handle(inactive_read_handle&& o) : _id(std::exchange(o._id, 0)) {
        }
        inactive_read_handle& operator=(inactive_read_handle&& o) {
            _id = std::exchange(o._id, 0);
            return *this;
        }
        explicit operator bool() const {
            return bool(_id);
        }
    };

    struct inactive_read_stats {
        // The number of inactive reads evicted to free up permits.
        uint64_t permit_based_evictions = 0;
        // The number of inactive reads currently registered.
        uint64_t population = 0;
    };

private:
    struct entry {
        promise<reader_permit::resource_units> pr;
        resources res;
        entry(promise<reader_permit::resource_units>&& pr, resources r) : pr(std::move(pr)), res(r) {}
    };

    class expiry_handler {
        sstring _semaphore_name;
    public:
        explicit expiry_handler(sstring semaphore_name)
            : _semaphore_name(std::move(semaphore_name)) {}
        void operator()(entry& e) noexcept {
            e.pr.set_exception(named_semaphore_timed_out(_semaphore_name));
        }
    };

private:
    resources _resources;

    expiring_fifo<entry, expiry_handler, db::timeout_clock> _wait_list;

    sstring _name;
    size_t _max_queue_length = std::numeric_limits<size_t>::max();
    std::function<void()> _prethrow_action;
    uint64_t _next_id = 1;
    std::map<uint64_t, std::unique_ptr<inactive_read>> _inactive_reads;
    inactive_read_stats _inactive_read_stats;

private:
    bool has_available_units(const resources& r) const {
        return bool(_resources) && _resources >= r;
    }

    bool may_proceed(const resources& r) const {
        return has_available_units(r) && _wait_list.empty();
    }

    future<reader_permit::resource_units> do_wait_admission(size_t memory, db::timeout_clock::time_point timeout);

public:
    struct no_limits { };

    reader_concurrency_semaphore(int count,
            ssize_t memory,
            sstring name,
            size_t max_queue_length = std::numeric_limits<size_t>::max(),
            std::function<void()> prethrow_action = nullptr)
        : _resources(count, memory)
        , _wait_list(expiry_handler(name))
        , _name(std::move(name))
        , _max_queue_length(max_queue_length)
        , _prethrow_action(std::move(prethrow_action)) {}

    /// Create a semaphore with practically unlimited count and memory.
    ///
    /// And conversely, no queue limit either.
    explicit reader_concurrency_semaphore(no_limits, sstring name = "unlimited reader_concurrency_semaphore")
        : reader_concurrency_semaphore(
                std::numeric_limits<int>::max(),
                std::numeric_limits<ssize_t>::max(),
                std::move(name)) {}

    ~reader_concurrency_semaphore();

    reader_concurrency_semaphore(const reader_concurrency_semaphore&) = delete;
    reader_concurrency_semaphore& operator=(const reader_concurrency_semaphore&) = delete;

    reader_concurrency_semaphore(reader_concurrency_semaphore&&) = delete;
    reader_concurrency_semaphore& operator=(reader_concurrency_semaphore&&) = delete;

    /// Returns the name of the semaphore
    ///
    /// If the semaphore has no name, "unnamed reader concurrency semaphore" is returned.
    std::string_view name() const {
        return _name.empty() ? "unnamed reader concurrency semaphore" : std::string_view(_name);
    }

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

    const inactive_read_stats& get_inactive_read_stats() const {
        return _inactive_read_stats;
    }

    reader_permit make_permit();

    const resources available_resources() const {
        return _resources;
    }

    void consume(resources r) {
        _resources -= r;
    }

    void signal(const resources& r) noexcept;

    size_t waiters() const {
        return _wait_list.size();
    }

    void broken(std::exception_ptr ex);
};
