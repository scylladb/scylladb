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
 * Copyright (C) 2017-present ScyllaDB
 */

#pragma once

#include <boost/intrusive/list.hpp>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include "reader_permit.hh"
#include "flat_mutation_reader.hh"

namespace bi = boost::intrusive;

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

    enum class evict_reason {
        permit, // evicted due to permit shortage
        time, // evicted due to expiring ttl
        manual, // evicted manually via `try_evict_one_inactive_read()`
    };

    using eviction_notify_handler = noncopyable_function<void(evict_reason)>;

    struct stats {
        // The number of inactive reads evicted to free up permits.
        uint64_t permit_based_evictions = 0;
        // The number of inactive reads evicted due to expiring.
        uint64_t time_based_evictions = 0;
        // The number of inactive reads currently registered.
        uint64_t inactive_reads = 0;
        // Total number of successful reads executed through this semaphore.
        uint64_t total_successful_reads = 0;
        // Total number of failed reads executed through this semaphore.
        uint64_t total_failed_reads = 0;
        // Total number of reads rejected because the admission queue reached its max capacity
        uint64_t total_reads_shed_due_to_overload = 0;
    };
    struct permit_stats {
        // Total number of permits created so far.
        uint64_t total_permits = 0;
        // Current number of permits.
        uint64_t current_permits = 0;
    };

    struct permit_list;

    class inactive_read_handle;

private:
    struct entry {
        promise<reader_permit::resource_units> pr;
        reader_permit permit;
        resources res;
        entry(promise<reader_permit::resource_units>&& pr, reader_permit permit, resources r)
            : pr(std::move(pr)), permit(std::move(permit)), res(r) {}
    };

    class expiry_handler {
        reader_concurrency_semaphore& _semaphore;
    public:
        explicit expiry_handler(reader_concurrency_semaphore& semaphore)
            : _semaphore(semaphore) {}
        void operator()(entry& e) noexcept;
    };

    struct inactive_read : public bi::list_base_hook<bi::link_mode<bi::auto_unlink>> {
        flat_mutation_reader reader;
        eviction_notify_handler notify_handler;
        timer<lowres_clock> ttl_timer;
        inactive_read_handle* handle = nullptr;

        explicit inactive_read(flat_mutation_reader reader_) noexcept
            : reader(std::move(reader_))
        { }
        ~inactive_read();
        void detach() noexcept;
    };

    using inactive_reads_type = bi::list<inactive_read, bi::constant_time_size<false>>;

public:
    class inactive_read_handle {
        reader_concurrency_semaphore* _sem = nullptr;
        inactive_read* _irp = nullptr;

        friend class reader_concurrency_semaphore;

    private:
        void abandon() noexcept;

        explicit inactive_read_handle(reader_concurrency_semaphore& sem, inactive_read& ir) noexcept
            : _sem(&sem), _irp(&ir) {
            _irp->handle = this;
        }
    public:
        inactive_read_handle() = default;
        inactive_read_handle(inactive_read_handle&& o) noexcept
            : _sem(std::exchange(o._sem, nullptr))
            , _irp(std::exchange(o._irp, nullptr)) {
            if (_irp) {
                _irp->handle = this;
            }
        }
        inactive_read_handle& operator=(inactive_read_handle&& o) noexcept {
            if (this == &o) {
                return *this;
            }
            abandon();
            _sem = std::exchange(o._sem, nullptr);
            _irp = std::exchange(o._irp, nullptr);
            if (_irp) {
                _irp->handle = this;
            }
            return *this;
        }
        ~inactive_read_handle() {
            abandon();
        }
        explicit operator bool() const noexcept {
            return bool(_irp);
        }
    };

private:
    const resources _initial_resources;
    resources _resources;

    expiring_fifo<entry, expiry_handler, db::timeout_clock> _wait_list;

    sstring _name;
    size_t _max_queue_length = std::numeric_limits<size_t>::max();
    std::function<void()> _prethrow_action;
    inactive_reads_type _inactive_reads;
    stats _stats;
    std::unique_ptr<permit_list> _permit_list;
    bool _stopped = false;
    gate _close_readers_gate;
    gate _permit_gate;

private:
    [[nodiscard]] flat_mutation_reader detach_inactive_reader(inactive_read&, evict_reason reason) noexcept;
    void evict(inactive_read&, evict_reason reason) noexcept;

    bool has_available_units(const resources& r) const;

    // Add the permit to the wait queue and return the future which resolves when
    // the permit is admitted (popped from the queue).
    future<reader_permit::resource_units> enqueue_waiter(reader_permit permit, resources r, db::timeout_clock::time_point timeout);
    void evict_readers_in_background();
    future<reader_permit::resource_units> do_wait_admission(reader_permit permit, size_t memory, db::timeout_clock::time_point timeout);
    void maybe_admit_waiters() noexcept;

    void on_permit_created(reader_permit::impl&);
    void on_permit_destroyed(reader_permit::impl&) noexcept;

    std::runtime_error stopped_exception();

    // closes reader in the background.
    void close_reader(flat_mutation_reader reader);

public:
    struct no_limits { };

    /// Create a semaphore with the specified limits
    ///
    /// The semaphore's name has to be unique!
    reader_concurrency_semaphore(int count,
            ssize_t memory,
            sstring name,
            size_t max_queue_length = std::numeric_limits<size_t>::max(),
            std::function<void()> prethrow_action = nullptr);

    /// Create a semaphore with practically unlimited count and memory.
    ///
    /// And conversely, no queue limit either.
    /// The semaphore's name has to be unique!
    explicit reader_concurrency_semaphore(no_limits, sstring name);

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
    /// stops being inactive and hence evictable, or to set the optional
    /// notify_handler and ttl.
    ///
    /// The semaphore takes ownership of the passed in reader for the duration
    /// of its inactivity and it may evict it to free up resources if necessary.
    inactive_read_handle register_inactive_read(flat_mutation_reader ir) noexcept;

    /// Set the inactive read eviction notification handler and optionally eviction ttl.
    ///
    /// The semaphore may evict this read when there is a shortage of
    /// permits or after the given ttl expired.
    ///
    /// The notification handler will be called when the inactive read is evicted
    /// passing with the reason it was evicted to the handler.
    ///
    /// Note that the inactive read might have already been evicted if
    /// the caller may yield after the register_inactive_read returned the handle
    /// and before calling set_notify_handler. In this case, the caller must revalidate
    /// the inactive_read_handle before calling this function.
    void set_notify_handler(inactive_read_handle& irh, eviction_notify_handler&& handler, std::optional<std::chrono::seconds> ttl);

    /// Unregister the previously registered inactive read.
    ///
    /// If the read was not evicted, the inactive read object, passed in to the
    /// register call, will be returned. Otherwise a nullptr is returned.
    flat_mutation_reader_opt unregister_inactive_read(inactive_read_handle irh);

    /// Try to evict an inactive read.
    ///
    /// Return true if an inactive read was evicted and false otherwise
    /// (if there was no reader to evict).
    bool try_evict_one_inactive_read(evict_reason = evict_reason::manual);

    /// Clear all inactive reads.
    void clear_inactive_reads();

    /// Stop the reader_concurrency_semaphore and clear all inactive reads.
    ///
    /// Wait on all async background work to complete.
    future<> stop() noexcept;

    const stats& get_stats() const {
        return _stats;
    }

    stats& get_stats() {
        return _stats;
    }

    /// Return stats about the currently existing permits.
    permit_stats get_permit_stats() const;

    /// Make a permit
    ///
    /// The permit is associated with a schema, which is the schema of the table
    /// the read is executed against, and the operation name, which should be a
    /// name such that we can identify the operation which created this permit.
    /// Ideally this should be a unique enough name that we not only can identify
    /// the kind of read, but the exact code-path that was taken.
    ///
    /// Some permits cannot be associated with any table, so passing nullptr as
    /// the schema parameter is allowed.
    reader_permit make_permit(const schema* const schema, const char* const op_name);
    reader_permit make_permit(const schema* const schema, sstring&& op_name);

    const resources initial_resources() const {
        return _initial_resources;
    }

    bool is_unlimited() const {
        return _initial_resources == reader_resources{std::numeric_limits<int>::max(), std::numeric_limits<ssize_t>::max()};
    }

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

    void broken(std::exception_ptr ex = {});

    /// Dump diagnostics printout
    ///
    /// Use max-lines to cap the number of (permit) lines in the report.
    /// Use 0 for unlimited.
    std::string dump_diagnostics(unsigned max_lines = 0) const;
};
