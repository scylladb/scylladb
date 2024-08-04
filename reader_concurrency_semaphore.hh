/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

/*
 * Copyright (C) 2017-present ScyllaDB
 */

#pragma once

#include <boost/intrusive/list.hpp>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/metrics_registration.hh>
#include "reader_permit.hh"
#include "utils/updateable_value.hh"
#include "dht/i_partitioner_fwd.hh"

namespace bi = boost::intrusive;

using namespace seastar;

class mutation_reader;
using mutation_reader_opt = optimized_optional<mutation_reader>;

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
/// `max_queue_length` (upon calling `obtain_permit()`) an exception of
/// type `std::runtime_error` is thrown. Optionally, some additional
/// code can be executed just before throwing (`prethrow_action` 
/// constructor parameter).
///
/// The semaphore has 3 layers of defense against consuming more memory
/// than desired:
/// 1) After memory consumption is larger than the configured memory limit,
///    no more reads are admitted
/// 2) After memory consumption is larger than `_serialize_limit_multiplier`
///    times the configured memory limit, reads are serialized: only one of them
///    is allowed to make progress, the rest is made to wait before they can
///    consume more memory. Enforced via `request_memory()`.
/// 4) After memory consumption is larger than `_kill_limit_multiplier`
///    times the configured memory limit, reads are killed, by `consume()`
///    throwing `std::bad_alloc`.
///
/// This makes `_kill_limit_multiplier` times the memory limit the effective
/// upper bound of the memory consumed by reads.
///
/// The semaphore also acts as an execution stage for reads. This
/// functionality is exposed via \ref with_permit() and \ref
/// with_ready_permit().
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
        // Total number of reads killed due to the memory consumption reaching the kill limit.
        uint64_t total_reads_killed_due_to_kill_limit = 0;
        // Total number of reads admitted, via all admission paths.
        uint64_t reads_admitted = 0;
        // Total number of reads enqueued to wait for admission.
        uint64_t reads_enqueued_for_admission = 0;
        // Total number of reads enqueued to wait for memory.
        uint64_t reads_enqueued_for_memory = 0;
        // Total number of reads admitted immediately, without queueing
        uint64_t reads_admitted_immediately = 0;
        // Total number of reads enqueued because ready_list wasn't empty
        uint64_t reads_queued_because_ready_list = 0;
        // Total number of reads enqueued because there are permits who need CPU to make progress
        uint64_t reads_queued_because_need_cpu_permits = 0;
        // Total number of reads enqueued because there weren't enough memory resources
        uint64_t reads_queued_because_memory_resources = 0;
        // Total number of reads enqueued because there weren't enough count resources
        uint64_t reads_queued_because_count_resources = 0;
        // Total number of reads enqueued to be maybe admitted after evicting some inactive reads
        uint64_t reads_queued_with_eviction = 0;
        // Total number of permits created so far.
        uint64_t total_permits = 0;
        // Current number of permits.
        uint64_t current_permits = 0;
        // Current number permits needing CPU to make progress.
        uint64_t need_cpu_permits = 0;
        // Current number of permits awaiting I/O or an operation running on a remote shard.
        uint64_t awaits_permits = 0;
        // Current number of reads reading from the disk.
        uint64_t disk_reads = 0;
        // The number of sstables read currently.
        uint64_t sstables_read = 0;
        // Permits waiting on something: admission, memory or execution
        uint64_t waiters = 0;
    };

    using permit_list_type = bi::list<
            reader_permit::impl,
            bi::base_hook<bi::list_base_hook<bi::link_mode<bi::auto_unlink>>>,
            bi::constant_time_size<false>>;

    using read_func = noncopyable_function<future<>(reader_permit)>;

private:
    struct inactive_read;

public:
    class inactive_read_handle {
        reader_permit_opt _permit;

        friend class reader_concurrency_semaphore;

    private:
        void abandon() noexcept;

        explicit inactive_read_handle(reader_permit permit) noexcept;
    public:
        inactive_read_handle() = default;
        inactive_read_handle(inactive_read_handle&& o) noexcept;
        inactive_read_handle& operator=(inactive_read_handle&& o) noexcept;
        ~inactive_read_handle() {
            abandon();
        }
        explicit operator bool() const noexcept {
            return bool(_permit);
        }
    };

private:
    resources _initial_resources;
    resources _resources;
    utils::observer<int> _count_observer;

    struct wait_queue {
        // Stores entries for permits waiting to be admitted.
        permit_list_type _admission_queue;
        // Stores entries for serialized permits waiting to obtain memory.
        permit_list_type _memory_queue;
    public:
        bool empty() const {
            return _admission_queue.empty() && _memory_queue.empty();
        }
        void push_to_admission_queue(reader_permit::impl& p);
        void push_to_memory_queue(reader_permit::impl& p);
        reader_permit::impl& front();
        const reader_permit::impl& front() const;
    };

    wait_queue _wait_list;
    permit_list_type _ready_list;
    condition_variable _ready_list_cv;
    permit_list_type _inactive_reads;
    // Stores permits that are not in any of the above list.
    permit_list_type _permit_list;

    sstring _name;
    size_t _max_queue_length = std::numeric_limits<size_t>::max();
    utils::updateable_value<uint32_t> _serialize_limit_multiplier;
    utils::updateable_value<uint32_t> _kill_limit_multiplier;
    utils::updateable_value<uint32_t> _cpu_concurrency;
    stats _stats;
    std::optional<seastar::metrics::metric_groups> _metrics;
    bool _stopped = false;
    bool _evicting = false;
    gate _close_readers_gate;
    gate _permit_gate;
    std::optional<future<>> _execution_loop_future;
    reader_permit::impl* _blessed_permit = nullptr;

private:
    void do_detach_inactive_reader(reader_permit::impl&, evict_reason reason) noexcept;
    [[nodiscard]] mutation_reader detach_inactive_reader(reader_permit::impl&, evict_reason reason) noexcept;
    void evict(reader_permit::impl&, evict_reason reason) noexcept;

    bool has_available_units(const resources& r) const;

    bool cpu_concurrency_limit_reached() const;

    [[nodiscard]] std::exception_ptr check_queue_size(std::string_view queue_name);

    // Add the permit to the wait queue and return the future which resolves when
    // the permit is admitted (popped from the queue).
    enum class wait_on { admission, memory };
    future<> enqueue_waiter(reader_permit::impl& permit, wait_on wait);
    void evict_readers_in_background();
    future<> do_wait_admission(reader_permit::impl& permit);

    // Check whether permit can be admitted or not.
    // The wait list is not taken into consideration, this is the caller's
    // responsibility.
    // A return value of can_admit::maybe means admission might be possible if
    // some of the inactive readers are evicted.
    enum class can_admit { no, maybe, yes };
    enum class reason { all_ok = 0, ready_list, need_cpu_permits, memory_resources, count_resources };
    struct admit_result { can_admit decision; reason why; };
    admit_result can_admit_read(const reader_permit::impl& permit) const noexcept;

    bool should_evict_inactive_read() const noexcept;

    void maybe_admit_waiters() noexcept;

    // Request more memory for the permit.
    // Request is instantly granted while memory consumption of all reads is
    // below _kill_limit_multiplier.
    // After memory consumption goes above the above limit, only one reader
    // (permit) is allowed to make progress, this method will block for all other
    // one, until:
    // * The blessed read finishes and a new blessed permit is chosen.
    // * Memory consumption falls below the limit.
    future<> request_memory(reader_permit::impl& permit, size_t memory);

    void dequeue_permit(reader_permit::impl&);

    void on_permit_created(reader_permit::impl&);
    void on_permit_destroyed(reader_permit::impl&) noexcept;

    void on_permit_need_cpu() noexcept;
    void on_permit_not_need_cpu() noexcept;

    void on_permit_awaits() noexcept;
    void on_permit_not_awaits() noexcept;

    std::runtime_error stopped_exception();

    // closes reader in the background.
    void close_reader(mutation_reader reader);

    future<> execution_loop() noexcept;

    uint64_t get_serialize_limit() const;
    uint64_t get_kill_limit() const;

    // Throws std::bad_alloc if memory consumed is oom_kill_limit_multiply_threshold more than the memory limit.
    void consume(reader_permit::impl& permit, resources r);
    void signal(const resources& r) noexcept;

    future<> with_ready_permit(reader_permit::impl& permit);

public:
    struct no_limits { };
    using register_metrics = bool_class<class register_metrics_clas>;

    /// Create a semaphore with the specified limits
    ///
    /// The semaphore's name has to be unique!
    reader_concurrency_semaphore(
            utils::updateable_value<int> count,
            ssize_t memory,
            sstring name,
            size_t max_queue_length,
            utils::updateable_value<uint32_t> serialize_limit_multiplier,
            utils::updateable_value<uint32_t> kill_limit_multiplier,
            utils::updateable_value<uint32_t> cpu_concurrency,
            register_metrics metrics);

    reader_concurrency_semaphore(
            int count,
            ssize_t memory,
            sstring name,
            size_t max_queue_length,
            utils::updateable_value<uint32_t> serialize_limit_multiplier,
            utils::updateable_value<uint32_t> kill_limit_multiplier,
            register_metrics metrics)
        : reader_concurrency_semaphore(utils::updateable_value(count), memory, std::move(name), max_queue_length,
                std::move(serialize_limit_multiplier), std::move(kill_limit_multiplier), utils::updateable_value<uint32_t>(1), metrics)
    { }

    /// Create a semaphore with practically unlimited count and memory.
    ///
    /// And conversely, no queue limit either.
    /// The semaphore's name has to be unique!
    explicit reader_concurrency_semaphore(no_limits, sstring name, register_metrics metrics);

    /// A helper constructor *only for tests* that supplies default arguments.
    /// The other constructors have default values removed so 'production-code'
    /// is forced to specify all of them manually to avoid bugs.
    struct for_tests{};
    reader_concurrency_semaphore(for_tests, sstring name,
            int count = std::numeric_limits<int>::max(),
            ssize_t memory = std::numeric_limits<ssize_t>::max(),
            size_t max_queue_length = std::numeric_limits<size_t>::max(),
            utils::updateable_value<uint32_t> serialize_limit_multipler = utils::updateable_value(std::numeric_limits<uint32_t>::max()),
            utils::updateable_value<uint32_t> kill_limit_multipler = utils::updateable_value(std::numeric_limits<uint32_t>::max()),
            utils::updateable_value<uint32_t> cpu_concurrency = utils::updateable_value<uint32_t>(1),
            register_metrics metrics = register_metrics::no)
        : reader_concurrency_semaphore(utils::updateable_value(count), memory, std::move(name), max_queue_length, std::move(serialize_limit_multipler),
                std::move(kill_limit_multipler), std::move(cpu_concurrency), metrics)
    {}

    virtual ~reader_concurrency_semaphore();

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
    inactive_read_handle register_inactive_read(mutation_reader ir, const dht::partition_range* range = nullptr) noexcept;

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
    mutation_reader_opt unregister_inactive_read(inactive_read_handle irh);

    /// Try to evict an inactive read.
    ///
    /// Return true if an inactive read was evicted and false otherwise
    /// (if there was no reader to evict).
    bool try_evict_one_inactive_read(evict_reason = evict_reason::manual);

    /// Clear all inactive reads.
    void clear_inactive_reads();

    /// Evict all inactive reads the belong to the table designated by the id.
    /// If a range is provided, only inactive reads whose range overlaps with the
    /// range are evicted.
    /// The range of the inactive read is provided in register_inactive_read().
    /// If the range for an inactive read was not provided, all reads for the
    /// table are evicted.
    future<> evict_inactive_reads_for_table(table_id id, const dht::partition_range* range = nullptr) noexcept;
private:
    // The following two functions are extension points for
    // future inheriting classes that needs to run some stop
    // logic just before or just after the current stop logic.
    virtual future<> stop_ext_pre() {
        return make_ready_future<>();
    }
    virtual future<> stop_ext_post() {
        return make_ready_future<>();
    }
public:
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

    /// Make an admitted permit
    ///
    /// The permit is already in an admitted state after being created, this
    /// method includes waiting for admission.
    /// The permit is associated with a schema, which is the schema of the table
    /// the read is executed against, and the operation name, which should be a
    /// name such that we can identify the operation which created this permit.
    /// Ideally this should be a unique enough name that we not only can identify
    /// the kind of read, but the exact code-path that was taken.
    ///
    /// Some permits cannot be associated with any table, so passing nullptr as
    /// the schema parameter is allowed.
    future<reader_permit> obtain_permit(schema_ptr schema, const char* const op_name, size_t memory, db::timeout_clock::time_point timeout, tracing::trace_state_ptr trace_ptr);
    future<reader_permit> obtain_permit(schema_ptr schema, sstring&& op_name, size_t memory, db::timeout_clock::time_point timeout, tracing::trace_state_ptr trace_ptr);

    /// Make a tracking only permit
    ///
    /// The permit is not admitted. It is intended for reads that bypass the
    /// normal concurrency control, but whose resource usage we still want to
    /// keep track of, as part of that concurrency control.
    /// The permit is associated with a schema, which is the schema of the table
    /// the read is executed against, and the operation name, which should be a
    /// name such that we can identify the operation which created this permit.
    /// Ideally this should be a unique enough name that we not only can identify
    /// the kind of read, but the exact code-path that was taken.
    ///
    /// Some permits cannot be associated with any table, so passing nullptr as
    /// the schema parameter is allowed.
    reader_permit make_tracking_only_permit(schema_ptr schema, const char* const op_name, db::timeout_clock::time_point timeout, tracing::trace_state_ptr trace_ptr);
    reader_permit make_tracking_only_permit(schema_ptr schema, sstring&& op_name, db::timeout_clock::time_point timeout, tracing::trace_state_ptr trace_ptr);

    /// Run the function through the semaphore's execution stage with an admitted permit
    ///
    /// First a permit is obtained via the normal admission route, as if
    /// it was created  with \ref obtain_permit(), then func is enqueued to be
    /// run by the semaphore's execution loop. This emulates an execution stage,
    /// as it allows batching multiple funcs to be run together. Unlike an
    /// execution stage, with_permit() accepts a type-erased function, which
    /// allows for more flexibility in what functions are batched together.
    /// Use only functions that share most of their code to benefit from the
    /// instruction-cache warm-up!
    ///
    /// The permit is associated with a schema, which is the schema of the table
    /// the read is executed against, and the operation name, which should be a
    /// name such that we can identify the operation which created this permit.
    /// Ideally this should be a unique enough name that we not only can identify
    /// the kind of read, but the exact code-path that was taken.
    ///
    /// Some permits cannot be associated with any table, so passing nullptr as
    /// the schema parameter is allowed.
    future<> with_permit(schema_ptr schema, const char* const op_name, size_t memory, db::timeout_clock::time_point timeout, tracing::trace_state_ptr trace_ptr, read_func func);

    /// Run the function through the semaphore's execution stage with a pre-admitted permit
    ///
    /// Same as \ref with_permit(), but it uses an already admitted
    /// permit. Should only be used when a permit is already readily
    /// available, e.g. when resuming a saved read. Using
    /// \ref obtain_permit(), then \ref with_ready_permit() is less
    /// optimal then just using \ref with_permit().
    future<> with_ready_permit(reader_permit permit, read_func func);

    /// Set the total resources of the semaphore to \p r.
    ///
    /// After this call, \ref initial_resources() will reflect the new value.
    /// Available resources will be adjusted by the delta.
    void set_resources(resources r);

    const resources initial_resources() const {
        return _initial_resources;
    }

    bool is_unlimited() const {
        return _initial_resources == reader_resources{std::numeric_limits<int>::max(), std::numeric_limits<ssize_t>::max()};
    }

    const resources available_resources() const {
        return _resources;
    }

    const resources consumed_resources() const {
        return _initial_resources - _resources;
    }

    void broken(std::exception_ptr ex = {});

    /// Dump diagnostics printout
    ///
    /// Use max-lines to cap the number of (permit) lines in the report.
    /// Use 0 for unlimited.
    std::string dump_diagnostics(unsigned max_lines = 0) const;

    void set_max_queue_length(size_t size) {
        _max_queue_length = size;
    }

    uint64_t active_reads() const noexcept {
        return _stats.current_permits - _stats.inactive_reads - _stats.waiters;
    }

    void foreach_permit(noncopyable_function<void(const reader_permit::impl&)> func) const;
    void foreach_permit(noncopyable_function<void(const reader_permit&)> func) const;

    uintptr_t get_blessed_permit() const noexcept { return reinterpret_cast<uintptr_t>(_blessed_permit); }
};
