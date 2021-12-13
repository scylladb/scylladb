/*
 * Copyright (C) 2015-present ScyllaDB
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

#include <vector>

#include <seastar/core/future.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/do_with.hh>
#include "tracing/trace_state.hh"
#include "flat_mutation_reader.hh"
#include "flat_mutation_reader_v2.hh"
#include "reader_concurrency_semaphore.hh"

class reader_selector {
protected:
    schema_ptr _s;
    dht::ring_position_view _selector_position;
public:
    reader_selector(schema_ptr s, dht::ring_position_view rpv) noexcept : _s(std::move(s)), _selector_position(std::move(rpv)) {}

    virtual ~reader_selector() = default;
    // Call only if has_new_readers() returned true.
    virtual std::vector<flat_mutation_reader_v2> create_new_readers(const std::optional<dht::ring_position_view>& pos) = 0;
    virtual std::vector<flat_mutation_reader_v2> fast_forward_to(const dht::partition_range& pr) = 0;

    // Can be false-positive but never false-negative!
    bool has_new_readers(const std::optional<dht::ring_position_view>& pos) const noexcept {
        dht::ring_position_comparator cmp(*_s);
        return !_selector_position.is_max() && (!pos || cmp(*pos, _selector_position) >= 0);
    }
};

// Creates a mutation reader which combines data return by supplied readers.
// Returns mutation of the same schema only when all readers return mutations
// of the same schema.
flat_mutation_reader_v2 make_combined_reader(schema_ptr schema,
        reader_permit permit,
        std::vector<flat_mutation_reader_v2>,
        streamed_mutation::forwarding fwd_sm = streamed_mutation::forwarding::no,
        mutation_reader::forwarding fwd_mr = mutation_reader::forwarding::yes);
flat_mutation_reader_v2 make_combined_reader(schema_ptr schema,
        reader_permit permit,
        std::unique_ptr<reader_selector>,
        streamed_mutation::forwarding,
        mutation_reader::forwarding);
flat_mutation_reader_v2 make_combined_reader(schema_ptr schema,
        reader_permit permit,
        flat_mutation_reader_v2&& a,
        flat_mutation_reader_v2&& b,
        streamed_mutation::forwarding fwd_sm = streamed_mutation::forwarding::no,
        mutation_reader::forwarding fwd_mr = mutation_reader::forwarding::yes);

template <typename MutationFilter>
requires requires(MutationFilter mf, const dht::decorated_key& dk) {
    { mf(dk) } -> std::same_as<bool>;
}
class filtering_reader : public flat_mutation_reader_v2::impl {
    flat_mutation_reader_v2 _rd;
    MutationFilter _filter;
    static_assert(std::is_same<bool, std::result_of_t<MutationFilter(const dht::decorated_key&)>>::value, "bad MutationFilter signature");
public:
    filtering_reader(flat_mutation_reader_v2 rd, MutationFilter&& filter)
        : impl(rd.schema(), rd.permit())
        , _rd(std::move(rd))
        , _filter(std::forward<MutationFilter>(filter)) {
    }
    virtual future<> fill_buffer() override {
        return do_until([this] { return is_buffer_full() || is_end_of_stream(); }, [this] {
            return _rd.fill_buffer().then([this] {
                return do_until([this] { return _rd.is_buffer_empty(); }, [this] {
                    auto mf = _rd.pop_mutation_fragment();
                    if (mf.is_partition_start()) {
                        auto& dk = mf.as_partition_start().key();
                        if (!_filter(dk)) {
                            return _rd.next_partition();
                        }
                    }
                    push_mutation_fragment(std::move(mf));
                    return make_ready_future<>();
                }).then([this] {
                    _end_of_stream = _rd.is_end_of_stream();
                });
            });
        });
    }
    virtual future<> next_partition() override {
        clear_buffer_to_next_partition();
        if (is_buffer_empty()) {
            _end_of_stream = false;
            return _rd.next_partition();
        }
        return make_ready_future<>();
    }
    virtual future<> fast_forward_to(const dht::partition_range& pr) override {
        clear_buffer();
        _end_of_stream = false;
        return _rd.fast_forward_to(pr);
    }
    virtual future<> fast_forward_to(position_range pr) override {
        forward_buffer_to(pr.start());
        _end_of_stream = false;
        return _rd.fast_forward_to(std::move(pr));
    }
    virtual future<> close() noexcept override {
        return _rd.close();
    }
};

// Creates a mutation_reader wrapper which creates a new stream of mutations
// with some mutations removed from the original stream.
// MutationFilter is a callable which decides which mutations are dropped. It
// accepts mutation const& and returns a bool. The mutation stays in the
// stream if and only if the filter returns true.
template <typename MutationFilter>
flat_mutation_reader_v2 make_filtering_reader(flat_mutation_reader_v2 rd, MutationFilter&& filter) {
    return make_flat_mutation_reader_v2<filtering_reader<MutationFilter>>(std::move(rd), std::forward<MutationFilter>(filter));
}

/// Create a wrapper that filters fragments according to partition range and slice.
flat_mutation_reader make_slicing_filtering_reader(flat_mutation_reader, const dht::partition_range&, const query::partition_slice&);

/// A partition_presence_checker quickly returns whether a key is known not to exist
/// in a data source (it may return false positives, but not false negatives).
enum class partition_presence_checker_result {
    definitely_doesnt_exist,
    maybe_exists
};
using partition_presence_checker = std::function<partition_presence_checker_result (const dht::decorated_key& key)>;

inline
partition_presence_checker make_default_partition_presence_checker() {
    return [] (const dht::decorated_key&) { return partition_presence_checker_result::maybe_exists; };
}

// mutation_source represents source of data in mutation form. The data source
// can be queried multiple times and in parallel. For each query it returns
// independent mutation_reader.
//
// The reader returns mutations having all the same schema, the one passed
// when invoking the source.
//
// When reading in reverse, a reverse schema has to be passed (compared to the
// table's schema), and a half-reverse (legacy) slice.
// See docs/design-notes/reverse-reads.md for more details.
// Partition-range forwarding is not yet supported in reverse mode.
class mutation_source {
    using partition_range = const dht::partition_range&;
    using io_priority = const io_priority_class&;
    using flat_reader_factory_type = std::function<flat_mutation_reader(schema_ptr,
                                                                        reader_permit,
                                                                        partition_range,
                                                                        const query::partition_slice&,
                                                                        io_priority,
                                                                        tracing::trace_state_ptr,
                                                                        streamed_mutation::forwarding,
                                                                        mutation_reader::forwarding)>;
    using flat_reader_v2_factory_type = std::function<flat_mutation_reader_v2(schema_ptr,
                                                                        reader_permit,
                                                                        partition_range,
                                                                        const query::partition_slice&,
                                                                        io_priority,
                                                                        tracing::trace_state_ptr,
                                                                        streamed_mutation::forwarding,
                                                                        mutation_reader::forwarding)>;
    // We could have our own version of std::function<> that is nothrow
    // move constructible and save some indirection and allocation.
    // Probably not worth the effort though.
    // Either _fn or _fn_v2 is engaged.
    lw_shared_ptr<flat_reader_factory_type> _fn;
    lw_shared_ptr<flat_reader_v2_factory_type> _fn_v2;
    lw_shared_ptr<std::function<partition_presence_checker()>> _presence_checker_factory;
private:
    mutation_source() = default;
    explicit operator bool() const { return bool(_fn) || bool(_fn_v2); }
    friend class optimized_optional<mutation_source>;
public:
    mutation_source(flat_reader_factory_type fn, std::function<partition_presence_checker()> pcf = [] { return make_default_partition_presence_checker(); })
        : _fn(make_lw_shared<flat_reader_factory_type>(std::move(fn)))
        , _presence_checker_factory(make_lw_shared<std::function<partition_presence_checker()>>(std::move(pcf)))
    { }

    mutation_source(flat_reader_v2_factory_type fn, std::function<partition_presence_checker()> pcf = [] { return make_default_partition_presence_checker(); })
        : _fn_v2(make_lw_shared<flat_reader_v2_factory_type>(std::move(fn)))
        , _presence_checker_factory(make_lw_shared<std::function<partition_presence_checker()>>(std::move(pcf)))
    { }

    // For sources which don't care about the mutation_reader::forwarding flag (always fast forwardable)
    mutation_source(std::function<flat_mutation_reader(schema_ptr, reader_permit, partition_range, const query::partition_slice&, io_priority,
                tracing::trace_state_ptr, streamed_mutation::forwarding)> fn)
        : mutation_source([fn = std::move(fn)] (schema_ptr s,
                    reader_permit permit,
                    partition_range range,
                    const query::partition_slice& slice,
                    io_priority pc,
                    tracing::trace_state_ptr tr,
                    streamed_mutation::forwarding fwd,
                    mutation_reader::forwarding) {
        return fn(std::move(s), std::move(permit), range, slice, pc, std::move(tr), fwd);
    }) {}
    mutation_source(std::function<flat_mutation_reader(schema_ptr, reader_permit, partition_range, const query::partition_slice&, io_priority)> fn)
        : mutation_source([fn = std::move(fn)] (schema_ptr s,
                    reader_permit permit,
                    partition_range range,
                    const query::partition_slice& slice,
                    io_priority pc,
                    tracing::trace_state_ptr,
                    streamed_mutation::forwarding fwd,
                    mutation_reader::forwarding) {
        assert(!fwd);
        return fn(std::move(s), std::move(permit), range, slice, pc);
    }) {}
    mutation_source(std::function<flat_mutation_reader(schema_ptr, reader_permit, partition_range, const query::partition_slice&)> fn)
        : mutation_source([fn = std::move(fn)] (schema_ptr s,
                    reader_permit permit,
                    partition_range range,
                    const query::partition_slice& slice,
                    io_priority,
                    tracing::trace_state_ptr,
                    streamed_mutation::forwarding fwd,
                    mutation_reader::forwarding) {
        assert(!fwd);
        return fn(std::move(s), std::move(permit), range, slice);
    }) {}
    mutation_source(std::function<flat_mutation_reader(schema_ptr, reader_permit, partition_range range)> fn)
        : mutation_source([fn = std::move(fn)] (schema_ptr s,
                    reader_permit permit,
                    partition_range range,
                    const query::partition_slice&,
                    io_priority,
                    tracing::trace_state_ptr,
                    streamed_mutation::forwarding fwd,
                    mutation_reader::forwarding) {
        assert(!fwd);
        return fn(std::move(s), std::move(permit), range);
    }) {}

    mutation_source(const mutation_source& other) = default;
    mutation_source& operator=(const mutation_source& other) = default;
    mutation_source(mutation_source&&) = default;
    mutation_source& operator=(mutation_source&&) = default;

    // Creates a new reader.
    //
    // All parameters captured by reference must remain live as long as returned
    // mutation_reader or streamed_mutation obtained through it are alive.
    flat_mutation_reader
    make_reader(
        schema_ptr s,
        reader_permit permit,
        partition_range range,
        const query::partition_slice& slice,
        io_priority pc = default_priority_class(),
        tracing::trace_state_ptr trace_state = nullptr,
        streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no,
        mutation_reader::forwarding fwd_mr = mutation_reader::forwarding::yes) const
    {
        if (_fn_v2) {
            return downgrade_to_v1(
                    (*_fn_v2)(std::move(s), std::move(permit), range, slice, pc, std::move(trace_state), fwd, fwd_mr));
        }
        return (*_fn)(std::move(s), std::move(permit), range, slice, pc, std::move(trace_state), fwd, fwd_mr);
    }

    flat_mutation_reader
    make_reader(
        schema_ptr s,
        reader_permit permit,
        partition_range range = query::full_partition_range) const
    {
        auto& full_slice = s->full_slice();
        return this->make_reader(std::move(s), std::move(permit), range, full_slice);
    }

    // Creates a new reader.
    //
    // All parameters captured by reference must remain live as long as returned
    // mutation_reader or streamed_mutation obtained through it are alive.
    flat_mutation_reader_v2
    make_reader_v2(
            schema_ptr s,
            reader_permit permit,
            partition_range range,
            const query::partition_slice& slice,
            io_priority pc = default_priority_class(),
            tracing::trace_state_ptr trace_state = nullptr,
            streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no,
            mutation_reader::forwarding fwd_mr = mutation_reader::forwarding::yes) const
    {
        if (_fn_v2) {
            return (*_fn_v2)(std::move(s), std::move(permit), range, slice, pc, std::move(trace_state), fwd, fwd_mr);
        }
        return upgrade_to_v2(
                (*_fn)(std::move(s), std::move(permit), range, slice, pc, std::move(trace_state), fwd, fwd_mr));
    }

    flat_mutation_reader_v2
    make_reader_v2(
            schema_ptr s,
            reader_permit permit,
            partition_range range = query::full_partition_range) const
    {
        auto& full_slice = s->full_slice();
        return make_reader_v2(std::move(s), std::move(permit), range, full_slice);
    }

    partition_presence_checker make_partition_presence_checker() {
        return (*_presence_checker_factory)();
    }

    enum class version { v1, v2 };
    version native_version() const { return _fn ? version::v1 : version::v2; }
};

// Returns a mutation_source which is the sum of given mutation_sources.
//
// Adding two mutation sources gives a mutation source which contains
// the sum of writes contained in the addends.
mutation_source make_combined_mutation_source(std::vector<mutation_source>);

// Represent mutation_source which can be snapshotted.
class snapshot_source {
private:
    std::function<mutation_source()> _func;
public:
    snapshot_source(std::function<mutation_source()> func)
        : _func(std::move(func))
    { }

    // Creates a new snapshot.
    // The returned mutation_source represents all earlier writes and only those.
    // Note though that the mutations in the snapshot may get compacted over time.
    mutation_source operator()() {
        return _func();
    }
};

mutation_source make_empty_mutation_source();
snapshot_source make_empty_snapshot_source();

using mutation_source_opt = optimized_optional<mutation_source>;

// Adapts a non-movable FlattenedConsumer to a movable one.
template<typename FlattenedConsumer>
class stable_flattened_mutations_consumer {
    std::unique_ptr<FlattenedConsumer> _ptr;
public:
    stable_flattened_mutations_consumer(std::unique_ptr<FlattenedConsumer> ptr) : _ptr(std::move(ptr)) {}
    auto consume_new_partition(const dht::decorated_key& dk) { return _ptr->consume_new_partition(dk); }
    auto consume(tombstone t) { return _ptr->consume(t); }
    auto consume(static_row&& sr) { return _ptr->consume(std::move(sr)); }
    auto consume(clustering_row&& cr) { return _ptr->consume(std::move(cr)); }
    auto consume(range_tombstone&& rt) { return _ptr->consume(std::move(rt)); }
    auto consume_end_of_partition() { return _ptr->consume_end_of_partition(); }
    auto consume_end_of_stream() { return _ptr->consume_end_of_stream(); }
};

template<typename FlattenedConsumer, typename... Args>
stable_flattened_mutations_consumer<FlattenedConsumer> make_stable_flattened_mutations_consumer(Args&&... args) {
    return { std::make_unique<FlattenedConsumer>(std::forward<Args>(args)...) };
}

/// Make a foreign_reader.
///
/// foreign_reader is a local representant of a reader located on a remote
/// shard. Manages its lifecycle and takes care of seamlessly transferring
/// produced fragments. Fragments are *copied* between the shards, a
/// bufferful at a time.
/// To maximize throughput read-ahead is used. After each fill_buffer() or
/// fast_forward_to() a read-ahead (a fill_buffer() on the remote reader) is
/// issued. This read-ahead runs in the background and is brough back to
/// foreground on the next fill_buffer() or fast_forward_to() call.
/// If the reader resides on this shard (the shard where make_foreign_reader()
/// is called) there is no need to wrap it in foreign_reader, just return it as
/// is.
flat_mutation_reader make_foreign_reader(schema_ptr schema,
        reader_permit permit,
        foreign_ptr<std::unique_ptr<flat_mutation_reader>> reader,
        streamed_mutation::forwarding fwd_sm = streamed_mutation::forwarding::no);

/// Make an auto-paused evictable reader.
///
/// The reader is paused after each use, that is after each call to any of its
/// members that cause actual reading to be done (`fill_buffer()` and
/// `fast_forward_to()`). When paused, the reader is made evictable, that it is
/// it is registered with reader concurrency semaphore as an inactive read.
/// The reader is resumed automatically on the next use. If it was evicted, it
/// will be recreated at the position it left off reading. This is all
/// transparent to its user.
/// Parameters passed by reference have to be kept alive while the reader is
/// alive.
flat_mutation_reader make_auto_paused_evictable_reader(
        mutation_source ms,
        schema_ptr schema,
        reader_permit permit,
        const dht::partition_range& pr,
        const query::partition_slice& ps,
        const io_priority_class& pc,
        tracing::trace_state_ptr trace_state,
        mutation_reader::forwarding fwd_mr);

class evictable_reader;

class evictable_reader_handle {
    friend std::pair<flat_mutation_reader, evictable_reader_handle> make_manually_paused_evictable_reader(mutation_source, schema_ptr, reader_permit,
            const dht::partition_range&, const query::partition_slice&, const io_priority_class&, tracing::trace_state_ptr, mutation_reader::forwarding);

private:
    evictable_reader* _r;

private:
    explicit evictable_reader_handle(evictable_reader& r);

public:
    void pause();
};

/// Make a manually-paused evictable reader.
///
/// The reader can be paused via the evictable reader handle when desired. The
/// intended usage is subsequent reads done in bursts, after which the reader is
/// not used for some time. When paused, the reader is made evictable, that is,
/// it is registered with reader concurrency semaphore as an inactive read.
/// The reader is resumed automatically on the next use. If it was evicted, it
/// will be recreated at the position it left off reading. This is all
/// transparent to its user.
/// Parameters passed by reference have to be kept alive while the reader is
/// alive.
std::pair<flat_mutation_reader, evictable_reader_handle> make_manually_paused_evictable_reader(
        mutation_source ms,
        schema_ptr schema,
        reader_permit permit,
        const dht::partition_range& pr,
        const query::partition_slice& ps,
        const io_priority_class& pc,
        tracing::trace_state_ptr trace_state,
        mutation_reader::forwarding fwd_mr);

/// Reader lifecycle policy for the mulitshard combining reader.
///
/// This policy is expected to make sure any additional resource the readers
/// might need is kept alive for the lifetime of the readers, not that
/// of the multishard reader. This is a very important distinction. As
/// destructors cannot return futures, the multishard reader will be
/// destroyed before all it's shard readers could stop properly. Hence it
/// is the duty of this policy to make sure all objects the shard readers
/// depend on stay alive until they are properly destroyed on their home
/// shards. Note that this also includes the passed in `range` and `slice`
/// parameters because although client code is required to keep them alive as
/// long as the top level reader lives, the shard readers might outlive the
/// multishard reader itself.
class reader_lifecycle_policy {
public:
    struct stopped_reader {
        reader_concurrency_semaphore::inactive_read_handle handle;
        flat_mutation_reader::tracked_buffer unconsumed_fragments;
    };

public:
    /// Create an appropriate reader on the shard it is called on.
    ///
    /// Will be called when the multishard reader visits a shard for the
    /// first time or when a reader has to be recreated after having been
    /// evicted (while paused). This method should also enter gates, take locks
    /// or whatever is appropriate to make sure resources it is using on the
    /// remote shard stay alive, during the lifetime of the created reader.
    ///
    /// The \c permit parameter shall be obtained via `obtain_reader_permit()`
    virtual flat_mutation_reader create_reader(
            schema_ptr schema,
            reader_permit permit,
            const dht::partition_range& range,
            const query::partition_slice& slice,
            const io_priority_class& pc,
            tracing::trace_state_ptr trace_state,
            mutation_reader::forwarding fwd_mr) = 0;

    /// Updates the read-range of the shard reader.
    ///
    /// Gives the lifecycle-policy a chance to update its stored read-range (if
    /// the case). Called after any modification to the read range (typically
    /// after fast_forward_to()). The range is identical to the one the reader
    /// holds a reference to after the modification happened. When this method
    /// is called, it is safe to destroy the previous range instance.
    ///
    /// This method has to be called on the shard the reader lives on.
    virtual void update_read_range(lw_shared_ptr<const dht::partition_range> pr) = 0;

    /// Destroy the shard reader.
    ///
    /// Will be called when the multishard reader is being destroyed. It will be
    /// called for each of the shard readers.
    /// This method is expected to do a proper cleanup, that is, leave any gates,
    /// release any locks or whatever is appropriate for the shard reader.
    ///
    /// This method has to be called on the shard the reader lives on.
    /// This method will be called from a destructor so it cannot throw.
    virtual future<> destroy_reader(stopped_reader reader) noexcept = 0;

    /// Get the relevant semaphore for this read.
    ///
    /// The semaphore is used to register paused readers with as inactive
    /// readers. The semaphore then can evict these readers when resources are
    /// in-demand.
    /// The multishard reader will pause and resume readers via the `pause()`
    /// and `try_resume()` helper methods. Clients can resume any paused readers
    /// after the multishard reader is destroyed via the same helper methods.
    ///
    /// This method will be called on the shard where the relevant reader lives.
    virtual reader_concurrency_semaphore& semaphore() = 0;

    /// Obtain an admitted permit.
    ///
    /// The permit will be associated with the semaphore returned by
    /// `semaphore()`.
    ///
    /// This method will be called on the shard where the relevant reader lives.
    virtual future<reader_permit> obtain_reader_permit(schema_ptr schema, const char* const description, db::timeout_clock::time_point timeout) = 0;
};

/// Make a multishard_combining_reader.
///
/// multishard_combining_reader takes care of reading a range from all shards
/// that own a subrange in the range. Shard reader are created on-demand, when
/// the shard is visited for the first time.
///
/// The read starts with a concurrency of one, that is the reader reads from a
/// single shard at a time. The concurrency is exponentially increased (to a
/// maximum of the number of shards) when a reader's buffer is empty after
/// moving the next shard. This condition is important as we only wan't to
/// increase concurrency for sparse tables that have little data and the reader
/// has to move between shards often. When concurrency is > 1, the reader
/// issues background read-aheads to the next shards so that by the time it
/// needs to move to them they have the data ready.
/// For dense tables (where we rarely cross shards) we rely on the
/// foreign_reader to issue sufficient read-aheads on its own to avoid blocking.
///
/// The readers' life-cycles are managed through the supplied lifecycle policy.
flat_mutation_reader make_multishard_combining_reader(
        shared_ptr<reader_lifecycle_policy> lifecycle_policy,
        schema_ptr schema,
        reader_permit permit,
        const dht::partition_range& pr,
        const query::partition_slice& ps,
        const io_priority_class& pc,
        tracing::trace_state_ptr trace_state = nullptr,
        mutation_reader::forwarding fwd_mr = mutation_reader::forwarding::no);

flat_mutation_reader make_multishard_combining_reader_for_tests(
        const dht::sharder& sharder,
        shared_ptr<reader_lifecycle_policy> lifecycle_policy,
        schema_ptr schema,
        reader_permit permit,
        const dht::partition_range& pr,
        const query::partition_slice& ps,
        const io_priority_class& pc,
        tracing::trace_state_ptr trace_state = nullptr,
        mutation_reader::forwarding fwd_mr = mutation_reader::forwarding::no);

class queue_reader;

/// Calls to different methods cannot overlap!
/// The handle can be used only while the reader is still alive. Once
/// `push_end_of_stream()` is called, the reader and the handle can be destroyed
/// in any order. The reader can be destroyed at any time.
class queue_reader_handle {
    friend std::pair<flat_mutation_reader, queue_reader_handle> make_queue_reader(schema_ptr, reader_permit);
    friend class queue_reader;

private:
    queue_reader* _reader = nullptr;
    std::exception_ptr _ex;

private:
    explicit queue_reader_handle(queue_reader& reader) noexcept;

    void abandon() noexcept;

public:
    queue_reader_handle(queue_reader_handle&& o) noexcept;
    ~queue_reader_handle();
    queue_reader_handle& operator=(queue_reader_handle&& o);

    future<> push(mutation_fragment mf);

    /// Terminate the queue.
    ///
    /// The reader will be set to EOS. The handle cannot be used anymore.
    void push_end_of_stream();

    /// Aborts the queue.
    ///
    /// All future operations on the handle or the reader will raise `ep`.
    void abort(std::exception_ptr ep);

    /// Checks if the queue is already terminated with either a success or failure (abort)
    bool is_terminated() const;

    /// Get the stored exception, if any
    std::exception_ptr get_exception() const noexcept;
};

std::pair<flat_mutation_reader, queue_reader_handle> make_queue_reader(schema_ptr s, reader_permit permit);

/// Creates a compacting reader.
///
/// The compaction is done with a \ref mutation_compactor, using compaction-type
/// compaction (`compact_for_sstables::yes`).
///
/// \param source the reader whose output to compact.
///
/// Params \c compaction_time and \c get_max_purgeable are forwarded to the
/// \ref mutation_compactor instance.
///
/// Inter-partition forwarding: `next_partition()` and
/// `fast_forward_to(const dht::partition_range&)` is supported if the source
/// reader supports it
/// Intra-partition forwarding: `fast_forward_to(position_range)` is *not*
/// supported.
flat_mutation_reader make_compacting_reader(flat_mutation_reader source, gc_clock::time_point compaction_time,
        std::function<api::timestamp_type(const dht::decorated_key&)> get_max_purgeable);

// A mutation reader together with an upper bound on the set of positions of fragments
// that the reader will return. The upper bound does not need to be exact.
struct reader_and_upper_bound {
    flat_mutation_reader_v2 reader;
    position_in_partition upper_bound;

    reader_and_upper_bound(flat_mutation_reader_v2 r, position_in_partition bound)
        : reader(std::move(r)), upper_bound(std::move(bound)) {}

    reader_and_upper_bound(flat_mutation_reader r, position_in_partition bound)
        : reader(upgrade_to_v2(std::move(r))), upper_bound(std::move(bound)) {}
};

// A queue of mutation readers returning fragments with the same schema from the same single partition.
//
// Intuitively, the order of returned readers is such that the positions of the first fragments
// returned by the readers inside the partition (after `partition_start`) are ``mostly increasing''.
//
// More formally:
// 1. The queue contains a sequence of readers.
//    Each call to `pop` consumes a batch of readers from the sequence.
// 2. Each position-in-partition `b` corresponds to a prefix of the sequence of readers in the queue.
//    Let's call it `pref(b)`.
// 3. If `b1 <= b2`, then `pref(b1)` is a prefix of `pref(b2)`.
// 4. `pref(position_in_partition::after_all_clustered_rows())` is the entire sequence.
// 5. For each `b`, `pop(b)` returns only readers from `pref(b)`.
// 6. For each `b`, all readers that lie in the sequence after `pref(b)`
//    satisfy the following property:
//        the first fragment returned by the reader has a position greater than `b`.
//    In other words, if `pop(b)` returns no readers, then we can be sure that all readers
//    returned later by the queue return fragments with positions greater than `b`.
//
// Considering the above properties, a simple legal implementation of this interface would
// return all readers on the first call to `pop(after_all_clustered_rows())` and would not return
// any readers on `pop(b)` for `b < after_all_clustered_rows()`.
//
// Better implementations may use information about positions returned by the readers
// to return some readers earlier, but they must not break property 6.
// For example, the following scenario is illegal:
// 1. pop(for_key(10)) returns r1
// 2. pop(for_key(10)) returns no readers => all readers from pref(for_key(10)) have been popped
// 3. pop(for_key(20)) returns r2 => due to the previous step we know that r2 is not in pref(for_key(10))
// 4. the first fragment (excluding partition_start) returned by r2 has position for_key(10)
//        => illegal, because for_key(10) is not greater than for_key(10).
//        The first position returned by r2 must be after_key(10) or higher.
//
// With each reader also comes an upper bound on the set of positions of fragments that the reader will return.
class position_reader_queue {
public:
    virtual ~position_reader_queue() = 0;

    // `empty(b)` <=>
    //      we have popped all readers from `pref(b)` so `pop(b)`
    //      will not return any more readers.
    virtual bool empty(position_in_partition_view bound) const = 0;

    // Return the next batch of readers from `pref(b)`.
    virtual std::vector<reader_and_upper_bound> pop(position_in_partition_view bound) = 0;

    // Close all readers
    virtual future<> close() noexcept = 0;
};

flat_mutation_reader make_clustering_combined_reader(schema_ptr schema,
        reader_permit,
        streamed_mutation::forwarding,
        std::unique_ptr<position_reader_queue>);
