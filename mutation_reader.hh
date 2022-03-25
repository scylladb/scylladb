/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <vector>

#include <seastar/core/future.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/do_with.hh>
#include "tracing/trace_state.hh"
#include "readers/flat_mutation_reader.hh"
#include "readers/flat_mutation_reader_v2.hh"
#include "reader_concurrency_semaphore.hh"
#include <seastar/core/io_priority_class.hh>

template <typename MutationFilter>
requires std::is_invocable_r_v<bool, MutationFilter, const dht::decorated_key&>
class filtering_reader : public flat_mutation_reader_v2::impl {
    flat_mutation_reader_v2 _rd;
    MutationFilter _filter;
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

    mutation_source(std::function<flat_mutation_reader_v2(schema_ptr, reader_permit, partition_range, const query::partition_slice&, io_priority,
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
    mutation_source(std::function<flat_mutation_reader_v2(schema_ptr, reader_permit, partition_range, const query::partition_slice&, io_priority)> fn)
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
    mutation_source(std::function<flat_mutation_reader_v2(schema_ptr, reader_permit, partition_range, const query::partition_slice&)> fn)
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
    mutation_source(std::function<flat_mutation_reader_v2(schema_ptr, reader_permit, partition_range range)> fn)
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

class queue_reader_v2;

/// Calls to different methods cannot overlap!
/// The handle can be used only while the reader is still alive. Once
/// `push_end_of_stream()` is called, the reader and the handle can be destroyed
/// in any order. The reader can be destroyed at any time.
class queue_reader_handle_v2 {
    friend std::pair<flat_mutation_reader_v2, queue_reader_handle_v2> make_queue_reader_v2(schema_ptr, reader_permit);
    friend class queue_reader_v2;

private:
    queue_reader_v2* _reader = nullptr;
    std::exception_ptr _ex;

private:
    explicit queue_reader_handle_v2(queue_reader_v2& reader) noexcept;

    void abandon() noexcept;

public:
    queue_reader_handle_v2(queue_reader_handle_v2&& o) noexcept;
    ~queue_reader_handle_v2();
    queue_reader_handle_v2& operator=(queue_reader_handle_v2&& o);

    future<> push(mutation_fragment_v2 mf);

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

std::pair<flat_mutation_reader_v2, queue_reader_handle_v2> make_queue_reader_v2(schema_ptr s, reader_permit permit);

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
/// Intra-partition forwarding: `fast_forward_to(position_range)` is supported
/// if the source reader supports it
flat_mutation_reader_v2 make_compacting_reader(flat_mutation_reader_v2 source, gc_clock::time_point compaction_time,
        std::function<api::timestamp_type(const dht::decorated_key&)> get_max_purgeable,
        streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no);
