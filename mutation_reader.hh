/*
 * Copyright (C) 2015 ScyllaDB
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

#include "mutation.hh"
#include "clustering_key_filter.hh"
#include "core/future.hh"
#include "core/future-util.hh"
#include "core/do_with.hh"
#include "tracing/trace_state.hh"
#include "flat_mutation_reader.hh"
#include "reader_concurrency_semaphore.hh"

namespace mutation_reader {
    // mutation_reader::forwarding determines whether fast_forward_to() may
    // be used on the mutation reader to change the partition range being
    // read. Enabling forwarding also changes read policy: forwarding::no
    // means we will stop reading from disk at the end of the given range,
    // but with forwarding::yes we may read ahead, anticipating the user to
    // make a small skip with fast_forward_to() and continuing to read.
    //
    // Note that mutation_reader::forwarding is similarly name but different
    // from streamed_mutation::forwarding - the former is about skipping to
    // a different partition range, while the latter is about skipping
    // inside a large partition.
    using forwarding = flat_mutation_reader::partition_range_forwarding;
}

class reader_selector {
protected:
    schema_ptr _s;
    dht::ring_position _selector_position;
public:
    reader_selector(schema_ptr s, dht::ring_position rp) noexcept : _s(std::move(s)), _selector_position(std::move(rp)) {}

    virtual ~reader_selector() = default;
    // Call only if has_new_readers() returned true.
    virtual std::vector<flat_mutation_reader> create_new_readers(const dht::token* const t) = 0;
    virtual std::vector<flat_mutation_reader> fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout) = 0;

    // Can be false-positive but never false-negative!
    bool has_new_readers(const dht::token* const t) const noexcept {
        dht::ring_position_comparator cmp(*_s);
        return !_selector_position.is_max() && (!t || cmp(dht::ring_position_view(*t), _selector_position) >= 0);
    }
};

// Creates a mutation reader which combines data return by supplied readers.
// Returns mutation of the same schema only when all readers return mutations
// of the same schema.
flat_mutation_reader make_combined_reader(schema_ptr schema,
        std::vector<flat_mutation_reader>,
        streamed_mutation::forwarding fwd_sm = streamed_mutation::forwarding::no,
        mutation_reader::forwarding fwd_mr = mutation_reader::forwarding::yes);
flat_mutation_reader make_combined_reader(schema_ptr schema,
        std::unique_ptr<reader_selector>,
        streamed_mutation::forwarding,
        mutation_reader::forwarding);
flat_mutation_reader make_combined_reader(schema_ptr schema,
        flat_mutation_reader&& a,
        flat_mutation_reader&& b,
        streamed_mutation::forwarding fwd_sm = streamed_mutation::forwarding::no,
        mutation_reader::forwarding fwd_mr = mutation_reader::forwarding::yes);

template <typename MutationFilter>
GCC6_CONCEPT(
    requires requires(MutationFilter mf, const dht::decorated_key& dk) {
        { mf(dk) } -> bool;
    }
)
class filtering_reader : public flat_mutation_reader::impl {
    flat_mutation_reader _rd;
    MutationFilter _filter;
    static_assert(std::is_same<bool, std::result_of_t<MutationFilter(const dht::decorated_key&)>>::value, "bad MutationFilter signature");
public:
    filtering_reader(flat_mutation_reader rd, MutationFilter&& filter)
        : impl(rd.schema())
        , _rd(std::move(rd))
        , _filter(std::forward<MutationFilter>(filter)) {
    }
    virtual future<> fill_buffer(db::timeout_clock::time_point timeout) override {
        return do_until([this] { return is_buffer_full() || is_end_of_stream(); }, [this, timeout] {
            return _rd.fill_buffer(timeout).then([this] {
                while (!_rd.is_buffer_empty()) {
                    auto mf = _rd.pop_mutation_fragment();
                    if (mf.is_partition_start()) {
                        auto& dk = mf.as_partition_start().key();
                        if (!_filter(dk)) {
                            _rd.next_partition();
                            continue;
                        }
                    }
                    push_mutation_fragment(std::move(mf));
                }
                _end_of_stream = _rd.is_end_of_stream();
            });
        });
    }
    virtual void next_partition() override {
        clear_buffer_to_next_partition();
        if (is_buffer_empty()) {
            _end_of_stream = false;
            _rd.next_partition();
        }
    }
    virtual future<> fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout) override {
        clear_buffer();
        _end_of_stream = false;
        return _rd.fast_forward_to(pr, timeout);
    }
    virtual future<> fast_forward_to(position_range pr, db::timeout_clock::time_point timeout) override {
        forward_buffer_to(pr.start());
        _end_of_stream = false;
        return _rd.fast_forward_to(std::move(pr), timeout);
    }
    virtual size_t buffer_size() const override {
        return flat_mutation_reader::impl::buffer_size() + _rd.buffer_size();
    }
};

// Creates a mutation_reader wrapper which creates a new stream of mutations
// with some mutations removed from the original stream.
// MutationFilter is a callable which decides which mutations are dropped. It
// accepts mutation const& and returns a bool. The mutation stays in the
// stream if and only if the filter returns true.
template <typename MutationFilter>
flat_mutation_reader make_filtering_reader(flat_mutation_reader rd, MutationFilter&& filter) {
    return make_flat_mutation_reader<filtering_reader<MutationFilter>>(std::move(rd), std::forward<MutationFilter>(filter));
}

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
// The reader returns mutations having all the same schema, the one passed
// when invoking the source.
class mutation_source {
    using partition_range = const dht::partition_range&;
    using io_priority = const io_priority_class&;
    using flat_reader_factory_type = std::function<flat_mutation_reader(schema_ptr,
                                                                        partition_range,
                                                                        const query::partition_slice&,
                                                                        io_priority,
                                                                        tracing::trace_state_ptr,
                                                                        streamed_mutation::forwarding,
                                                                        mutation_reader::forwarding,
                                                                        reader_resource_tracker)>;
    // We could have our own version of std::function<> that is nothrow
    // move constructible and save some indirection and allocation.
    // Probably not worth the effort though.
    lw_shared_ptr<flat_reader_factory_type> _fn;
    lw_shared_ptr<std::function<partition_presence_checker()>> _presence_checker_factory;
private:
    mutation_source() = default;
    explicit operator bool() const { return bool(_fn); }
    friend class optimized_optional<mutation_source>;
public:
    mutation_source(flat_reader_factory_type fn, std::function<partition_presence_checker()> pcf = [] { return make_default_partition_presence_checker(); })
        : _fn(make_lw_shared(std::move(fn)))
        , _presence_checker_factory(make_lw_shared(std::move(pcf)))
    { }

    mutation_source(std::function<flat_mutation_reader(schema_ptr, partition_range, const query::partition_slice&, io_priority,
                tracing::trace_state_ptr, streamed_mutation::forwarding, mutation_reader::forwarding)> fn,
            std::function<partition_presence_checker()> pcf = [] { return make_default_partition_presence_checker(); })
        : mutation_source([fn = std::move(fn)] (schema_ptr s,
                    partition_range range,
                    const query::partition_slice& slice,
                    io_priority pc,
                    tracing::trace_state_ptr tr,
                    streamed_mutation::forwarding fwd,
                    mutation_reader::forwarding fwd_mr,
                    reader_resource_tracker) {
            return fn(s, range, slice, pc, std::move(tr), fwd, fwd_mr);
        }
        , std::move(pcf))
    { }

    // For sources which don't care about the mutation_reader::forwarding flag (always fast forwardable)
    mutation_source(std::function<flat_mutation_reader(schema_ptr, partition_range, const query::partition_slice&, io_priority,
                tracing::trace_state_ptr, streamed_mutation::forwarding)> fn)
        : mutation_source([fn = std::move(fn)] (schema_ptr s,
                    partition_range range,
                    const query::partition_slice& slice,
                    io_priority pc,
                    tracing::trace_state_ptr tr,
                    streamed_mutation::forwarding fwd,
                    mutation_reader::forwarding,
                    reader_resource_tracker) {
        return fn(s, range, slice, pc, std::move(tr), fwd);
    }) {}
    mutation_source(std::function<flat_mutation_reader(schema_ptr, partition_range, const query::partition_slice&, io_priority)> fn)
        : mutation_source([fn = std::move(fn)] (schema_ptr s,
                    partition_range range,
                    const query::partition_slice& slice,
                    io_priority pc,
                    tracing::trace_state_ptr,
                    streamed_mutation::forwarding fwd,
                    mutation_reader::forwarding,
                    reader_resource_tracker) {
        assert(!fwd);
        return fn(s, range, slice, pc);
    }) {}
    mutation_source(std::function<flat_mutation_reader(schema_ptr, partition_range, const query::partition_slice&)> fn)
        : mutation_source([fn = std::move(fn)] (schema_ptr s,
                    partition_range range,
                    const query::partition_slice& slice,
                    io_priority,
                    tracing::trace_state_ptr,
                    streamed_mutation::forwarding fwd,
                    mutation_reader::forwarding,
                    reader_resource_tracker) {
        assert(!fwd);
        return fn(s, range, slice);
    }) {}
    mutation_source(std::function<flat_mutation_reader(schema_ptr, partition_range range)> fn)
        : mutation_source([fn = std::move(fn)] (schema_ptr s,
                    partition_range range,
                    const query::partition_slice&,
                    io_priority,
                    tracing::trace_state_ptr,
                    streamed_mutation::forwarding fwd,
                    mutation_reader::forwarding,
                    reader_resource_tracker) {
        assert(!fwd);
        return fn(s, range);
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
        partition_range range,
        const query::partition_slice& slice,
        io_priority pc = default_priority_class(),
        tracing::trace_state_ptr trace_state = nullptr,
        streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no,
        mutation_reader::forwarding fwd_mr = mutation_reader::forwarding::yes,
        reader_resource_tracker tracker = no_resource_tracking()) const
    {
        return (*_fn)(std::move(s), range, slice, pc, std::move(trace_state), fwd, fwd_mr, tracker);
    }

    flat_mutation_reader
    make_reader(
        schema_ptr s,
        partition_range range = query::full_partition_range) const
    {
        auto& full_slice = s->full_slice();
        return this->make_reader(std::move(s), range, full_slice);
    }

    partition_presence_checker make_partition_presence_checker() {
        return (*_presence_checker_factory)();
    }
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

// Creates a restricted reader whose resource usages will be tracked
// during it's lifetime. If there are not enough resources (dues to
// existing readers) to create the new reader, it's construction will
// be deferred until there are sufficient resources.
// The internal reader once created will not be hindered in it's work
// anymore. Reusorce limits are determined by the config which contains
// a semaphore to track and limit the memory usage of readers. It also
// contains a timeout and a maximum queue size for inactive readers
// whose construction is blocked.
flat_mutation_reader make_restricted_flat_reader(reader_concurrency_semaphore& semaphore,
        mutation_source ms,
        schema_ptr s,
        const dht::partition_range& range,
        const query::partition_slice& slice,
        const io_priority_class& pc = default_priority_class(),
        tracing::trace_state_ptr trace_state = nullptr,
        streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no,
        mutation_reader::forwarding fwd_mr = mutation_reader::forwarding::yes);

inline flat_mutation_reader make_restricted_flat_reader(reader_concurrency_semaphore& semaphore,
                                              mutation_source ms,
                                              schema_ptr s,
                                              const dht::partition_range& range = query::full_partition_range) {
    auto& full_slice = s->full_slice();
    return make_restricted_flat_reader(semaphore, std::move(ms), std::move(s), range, full_slice);
}

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
