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

// A mutation_reader is an object which allows iterating on mutations: invoke
// the function to get a future for the next mutation, with an unset optional
// marking the end of iteration. After calling mutation_reader's operator(),
// caller must keep the object alive until the returned future is fulfilled.
//
// streamed_mutation object emitted by mutation_reader remains valid after the
// destruction of the mutation_reader.
//
// Asking mutation_reader for another streamed_mutation (i.e. invoking
// mutation_reader::operator()) invalidates all streamed_mutation objects
// previously produced by that reader.
//
// The mutations returned have strictly monotonically increasing keys. Two
// consecutive mutations never have equal keys.
//
// TODO: When iterating over mutations, we don't need a schema_ptr for every
// single one as it is normally the same for all of them. So "mutation" might
// not be the optimal object to use here.
class mutation_reader final {
public:
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

    class impl {
    public:
        virtual ~impl() {}
        virtual future<streamed_mutation_opt> operator()() = 0;
        virtual future<> fast_forward_to(const dht::partition_range&) {
            throw std::bad_function_call();
        }
    };
private:
    class null_impl final : public impl {
    public:
        virtual future<streamed_mutation_opt> operator()() override { throw std::bad_function_call(); }
    };
private:
    std::unique_ptr<impl> _impl;
public:
    mutation_reader(std::unique_ptr<impl> impl) noexcept : _impl(std::move(impl)) {}
    mutation_reader() : mutation_reader(std::make_unique<null_impl>()) {}
    mutation_reader(mutation_reader&&) = default;
    mutation_reader(const mutation_reader&) = delete;
    mutation_reader& operator=(mutation_reader&&) = default;
    mutation_reader& operator=(const mutation_reader&) = delete;
    future<streamed_mutation_opt> operator()() { return _impl->operator()(); }

    // Changes the range of partitions to pr. The range can only be moved
    // forwards. pr.begin() needs to be larger than pr.end() of the previousl
    // used range (i.e. either the initial one passed to the constructor or a
    // previous fast forward target).
    // pr needs to be valid until the reader is destroyed or fast_forward_to()
    // is called again.
    future<> fast_forward_to(const dht::partition_range& pr) { return _impl->fast_forward_to(pr); }
};

// Impl: derived from mutation_reader::impl; Args/args: arguments for Impl's constructor
template <typename Impl, typename... Args>
inline
mutation_reader
make_mutation_reader(Args&&... args) {
    return mutation_reader(std::make_unique<Impl>(std::forward<Args>(args)...));
}

class reader_selector {
protected:
    dht::token _selector_position;
public:
    virtual ~reader_selector() = default;
    // Call only if has_new_readers() returned true.
    virtual std::vector<mutation_reader> create_new_readers(const dht::token* const t) = 0;
    virtual std::vector<mutation_reader> fast_forward_to(const dht::partition_range& pr) = 0;

    // Can be false-positive but never false-negative!
    bool has_new_readers(const dht::token* const t) const noexcept {
        return !_selector_position.is_maximum() && (!t || *t >= _selector_position);
    }
};

// Combines multiple mutation_readers into one.
class combined_mutation_reader : public mutation_reader::impl {
    std::unique_ptr<reader_selector> _selector;
    std::list<mutation_reader> _all_readers;

    struct mutation_and_reader {
        streamed_mutation m;
        mutation_reader* read;

        bool operator<(const mutation_and_reader& other) const {
            return read < other.read;
        }

        struct less_compare {
            bool operator()(const mutation_and_reader& a, mutation_reader* b) const {
                return a.read < b;
            }
            bool operator()(mutation_reader* a, const mutation_and_reader& b) const {
                return a < b.read;
            }
            bool operator()(const mutation_and_reader& a, const mutation_and_reader& b) const {
                return a < b;
            }
        };
    };
    std::vector<mutation_and_reader> _ptables;
    // comparison function for std::make_heap()/std::push_heap()
    static bool heap_compare(const mutation_and_reader& a, const mutation_and_reader& b) {
        auto&& s = a.m.schema();
        // order of comparison is inverted, because heaps produce greatest value first
        return b.m.decorated_key().less_compare(*s, a.m.decorated_key());
    }
    std::vector<streamed_mutation> _current;
    std::vector<mutation_reader*> _next;
    mutation_reader::forwarding _fwd_mr;
private:
    const dht::token* current_position() const;
    void maybe_add_readers(const dht::token* const t);
    void add_readers(std::vector<mutation_reader> new_readers);
    future<> prepare_next();
    // Produces next mutation or disengaged optional if there are no more.
    future<streamed_mutation_opt> next();
public:
    // The specified mutation_reader::forwarding tag must be the same for all included readers.
    combined_mutation_reader(std::unique_ptr<reader_selector> selector, mutation_reader::forwarding fwd_mr);
    virtual future<streamed_mutation_opt> operator()() override;
    virtual future<> fast_forward_to(const dht::partition_range& pr) override;
};

// Creates a mutation reader which combines data return by supplied readers.
// Returns mutation of the same schema only when all readers return mutations
// of the same schema.
mutation_reader make_combined_reader(std::vector<mutation_reader>, mutation_reader::forwarding);
mutation_reader make_combined_reader(mutation_reader&& a, mutation_reader&& b, mutation_reader::forwarding fwd_mr = mutation_reader::forwarding::yes);
// reads from the input readers, in order
mutation_reader make_reader_returning(mutation, streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no);
mutation_reader make_reader_returning(streamed_mutation);
mutation_reader make_reader_returning_many(std::vector<mutation>,
    const query::partition_slice& slice,
    streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no);
mutation_reader make_reader_returning_many(std::vector<mutation>, const dht::partition_range& = query::full_partition_range);
mutation_reader make_reader_returning_many(std::vector<streamed_mutation>);
mutation_reader make_empty_reader();

/*
template<typename T>
concept bool StreamedMutationFilter() {
    return requires(T t, const streamed_mutation& sm) {
        { t(sm) } -> bool;
    };
}
*/
template <typename MutationFilter>
class filtering_reader : public mutation_reader::impl {
    mutation_reader _rd;
    MutationFilter _filter;
    streamed_mutation_opt _current;
    static_assert(std::is_same<bool, std::result_of_t<MutationFilter(const streamed_mutation&)>>::value, "bad MutationFilter signature");
public:
    filtering_reader(mutation_reader rd, MutationFilter&& filter)
            : _rd(std::move(rd)), _filter(std::forward<MutationFilter>(filter)) {
    }
    virtual future<streamed_mutation_opt> operator()() override {\
        return repeat([this] {
            return _rd().then([this] (streamed_mutation_opt&& mo) mutable {
                if (!mo) {
                    _current = std::move(mo);
                    return stop_iteration::yes;
                } else {
                    if (_filter(*mo)) {
                        _current = std::move(mo);
                        return stop_iteration::yes;
                    }
                    return stop_iteration::no;
                }
            });
        }).then([this] {
            return make_ready_future<streamed_mutation_opt>(std::move(_current));
        });
    };
    virtual future<> fast_forward_to(const dht::partition_range& pr) override {
        return _rd.fast_forward_to(pr);
    }
};

// Creates a mutation_reader wrapper which creates a new stream of mutations
// with some mutations removed from the original stream.
// MutationFilter is a callable which decides which mutations are dropped. It
// accepts mutation const& and returns a bool. The mutation stays in the
// stream if and only if the filter returns true.
template <typename MutationFilter>
mutation_reader make_filtering_reader(mutation_reader rd, MutationFilter&& filter) {
    return make_mutation_reader<filtering_reader<MutationFilter>>(std::move(rd), std::forward<MutationFilter>(filter));
}

// Calls the consumer for each element of the reader's stream until end of stream
// is reached or the consumer requests iteration to stop by returning stop_iteration::yes.
// The consumer should accept mutation as the argument and return stop_iteration.
// The returned future<> resolves when consumption ends.
template <typename Consumer>
inline
future<> consume(mutation_reader& reader, Consumer consumer) {
    static_assert(std::is_same<future<stop_iteration>, futurize_t<std::result_of_t<Consumer(mutation&&)>>>::value, "bad Consumer signature");
    using futurator = futurize<std::result_of_t<Consumer(mutation&&)>>;

    return do_with(std::move(consumer), [&reader] (Consumer& c) -> future<> {
        return repeat([&reader, &c] () {
            return reader().then([] (auto sm) {
                return mutation_from_streamed_mutation(std::move(sm));
            }).then([&c] (mutation_opt&& mo) -> future<stop_iteration> {
                if (!mo) {
                    return make_ready_future<stop_iteration>(stop_iteration::yes);
                }
                return futurator::apply(c, std::move(*mo));
            });
        });
    });
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

mutation_reader mutation_reader_from_flat_mutation_reader(flat_mutation_reader&&);

// mutation_source represents source of data in mutation form. The data source
// can be queried multiple times and in parallel. For each query it returns
// independent mutation_reader.
// The reader returns mutations having all the same schema, the one passed
// when invoking the source.
class mutation_source {
    using partition_range = const dht::partition_range&;
    using io_priority = const io_priority_class&;
    using func_type = std::function<mutation_reader(schema_ptr,
        partition_range,
        const query::partition_slice&,
        io_priority,
        tracing::trace_state_ptr,
        streamed_mutation::forwarding,
        mutation_reader::forwarding
    )>;
    using flat_reader_factory_type = std::function<flat_mutation_reader(schema_ptr,
                                                                        partition_range,
                                                                        const query::partition_slice&,
                                                                        io_priority,
                                                                        tracing::trace_state_ptr,
                                                                        streamed_mutation::forwarding,
                                                                        mutation_reader::forwarding)>;
    class impl {
    public:
        virtual ~impl() { }
        virtual mutation_reader make_mutation_reader(schema_ptr s,
                                                     partition_range range,
                                                     const query::partition_slice& slice,
                                                     io_priority pc,
                                                     tracing::trace_state_ptr trace_state,
                                                     streamed_mutation::forwarding fwd,
                                                     mutation_reader::forwarding fwd_mr) = 0;
        virtual flat_mutation_reader make_flat_mutation_reader(schema_ptr s,
                                                               partition_range range,
                                                               const query::partition_slice& slice,
                                                               io_priority pc,
                                                               tracing::trace_state_ptr trace_state,
                                                               streamed_mutation::forwarding fwd,
                                                               mutation_reader::forwarding fwd_mr) = 0;
    };
    class mutation_reader_mutation_source : public impl {
        func_type _fn;
    public:
        mutation_reader_mutation_source(func_type&& fn) : _fn(std::move(fn)) { }
        virtual mutation_reader make_mutation_reader(schema_ptr s,
                                                     partition_range range,
                                                     const query::partition_slice& slice,
                                                     io_priority pc,
                                                     tracing::trace_state_ptr trace_state,
                                                     streamed_mutation::forwarding fwd,
                                                     mutation_reader::forwarding fwd_mr) override {
            return _fn(std::move(s), range, slice, pc, std::move(trace_state), fwd, fwd_mr);
        }
        virtual flat_mutation_reader make_flat_mutation_reader(schema_ptr s,
                                                               partition_range range,
                                                               const query::partition_slice& slice,
                                                               io_priority pc,
                                                               tracing::trace_state_ptr trace_state,
                                                               streamed_mutation::forwarding fwd,
                                                               mutation_reader::forwarding fwd_mr) override {
            return flat_mutation_reader_from_mutation_reader(s,
                                                             _fn(s, range, slice, pc, std::move(trace_state), fwd, fwd_mr),
                                                             fwd);
        }
    };
    class flat_mutation_reader_mutation_source : public impl {
        flat_reader_factory_type _fn;
    public:
        flat_mutation_reader_mutation_source(flat_reader_factory_type&& fn) : _fn(std::move(fn)) { }
        virtual mutation_reader make_mutation_reader(schema_ptr s,
                                                     partition_range range,
                                                     const query::partition_slice& slice,
                                                     io_priority pc,
                                                     tracing::trace_state_ptr trace_state,
                                                     streamed_mutation::forwarding fwd,
                                                     mutation_reader::forwarding fwd_mr) override {
            return mutation_reader_from_flat_mutation_reader(_fn(std::move(s), range, slice, pc, std::move(trace_state), fwd, fwd_mr));
        }
        virtual flat_mutation_reader make_flat_mutation_reader(schema_ptr s,
                                                               partition_range range,
                                                               const query::partition_slice& slice,
                                                               io_priority pc,
                                                               tracing::trace_state_ptr trace_state,
                                                               streamed_mutation::forwarding fwd,
                                                               mutation_reader::forwarding fwd_mr) override {
            return _fn(std::move(s), range, slice, pc, std::move(trace_state), fwd, fwd_mr);
        }
    };
    // We could have our own version of std::function<> that is nothrow
    // move constructible and save some indirection and allocation.
    // Probably not worth the effort though.
    shared_ptr<impl> _impl;
    lw_shared_ptr<std::function<partition_presence_checker()>> _presence_checker_factory;
private:
    mutation_source() = default;
    explicit operator bool() const { return bool(_impl); }
    friend class optimized_optional<mutation_source>;
public:
    mutation_source(func_type fn, std::function<partition_presence_checker()> pcf = [] { return make_default_partition_presence_checker(); })
        : _impl(seastar::make_shared<mutation_reader_mutation_source>(std::move(fn)))
        , _presence_checker_factory(make_lw_shared(std::move(pcf)))
    { }
    mutation_source(flat_reader_factory_type fn, std::function<partition_presence_checker()> pcf = [] { return make_default_partition_presence_checker(); })
        : _impl(seastar::make_shared<flat_mutation_reader_mutation_source>(std::move(fn)))
        , _presence_checker_factory(make_lw_shared(std::move(pcf)))
    { }
    // For sources which don't care about the mutation_reader::forwarding flag (always fast forwardable)
    mutation_source(std::function<mutation_reader(schema_ptr s, partition_range range, const query::partition_slice& slice, io_priority pc, tracing::trace_state_ptr, streamed_mutation::forwarding)> fn)
        : mutation_source([fn = std::move(fn)] (schema_ptr s, partition_range range, const query::partition_slice& slice, io_priority pc, tracing::trace_state_ptr tr, streamed_mutation::forwarding fwd, mutation_reader::forwarding) {
            return fn(s, range, slice, pc, std::move(tr), fwd);
        }) {}
    mutation_source(std::function<mutation_reader(schema_ptr, partition_range, const query::partition_slice&, io_priority)> fn)
        : mutation_source([fn = std::move(fn)] (schema_ptr s, partition_range range, const query::partition_slice& slice, io_priority pc, tracing::trace_state_ptr, streamed_mutation::forwarding fwd, mutation_reader::forwarding) {
            assert(!fwd);
            return fn(s, range, slice, pc);
        }) {}
    mutation_source(std::function<mutation_reader(schema_ptr, partition_range, const query::partition_slice&)> fn)
        : mutation_source([fn = std::move(fn)] (schema_ptr s, partition_range range, const query::partition_slice& slice, io_priority, tracing::trace_state_ptr, streamed_mutation::forwarding fwd, mutation_reader::forwarding) {
            assert(!fwd);
            return fn(s, range, slice);
        }) {}
    mutation_source(std::function<mutation_reader(schema_ptr, partition_range range)> fn)
        : mutation_source([fn = std::move(fn)] (schema_ptr s, partition_range range, const query::partition_slice&, io_priority, tracing::trace_state_ptr, streamed_mutation::forwarding fwd, mutation_reader::forwarding) {
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
    mutation_reader operator()(schema_ptr s,
        partition_range range,
        const query::partition_slice& slice,
        io_priority pc = default_priority_class(),
        tracing::trace_state_ptr trace_state = nullptr,
        streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no,
        mutation_reader::forwarding fwd_mr = mutation_reader::forwarding::yes) const
    {
        return _impl->make_mutation_reader(std::move(s), range, slice, pc, std::move(trace_state), fwd, fwd_mr);
    }

    mutation_reader operator()(schema_ptr s, partition_range range = query::full_partition_range) const {
        auto& full_slice = s->full_slice();
        return (*this)(std::move(s), range, full_slice);
    }

    flat_mutation_reader
    make_flat_mutation_reader(
        schema_ptr s,
        partition_range range,
        const query::partition_slice& slice,
        io_priority pc = default_priority_class(),
        tracing::trace_state_ptr trace_state = nullptr,
        streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no,
        mutation_reader::forwarding fwd_mr = mutation_reader::forwarding::yes) const
    {
        return _impl->make_flat_mutation_reader(std::move(s), range, slice, pc, std::move(trace_state), fwd, fwd_mr);
    }

    flat_mutation_reader
    make_flat_mutation_reader(
        schema_ptr s,
        partition_range range = query::full_partition_range) const
    {
        auto& full_slice = s->full_slice();
        return this->make_flat_mutation_reader(std::move(s), range, full_slice);
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

struct restricted_mutation_reader_config {
    semaphore* resources_sem = nullptr;
    uint64_t* active_reads = nullptr;
    std::chrono::nanoseconds timeout = {};
    size_t max_queue_length = std::numeric_limits<size_t>::max();
    std::function<void ()> raise_queue_overloaded_exception = default_raise_queue_overloaded_exception;

    static void default_raise_queue_overloaded_exception() {
        throw std::runtime_error("restricted mutation reader queue overload");
    }
};

// Creates a restricted reader whose resource usages will be tracked
// during it's lifetime. If there are not enough resources (dues to
// existing readers) to create the new reader, it's construction will
// be deferred until there are sufficient resources.
// The internal reader once created will not be hindered in it's work
// anymore. Reusorce limits are determined by the config which contains
// a semaphore to track and limit the memory usage of readers. It also
// contains a timeout and a maximum queue size for inactive readers
// whose construction is blocked.
mutation_reader make_restricted_reader(const restricted_mutation_reader_config& config,
        mutation_source ms,
        schema_ptr s,
        const dht::partition_range& range,
        const query::partition_slice& slice,
        const io_priority_class& pc = default_priority_class(),
        tracing::trace_state_ptr trace_state = nullptr,
        streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no,
        mutation_reader::forwarding fwd_mr = mutation_reader::forwarding::yes);

inline mutation_reader make_restricted_reader(const restricted_mutation_reader_config& config,
        mutation_source ms,
        schema_ptr s,
        const dht::partition_range& range = query::full_partition_range) {
    auto& full_slice = s->full_slice();
    return make_restricted_reader(config, std::move(ms), std::move(s), range, full_slice);
}

template<>
struct move_constructor_disengages<mutation_source> {
    enum { value = true };
};
using mutation_source_opt = optimized_optional<mutation_source>;

template<typename Consumer>
future<stop_iteration> do_consume_streamed_mutation_flattened(streamed_mutation& sm, Consumer& c)
{
    do {
        if (sm.is_buffer_empty()) {
            if (sm.is_end_of_stream()) {
                break;
            }
            auto f = sm.fill_buffer();
            if (!f.available()) {
                return f.then([&] { return do_consume_streamed_mutation_flattened(sm, c); });
            }
            f.get();
        } else {
            if (sm.pop_mutation_fragment().consume_streamed_mutation(c) == stop_iteration::yes) {
                break;
            }
        }
    } while (true);
    return make_ready_future<stop_iteration>(c.consume_end_of_partition());
}

/*
template<typename T>
concept bool StreamedMutationFilter() {
    return requires(T obj, const streamed_mutation& sm) {
        { filter(sm); } -> bool;
    };
}
*/
// This version of consume_flattened() must be run inside a thread and
// guarantees that all FlattenedConsumer functions will also be called in the same thread
// context.
template<typename FlattenedConsumer, typename StreamedMutationFilter>
auto consume_flattened_in_thread(mutation_reader& mr, FlattenedConsumer& c, StreamedMutationFilter&& filter)
{
    while (true) {
        auto smopt = mr().get0();
        if (!smopt) {
            break;
        }
        auto& sm = *smopt;
        if (!filter(sm)) {
            continue;
        }
        c.consume_new_partition(sm.decorated_key());
        if (sm.partition_tombstone()) {
            c.consume(sm.partition_tombstone());
        }
        do {
            if (sm.is_buffer_empty()) {
                if (sm.is_end_of_stream()) {
                    break;
                }
                sm.fill_buffer().get0();
            } else {
                if (sm.pop_mutation_fragment().consume_streamed_mutation(c) == stop_iteration::yes) {
                    break;
                }
            }
        } while (true);
        if (c.consume_end_of_partition() == stop_iteration::yes) {
            break;
        }
    }
    return c.consume_end_of_stream();
}

template<typename FlattenedConsumer>
auto consume_flattened_in_thread(mutation_reader& mr, FlattenedConsumer& c)
{
    return consume_flattened_in_thread(mr, c, [] (auto&&) { return true; });
}

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

future<streamed_mutation_opt> streamed_mutation_from_flat_mutation_reader(flat_mutation_reader&&);
