/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "utils/assert.hh"
#include "query-request.hh"
#include "tracing/trace_state.hh"
#include "readers/mutation_reader_fwd.hh"
#include "readers/mutation_reader.hh"
#include "readers/mutation_fragment_v1_stream.hh"

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
// table's schema), and a reverse (native) slice.
// See docs/dev/reverse-reads.md for more details.
// Partition-range forwarding is not yet supported in reverse mode.
class mutation_source {
    using partition_range = const dht::partition_range&;
    using flat_reader_v2_factory_type = std::function<mutation_reader(schema_ptr,
                                                                        reader_permit,
                                                                        partition_range,
                                                                        const query::partition_slice&,
                                                                        tracing::trace_state_ptr,
                                                                        streamed_mutation::forwarding,
                                                                        mutation_reader::forwarding)>;
    // We could have our own version of std::function<> that is nothrow
    // move constructible and save some indirection and allocation.
    // Probably not worth the effort though.
    lw_shared_ptr<flat_reader_v2_factory_type> _fn;
    lw_shared_ptr<std::function<partition_presence_checker()>> _presence_checker_factory;
private:
    mutation_source() = default;
    explicit operator bool() const { return bool(_fn); }
    friend class optimized_optional<mutation_source>;
public:
    mutation_source(flat_reader_v2_factory_type fn, std::function<partition_presence_checker()> pcf = [] { return make_default_partition_presence_checker(); })
        : _fn(make_lw_shared<flat_reader_v2_factory_type>(std::move(fn)))
        , _presence_checker_factory(make_lw_shared<std::function<partition_presence_checker()>>(std::move(pcf)))
    { }

    mutation_source(std::function<mutation_reader(schema_ptr, reader_permit, partition_range, const query::partition_slice&,
                tracing::trace_state_ptr, streamed_mutation::forwarding)> fn)
        : mutation_source([fn = std::move(fn)] (schema_ptr s,
                    reader_permit permit,
                    partition_range range,
                    const query::partition_slice& slice,
                    tracing::trace_state_ptr tr,
                    streamed_mutation::forwarding fwd,
                    mutation_reader::forwarding) {
        return fn(std::move(s), std::move(permit), range, slice, std::move(tr), fwd);
    }) {}
    mutation_source(std::function<mutation_reader(schema_ptr, reader_permit, partition_range, const query::partition_slice&)> fn)
        : mutation_source([fn = std::move(fn)] (schema_ptr s,
                    reader_permit permit,
                    partition_range range,
                    const query::partition_slice& slice,
                    tracing::trace_state_ptr,
                    streamed_mutation::forwarding fwd,
                    mutation_reader::forwarding) {
        SCYLLA_ASSERT(!fwd);
        return fn(std::move(s), std::move(permit), range, slice);
    }) {}
    mutation_source(std::function<mutation_reader(schema_ptr, reader_permit, partition_range range)> fn)
        : mutation_source([fn = std::move(fn)] (schema_ptr s,
                    reader_permit permit,
                    partition_range range,
                    const query::partition_slice&,
                    tracing::trace_state_ptr,
                    streamed_mutation::forwarding fwd,
                    mutation_reader::forwarding) {
        SCYLLA_ASSERT(!fwd);
        return fn(std::move(s), std::move(permit), range);
    }) {}

    mutation_source(const mutation_source& other) = default;
    mutation_source& operator=(const mutation_source& other) = default;
    mutation_source(mutation_source&&) = default;
    mutation_source& operator=(mutation_source&&) = default;

    mutation_fragment_v1_stream
    make_fragment_v1_stream(
        schema_ptr s,
        reader_permit permit,
        partition_range range,
        const query::partition_slice& slice,
        tracing::trace_state_ptr trace_state = nullptr,
        streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no,
        mutation_reader::forwarding fwd_mr = mutation_reader::forwarding::yes) const
    {
        return mutation_fragment_v1_stream(
                    (*_fn)(std::move(s), std::move(permit), range, slice, std::move(trace_state), fwd, fwd_mr));
    }

    mutation_fragment_v1_stream
    make_fragment_v1_stream(
        schema_ptr s,
        reader_permit permit,
        partition_range range = query::full_partition_range) const
    {
        auto& full_slice = s->full_slice();
        return this->make_fragment_v1_stream(std::move(s), std::move(permit), range, full_slice);
    }

    // Creates a new reader.
    //
    // All parameters captured by reference must remain live as long as returned
    // mutation_reader or streamed_mutation obtained through it are alive.
    mutation_reader
    make_reader_v2(
            schema_ptr s,
            reader_permit permit,
            partition_range range,
            const query::partition_slice& slice,
            tracing::trace_state_ptr trace_state = nullptr,
            streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no,
            mutation_reader::forwarding fwd_mr = mutation_reader::forwarding::yes) const
    {
        return (*_fn)(std::move(s), std::move(permit), range, slice, std::move(trace_state), fwd, fwd_mr);
    }

    mutation_reader
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
