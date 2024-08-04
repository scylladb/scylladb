/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "utils/assert.hh"
#include "mutation.hh"
#include "range_tombstone_assembler.hh"

class mutation_rebuilder {
    schema_ptr _s;
    mutation_opt _m;

public:
    explicit mutation_rebuilder(schema_ptr s) : _s(std::move(s)) { }

    // Returned reference is valid until consume_end_of_stream() or flush() is called.
    const mutation& consume_new_partition(const dht::decorated_key& dk) {
        SCYLLA_ASSERT(!_m);
        _m = mutation(_s, dk);
        return *_m;
    }

    stop_iteration consume(tombstone t) {
        SCYLLA_ASSERT(_m);
        _m->partition().apply(t);
        return stop_iteration::no;
    }

    stop_iteration consume(range_tombstone&& rt) {
        SCYLLA_ASSERT(_m);
        _m->partition().apply_row_tombstone(*_s, std::move(rt));
        return stop_iteration::no;
    }

    stop_iteration consume(static_row&& sr) {
        SCYLLA_ASSERT(_m);
        _m->partition().static_row().apply(*_s, column_kind::static_column, std::move(sr.cells()));
        return stop_iteration::no;
    }

    stop_iteration consume(clustering_row&& cr) {
        SCYLLA_ASSERT(_m);
        auto& dr = _m->partition().clustered_row(*_s, std::move(cr.key()));
        dr.apply(cr.tomb());
        dr.apply(cr.marker());
        dr.cells().apply(*_s, column_kind::regular_column, std::move(cr.cells()));
        return stop_iteration::no;
    }

    stop_iteration consume_end_of_partition() {
        SCYLLA_ASSERT(_m);
        return stop_iteration::yes;
    }

    // Might only be called between consume_new_partition()
    // and consume_end_of_partition().
    //
    // Returns (and forgets) the partition contents consumed so far.
    // Can be used to split the processing of a large mutation into
    // multiple smaller `mutation` objects (which add up to the full mutation).
    mutation flush() {
        SCYLLA_ASSERT(_m);
        return std::exchange(*_m, mutation(_s, _m->decorated_key()));
    }

    mutation_opt consume_end_of_stream() {
        return std::move(_m);
    }
};

// Builds the mutation corresponding to the next partition in the mutation fragment stream.
// Implements FlattenedConsumerV2, MutationFragmentConsumerV2 and FlatMutationReaderConsumerV2.
// Does not work with streams in streamed_mutation::forwarding::yes mode.
class mutation_rebuilder_v2 {
    schema_ptr _s;
    mutation_rebuilder _builder;
    range_tombstone_assembler _rt_assembler;
    position_in_partition _pos = position_in_partition::before_all_clustered_rows();
public:
    mutation_rebuilder_v2(schema_ptr s) : _s(std::move(s)), _builder(_s) { }
public:
    stop_iteration consume(partition_start mf) {
        consume_new_partition(mf.key());
        return consume(mf.partition_tombstone());
    }
    stop_iteration consume(partition_end) {
        return consume_end_of_partition();
    }
    stop_iteration consume(mutation_fragment_v2&& mf) {
        return std::move(mf).consume(*this);
    }
public:
    // Returned reference is valid until consume_end_of_stream() or flush() is called.
    const mutation& consume_new_partition(const dht::decorated_key& dk) {
        return _builder.consume_new_partition(dk);
    }

    stop_iteration consume(tombstone t) {
        _builder.consume(t);
        return stop_iteration::no;
    }

    stop_iteration consume(range_tombstone_change&& rt) {
        _pos = rt.position();
        if (auto rt_opt = _rt_assembler.consume(*_s, std::move(rt))) {
            _builder.consume(std::move(*rt_opt));
        }
        return stop_iteration::no;
    }

    stop_iteration consume(static_row&& sr) {
        _builder.consume(std::move(sr));
        return stop_iteration::no;
    }

    stop_iteration consume(clustering_row&& cr) {
        _pos = position_in_partition::after_key(*_s, cr.position());
        _builder.consume(std::move(cr));
        return stop_iteration::no;
    }

    stop_iteration consume_end_of_partition() {
        _rt_assembler.on_end_of_stream();
        return stop_iteration::yes;
    }

    mutation_opt consume_end_of_stream() {
        _rt_assembler.on_end_of_stream();
        return _builder.consume_end_of_stream();
    }

    // Might only be called between consume_new_partition()
    // and consume_end_of_partition().
    //
    // Returns (and forgets) the partition contents consumed so far.
    // Can be used to split the processing of a large mutation into
    // multiple smaller `mutation` objects (which add up to the full mutation).
    //
    // The active range tombstone (if present) is flushed with end bound
    // just after the last seen clustered position, but the range tombstone
    // remains active, and the next mutation will see it restarted at the
    // position it was flushed at.
    mutation flush() {
        if (auto rt_opt = _rt_assembler.flush(*_s, _pos)) {
            _builder.consume(std::move(*rt_opt));
        }
        return _builder.flush();
    }
};
