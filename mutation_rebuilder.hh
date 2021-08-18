/*
 * Copyright (C) 2018-present ScyllaDB
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

#include "mutation.hh"
#include "range_tombstone_assembler.hh"

class mutation_rebuilder {
    schema_ptr _s;
    mutation_opt _m;

public:
    explicit mutation_rebuilder(schema_ptr s) : _s(std::move(s)) { }

    void consume_new_partition(const dht::decorated_key& dk) {
        assert(!_m);
        _m = mutation(_s, std::move(dk));
    }

    stop_iteration consume(tombstone t) {
        assert(_m);
        _m->partition().apply(t);
        return stop_iteration::no;
    }

    stop_iteration consume(range_tombstone&& rt) {
        assert(_m);
        _m->partition().apply_row_tombstone(*_s, std::move(rt));
        return stop_iteration::no;
    }

    stop_iteration consume(static_row&& sr) {
        assert(_m);
        _m->partition().static_row().apply(*_s, column_kind::static_column, std::move(sr.cells()));
        return stop_iteration::no;
    }

    stop_iteration consume(clustering_row&& cr) {
        assert(_m);
        auto& dr = _m->partition().clustered_row(*_s, std::move(cr.key()));
        dr.apply(cr.tomb());
        dr.apply(cr.marker());
        dr.cells().apply(*_s, column_kind::regular_column, std::move(cr.cells()));
        return stop_iteration::no;
    }

    stop_iteration consume_end_of_partition() {
        assert(_m);
        return stop_iteration::yes;
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
    void consume_new_partition(const dht::decorated_key& dk) {
        _builder.consume_new_partition(dk);
    }

    stop_iteration consume(tombstone t) {
        _builder.consume(t);
        return stop_iteration::no;
    }

    stop_iteration consume(range_tombstone_change&& rt) {
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
};
