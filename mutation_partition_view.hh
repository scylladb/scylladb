/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "replica/database_fwd.hh"
#include "mutation_partition_visitor.hh"
#include "utils/input_stream.hh"
#include "atomic_cell.hh"
#include "idl/mutation.dist.hh"
#include "idl/mutation.dist.impl.hh"
#include "mutation_consumer.hh"

namespace ser {
class mutation_partition_view;
}

class partition_builder;
class converting_mutation_partition_applier;

template<typename T>
concept MutationViewVisitor = requires (T& visitor, tombstone t, atomic_cell ac,
                                             collection_mutation_view cmv, range_tombstone rt,
                                             position_in_partition_view pipv, row_tombstone row_tomb,
                                             row_marker rm) {
    visitor.accept_partition_tombstone(t);
    visitor.accept_static_cell(column_id(), std::move(ac));
    visitor.accept_static_cell(column_id(), cmv);
    visitor.accept_row_tombstone(rt);
    visitor.accept_row(pipv, row_tomb, rm,
            is_dummy::no, is_continuous::yes);
    visitor.accept_row_cell(column_id(), std::move(ac));
    visitor.accept_row_cell(column_id(), cmv);
};

class mutation_partition_view_virtual_visitor {
public:
    virtual ~mutation_partition_view_virtual_visitor();
    virtual void accept_partition_tombstone(tombstone t) = 0;
    virtual void accept_static_cell(column_id, atomic_cell ac) = 0;
    virtual void accept_static_cell(column_id, collection_mutation_view cmv) = 0;
    virtual stop_iteration accept_row_tombstone(range_tombstone rt) = 0;
    virtual stop_iteration accept_row(position_in_partition_view pipv, row_tombstone rt, row_marker rm, is_dummy, is_continuous) = 0;
    virtual void accept_row_cell(column_id, atomic_cell ac) = 0;
    virtual void accept_row_cell(column_id, collection_mutation_view cmv) = 0;
};

// View on serialized mutation partition. See mutation_partition_serializer.
class mutation_partition_view {
    utils::input_stream _in;
private:
    mutation_partition_view(utils::input_stream v)
        : _in(v)
    { }

    template<typename Visitor>
    requires MutationViewVisitor<Visitor>
    void do_accept(const column_mapping&, Visitor& visitor) const;

    template<typename Visitor>
    requires MutationViewVisitor<Visitor>
    future<> do_accept_gently(const column_mapping&, Visitor& visitor) const;

    template <consume_in_reverse reverse>
    struct accept_ordered_cookie {
        schema_ptr schema;
        bool accepted_partition_tombstone = false;
        bool accepted_static_row = false;

        // We can read the range_tombstone_list in reverse order in consume_in_reverse::yes mode
        // since we assume they are deoverlapped.
        using rts_type = ser::vector_deserializer<ser::range_tombstone_view, reverse == consume_in_reverse::no>;
        using rts_iterator_type = typename rts_type::const_iterator;
        using crs_type = ser::vector_deserializer<ser::deletable_row_view, reverse == consume_in_reverse::no>;
        using crs_iterator_type = typename crs_type::const_iterator;

        struct rts_crs_iterators {
            rts_type rts;
            rts_iterator_type rts_begin;
            rts_iterator_type rts_end;

            crs_type crs;
            crs_iterator_type crs_begin;
            crs_iterator_type crs_end;

            rts_crs_iterators(ser::mutation_partition_view& mpv);
        };
        std::optional<rts_crs_iterators> iterators;
    };

    template <consume_in_reverse reverse>
    struct accept_ordered_result {
        stop_iteration stop = stop_iteration::no;
        accept_ordered_cookie<reverse> cookie;
    };

    template <bool is_preemptible, consume_in_reverse reverse>
    accept_ordered_result<reverse> do_accept_ordered(const schema& schema, mutation_partition_view_virtual_visitor& mpvvv, accept_ordered_cookie<reverse> cookie) const;

public:
    static mutation_partition_view from_stream(utils::input_stream v) {
        return { v };
    }
    static mutation_partition_view from_view(ser::mutation_partition_view v);
    void accept(const schema& schema, partition_builder& visitor) const;
    future<> accept_gently(const schema& schema, partition_builder& visitor) const;
    void accept(const column_mapping&, converting_mutation_partition_applier& visitor) const;
    future<> accept_gently(const column_mapping&, converting_mutation_partition_applier& visitor) const;
    void accept(const column_mapping&, mutation_partition_view_virtual_visitor& mpvvv) const;
    void accept_ordered(const schema& schema, mutation_partition_view_virtual_visitor& mpvvv, consume_in_reverse reverse) const;
    future<> accept_gently_ordered(const schema&, mutation_partition_view_virtual_visitor& mpvvv, consume_in_reverse reverse) const;

    std::optional<clustering_key> first_row_key() const;
    std::optional<clustering_key> last_row_key() const;
};
