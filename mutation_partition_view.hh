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

#include "database_fwd.hh"
#include "mutation_partition_visitor.hh"
#include "utils/input_stream.hh"

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
    virtual void accept_row_tombstone(range_tombstone rt) = 0;
    virtual void accept_row(position_in_partition_view pipv, row_tombstone rt, row_marker rm, is_dummy, is_continuous) = 0;
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
public:
    static mutation_partition_view from_stream(utils::input_stream v) {
        return { v };
    }
    static mutation_partition_view from_view(ser::mutation_partition_view v);
    void accept(const schema& schema, partition_builder& visitor) const;
    void accept(const column_mapping&, converting_mutation_partition_applier& visitor) const;
    void accept(const column_mapping&, mutation_partition_view_virtual_visitor& mpvvv) const;

    std::optional<clustering_key> first_row_key() const;
    std::optional<clustering_key> last_row_key() const;
};
