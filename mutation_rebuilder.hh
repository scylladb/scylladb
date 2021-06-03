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

class mutation_rebuilder {
    mutation _m;

public:
    mutation_rebuilder(dht::decorated_key dk, schema_ptr s)
        : _m(std::move(s), std::move(dk)) {
    }

    stop_iteration consume(tombstone t) {
        _m.partition().apply(t);
        return stop_iteration::no;
    }

    stop_iteration consume(range_tombstone&& rt) {
        _m.partition().apply_row_tombstone(*_m.schema(), std::move(rt));
        return stop_iteration::no;
    }

    stop_iteration consume(static_row&& sr) {
        _m.partition().static_row().apply(*_m.schema(), column_kind::static_column, std::move(sr.cells()));
        return stop_iteration::no;
    }

    stop_iteration consume(clustering_row&& cr) {
        auto& dr = _m.partition().clustered_row(*_m.schema(), std::move(cr.key()));
        dr.apply(cr.tomb());
        dr.apply(cr.marker());
        dr.cells().apply(*_m.schema(), column_kind::regular_column, std::move(cr.cells()));
        return stop_iteration::no;
    }

    mutation_opt consume_end_of_stream() {
        return mutation_opt(std::move(_m));
    }
};
