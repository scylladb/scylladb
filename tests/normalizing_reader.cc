/*
 * Copyright (C) 2018 ScyllaDB
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

#include "normalizing_reader.hh"
#include "core/future-util.hh"

normalizing_reader::normalizing_reader(flat_mutation_reader rd)
    : impl(rd.schema())
    , _rd(std::move(rd))
    , _range_tombstones(*_rd.schema())
{}

future<> normalizing_reader::fill_buffer(db::timeout_clock::time_point timeout) {
    return do_until([this] { return is_buffer_full() || is_end_of_stream(); }, [this, timeout] {
        return _rd.fill_buffer(timeout).then([this] {
            position_in_partition::less_compare less{*_rd.schema()};
            position_in_partition::equal_compare eq{*_rd.schema()};
            while (!_rd.is_buffer_empty()) {
                auto mf = _rd.pop_mutation_fragment();
                if (mf.is_end_of_partition()) {
                    while (auto mfo = _range_tombstones.get_next()) {
                        push_mutation_fragment(std::move(*mfo));
                    }
                } else if (mf.is_range_tombstone()) {
                    _range_tombstones.apply(std::move(mf).as_range_tombstone());
                    continue;
                } else if (mf.is_clustering_row()) {
                    const clustering_row& cr = mf.as_clustering_row();
                    auto ck = cr.key();
                    auto end_kind = clustering_key::make_full(*_schema, ck) ? bound_kind::excl_end : bound_kind::incl_end;
                    while (auto mfo = _range_tombstones.get_next(mf)) {
                        range_tombstone&& rt = std::move(*mfo).as_range_tombstone();
                        if (less(rt.end_position(), cr.position())
                            || eq(rt.end_position(), position_in_partition::after_key(ck))) {
                            push_mutation_fragment(std::move(rt));
                        } else {
                            push_mutation_fragment(range_tombstone{
                                    rt.start_bound(),
                                    bound_view{ck, end_kind},
                                    rt.tomb});

                            rt.trim_front(*_rd.schema(), position_in_partition::after_key(ck));
                            _range_tombstones.apply(std::move(rt));
                            break;
                        }
                    }
                }

                push_mutation_fragment(std::move(mf));
            }
            _end_of_stream = _rd.is_end_of_stream() && _range_tombstones.empty();
        });
    });
}

void normalizing_reader::next_partition() {
    clear_buffer_to_next_partition();
    if (is_buffer_empty()) {
        _end_of_stream = false;
        _rd.next_partition();
    }
}
future<> normalizing_reader::fast_forward_to(
        const dht::partition_range& pr, db::timeout_clock::time_point timeout) {
    clear_buffer();
    _end_of_stream = false;
    return _rd.fast_forward_to(pr, timeout);
}
future<> normalizing_reader::fast_forward_to(
        position_range pr, db::timeout_clock::time_point timeout) {
    forward_buffer_to(pr.start());
    _end_of_stream = false;
    return _rd.fast_forward_to(std::move(pr), timeout);
}
size_t normalizing_reader::buffer_size() const {
    return flat_mutation_reader::impl::buffer_size() + _rd.buffer_size();
}

flat_mutation_reader make_normalizing_reader(flat_mutation_reader rd) {
    return make_flat_mutation_reader<normalizing_reader>(std::move(rd));
}

