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

#include "test/lib/normalizing_reader.hh"
#include <seastar/core/future-util.hh>

normalizing_reader::normalizing_reader(flat_mutation_reader rd)
    : impl(rd.schema(), rd.permit())
    , _rd(std::move(rd))
    , _range_tombstones(*_rd.schema(), _rd.permit())
{}

future<> normalizing_reader::fill_buffer(db::timeout_clock::time_point timeout) {
    return do_until([this] { return is_buffer_full() || is_end_of_stream(); }, [this, timeout] {
        return _rd.fill_buffer(timeout).then([this] {
            position_in_partition::less_compare less{*_rd.schema()};
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
                    auto ck = mf.as_clustering_row().key();
                    clustering_key::make_full(*_rd.schema(), ck);
                    auto after_pos = position_in_partition::after_key(ck);
                    while (auto mfo = _range_tombstones.get_next(mf)) {
                        range_tombstone&& rt = std::move(*mfo).as_range_tombstone();
                        if (!less(after_pos, rt.end_position())) {
                            push_mutation_fragment(*_schema, _permit, std::move(rt));
                        } else {
                            push_mutation_fragment(*_schema, _permit, range_tombstone{rt.position(), after_pos, rt.tomb});
                            _range_tombstones.apply(range_tombstone{after_pos, rt.end_position(), rt.tomb});
                        }
                    }
                }

                push_mutation_fragment(std::move(mf));
            }

            if (_rd.is_end_of_stream()) {
                while (auto mfo = _range_tombstones.get_next()) {
                    push_mutation_fragment(std::move(*mfo));
                }
                _end_of_stream = true;
            }
        });
    });
}

future<> normalizing_reader::next_partition() {
    _range_tombstones.reset();
    clear_buffer_to_next_partition();
    if (is_buffer_empty()) {
        _end_of_stream = false;
        return _rd.next_partition();
    }
    return make_ready_future<>();
}
future<> normalizing_reader::fast_forward_to(
        const dht::partition_range& pr, db::timeout_clock::time_point timeout) {
    _range_tombstones.reset();
    clear_buffer();
    _end_of_stream = false;
    return _rd.fast_forward_to(pr, timeout);
}
future<> normalizing_reader::fast_forward_to(
        position_range pr, db::timeout_clock::time_point timeout) {
    _range_tombstones.forward_to(pr.start());
    forward_buffer_to(pr.start());
    _end_of_stream = false;
    return _rd.fast_forward_to(std::move(pr), timeout);
}
future<> normalizing_reader::close() noexcept {
    return _rd.close();
}

flat_mutation_reader make_normalizing_reader(flat_mutation_reader rd) {
    return make_flat_mutation_reader<normalizing_reader>(std::move(rd));
}

