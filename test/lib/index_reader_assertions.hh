/*
 * Copyright (C) 2017-present ScyllaDB
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

#include <boost/test/unit_test.hpp>

#include "dht/i_partitioner.hh"
#include "schema.hh"
#include "sstables/index_reader.hh"
#include "reader_permit.hh"

class index_reader_assertions {
    std::unique_ptr<sstables::index_reader> _r;
public:
    index_reader_assertions(std::unique_ptr<sstables::index_reader> r)
        : _r(std::move(r))
    { }

    index_reader_assertions& has_monotonic_positions(const schema& s) {
        auto pos_cmp = sstables::promoted_index_block_compare(s);
        auto rp_cmp = dht::ring_position_comparator(s);
        auto prev = dht::ring_position::min();
        _r->read_partition_data().get();
        while (!_r->eof()) {
            auto k = _r->get_partition_key();
            auto rp = dht::ring_position(dht::decorate_key(s, k));

            if (rp_cmp(prev, rp) >= 0) {
                BOOST_FAIL(format("Partitions have invalid order: {} >= {}", prev, rp));
            }

            prev = rp;

            sstables::clustered_index_cursor* cur = _r->current_clustered_cursor();
            std::optional<sstables::promoted_index_block_position> prev_end;
            while (auto ei_opt = cur->next_entry().get0()) {
                sstables::clustered_index_cursor::entry_info& ei = *ei_opt;
                if (prev_end && pos_cmp(ei.start, sstables::to_view(*prev_end))) {
                    BOOST_FAIL(format("Index blocks are not monotonic: {} > {}", *prev_end, ei.start));
                }
                prev_end = sstables::materialize(ei.end);
            }
            _r->advance_to_next_partition().get();
        }
        return *this;
    }

    index_reader_assertions& is_empty(const schema& s) {
        _r->read_partition_data().get();
        while (!_r->eof()) {
            BOOST_REQUIRE(_r->get_promoted_index_size() == 0);
            _r->advance_to_next_partition().get();
        }
        return *this;
    }
};

inline
index_reader_assertions assert_that(std::unique_ptr<sstables::index_reader> r) {
    return { std::move(r) };
}
