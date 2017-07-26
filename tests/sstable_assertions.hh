/*
 * Copyright (C) 2017 ScyllaDB
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

#include "dht/i_partitioner.hh"
#include "schema.hh"
#include "sstables/index_reader.hh"

class index_reader_assertions {
    std::unique_ptr<sstables::index_reader> _r;
public:
    index_reader_assertions(std::unique_ptr<sstables::index_reader> r)
        : _r(std::move(r))
    { }

    index_reader_assertions& has_monotonic_positions(const schema& s) {
        auto pos_cmp = position_in_partition::composite_less_compare(s);
        auto rp_cmp = dht::ring_position_comparator(s);
        auto prev = dht::ring_position::min();
        _r->read_partition_data().get();
        while (!_r->eof()) {
            auto k = _r->current_partition_entry().get_decorated_key();
            auto rp = dht::ring_position(k.token(), k.key().to_partition_key(s));

            if (!rp_cmp(prev, rp)) {
                BOOST_FAIL(sprint("Partitions have invalid order: %s >= %s", prev, rp));
            }

            prev = rp;

            auto* pi = _r->current_partition_entry().get_promoted_index(s);
            if (!pi->entries.empty()) {
                auto& prev = pi->entries[0];
                for (size_t i = 1; i < pi->entries.size(); ++i) {
                    auto& cur = pi->entries[i];
                    if (!pos_cmp(prev.end, cur.start)) {
                        std::cout << "promoted index:\n";
                        for (auto& e : pi->entries) {
                            std::cout << "  " << e.start << "-" << e.end << ": +" << e.offset << " len=" << e.width << std::endl;
                        }
                        BOOST_FAIL(sprint("Index blocks are not monotonic: %s >= %s", prev.end, cur.start));
                    }
                    cur = prev;
                }
            }
            _r->advance_to_next_partition().get();
        }
        return *this;
    }
};

inline
index_reader_assertions assert_that(std::unique_ptr<sstables::index_reader> r) {
    return { std::move(r) };
}
