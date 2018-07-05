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
        auto pos_cmp = [&s] (const sstables::promoted_index_block& lhs, const sstables::promoted_index_block& rhs) {
            sstables::promoted_index_block_compare cmp(s);
            return cmp(rhs.start(s), lhs.end(s));
        };
        auto rp_cmp = dht::ring_position_comparator(s);
        auto prev = dht::ring_position::min();
        _r->read_partition_data().get();
        while (!_r->eof()) {
            auto& e = _r->current_partition_entry();
            auto k = e.get_decorated_key();
            auto token = dht::token(k.token());
            auto rp = dht::ring_position(token, k.key().to_partition_key(s));

            if (!rp_cmp(prev, rp)) {
                BOOST_FAIL(sprint("Partitions have invalid order: %s >= %s", prev, rp));
            }

            prev = rp;

            while (e.get_read_pi_blocks_count() < e.get_total_pi_blocks_count()) {
                e.get_next_pi_blocks().get();
                auto* infos = e.get_pi_blocks();
                if (infos->empty()) {
                    continue;
                }
                auto it = std::adjacent_find(infos->begin(), infos->end(), pos_cmp);
                if (it != infos->end()) {
                    std::cout << "promoted index:\n";
                    for (auto& e : *infos) {
                        std::cout << "  " << e.start(s) << "-" << e.end(s)
                                  << ": +" << e.offset() << " len=" << e.width() << std::endl;
                    }
                    auto next = std::next(it);
                    BOOST_FAIL(sprint("Index blocks are not monotonic: %s >= %s", it->end(s), next->start(s)));
                }
            }
            _r->advance_to_next_partition().get();
        }
        return *this;
    }

    index_reader_assertions& is_empty(const schema& s) {
        _r->read_partition_data().get();
        while (!_r->eof()) {
            BOOST_REQUIRE(_r->current_partition_entry().get_total_pi_blocks_count() == 0);
            _r->advance_to_next_partition().get();
        }
        return *this;
    }
};

inline
index_reader_assertions assert_that(std::unique_ptr<sstables::index_reader> r) {
    return { std::move(r) };
}
