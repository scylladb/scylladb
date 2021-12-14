/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 *
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

#include "mutation_reader.hh"

/*
 * Single-partition reader, a lower bound and an upper bound for the set of positions
 * of fragments returned by the reader. The bounds don't need to be exact.
 */
struct reader_bounds {
    flat_mutation_reader_v2 r;
    position_in_partition lower;
    position_in_partition upper;
};

/*
 * Returns readers from an a-priori prepared set of readers with determined lower and upper bounds
 * on the positions of their fragments.
 */
struct simple_position_reader_queue : public position_reader_queue {
    position_in_partition::tri_compare _cmp;

    using container_t = std::vector<reader_bounds>;
    container_t _rs;
    container_t::iterator _it;

    simple_position_reader_queue(const schema& s, std::vector<reader_bounds> rs)
        // precondition: rs sorted w.r.t lower.
        // `s` must be kept alive until the last call to `pop` or `empty`.
        : _cmp(s), _rs(std::move(rs)), _it(_rs.begin())
    { }

    virtual ~simple_position_reader_queue() override = default;

    virtual std::vector<reader_and_upper_bound> pop(position_in_partition_view bound) override {
        if (empty(bound)) {
            return {};
        }

        // !empty(bound) implies that _it->lower <= bound.

        std::vector<reader_and_upper_bound> ret;
        auto it = _it;
        for (; it != _rs.end() && _cmp(_it->lower, it->lower) == 0; ++it) {
            ret.emplace_back(std::move(it->r), std::move(it->upper));
        }
        _it = it;
        return ret;
    }

    virtual bool empty(position_in_partition_view bound) const override {
        return _it == _rs.end() || _cmp(bound, _it->lower) < 0;
    }

    virtual future<> close() noexcept override {
        return do_for_each(_it, _rs.end(), [this] (reader_bounds& rb) {
            auto r = std::move(rb.r);
            return r.close();
        }).finally([this] {
            _it = _rs.end();
        });
    }
};
