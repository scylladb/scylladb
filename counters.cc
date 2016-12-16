/*
 * Copyright (C) 2016 ScyllaDB
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

#include "service/storage_service.hh"
#include "counters.hh"
#include "mutation.hh"
#include "combine.hh"
counter_id counter_id::local()
{
    return counter_id(service::get_local_storage_service().get_local_id());
}

std::ostream& operator<<(std::ostream& os, const counter_id& id) {
    return os << id.to_uuid();
}

std::ostream& operator<<(std::ostream& os, counter_shard_view csv) {
    return os << "{global_shard id: " << csv.id() << " value: " << csv.value()
              << " clock: " << csv.logical_clock() << "}";
}

std::ostream& operator<<(std::ostream& os, counter_cell_view ccv) {
    return os << "{counter_cell timestamp: " << ccv.timestamp() << " shards: {" << ::join(", ", ccv.shards()) << "}}";
}

bool counter_cell_view::apply_reversibly(atomic_cell_or_collection& dst, atomic_cell_or_collection& src)
{
    // TODO: optimise for single shard existing in the other
    // TODO: optimise for no new shards?
    auto dst_ac = dst.as_atomic_cell();
    auto src_ac = src.as_atomic_cell();

    if (!dst_ac.is_live() || !src_ac.is_live()) {
        if (dst_ac.is_live() || (!src_ac.is_live() && compare_atomic_cell_for_merge(dst_ac, src_ac) < 0)) {
            std::swap(dst, src);
            return true;
        }
        return false;
    }

    if (dst_ac.is_counter_update() && src_ac.is_counter_update()) {
        // FIXME: store deltas just as a normal int64_t and get rid of these calls
        // to long_type
        auto src_v = value_cast<int64_t>(long_type->deserialize_value(src_ac.value()));
        auto dst_v = value_cast<int64_t>(long_type->deserialize_value(dst_ac.value()));
        dst = atomic_cell::make_live_counter_update(std::max(dst_ac.timestamp(), src_ac.timestamp()),
                                                    long_type->decompose(src_v + dst_v));
        return true;
    }

    assert(!dst_ac.is_counter_update());
    assert(!src_ac.is_counter_update());

    auto a_shards = counter_cell_view(dst_ac).shards();
    auto b_shards = counter_cell_view(src_ac).shards();

    counter_cell_builder result;
    combine(a_shards.begin(), a_shards.end(), b_shards.begin(), b_shards.end(),
            result.inserter(), counter_shard_view::less_compare_by_id(), [] (auto& x, auto& y) {
                return x.logical_clock() < y.logical_clock() ? y : x;
            });

    auto cell = result.build(std::max(dst_ac.timestamp(), src_ac.timestamp()));
    src = std::exchange(dst, atomic_cell_or_collection(cell));
    return true;
}

void counter_cell_view::revert_apply(atomic_cell_or_collection& dst, atomic_cell_or_collection& src)
{
    if (dst.as_atomic_cell().is_counter_update()) {
        auto src_v = value_cast<int64_t>(long_type->deserialize_value(src.as_atomic_cell().value()));
        auto dst_v = value_cast<int64_t>(long_type->deserialize_value(dst.as_atomic_cell().value()));
        dst = atomic_cell::make_live(dst.as_atomic_cell().timestamp(),
                                     long_type->decompose(dst_v - src_v));
    } else {
        std::swap(dst, src);
    }
}

stdx::optional<atomic_cell> counter_cell_view::difference(atomic_cell_view a, atomic_cell_view b)
{
    assert(!a.is_counter_update());
    assert(!b.is_counter_update());

    if (!b.is_live()) {
        return { };
    } else if (!a.is_live()) {
        return a;
    }

    auto a_shards = counter_cell_view(a).shards();
    auto b_shards = counter_cell_view(b).shards();

    auto a_it = a_shards.begin();
    auto a_end = a_shards.end();
    auto b_it = b_shards.begin();
    auto b_end = b_shards.end();

    counter_cell_builder result;
    while (a_it != a_end) {
        while (b_it != b_end && (*b_it).id() < (*a_it).id()) {
            ++b_it;
        }
        if (b_it == b_end || (*a_it).id() != (*b_it).id() || (*a_it).logical_clock() > (*b_it).logical_clock()) {
            result.add_shard(counter_shard(*a_it));
        }
        ++a_it;
    }

    stdx::optional<atomic_cell> diff;
    if (!result.empty()) {
        diff = result.build(std::max(a.timestamp(), b.timestamp()));
    } else if (a.timestamp() > b.timestamp()) {
        diff = atomic_cell::make_live(a.timestamp(), bytes_view());
    }
    return diff;
}
