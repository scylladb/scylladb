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

static bool apply_in_place(atomic_cell_or_collection& dst, atomic_cell_or_collection& src)
{
    auto dst_ccmv = counter_cell_mutable_view(dst.as_mutable_atomic_cell());
    auto src_ccmv = counter_cell_mutable_view(src.as_mutable_atomic_cell());
    auto dst_shards = dst_ccmv.shards();
    auto src_shards = src_ccmv.shards();

    auto dst_it = dst_shards.begin();
    auto src_it = src_shards.begin();

    while (src_it != src_shards.end()) {
        while (dst_it != dst_shards.end() && dst_it->id() < src_it->id()) {
            ++dst_it;
        }
        if (dst_it == dst_shards.end() || dst_it->id() != src_it->id()) {
            // Fast-path failed. Revert and fall back to the slow path.
            if (dst_it == dst_shards.end()) {
                --dst_it;
            }
            while (src_it != src_shards.begin()) {
                --src_it;
                while (dst_it->id() != src_it->id()) {
                    --dst_it;
                }
                src_it->swap_value_and_clock(*dst_it);
            }
            return false;
        }
        if (dst_it->logical_clock() < src_it->logical_clock()) {
            dst_it->swap_value_and_clock(*src_it);
        } else {
            src_it->set_value_and_clock(*dst_it);
        }
        ++src_it;
    }

    auto dst_ts = dst_ccmv.timestamp();
    auto src_ts = src_ccmv.timestamp();
    dst_ccmv.set_timestamp(std::max(dst_ts, src_ts));
    src_ccmv.set_timestamp(dst_ts);
    src.as_mutable_atomic_cell().set_counter_in_place_revert(true);
    return true;
}

static void revert_in_place_apply(atomic_cell_or_collection& dst, atomic_cell_or_collection& src)
{
    assert(dst.can_use_mutable_view() && src.can_use_mutable_view());
    auto dst_ccmv = counter_cell_mutable_view(dst.as_mutable_atomic_cell());
    auto src_ccmv = counter_cell_mutable_view(src.as_mutable_atomic_cell());
    auto dst_shards = dst_ccmv.shards();
    auto src_shards = src_ccmv.shards();

    auto dst_it = dst_shards.begin();
    auto src_it = src_shards.begin();

    while (src_it != src_shards.end()) {
        while (dst_it != dst_shards.end() && dst_it->id() < src_it->id()) {
            ++dst_it;
        }
        assert(dst_it != dst_shards.end() && dst_it->id() == src_it->id());
        dst_it->swap_value_and_clock(*src_it);
        ++src_it;
    }

    auto dst_ts = dst_ccmv.timestamp();
    auto src_ts = src_ccmv.timestamp();
    dst_ccmv.set_timestamp(src_ts);
    src_ccmv.set_timestamp(dst_ts);
    src.as_mutable_atomic_cell().set_counter_in_place_revert(false);
}

bool counter_cell_view::apply_reversibly(atomic_cell_or_collection& dst, atomic_cell_or_collection& src)
{
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
        auto src_v = src_ac.counter_update_value();
        auto dst_v = dst_ac.counter_update_value();
        dst = atomic_cell::make_live_counter_update(std::max(dst_ac.timestamp(), src_ac.timestamp()),
                                                    src_v + dst_v);
        return true;
    }

    assert(!dst_ac.is_counter_update());
    assert(!src_ac.is_counter_update());

    if (counter_cell_view(dst_ac).shard_count() >= counter_cell_view(src_ac).shard_count()
        && dst.can_use_mutable_view() && src.can_use_mutable_view()) {
        if (apply_in_place(dst, src)) {
            return true;
        }
    }

    src.as_mutable_atomic_cell().set_counter_in_place_revert(false);
    auto dst_shards = counter_cell_view(dst_ac).shards();
    auto src_shards = counter_cell_view(src_ac).shards();

    counter_cell_builder result;
    combine(dst_shards.begin(), dst_shards.end(), src_shards.begin(), src_shards.end(),
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
        auto src_v = src.as_atomic_cell().counter_update_value();
        auto dst_v = dst.as_atomic_cell().counter_update_value();
        dst = atomic_cell::make_live(dst.as_atomic_cell().timestamp(),
                                     long_type->decompose(dst_v - src_v));
    } else if (src.as_atomic_cell().is_counter_in_place_revert_set()) {
        revert_in_place_apply(dst, src);
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
        return atomic_cell(a);
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


void transform_counter_updates_to_shards(mutation& m, const mutation* current_state, uint64_t clock_offset) {
    // FIXME: allow current_state to be frozen_mutation

    auto transform_new_row_to_shards = [clock_offset] (auto& cr) {
        cr.row().cells().for_each_cell([clock_offset] (auto, atomic_cell_or_collection& ac_o_c) {
            auto acv = ac_o_c.as_atomic_cell();
            if (!acv.is_live()) {
                return; // continue -- we are in lambda
            }
            auto delta = acv.counter_update_value();
            auto cs = counter_shard(counter_id::local(), delta, clock_offset + 1);
            ac_o_c = counter_cell_builder::from_single_shard(acv.timestamp(), cs);
        });
    };

    if (!current_state) {
        for (auto& cr : m.partition().clustered_rows()) {
            transform_new_row_to_shards(cr);
        }
        return;
    }

    clustering_key::less_compare cmp(*m.schema());

    auto& cstate = current_state->partition();
    auto it = cstate.clustered_rows().begin();
    auto end = cstate.clustered_rows().end();
    for (auto& cr : m.partition().clustered_rows()) {
        while (it != end && cmp(it->key(), cr.key())) {
            ++it;
        }
        if (it == end || cmp(cr.key(), it->key())) {
            transform_new_row_to_shards(cr);
            continue;
        }

        std::deque<std::pair<column_id, counter_shard>> shards;
        it->row().cells().for_each_cell([&] (column_id id, const atomic_cell_or_collection& ac_o_c) {
            auto acv = ac_o_c.as_atomic_cell();
            if (!acv.is_live()) {
                return; // continue -- we are in lambda
            }
            counter_cell_view ccv(acv);
            auto cs = ccv.local_shard();
            if (!cs) {
                return; // continue
            }
            shards.emplace_back(std::make_pair(id, counter_shard(*cs)));
        });

        cr.row().cells().for_each_cell([&] (column_id id, atomic_cell_or_collection& ac_o_c) {
            auto acv = ac_o_c.as_atomic_cell();
            if (!acv.is_live()) {
                return; // continue -- we are in lambda
            }
            while (!shards.empty() && shards.front().first < id) {
                shards.pop_front();
            }

            auto delta = acv.counter_update_value();

            if (shards.empty() || shards.front().first > id) {
                auto cs = counter_shard(counter_id::local(), delta, clock_offset + 1);
                ac_o_c = counter_cell_builder::from_single_shard(acv.timestamp(), cs);
            } else {
                auto& cs = shards.front().second;
                cs.update(delta, clock_offset + 1);
                ac_o_c = counter_cell_builder::from_single_shard(acv.timestamp(), cs);
                shards.pop_front();
            }
        });
    }
}
