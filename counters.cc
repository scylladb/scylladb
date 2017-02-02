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
            auto delta = value_cast<int64_t>(long_type->deserialize_value(acv.value()));
            counter_cell_builder ccb;
            ccb.add_shard(counter_shard(counter_id::local(), delta, clock_offset + 1));
            ac_o_c = ccb.build(acv.timestamp());
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

        struct counter_shard_or_tombstone {
            stdx::optional<counter_shard> shard;
            tombstone tomb;
        };
        std::deque<std::pair<column_id, counter_shard_or_tombstone>> shards;
        it->row().cells().for_each_cell([&] (column_id id, const atomic_cell_or_collection& ac_o_c) {
            auto acv = ac_o_c.as_atomic_cell();
            if (!acv.is_live()) {
                counter_shard_or_tombstone cs_o_t { { },
                                                    tombstone(acv.timestamp(), acv.deletion_time()) };
                shards.emplace_back(std::make_pair(id, cs_o_t));
                return; // continue -- we are in lambda
            }
            counter_cell_view ccv(acv);
            auto cs = ccv.local_shard();
            if (!cs) {
                return; // continue
            }
            shards.emplace_back(std::make_pair(id, counter_shard_or_tombstone { counter_shard(*cs), tombstone() }));
        });

        cr.row().cells().for_each_cell([&] (column_id id, atomic_cell_or_collection& ac_o_c) {
            auto acv = ac_o_c.as_atomic_cell();
            if (!acv.is_live()) {
                return; // continue -- we are in lambda
            }
            while (!shards.empty() && shards.front().first < id) {
                shards.pop_front();
            }

            auto delta = value_cast<int64_t>(long_type->deserialize_value(acv.value()));

            counter_cell_builder ccb;
            if (shards.empty() || shards.front().first > id) {
                ccb.add_shard(counter_shard(counter_id::local(), delta, clock_offset + 1));
            } else if (shards.front().second.tomb.timestamp == api::missing_timestamp) {
                auto& cs = *shards.front().second.shard;
                cs.update(delta, clock_offset + 1);
                ccb.add_shard(cs);
                shards.pop_front();
            } else {
                // We are apply the tombstone that's already there second time.
                // It is not necessary but there is no easy way to remove cell
                // from a mutation.
                tombstone t = shards.front().second.tomb;
                ac_o_c = atomic_cell::make_dead(t.timestamp, t.deletion_time);
                shards.pop_front();
                return; // continue -- we are in lambda
            }
            ac_o_c = ccb.build(acv.timestamp());
        });
    }
}
