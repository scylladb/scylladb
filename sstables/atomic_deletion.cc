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

#include "atomic_deletion.hh"
#include "to_string.hh"
#include <seastar/core/shared_future.hh>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/algorithm/copy.hpp>

namespace sstables {

atomic_deletion_manager::atomic_deletion_manager(unsigned shard_count,
        std::function<future<> (std::vector<sstring> sstables)> delete_sstables)
        : _shard_count(shard_count)
        , _delete_sstables(std::move(delete_sstables)) {
}

future<>
atomic_deletion_manager::delete_atomically(std::vector<sstable_to_delete> atomic_deletion_set, unsigned deleting_shard) {
    // runs on shard 0 only
    _deletion_logger.debug("shard {} atomically deleting {}", deleting_shard, atomic_deletion_set);

    if (_atomic_deletions_cancelled) {
        _deletion_logger.debug("atomic deletions disabled, erroring out");
        using boost::adaptors::transformed;
        throw atomic_deletion_cancelled(atomic_deletion_set
                                        | transformed(std::mem_fn(&sstable_to_delete::name)));
    }

    // Insert atomic_deletion_set into the list of sets pending deletion.  If the new set
    // overlaps with an existing set, merge them (the merged set will be deleted atomically).
    std::unordered_map<sstring, lw_shared_ptr<pending_deletion>> new_atomic_deletion_sets;
    auto merged_set = make_lw_shared(pending_deletion());
    for (auto&& sst_to_delete : atomic_deletion_set) {
        merged_set->names.insert(sst_to_delete.name);
        if (!sst_to_delete.shared) {
            for (auto shard : boost::irange<shard_id>(0, _shard_count)) {
                _shards_agreeing_to_delete_sstable[sst_to_delete.name].insert(shard);
            }
        }
        new_atomic_deletion_sets.emplace(sst_to_delete.name, merged_set);
    }
    auto pr = make_lw_shared<promise<>>();
    merged_set->completions.insert(pr);
    auto ret = pr->get_future();
    for (auto&& sst_to_delete : atomic_deletion_set) {
        auto i = _atomic_deletion_sets.find(sst_to_delete.name);
        // merge from old deletion set to new deletion set
        // i->second can be nullptr, see below why
        if (i != _atomic_deletion_sets.end() && i->second) {
            boost::copy(i->second->names, std::inserter(merged_set->names, merged_set->names.end()));
            boost::copy(i->second->completions, std::inserter(merged_set->completions, merged_set->completions.end()));
        }
    }
    _deletion_logger.debug("new atomic set: {}", merged_set->names);
    // we need to merge new_atomic_deletion_sets into g_atomic_deletion_sets,
    // but beware of exceptions.  We do that with a first pass that inserts
    // nullptr as the value, so the second pass only replaces, and does not allocate
    for (auto&& sst_to_delete : atomic_deletion_set) {
        _atomic_deletion_sets.emplace(sst_to_delete.name, nullptr);
    }
    // now, no allocations are involved, so this commits the operation atomically
    for (auto&& n : merged_set->names) {
        auto i = _atomic_deletion_sets.find(n);
        i->second = merged_set;
    }

    // Mark each sstable as being deleted from deleting_shard.  We have to do
    // this in a separate pass, so the consideration whether we can delete or not
    // sees all the data from this pass.
    for (auto&& sst : atomic_deletion_set) {
        _shards_agreeing_to_delete_sstable[sst.name].insert(deleting_shard);
    }

    // Figure out if the (possibly merged) set can be deleted
    for (auto&& sst : merged_set->names) {
        if (_shards_agreeing_to_delete_sstable[sst].size() != _shard_count) {
            // Not everyone agrees, leave the set pending
            _deletion_logger.debug("deferring deletion until all shards agree");
            return ret;
        }
    }

    // Cannot recover from a failed deletion
    for (auto&& name : merged_set->names) {
        _atomic_deletion_sets.erase(name);
        _shards_agreeing_to_delete_sstable.erase(name);
    }

    // Everyone agrees, let's delete
    auto names = boost::copy_range<std::vector<sstring>>(merged_set->names);
    _deletion_logger.debug("deleting {}", names);
    return _delete_sstables(names).then_wrapped([this, merged_set] (future<> result) {
        _deletion_logger.debug("atomic deletion completed: {}", merged_set->names);
        shared_future<> sf(std::move(result));
        for (auto&& comp : merged_set->completions) {
            sf.get_future().forward_to(std::move(*comp));
        }
    });

    return ret;
}

void
atomic_deletion_manager::cancel_atomic_deletions() {
    _atomic_deletions_cancelled = true;
    for (auto&& pd : _atomic_deletion_sets) {
        if (!pd.second) {
            // Could happen if a delete_atomically() failed
            continue;
        }
        for (auto&& c : pd.second->completions) {
            c->set_exception(atomic_deletion_cancelled(pd.second->names));
        }
        // since sets are shared, make sure we don't hit the same one again
        pd.second->completions.clear();
    }
    _atomic_deletion_sets.clear();
    _shards_agreeing_to_delete_sstable.clear();
}

}
