/*
 * Copyright (C) 2020 ScyllaDB
 *
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

#include <functional>
#include <optional>
#include <variant>
#include <seastar/core/smp.hh>
#include <seastar/core/file.hh>
#include "shared_sstable.hh"
#include "sstable_set.hh"
#include "utils/UUID.hh"
#include "dht/i_partitioner.hh"
#include "compaction_weight_registration.hh"

namespace sstables {

enum class compaction_type {
    Compaction = 0,
    Cleanup = 1,
    Validation = 2,
    Scrub = 3,
    Index_build = 4,
    Reshard = 5,
    Upgrade = 6,
    Reshape = 7,
};

std::ostream& operator<<(std::ostream& os, compaction_type type);

struct compaction_completion_desc {
    // Old, existing SSTables that should be deleted and removed from the SSTable set.
    std::vector<shared_sstable> old_sstables;
    // New, fresh SSTables that should be added to SSTable set, replacing the old ones.
    std::vector<shared_sstable> new_sstables;
    // Set of compacted partition ranges that should be invalidated in the cache.
    dht::partition_range_vector ranges_for_cache_invalidation;
};

// creates a new SSTable for a given shard
using compaction_sstable_creator_fn = std::function<shared_sstable(shard_id shard)>;
// Replaces old sstable(s) by new one(s) which contain all non-expired data.
using compaction_sstable_replacer_fn = std::function<void(compaction_completion_desc)>;

class compaction_options {
public:
    struct regular {
    };
    struct cleanup {
    };
    struct upgrade {
    };
    struct scrub {
        bool skip_corrupted;
    };
    struct reshard {
    };
    struct reshape {
    };
private:
    using options_variant = std::variant<regular, cleanup, upgrade, scrub, reshard, reshape>;

private:
    options_variant _options;

private:
    explicit compaction_options(options_variant options) : _options(std::move(options)) {
    }

public:
    static compaction_options make_reshape() {
        return compaction_options(reshape{});
    }

    static compaction_options make_reshard() {
        return compaction_options(reshard{});
    }

    static compaction_options make_regular() {
        return compaction_options(regular{});
    }

    static compaction_options make_cleanup() {
        return compaction_options(cleanup{});
    }

    static compaction_options make_upgrade() {
        return compaction_options(upgrade{});
    }

    static compaction_options make_scrub(bool skip_corrupted) {
        return compaction_options(scrub{skip_corrupted});
    }

    template <typename... Visitor>
    auto visit(Visitor&&... visitor) const {
        return std::visit(std::forward<Visitor>(visitor)..., _options);
    }

    compaction_type type() const;
};

struct compaction_descriptor {
    // List of sstables to be compacted.
    std::vector<sstables::shared_sstable> sstables;
    // This is a snapshot of the table's sstable set, used only for the purpose of expiring tombstones.
    // If this sstable set cannot be provided, expiration will be disabled to prevent data from being resurrected.
    std::optional<sstables::sstable_set> all_sstables_snapshot;
    // Level of sstable(s) created by compaction procedure.
    int level;
    // Threshold size for sstable(s) to be created.
    uint64_t max_sstable_bytes;
    // Run identifier of output sstables.
    utils::UUID run_identifier;
    // Holds ownership of a weight assigned to this compaction iff it's a regular one.
    std::optional<compaction_weight_registration> weight_registration;
    // Calls compaction manager's task for this compaction to release reference to exhausted sstables.
    std::function<void(const std::vector<shared_sstable>& exhausted_sstables)> release_exhausted;
    // The options passed down to the compaction code.
    // This also selects the kind of compaction to do.
    compaction_options options = compaction_options::make_regular();

    compaction_sstable_creator_fn creator;
    compaction_sstable_replacer_fn replacer;

    ::io_priority_class io_priority = default_priority_class();

    compaction_descriptor() = default;

    static constexpr int default_level = 0;
    static constexpr uint64_t default_max_sstable_bytes = std::numeric_limits<uint64_t>::max();

    explicit compaction_descriptor(std::vector<sstables::shared_sstable> sstables,
                                   std::optional<sstables::sstable_set> all_sstables_snapshot,
                                   ::io_priority_class io_priority,
                                   int level = default_level,
                                   uint64_t max_sstable_bytes = default_max_sstable_bytes,
                                   utils::UUID run_identifier = utils::make_random_uuid(),
                                   compaction_options options = compaction_options::make_regular())
        : sstables(std::move(sstables))
        , all_sstables_snapshot(std::move(all_sstables_snapshot))
        , level(level)
        , max_sstable_bytes(max_sstable_bytes)
        , run_identifier(run_identifier)
        , options(options)
        , io_priority(io_priority)
    {}
};

}
