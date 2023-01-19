/*
 * Copyright (C) 2020-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <functional>
#include <optional>
#include <variant>
#include <seastar/core/smp.hh>
#include <seastar/core/file.hh>
#include "sstables/types_fwd.hh"
#include "sstables/sstable_set.hh"
#include "utils/UUID.hh"
#include "dht/i_partitioner.hh"
#include "compaction_weight_registration.hh"
#include "compaction_fwd.hh"

namespace sstables {

enum class compaction_type {
    Compaction = 0,
    Cleanup = 1,
    Validation = 2, // Origin uses this for a compaction that is used exclusively for repair
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

class compaction_type_options {
public:
    struct regular {
    };
    struct cleanup {
    };
    struct upgrade {
    };
    struct scrub {
        enum class mode {
            abort, // abort scrub on the first sign of corruption
            skip, // skip corrupt data, including range of rows and/or partitions that are out-of-order
            segregate, // segregate out-of-order data into streams that all contain data with correct order
            validate, // validate data, printing all errors found (sstables are only read, not rewritten)
        };
        mode operation_mode = mode::abort;

        enum class quarantine_mode {
            include, // scrub all sstables, including quarantined
            exclude, // scrub only non-quarantined sstables
            only, // scrub only quarantined sstables
        };
        quarantine_mode quarantine_operation_mode = quarantine_mode::include;
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
    explicit compaction_type_options(options_variant options) : _options(std::move(options)) {
    }

public:
    static compaction_type_options make_reshape() {
        return compaction_type_options(reshape{});
    }

    static compaction_type_options make_reshard() {
        return compaction_type_options(reshard{});
    }

    static compaction_type_options make_regular() {
        return compaction_type_options(regular{});
    }

    static compaction_type_options make_cleanup() {
        return compaction_type_options(cleanup{});
    }

    static compaction_type_options make_upgrade() {
        return compaction_type_options(upgrade{});
    }

    static compaction_type_options make_scrub(scrub::mode mode) {
        return compaction_type_options(scrub{mode});
    }

    template <typename... Visitor>
    auto visit(Visitor&&... visitor) const {
        return std::visit(std::forward<Visitor>(visitor)..., _options);
    }

    const options_variant& options() const { return _options; }

    compaction_type type() const;
};

std::string_view to_string(compaction_type_options::scrub::mode);
std::ostream& operator<<(std::ostream& os, compaction_type_options::scrub::mode scrub_mode);

std::string_view to_string(compaction_type_options::scrub::quarantine_mode);
std::ostream& operator<<(std::ostream& os, compaction_type_options::scrub::quarantine_mode quarantine_mode);

class dummy_tag {};
using has_only_fully_expired = seastar::bool_class<dummy_tag>;

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
    // Can split large partitions at clustering boundary.
    bool can_split_large_partition = false;
    // Run identifier of output sstables.
    sstables::run_id run_identifier;
    // The options passed down to the compaction code.
    // This also selects the kind of compaction to do.
    compaction_type_options options = compaction_type_options::make_regular();
    // If engaged, compaction will cleanup the input sstables by skipping non-owned ranges.
    compaction::owned_ranges_ptr owned_ranges;

    compaction_sstable_creator_fn creator;
    compaction_sstable_replacer_fn replacer;

    ::io_priority_class io_priority = default_priority_class();

    // Denotes if this compaction task is comprised solely of completely expired SSTables
    sstables::has_only_fully_expired has_only_fully_expired = has_only_fully_expired::no;

    compaction_descriptor() = default;

    static constexpr int default_level = 0;
    static constexpr uint64_t default_max_sstable_bytes = std::numeric_limits<uint64_t>::max();

    explicit compaction_descriptor(std::vector<sstables::shared_sstable> sstables,
                                   ::io_priority_class io_priority,
                                   int level = default_level,
                                   uint64_t max_sstable_bytes = default_max_sstable_bytes,
                                   run_id run_identifier = run_id::create_random_id(),
                                   compaction_type_options options = compaction_type_options::make_regular(),
                                   compaction::owned_ranges_ptr owned_ranges_ = {})
        : sstables(std::move(sstables))
        , level(level)
        , max_sstable_bytes(max_sstable_bytes)
        , run_identifier(run_identifier)
        , options(options)
        , owned_ranges(std::move(owned_ranges_))
        , io_priority(io_priority)
    {}

    explicit compaction_descriptor(sstables::has_only_fully_expired has_only_fully_expired,
                                   std::vector<sstables::shared_sstable> sstables,
                                   ::io_priority_class io_priority)
        : sstables(std::move(sstables))
        , level(default_level)
        , max_sstable_bytes(default_max_sstable_bytes)
        , run_identifier(run_id::create_random_id())
        , options(compaction_type_options::make_regular())
        , io_priority(io_priority)
        , has_only_fully_expired(has_only_fully_expired)
    {}

    // Return fan-in of this job, which is equal to its number of runs.
    unsigned fan_in() const;
    // Enables garbage collection for this descriptor, meaning that compaction will be able to purge expired data
    void enable_garbage_collection(sstables::sstable_set snapshot) { all_sstables_snapshot = std::move(snapshot); }
    // Returns total size of all sstables contained in this descriptor
    uint64_t sstables_size() const;
};

}
