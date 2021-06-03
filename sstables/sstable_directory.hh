/*
 * Copyright (C) 2020-present ScyllaDB
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

#include "lister.hh"
#include <filesystem>
#include <seastar/core/file.hh>
#include <seastar/core/sharded.hh>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <functional>
#include "seastarx.hh"
#include "sstables/shared_sstable.hh"            // sstables::shared_sstable
#include "sstables/version.hh"                   // sstable versions
#include "sstables/compaction_descriptor.hh"     // for compaction_sstable_creator_fn
#include "sstables/open_info.hh"                 // for entry_descriptor and foreign_sstable_open_info, chunked_vector wants to know if they are move constructible
#include "utils/chunked_vector.hh"
#include "utils/phased_barrier.hh"

class compaction_manager;

namespace sstables {

bool manifest_json_filter(const std::filesystem::path&, const directory_entry& entry);

// Handles a directory containing SSTables. It could be an auxiliary directory (like upload),
// or the main directory.
class sstable_directory {
public:
    using lack_of_toc_fatal = bool_class<class lack_of_toc_fatal_tag>;
    using need_mutate_level = bool_class<class need_mutate_level_tag>;
    using enable_dangerous_direct_import_of_cassandra_counters = bool_class<class enable_dangerous_direct_import_of_cassandra_counters_tag>;
    using allow_loading_materialized_view = bool_class<class allow_loading_materialized_view_tag>;

    using sstable_object_from_existing_fn =
        noncopyable_function<sstables::shared_sstable(std::filesystem::path,
                                                      int64_t,
                                                      sstables::sstable_version_types,
                                                      sstables::sstable_format_types)>;

    // favor chunked vectors when dealing with file lists: they can grow to hundreds of thousands
    // of elements.
    using sstable_info_vector = utils::chunked_vector<sstables::foreign_sstable_open_info>;
private:
    using scan_multimap = std::unordered_multimap<int64_t, std::filesystem::path>;
    using scan_descriptors = utils::chunked_vector<sstables::entry_descriptor>;
    using scan_descriptors_map = std::unordered_map<int64_t, sstables::entry_descriptor>;

    struct scan_state {
        scan_multimap generations_found;
        scan_descriptors temp_toc_found;
        scan_descriptors_map descriptors;
    };

    // SSTable files to be deleted: things with a Temporary TOC, missing TOC files,
    // TemporaryStatistics, etc. Not part of the scan state, because we want to do a 2-phase
    // delete: maybe one of the shards will have signaled an error. And in the case of an error
    // we don't want to delete anything.
    std::unordered_set<sstring> _files_for_removal;

    // prevents an object that respects a phaser (usually a table) from disappearing in the middle of the operation.
    // Will be destroyed when this object is destroyed.
    std::optional<utils::phased_barrier::operation> _operation_barrier;

    std::filesystem::path _sstable_dir;

    // We may have hundreds of thousands of files to load. To protect against OOMs we will limit
    // how many of them we process at the same time.
    const size_t _load_parallelism;
    semaphore& _load_semaphore;

    // Flags below control how to behave when scanning new SSTables.
    need_mutate_level _need_mutate_level;
    lack_of_toc_fatal _throw_on_missing_toc;
    enable_dangerous_direct_import_of_cassandra_counters _enable_dangerous_direct_import_of_cassandra_counters;
    allow_loading_materialized_view _allow_loading_materialized_view;

    // How to create an SSTable object from an existing SSTable file (respecting generation, etc)
    sstable_object_from_existing_fn _sstable_object_from_existing_sstable;

    int64_t _max_generation_seen = 0;
    sstables::sstable_version_types _max_version_seen = sstables::sstable_version_types::ka;

    // SSTables that are unshared and belong to this shard. They are already stored as an
    // SSTable object.
    std::vector<sstables::shared_sstable> _unshared_local_sstables;

    // SSTables that are unshared and belong to foreign shards. Because they are more conveniently
    // stored as a foreign_sstable_open_info object, they are in a different attribute separate from the
    // local SSTables.
    //
    // The indexes of the outer vector represent the shards. Having anything in the index
    // representing this shard is illegal.
    std::vector<sstable_info_vector> _unshared_remote_sstables;

    // SSTables that are shared. Stored as foreign_sstable_open_info objects. Reason is those are
    // the SSTables that we found, and not necessarily the ones we will reshard. We want to balance
    // the amount of data resharded per shard, so a coordinator may redistribute this.
    sstable_info_vector _shared_sstable_info;

    future<> process_descriptor(sstables::entry_descriptor desc, const ::io_priority_class& iop, bool sort_sstables_according_to_owner = true);
    void validate(sstables::shared_sstable sst) const;
    void handle_component(scan_state& state, sstables::entry_descriptor desc, std::filesystem::path filename);
    future<> remove_input_sstables_from_resharding(std::vector<sstables::shared_sstable> sstlist);
    future<> collect_output_sstables_from_resharding(std::vector<sstables::shared_sstable> resharded_sstables);

    future<> remove_input_sstables_from_reshaping(std::vector<sstables::shared_sstable> sstlist);
    future<> collect_output_sstables_from_reshaping(std::vector<sstables::shared_sstable> reshaped_sstables);

    template <typename Container, typename Func>
    future<> parallel_for_each_restricted(Container&& C, Func&& func);
    future<> load_foreign_sstables(sstable_info_vector info_vec);

    std::vector<sstables::shared_sstable> _unsorted_sstables;
public:
    sstable_directory(std::filesystem::path sstable_dir,
            unsigned load_parallelism,
            semaphore& load_semaphore,
            need_mutate_level need_mutate,
            lack_of_toc_fatal fatal_nontoc,
            enable_dangerous_direct_import_of_cassandra_counters eddiocc,
            allow_loading_materialized_view,
            sstable_object_from_existing_fn sstable_from_existing);

    std::vector<sstables::shared_sstable>& get_unsorted_sstables() {
        return _unsorted_sstables;
    }

    // moves unshared SSTables that don't belong to this shard to the right shards.
    future<> move_foreign_sstables(sharded<sstable_directory>& source_directory);

    // returns what is the highest generation seen in this directory.
    int64_t highest_generation_seen() const;

    // returns what is the highest version seen in this directory.
    sstables::sstable_version_types highest_version_seen() const;

    // scans a directory containing SSTables. Every generation that is believed to belong to this
    // shard is processed, the ones that are not are skipped. Potential pertinence is decided as
    // generation % smp::count.
    //
    // Once this method return, every SSTable that this shard processed can be in one of 3 states:
    //  - unshared, local: not a shared SSTable, and indeed belongs to this shard.
    //  - unshared, remote: not s shared SSTable, but belongs to a remote shard.
    //  - shared : shared SSTable that belongs to many shards. Must be resharded before using
    //
    // This function doesn't change on-storage state. If files are to be removed, a separate call
    // (commit_file_removals()) has to be issued. This is to make sure that all instances of this
    // class in a sharded service have the opportunity to validate its files.
    future<> process_sstable_dir(const ::io_priority_class& iop, bool sort_sstables_according_to_owner = true);

    // Sort the sstable according to owner
    future<> sort_sstable(sstables::shared_sstable sst);

    // If files were scheduled to be removed, they will be removed after this call.
    future<> commit_directory_changes();

    // reshards a collection of SSTables.
    //
    // A reference to the compaction manager must be passed so we can register with it. Knowing
    // which table is being processed is a requirement of the compaction manager, so this must be
    // passed too.
    //
    // We will reshard max_sstables_per_job at once.
    //
    // A creator function must be passed that will create an SSTable object in the correct shard,
    // and an I/O priority must be specified.
    future<> reshard(sstable_info_vector info, compaction_manager& cm, table& table,
                     unsigned max_sstables_per_job, sstables::compaction_sstable_creator_fn creator,
                     const ::io_priority_class& iop);

    // reshapes a collection of SSTables, and returns the total amount of bytes reshaped.
    future<uint64_t> reshape(compaction_manager& cm, table& table,
                     sstables::compaction_sstable_creator_fn creator,
                     const ::io_priority_class& iop,
                     sstables::reshape_mode mode);

    // Store a phased operation. Usually used to keep an object alive while the directory is being
    // processed. One example is preventing table drops concurrent to the processing of this
    // directory.
    void store_phaser(utils::phased_barrier::operation op);

    // Helper function that processes all unshared SSTables belonging to this shard, respecting the
    // concurrency limit.
    future<> do_for_each_sstable(std::function<future<>(sstables::shared_sstable)> func);

    // Retrieves the list of shared SSTables in this object. The list will be reset once this
    // is called.
    sstable_info_vector retrieve_shared_sstables();

    std::filesystem::path sstable_dir() const noexcept {
        return _sstable_dir;
    }
};

}
