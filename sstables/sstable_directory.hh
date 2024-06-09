/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

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
#include "compaction/compaction_descriptor.hh"     // for compaction_sstable_creator_fn
#include "sstables/open_info.hh"                 // for entry_descriptor and foreign_sstable_open_info, chunked_vector wants to know if they are move constructible
#include "utils/chunked_vector.hh"
#include "utils/phased_barrier.hh"
#include "utils/disk-error-handler.hh"
#include "utils/lister.hh"
#include "sstables/generation_type.hh"

class compaction_manager;

namespace sstables {

class sstables_manager;
bool manifest_json_filter(const std::filesystem::path&, const directory_entry& entry);

class directory_semaphore {
    unsigned _concurrency;
    semaphore _sem;
public:
    directory_semaphore(unsigned concurrency)
            : _concurrency(concurrency)
            , _sem(concurrency)
    {
    }

    friend class sstable_directory;
    friend class ::replica::table; // FIXME table snapshots should switch to sstable_directory
};

// Handles a directory containing SSTables. It could be an auxiliary directory (like upload),
// or the main directory.
class sstable_directory {
public:
    // favor chunked vectors when dealing with file lists: they can grow to hundreds of thousands
    // of elements.
    using sstable_info_vector = utils::chunked_vector<sstables::foreign_sstable_open_info>;

    // Flags below control how to behave when scanning new SSTables.
    struct process_flags {
        bool need_mutate_level = false;
        bool throw_on_missing_toc = false;
        bool enable_dangerous_direct_import_of_cassandra_counters = false;
        bool allow_loading_materialized_view = false;
        bool sort_sstables_according_to_owner = true;
    };

    class components_lister {
        directory_lister _lister;
    public:
        future<sstring> get();
        components_lister(std::filesystem::path dir);
        future<> close();
    };

private:
    using scan_multimap = std::unordered_multimap<generation_type, std::filesystem::path>;
    using scan_descriptors = utils::chunked_vector<sstables::entry_descriptor>;
    using scan_descriptors_map = std::unordered_map<generation_type, sstables::entry_descriptor>;

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

    sstables_manager& _manager;
    schema_ptr _schema;
    std::filesystem::path _sstable_dir;
    ::io_priority_class _io_priority;
    io_error_handler_gen _error_handler_gen;

    generation_type _max_generation_seen = generation_from_value(0);
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

    future<> process_descriptor(sstables::entry_descriptor desc, process_flags flags);
    void validate(sstables::shared_sstable sst, process_flags flags) const;
    void handle_component(scan_state& state, sstables::entry_descriptor desc, std::filesystem::path filename);
    future<> remove_input_sstables_from_resharding(std::vector<sstables::shared_sstable> sstlist);
    future<> collect_output_sstables_from_resharding(std::vector<sstables::shared_sstable> resharded_sstables);

    future<> remove_input_sstables_from_reshaping(std::vector<sstables::shared_sstable> sstlist);
    future<> collect_output_sstables_from_reshaping(std::vector<sstables::shared_sstable> reshaped_sstables);

    template <typename Container, typename Func>
    requires std::is_invocable_r_v<future<>, Func, typename std::decay_t<Container>::value_type&>
    future<> parallel_for_each_restricted(Container& C, Func func);
    future<> load_foreign_sstables(sstable_info_vector info_vec);

    std::vector<sstables::shared_sstable> _unsorted_sstables;
public:
    sstable_directory(sstables_manager& manager,
            schema_ptr schema,
            std::filesystem::path sstable_dir,
            ::io_priority_class io_prio,
            io_error_handler_gen error_handler_gen);

    std::vector<sstables::shared_sstable>& get_unsorted_sstables() {
        return _unsorted_sstables;
    }

    // moves unshared SSTables that don't belong to this shard to the right shards.
    future<> move_foreign_sstables(sharded<sstable_directory>& source_directory);

    // returns what is the highest generation seen in this directory.
    generation_type highest_generation_seen() const;

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
    future<> process_sstable_dir(process_flags flags);

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
    future<> reshard(sstable_info_vector info, compaction_manager& cm, replica::table& table,
                     unsigned max_sstables_per_job, sstables::compaction_sstable_creator_fn creator,
                     compaction::owned_ranges_ptr owned_ranges_ptr = nullptr);

    using sstable_filter_func_t = std::function<bool (const sstables::shared_sstable&)>;

    // reshapes a collection of SSTables, and returns the total amount of bytes reshaped.
    future<uint64_t> reshape(compaction_manager& cm, replica::table& table,
                     sstables::compaction_sstable_creator_fn creator,
                     sstables::reshape_mode mode,
                     sstable_filter_func_t sstable_filter);

    // Store a phased operation. Usually used to keep an object alive while the directory is being
    // processed. One example is preventing table drops concurrent to the processing of this
    // directory.
    void store_phaser(utils::phased_barrier::operation op);

    // Helper function that processes all unshared SSTables belonging to this shard, respecting the
    // concurrency limit.
    // Note that this function is destructive, draining _shared_local_sstables.
    future<> do_for_each_sstable(std::function<future<>(sstables::shared_sstable)> func);

    // Helper function that processes all unshared SSTables belonging to this shard, respecting the
    // concurrency limit.
    // sstables for which `func` returns a true value are kept in _shared_local_sstables, while
    // those for which `func` returns false are erased from the list.
    future<> filter_sstables(std::function<future<bool>(sstables::shared_sstable)> func);

    // Retrieves the list of shared SSTables in this object. The list will be reset once this
    // is called.
    sstable_info_vector retrieve_shared_sstables();

    std::filesystem::path sstable_dir() const noexcept {
        return _sstable_dir;
    }

    // When we compact sstables, we have to atomically instantiate the new
    // sstable and delete the old ones.  Otherwise, if we compact A+B into C,
    // and if A contained some data that was tombstoned by B, and if B was
    // deleted but A survived, then data from A will be resurrected.
    //
    // There are two violators of the requirement to atomically delete
    // sstables: first sstable instantiation and deletion on disk is atomic
    // only wrt. itself, not other sstables, and second when an sstable is
    // shared among shard, so actual on-disk deletion of an sstable is deferred
    // until all shards agree it can be deleted.
    //
    // This function only solves the second problem for now.
    static future<> delete_atomically(std::vector<shared_sstable> ssts);
    static future<> replay_pending_delete_log(std::filesystem::path log_file);
};

}
