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
#include "data_dictionary/storage_options.hh"
#include "sstables/open_info.hh"                 // for entry_descriptor and foreign_sstable_open_info, chunked_vector wants to know if they are move constructible
#include "utils/chunked_vector.hh"
#include "utils/phased_barrier.hh"
#include "utils/disk-error-handler.hh"
#include "sstables/generation_type.hh"
#include "sstables/sstables_registry.hh"

class compaction_manager;
namespace s3 { class client; }

namespace sstables {

enum class sstable_state;
class storage;
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

    template <std::ranges::range Container, typename Func>
    requires std::is_invocable_r_v<future<>, Func, typename std::ranges::range_value_t<Container>&>
    future<> parallel_for_each(Container& c, Func func) {
        co_await max_concurrent_for_each(c, _concurrency, [&] (auto& el) -> future<>{
            auto units = co_await get_units(_sem, 1);
            co_await func(el);
        });
    }
};

// Handles a directory containing SSTables. It could be an auxiliary directory (like upload),
// or the main directory.
class sstable_directory {
public:
    // favor chunked vectors when dealing with file lists: they can grow to hundreds of thousands
    // of elements.
    using sstable_open_info_vector = utils::chunked_vector<sstables::foreign_sstable_open_info>;
    using sstable_entry_descriptor_vector = utils::chunked_vector<sstables::entry_descriptor>;

    // Flags below control how to behave when scanning new SSTables.
    struct process_flags {
        bool need_mutate_level = false;
        bool throw_on_missing_toc = false;
        bool enable_dangerous_direct_import_of_cassandra_counters = false;
        bool allow_loading_materialized_view = false;
        bool sort_sstables_according_to_owner = true;
        bool garbage_collect = false;
        sstables::sstable_open_config sstable_open_config;
    };

    class components_lister {
    public:
        virtual future<> process(sstable_directory& directory, process_flags flags) = 0;
        virtual future<> commit() = 0;
        virtual future<> prepare(sstable_directory&, process_flags, storage&) = 0;
        virtual ~components_lister() {}
    };

    class filesystem_components_lister final : public components_lister {
        struct scan_state {
            using scan_multimap = std::unordered_multimap<generation_type, std::filesystem::path>;
            using scan_descriptors = utils::chunked_vector<sstables::entry_descriptor>;
            using scan_descriptors_map = std::unordered_map<generation_type, sstables::entry_descriptor>;

            scan_multimap generations_found;
            scan_descriptors temp_toc_found;
            scan_descriptors_map descriptors;

            // SSTable files to be deleted: things with a Temporary TOC, missing TOC files,
            // TemporaryStatistics, etc. Not part of the scan state, because we want to do a 2-phase
            // delete: maybe one of the shards will have signaled an error. And in the case of an error
            // we don't want to delete anything.
            std::unordered_set<sstring> files_for_removal;
        };

        void handle(sstables::entry_descriptor desc, std::filesystem::path filename);

        std::filesystem::path _directory;
        std::unique_ptr<scan_state> _state;
        shared_ptr<s3::client> _client;
        sstring _bucket;

        future<> garbage_collect(storage&);
        future<> cleanup_column_family_temp_sst_dirs();
        future<> handle_sstables_pending_delete();
        future<> replay_pending_delete_log(std::filesystem::path log_file);


    public:
        filesystem_components_lister(std::filesystem::path dir);
        filesystem_components_lister(std::filesystem::path dir, sstables_manager&, const data_dictionary::storage_options::s3&);

        virtual future<> process(sstable_directory& directory, process_flags flags) override;
        virtual future<> commit() override;
        virtual future<> prepare(sstable_directory&, process_flags, storage&) override;
    };

    class sstables_registry_components_lister final : public components_lister {
        sstables_registry& _sstables_registry;
        sstring _location;

        future<> garbage_collect(storage&);

    public:
        sstables_registry_components_lister(sstables::sstables_registry& sstables_registry, sstring location);

        virtual future<> process(sstable_directory& directory, process_flags flags) override;
        virtual future<> commit() override;
        virtual future<> prepare(sstable_directory&, process_flags, storage&) override;
    };

private:

    // prevents an object that respects a phaser (usually a table) from disappearing in the middle of the operation.
    // Will be destroyed when this object is destroyed.
    std::optional<utils::phased_barrier::operation> _operation_barrier;

    sstables_manager& _manager;
    schema_ptr _schema;
    lw_shared_ptr<const data_dictionary::storage_options> _storage_opts;
    sstring _table_dir;
    sstable_state _state;
    io_error_handler_gen _error_handler_gen;
    std::unique_ptr<storage> _storage;
    std::unique_ptr<components_lister> _lister;
    std::unique_ptr<dht::sharder> _sharder_ptr;
    const dht::sharder& _sharder;

    generation_type _max_generation_seen;
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
    std::vector<sstable_entry_descriptor_vector> _unshared_remote_sstables;

    // SSTables that are shared. Stored as foreign_sstable_open_info objects. Reason is those are
    // the SSTables that we found, and not necessarily the ones we will reshard. We want to balance
    // the amount of data resharded per shard, so a coordinator may redistribute this.
    sstable_open_info_vector _shared_sstable_info;

    std::vector<sstables::shared_sstable> _unsorted_sstables;
private:
    std::unique_ptr<sstable_directory::components_lister> make_components_lister();

    future<> process_descriptor(sstables::entry_descriptor desc, process_flags flags);
    void validate(sstables::shared_sstable sst, process_flags flags) const;
    future<sstables::shared_sstable> load_sstable(sstables::entry_descriptor desc, sstables::sstable_open_config cfg = {}) const;

    future<> load_foreign_sstables(sstable_entry_descriptor_vector info_vec);

    sstable_directory(sstables_manager& manager,
          schema_ptr schema,
          std::variant<std::unique_ptr<dht::sharder>, const dht::sharder*> sharder,
          lw_shared_ptr<const data_dictionary::storage_options> storage_opts,
          sstring table_dir,
          sstable_state state,
          io_error_handler_gen error_handler_gen);
public:
    sstable_directory(replica::table& table,
            sstable_state state,
            io_error_handler_gen error_handler_gen);
    sstable_directory(replica::table& table,
            lw_shared_ptr<const data_dictionary::storage_options> storage_opts,
            sstring table_dir,
            io_error_handler_gen error_handler_gen);
    sstable_directory(sstables_manager& manager,
            schema_ptr schema,
            const dht::sharder& sharder,
            sstring table_dir,
            sstable_state state,
            io_error_handler_gen error_handler_gen);

    std::vector<sstables::shared_sstable>& get_unsorted_sstables() {
        return _unsorted_sstables;
    }

    future<shared_sstable> load_foreign_sstable(foreign_sstable_open_info& info);

    // moves unshared SSTables that don't belong to this shard to the right shards.
    future<> move_foreign_sstables(sharded<sstable_directory>& source_directory);

    // returns what is the highest generation seen in this directory.
    generation_type highest_generation_seen() const;

    // returns what is the highest version seen in this directory.
    sstables::sstable_version_types highest_version_seen() const;

    future<> prepare(process_flags flags);

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

    // If files were scheduled to be removed, they will be removed after this call.
    future<> commit_directory_changes();

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
    sstable_open_info_vector retrieve_shared_sstables();
    std::vector<sstables::shared_sstable>& get_unshared_local_sstables() { return _unshared_local_sstables; }

    future<> remove_sstables(std::vector<sstables::shared_sstable> sstlist);
    future<> remove_unshared_sstables(std::vector<sstables::shared_sstable> sstlist);

    using can_be_remote = bool_class<struct can_be_remote_tag>;
    future<> collect_output_unshared_sstables(std::vector<sstables::shared_sstable> resharded_sstables, can_be_remote);

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

    // Creates the deletion log for atomic deletion of sstables (helper for the
    // above function that's also used by tests)
    // Returns an unordered_map of <directory with sstables, logfile_name> for every sstable prefix.
    // Currently, atomicity is guaranteed only within each unique prefix and not across prefixes (See #18862)
    static future<std::unordered_map<sstring, sstring>> create_pending_deletion_log(const std::vector<shared_sstable>& ssts);

    static bool compare_sstable_storage_prefix(const sstring& a, const sstring& b) noexcept;
};

future<sstables::generation_type> highest_generation_seen(sharded<sstables::sstable_directory>& directory);

}
