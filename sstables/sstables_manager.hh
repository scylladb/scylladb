/*
 * Copyright (C) 2019-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/gate.hh>

#include "utils/assert.hh"
#include "utils/disk-error-handler.hh"
#include "db_clock.hh"
#include "sstables/sstables.hh"
#include "sstables/shareable_components.hh"
#include "sstables/shared_sstable.hh"
#include "sstables/version.hh"
#include "db/cache_tracker.hh"
#include "locator/host_id.hh"
#include "reader_concurrency_semaphore.hh"
#include "utils/s3/creds.hh"
#include <boost/intrusive/list.hpp>
#include "sstable_compressor_factory.hh"
#include "sstables/sstables_manager_subscription.hh"

namespace db {

class large_data_handler;
class config;
class object_storage_endpoint_param;

}   // namespace db

namespace s3 { class client; }

namespace gms { class feature_service; }

namespace sstables {

class directory_semaphore;
using schema_ptr = lw_shared_ptr<const schema>;
using shareable_components_ptr = lw_shared_ptr<shareable_components>;

static constexpr size_t default_sstable_buffer_size = 128 * 1024;

class storage_manager : public peering_sharded_service<storage_manager> {
    struct config_updater {
        serialized_action action;
        utils::observer<std::vector<db::object_storage_endpoint_param>> observer;
        config_updater(const db::config& cfg, storage_manager&);
    };

    struct s3_endpoint {
        s3::endpoint_config_ptr cfg;
        shared_ptr<s3::client> client;
        s3_endpoint(s3::endpoint_config_ptr c) noexcept : cfg(std::move(c)) {}
    };

    semaphore _s3_clients_memory;
    std::unordered_map<sstring, s3_endpoint> _s3_endpoints;
    std::unique_ptr<config_updater> _config_updater;

    future<> update_config(const db::config&);

public:
    struct config {
        size_t s3_clients_memory = 16 << 20; // 16M by default
    };

    storage_manager(const db::config&, config cfg);
    shared_ptr<s3::client> get_endpoint_client(sstring endpoint);
    bool is_known_endpoint(sstring endpoint) const;
    future<> stop();
};

class sstables_manager {
    using list_type = boost::intrusive::list<sstable,
            boost::intrusive::member_hook<sstable, sstable::manager_list_link_type, &sstable::_manager_list_link>,
            boost::intrusive::constant_time_size<false>>;
    using set_type = boost::intrusive::set<sstable,
            boost::intrusive::member_hook<sstable, sstable::manager_set_link_type, &sstable::_manager_set_link>,
            boost::intrusive::constant_time_size<false>,
            boost::intrusive::compare<sstable::lesser_reclaimed_memory>>;

private:
    enum class notification_event_type {
        // Note: other event types like "added" may be needed in the future
        deleted
    };
    using signal_type = boost::signals2::signal_type<void (sstables::generation_type, notification_event_type), boost::signals2::keywords::mutex_type<boost::signals2::dummy_mutex>>::type;

    storage_manager* _storage;
    size_t _available_memory;
    db::large_data_handler& _large_data_handler;
    const db::config& _db_config;
    gms::feature_service& _features;
    // _sstables_format is the format used for writing new sstables.
    // Here we set its default value, but if we discover that all the nodes
    // in the cluster support a newer format, _sstables_format will be set to
    // that format. read_sstables_format() also overwrites _sstables_format
    // if an sstable format was chosen earlier (and this choice was persisted
    // in the system table).
    sstable_version_types _format = sstable_version_types::me;

    // _active and _undergoing_close are used in scylla-gdb.py to fetch all sstables
    // on current shard using "scylla sstables" command. If those fields are renamed,
    // update scylla-gdb.py as well.
    list_type _active;
    list_type _undergoing_close;

    // Total reclaimable memory used by components of sstables in _active list
    size_t _total_reclaimable_memory{0};
    // Total memory reclaimed so far across all sstables
    size_t _total_memory_reclaimed{0};
    // Set of sstables from which memory has been reclaimed
    set_type _reclaimed;
    // Condition variable that needs to be notified when an sstable is created or deleted
    seastar::condition_variable _components_memory_change_event;
    future<> _components_reloader_status = make_ready_future<>();

    bool _closing = false;
    promise<> _done;
    cache_tracker& _cache_tracker;

    reader_concurrency_semaphore _sstable_metadata_concurrency_sem;
    directory_semaphore& _dir_semaphore;
    std::unique_ptr<sstables::sstables_registry> _sstables_registry;
    // This function is bound to token_metadata.get_my_id() in the database constructor,
    // it can return unset value (bool(host_id) == false) until host_id is loaded
    // after system_keyspace initialization.
    noncopyable_function<locator::host_id()> _resolve_host_id;

    scheduling_group _maintenance_sg;

    sstable_compressor_factory& _compressor_factory;

    const abort_source& _abort;

    named_gate _signal_gate;
    signal_type _signal_source;

public:
    explicit sstables_manager(sstring name, db::large_data_handler& large_data_handler, const db::config& dbcfg, gms::feature_service& feat, cache_tracker&, size_t available_memory, directory_semaphore& dir_sem,
                              noncopyable_function<locator::host_id()>&& resolve_host_id, sstable_compressor_factory&, const abort_source& abort, scheduling_group maintenance_sg = current_scheduling_group(), storage_manager* shared = nullptr);
    virtual ~sstables_manager();

    shared_sstable make_sstable(schema_ptr schema,
            const data_dictionary::storage_options& storage,
            generation_type generation,
            sstable_state state = sstable_state::normal,
            sstable_version_types v = get_highest_sstable_version(),
            sstable_format_types f = sstable_format_types::big,
            db_clock::time_point now = db_clock::now(),
            io_error_handler_gen error_handler_gen = default_io_error_handler_gen(),
            size_t buffer_size = default_sstable_buffer_size);

    shared_ptr<s3::client> get_endpoint_client(sstring endpoint) const {
        SCYLLA_ASSERT(_storage != nullptr);
        return _storage->get_endpoint_client(std::move(endpoint));
    }

    bool is_known_endpoint(sstring endpoint) const {
        SCYLLA_ASSERT(_storage != nullptr);
        return _storage->is_known_endpoint(std::move(endpoint));
    }

    virtual sstable_writer_config configure_writer(sstring origin) const;
    bool uuid_sstable_identifiers() const;
    const db::config& config() const { return _db_config; }
    cache_tracker& get_cache_tracker() { return _cache_tracker; }

    void set_format(sstable_version_types format) noexcept { _format = format; }
    sstables::sstable::version_types get_highest_supported_format() const noexcept { return _format; }

    locator::host_id get_local_host_id() const;

    reader_concurrency_semaphore& sstable_metadata_concurrency_sem() noexcept { return _sstable_metadata_concurrency_sem; }

    // Wait until all sstables managed by this sstables_manager instance
    // (previously created by make_sstable()) have been disposed of:
    //   - if they were marked for deletion, the files are deleted
    //   - in any case, the open file handles are closed
    //   - all memory resources are freed
    //
    // Note that close() will not complete until all references to all
    // sstables have been destroyed.
    future<> close();
    directory_semaphore& dir_semaphore() noexcept { return _dir_semaphore; }

    void plug_sstables_registry(std::unique_ptr<sstables_registry>) noexcept;
    void unplug_sstables_registry() noexcept;

    // Only for sstable::storage usage
    sstables::sstables_registry& sstables_registry() const noexcept {
        SCYLLA_ASSERT(_sstables_registry && "sstables_registry is not plugged");
        return *_sstables_registry;
    }

    future<> delete_atomically(std::vector<shared_sstable> ssts);
    future<lw_shared_ptr<const data_dictionary::storage_options>> init_table_storage(const schema& s, const data_dictionary::storage_options& so);
    future<> destroy_table_storage(const data_dictionary::storage_options& so);
    future<> init_keyspace_storage(const data_dictionary::storage_options& so, sstring dir);

    void validate_new_keyspace_storage_options(const data_dictionary::storage_options&);

    const abort_source& get_abort_source() const noexcept { return _abort; }

    // To be called by the sstable to signal its unlinking
    void on_unlink(sstable* sst);

    std::vector<std::filesystem::path> get_local_directories(const data_dictionary::storage_options::local& so) const;

    sstable_compressor_factory& get_compressor_factory() const { return _compressor_factory; }

    // unsubscribe happens automatically when the handler is destroyed
    void subscribe(sstables_manager_event_handler& handler);

private:
    void add(sstable* sst);
    // Transition the sstable to the "inactive" state. It has no
    // visible references at this point, and only waits for its
    // files to be deleted (if necessary) and closed.
    void deactivate(sstable* sst);
    void remove(sstable* sst);
    void maybe_done();

    static constexpr size_t max_count_sstable_metadata_concurrent_reads{10};
    // Allow at most 10% of memory to be filled with such reads.
    size_t max_memory_sstable_metadata_concurrent_reads(size_t available_memory) { return available_memory * 0.1; }

    // Increment the _total_reclaimable_memory with the new SSTable's reclaimable memory
    void increment_total_reclaimable_memory(sstable* sst);
    // Fiber to reload reclaimed components back into memory when memory becomes available.
    future<> components_reclaim_reload_fiber();
    // Reclaims components from SSTables if total memory usage exceeds the threshold.
    future<> maybe_reclaim_components();
    // Reloads components from reclaimed SSTables if memory is available.
    future<> maybe_reload_components();
    size_t get_components_memory_reclaim_threshold() const;
    size_t get_memory_available_for_reclaimable_components() const;
    // Reclaim memory from the SSTable and remove it from the memory tracking metrics.
    // The method is idempotent and for an sstable that is deleted, it is called both
    // during unlink and during deactivation.
    void reclaim_memory_and_stop_tracking_sstable(sstable* sst);
private:
    db::large_data_handler& get_large_data_handler() const {
        return _large_data_handler;
    }
    friend class sstable;

    // Allow testing private methods/variables via test_env_sstables_manager
    friend class test_env_sstables_manager;
};

}   // namespace sstables
