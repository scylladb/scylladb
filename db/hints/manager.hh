/*
 * Modified by ScyllaDB
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <unordered_map>
#include <vector>
#include <list>
#include <chrono>
#include <optional>
#include <map>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/timer.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_mutex.hh>
#include <seastar/core/abort_source.hh>
#include "inet_address_vectors.hh"
#include "db/commitlog/commitlog.hh"
#include "utils/loading_shared_values.hh"
#include "db/hints/resource_manager.hh"
#include "db/hints/host_filter.hh"
#include "db/hints/sync_point.hh"
#include "locator/abstract_replication_strategy.hh"
#include "db/hints/internal/common.hh"
#include "db/hints/internal/hint_sender.hh"

class fragmented_temporary_buffer;

namespace utils {
class directories;
}

namespace gms {
class gossiper;
}

namespace db {
namespace hints {

namespace internal {
class hint_sender;
} // namespace internal

using node_to_hint_store_factory_type = utils::loading_shared_values<internal::endpoint_id, db::commitlog>;
using hints_store_ptr = node_to_hint_store_factory_type::entry_ptr;
using hint_entry_reader = commitlog_entry_reader;

/// A helper class which tracks hints directory creation
/// and allows to perform hints directory initialization lazily.
class directory_initializer {
private:
    class impl;
    ::std::shared_ptr<impl> _impl;

    directory_initializer(::std::shared_ptr<impl> impl);

public:
    /// Creates an initializer that does nothing. Useful in tests.
    static directory_initializer make_dummy();
    static future<directory_initializer> make(utils::directories& dirs, sstring hints_directory);

    ~directory_initializer();
    future<> ensure_created_and_verified();
    future<> ensure_rebalanced();
};

class manager {
private:
    using hint_stats = internal::hint_stats;
    using endpoint_id = internal::endpoint_id;

    // map: shard -> segments
    using hints_ep_segments_map = std::unordered_map<unsigned, std::list<fs::path>>;
    // map: IP -> map: shard -> segments
    using hints_segments_map = std::unordered_map<sstring, hints_ep_segments_map>;

    using drain = internal::drain;

    friend class internal::hint_sender;
    friend class space_watchdog;

public:
    class end_point_hints_manager {
    private:
        friend class internal::hint_sender;
        using hint_sender = internal::hint_sender;
    private:
        endpoint_id _key;
        manager& _shard_manager;
        hints_store_ptr _hints_store_anchor;
        seastar::gate _store_gate;
        lw_shared_ptr<seastar::shared_mutex> _file_update_mutex_ptr;
        seastar::shared_mutex& _file_update_mutex;

        enum class state {
            can_hint,               // hinting is currently allowed (used by the space_watchdog)
            stopping,               // stopping is in progress (stop() method has been called)
            stopped                 // stop() has completed
        };

        using state_set = enum_set<super_enum<state,
            state::can_hint,
            state::stopping,
            state::stopped>>;

        state_set _state;
        const fs::path _hints_dir;
        uint64_t _hints_in_progress = 0;
        db::replay_position _last_written_rp;
        hint_sender _sender;

    public:
        end_point_hints_manager(const endpoint_id& key, manager& shard_manager);
        end_point_hints_manager(end_point_hints_manager&&);
        ~end_point_hints_manager();

        const endpoint_id& end_point_key() const noexcept {
            return _key;
        }

        /// \brief Get the corresponding hints_store object. Create it if needed.
        /// \note Must be called under the \ref _file_update_mutex.
        /// \return The corresponding hints_store object.
        future<hints_store_ptr> get_or_load();

        /// \brief Store a single mutation hint.
        /// \param s column family descriptor
        /// \param fm frozen mutation object
        /// \param tr_state trace_state handle
        /// \return FALSE if hint is definitely not going to be stored
        bool store_hint(schema_ptr s, lw_shared_ptr<const frozen_mutation> fm, tracing::trace_state_ptr tr_state) noexcept;

        /// \brief Populates the _segments_to_replay list.
        ///  Populates the _segments_to_replay list with the names of the files in the <manager hints files directory> directory
        ///  in the order they should be sent out.
        ///
        /// \return Ready future when end point hints manager is initialized.
        future<> populate_segments_to_replay();

        /// \brief Waits till all writers complete and shuts down the hints store. Drains hints if needed.
        ///
        /// If "draining" is requested - sends all pending hints out.
        ///
        /// When hints are being drained we will not stop sending after a single hint sending has failed and will continue sending hints
        /// till the end of the current segment. After that we will remove the current segment and move to the next one till
        /// there isn't any segment left.
        ///
        /// \param should_drain is drain::yes - drain all pending hints
        /// \return Ready future when all operations are complete
        future<> stop(drain should_drain = drain::no) noexcept;

        /// \brief Start the timer.
        void start();

        /// \return Number of in-flight (towards the file) hints.
        uint64_t hints_in_progress() const noexcept {
            return _hints_in_progress;
        }

        bool replay_allowed() const noexcept {
            return _shard_manager.replay_allowed();
        }

        bool can_hint() const noexcept {
            return _state.contains(state::can_hint);
        }

        void allow_hints() noexcept {
            _state.set(state::can_hint);
        }

        void forbid_hints() noexcept {
            _state.remove(state::can_hint);
        }

        void set_stopping() noexcept {
            _state.set(state::stopping);
        }

        bool stopping() const noexcept {
            return _state.contains(state::stopping);
        }

        void set_stopped() noexcept {
            _state.set(state::stopped);
        }

        void clear_stopped() noexcept {
            _state.remove(state::stopped);
        }

        bool stopped() const noexcept {
            return _state.contains(state::stopped);
        }

        /// \brief Returns replay position of the most recently written hint.
        ///
        /// If there weren't any hints written during this endpoint manager's lifetime, a zero replay_position is returned.
        db::replay_position last_written_replay_position() const {
            return _last_written_rp;
        }

        /// \brief Waits until hints are replayed up to a given replay position, or given abort source is triggered.
        future<> wait_until_hints_are_replayed_up_to(abort_source& as, db::replay_position up_to_rp) {
            return _sender.wait_until_hints_are_replayed_up_to(as, up_to_rp);
        }

        /// \brief Safely runs a given functor under the file_update_mutex of \ref ep_man
        ///
        /// Runs a given functor under the file_update_mutex of the given end_point_hints_manager instance.
        /// This function is safe even if \ref ep_man gets destroyed before the future this function returns resolves
        /// (as long as the \ref func call itself is safe).
        ///
        /// \tparam Func Functor type.
        /// \param ep_man end_point_hints_manager instance which file_update_mutex we want to lock.
        /// \param func Functor to run under the lock.
        /// \return Whatever \ref func returns.
        template <typename Func>
        friend inline auto with_file_update_mutex(end_point_hints_manager& ep_man, Func&& func) {
            return with_lock(*ep_man._file_update_mutex_ptr, std::forward<Func>(func)).finally([lock_ptr = ep_man._file_update_mutex_ptr] {});
        }

        const fs::path& hints_dir() const noexcept {
            return _hints_dir;
        }

    private:
        seastar::shared_mutex& file_update_mutex() noexcept {
            return _file_update_mutex;
        }

        /// \brief Creates a new hints store object.
        ///
        /// - Creates a hints store directory if doesn't exist: <shard_hints_dir>/<ep_key>
        /// - Creates a store object.
        /// - Populate _segments_to_replay if it's empty.
        ///
        /// \return A new hints store object.
        future<commitlog> add_store() noexcept;

        /// \brief Flushes all hints written so far to the disk.
        ///  - Repopulates the _segments_to_replay list if needed.
        ///
        /// \return Ready future when the procedure above completes.
        future<> flush_current_hints() noexcept;

        hint_stats& shard_stats() {
            return _shard_manager._stats;
        }

        resource_manager& shard_resource_manager() {
            return _shard_manager._resource_manager;
        }
    };

    enum class state {
        started,                // hinting is currently allowed (start() call is complete)
        replay_allowed,         // replaying (hints sending) is allowed
        draining_all,           // hinting is not allowed - all ep managers are being stopped because this node is leaving the cluster
        stopping                // hinting is not allowed - stopping is in progress (stop() method has been called)
    };

    using state_set = enum_set<super_enum<state,
        state::started,
        state::replay_allowed,
        state::draining_all,
        state::stopping>>;

private:
    using ep_managers_map_type = std::unordered_map<endpoint_id, end_point_hints_manager>;

public:
    static const std::string FILENAME_PREFIX;
    // Non-const - can be modified with an error injection.
    static std::chrono::seconds hints_flush_period;
    static const std::chrono::seconds hint_file_write_timeout;

private:
    static constexpr uint64_t max_size_of_hints_in_progress = 10 * 1024 * 1024; // 10MB
    state_set _state;
    const fs::path _hints_dir;
    dev_t _hints_dir_device_id = 0;

    node_to_hint_store_factory_type _store_factory;
    host_filter _host_filter;
    shared_ptr<service::storage_proxy> _proxy_anchor;
    shared_ptr<gms::gossiper> _gossiper_anchor;
    int64_t _max_hint_window_us = 0;
    replica::database& _local_db;

    seastar::gate _draining_eps_gate; // gate used to control the progress of ep_managers stopping not in the context of manager::stop() call

    resource_manager& _resource_manager;

    ep_managers_map_type _ep_managers;
    hint_stats _stats;
    seastar::metrics::metric_groups _metrics;
    std::unordered_set<endpoint_id> _eps_with_pending_hints;
    seastar::named_semaphore _drain_lock = {1, named_semaphore_exception_factory{"drain lock"}};

public:
    manager(sstring hints_directory, host_filter filter, int64_t max_hint_window_ms, resource_manager&res_manager, sharded<replica::database>& db);
    virtual ~manager();
    manager(manager&&) = delete;
    manager& operator=(manager&&) = delete;
    void register_metrics(const sstring& group_name);
    future<> start(shared_ptr<service::storage_proxy> proxy_ptr, shared_ptr<gms::gossiper> gossiper_ptr);
    future<> stop();
    bool store_hint(endpoint_id ep, schema_ptr s, lw_shared_ptr<const frozen_mutation> fm, tracing::trace_state_ptr tr_state) noexcept;

    /// \brief Changes the host_filter currently used, stopping and starting ep_managers relevant to the new host_filter.
    /// \param filter the new host_filter
    /// \return A future that resolves when the operation is complete.
    future<> change_host_filter(host_filter filter);

    const host_filter& get_host_filter() const noexcept {
        return _host_filter;
    }

    /// \brief Check if a hint may be generated to the give end point
    /// \param ep end point to check
    /// \return true if we should generate the hint to the given end point if it becomes unavailable
    bool can_hint_for(endpoint_id ep) const noexcept;

    /// \brief Check if there aren't too many in-flight hints
    ///
    /// This function checks if there are too many "in-flight" hints on the current shard - hints that are being stored
    /// and which storing is not complete yet. This is meant to stabilize the memory consumption of the hints storing path
    /// which is initialed from the storage_proxy WRITE flow. storage_proxy is going to check this condition and if it
    /// returns TRUE it won't attempt any new WRITEs thus eliminating the possibility of new hints generation. If new hints
    /// are not generated the amount of in-flight hints amount and thus the memory they are consuming is going to drop eventualy
    /// because the hints are going to be either stored or dropped. After that the things are going to get back to normal again.
    ///
    /// Note that we can't consider the disk usage consumption here because the disk usage is not promissed to drop down shortly
    /// because it requires the remote node to be UP.
    ///
    /// \param ep end point to check
    /// \return TRUE if we are allowed to generate hint to the given end point but there are too many in-flight hints
    bool too_many_in_flight_hints_for(endpoint_id ep) const noexcept;

    /// \brief Check if DC \param ep belongs to is "hintable"
    /// \param ep End point identificator
    /// \return TRUE if hints are allowed to be generated to \param ep.
    bool check_dc_for(endpoint_id ep) const noexcept;

    /// \brief Checks if hints are disabled for all endpoints
    /// \return TRUE if hints are disabled.
    bool is_disabled_for_all() const noexcept {
        return _host_filter.is_disabled_for_all();
    }

    /// \return Size of mutations of hints in-flight (to the disk) at the moment.
    uint64_t size_of_hints_in_progress() const noexcept {
        return _stats.size_of_hints_in_progress;
    }

    /// \brief Get the number of in-flight (to the disk) hints to a given end point.
    /// \param ep End point identificator
    /// \return Number of hints in-flight to \param ep.
    uint64_t hints_in_progress_for(endpoint_id ep) const noexcept {
        auto it = find_ep_manager(ep);
        if (it == ep_managers_end()) {
            return 0;
        }
        return it->second.hints_in_progress();
    }

    void add_ep_with_pending_hints(endpoint_id key) {
        _eps_with_pending_hints.insert(key);
    }

    void clear_eps_with_pending_hints() {
        _eps_with_pending_hints.clear();
        _eps_with_pending_hints.reserve(_ep_managers.size());
    }

    bool has_ep_with_pending_hints(endpoint_id key) const {
        return _eps_with_pending_hints.contains(key);
    }

    size_t ep_managers_size() const {
        return _ep_managers.size();
    }

    const fs::path& hints_dir() const {
        return _hints_dir;
    }

    dev_t hints_dir_device_id() const {
        return _hints_dir_device_id;
    }

    seastar::named_semaphore& drain_lock() noexcept {
        return _drain_lock;
    }

    void allow_hints();
    void forbid_hints();
    void forbid_hints_for_eps_with_pending_hints();

    void allow_replaying() noexcept {
        _state.set(state::replay_allowed);
    }

    /// \brief Returns a set of replay positions for hint queues towards endpoints from the `target_hosts`.
    sync_point::shard_rps calculate_current_sync_point(const std::vector<endpoint_id>& target_hosts) const;

    /// \brief Waits until hint replay reach replay positions described in `rps`.
    future<> wait_for_sync_point(abort_source& as, const sync_point::shard_rps& rps);

    /// \brief Creates an object which aids in hints directory initialization.
    /// This object can saafely be copied and used from any shard.
    /// \arg dirs The utils::directories object, used to create and lock hints directories
    /// \arg hints_directory The directory with hints which should be initialized
    directory_initializer make_directory_initializer(utils::directories& dirs, fs::path hints_directory);

    /// \brief Rebalance hints segments among all present shards.
    ///
    /// The difference between the number of segments on every two shard will be not greater than 1 after the
    /// rebalancing.
    ///
    /// Removes the sub-directories of \ref hints_directory that correspond to shards that are not relevant any more
    /// (re-sharding to a lower shards number case).
    ///
    /// Complexity: O(N+K), where N is a total number of present hints' segments and
    ///                           K = <number of shards during the previous boot> * <number of end points for which hints where ever created>
    ///
    /// \param hints_directory A hints directory to rebalance
    /// \return A future that resolves when the operation is complete.
    static future<> rebalance(fs::path hints_directory);

private:
    future<> compute_hints_dir_device_id();

    /// \brief Scan the given hints directory and build the map of all present hints segments.
    ///
    /// Complexity: O(N+K), where N is a total number of present hints' segments and
    ///                           K = <number of shards during the previous boot> * <number of end points for which hints where ever created>
    ///
    /// \note Should be called from a seastar::thread context.
    ///
    /// \param hints_directory directory to scan
    /// \return a map: ep -> map: shard -> segments (full paths)
    static hints_segments_map get_current_hints_segments(const fs::path& hints_directory);

    /// \brief Rebalance hints segments for a given (destination) end point
    ///
    /// This method is going to consume files from the \ref segments_to_move and distribute them between the present
    /// shards (taking into an account the \ref ep_segments state - there may be zero or more segments that belong to a
    /// particular shard in it) until we either achieve the requested \ref segments_per_shard level on each shard
    /// or until we are out of files to move.
    ///
    /// As a result (in addition to the actual state on the disk) both \ref ep_segments and \ref segments_to_move are going
    /// to be modified.
    ///
    /// Complexity: O(N), where N is a total number of present hints' segments for the \ref ep end point (as a destination).
    ///
    /// \note Should be called from a seastar::thread context.
    ///
    /// \param ep destination end point ID (a string with its IP address)
    /// \param segments_per_shard number of hints segments per-shard we want to achieve
    /// \param hints_directory a root hints directory
    /// \param ep_segments a map that was originally built by get_current_hints_segments() for this end point
    /// \param segments_to_move a list of segments we are allowed to move
    static void rebalance_segments_for(
            const sstring& ep,
            size_t segments_per_shard,
            const fs::path& hints_directory,
            hints_ep_segments_map& ep_segments,
            std::list<fs::path>& segments_to_move);

    /// \brief Rebalance all present hints segments.
    ///
    /// The difference between the number of segments on every two shard will be not greater than 1 after the
    /// rebalancing.
    ///
    /// Complexity: O(N), where N is a total number of present hints' segments.
    ///
    /// \note Should be called from a seastar::thread context.
    ///
    /// \param hints_directory a root hints directory
    /// \param segments_map a map that was built by get_current_hints_segments()
    static void rebalance_segments(const fs::path& hints_directory, hints_segments_map& segments_map);

    /// \brief Remove sub-directories of shards that are not relevant any more (re-sharding to a lower number of shards case).
    ///
    /// Complexity: O(S*E), where S is a number of shards during the previous boot and
    ///                           E is a number of end points for which hints where ever created.
    ///
    /// \param hints_directory a root hints directory
    static void remove_irrelevant_shards_directories(const fs::path& hints_directory);

    node_to_hint_store_factory_type& store_factory() noexcept {
        return _store_factory;
    }

    service::storage_proxy& local_storage_proxy() const noexcept {
        return *_proxy_anchor;
    }

    gms::gossiper& local_gossiper() const noexcept {
        return *_gossiper_anchor;
    }

    replica::database& local_db() noexcept {
        return _local_db;
    }

    end_point_hints_manager& get_ep_manager(endpoint_id ep);
    bool have_ep_manager(endpoint_id ep) const noexcept;

public:
    /// \brief Initiate the draining when we detect that the node has left the cluster.
    ///
    /// If the node that has left is the current node - drains all pending hints to all nodes.
    /// Otherwise drains hints to the node that has left.
    ///
    /// In both cases - removes the corresponding hints' directories after all hints have been drained and erases the
    /// corresponding end_point_hints_manager objects.
    ///
    /// \param endpoint node that left the cluster
    void drain_for(endpoint_id endpoint);

private:
    void update_backlog(size_t backlog, size_t max_backlog);

    bool stopping() const noexcept {
        return _state.contains(state::stopping);
    }

    void set_stopping() noexcept {
        _state.set(state::stopping);
    }

    bool started() const noexcept {
        return _state.contains(state::started);
    }

    void set_started() noexcept {
        _state.set(state::started);
    }

    bool replay_allowed() const noexcept {
        return _state.contains(state::replay_allowed);
    }

    void set_draining_all() noexcept {
        _state.set(state::draining_all);
    }

    bool draining_all() noexcept {
        return _state.contains(state::draining_all);
    }

public:
    ep_managers_map_type::iterator find_ep_manager(endpoint_id ep_key) noexcept {
        return _ep_managers.find(ep_key);
    }

    ep_managers_map_type::const_iterator find_ep_manager(endpoint_id ep_key) const noexcept {
        return _ep_managers.find(ep_key);
    }

    ep_managers_map_type::iterator ep_managers_end() noexcept {
        return _ep_managers.end();
    }

    ep_managers_map_type::const_iterator ep_managers_end() const noexcept {
        return _ep_managers.end();
    }
};

}
}
