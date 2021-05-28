/*
 * Copyright (C) 2021 ScyllaDB
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

#include <vector>
#include <unordered_map>
#include <memory>
#include <exception>
#include <cstdint>
#include <functional>

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/timer.hh>
#include <seastar/core/abort_source.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/util/noncopyable_function.hh>
#include "seastarx.hh"
#include "gms/inet_address.hh"
#include "utils/UUID.hh"
#include "message/messaging_service_fwd.hh"

namespace db {
namespace hints {

class manager;

using sync_point_id = utils::UUID;

/// \brief A service which manages logic related to hint sync points.
///
/// The purpose of this service is to make it possible to wait until hints
/// are replayed. This is done with help of coordinator-side sync points
/// and follower-side sync points.
///
/// A follower-side sync point tracks the progress of hints sent towards
/// a chosen set of nodes (`target_nodes`). Other nodes can use it
/// to check if hints were replayed, are still being replayed or an error
/// was encountered while hints were being replayed.
///
/// A coordinator-side sync point tracks progress of follower-side sync points
/// on a chosen set of nodes. Similarly to follower-side sync point, it can
/// be used to check if hints were replayed on all of the tracked nodes,
/// are still being replayed on some of the nodes or an error was encountered
/// on some of the nodes.
///
/// Coordinator-side sync points are created, checked and deleted by users,
/// while follower-side sync points are managed by the sync point service.
///
/// Sync points will only wait for hints which were written at the moment
/// of the point's creation. If more hints are created while the point exists,
/// they will not be waited for.
class sync_point_service : public seastar::peering_sharded_service<sync_point_service> {
public:
    using clock_type = lowres_clock;

private:
    struct task;
    using task_map = std::unordered_map<sync_point_id, task>;

    /// \brief An asynchronous, cancellable task which is kept until it is deleted.
    ///
    /// The task takes a function which takes an abort_source and returns a future,
    /// then executes it and keeps the result of the function (success or exception).
    ///
    /// The task can be cancelled at any point. If it is cancelled before the function
    /// passed to it completes, the abort_source passed to the function is triggered.
    /// After being cancelled, the task waits until the internal function completes
    /// and then removes itself from the parent task map.
    ///
    /// It is responsibility of the task to remove itself from the parent task map,
    /// and tasks should not be removed from outside.
    struct task {
    private:
        /// \brief A future which resolves after it stops running.
        ///
        /// This future will contain the result of the task which can either
        /// be a success or an failure (in the form of an exception).
        shared_future<> _result;

        /// \brief A future which resolves when the task is finished.
        ///
        /// The task is considered finished after it stops running
        /// and the task is removed from the parent task map.
        shared_future<> _finish;

        /// \brief Abort source used to cancel the internal function.
        abort_source _as;

        /// \brief Timer which can be used to schedule cancellation of this task.
        seastar::timer<clock_type> _expiry_timer;

        /// \brief ID of the sync point associated with this task.
        ///
        /// The task uses it to remove itself from the parent map.
        sync_point_id _id;
        /// \brief A short label describing the type of the task.
        ///
        /// Coordinators and followers use the same sync point ids,
        /// so it can be used to differentiate them in the logs.

        sstring _typ;
        /// \brief The parent task map.
        ///
        /// The task removes itself from it after finishing.
        task_map& _parent_task_map;

    public:
        /// \brief Creates a task.
        ///
        /// The task isn't running yet; use task::run() in order to run it.
        task(task_map& parent, sstring typ, sync_point_id id);

        /// \brief Runs the task with the given function.
        ///
        /// This function should only be called once in task's lifetime.
        /// The function is passed an abort source which will be triggered
        /// when the task is cancelled - when it happens, the function should
        /// abandon its work and complete ASAP, possibly with
        /// abort_requested_exception.
        void run(noncopyable_function<future<> (abort_source&)>&& f);

        /// \brief Checks the result of the task.
        ///
        /// \returns true if the task completed successfully,
        ///          false if it is still running.
        /// \throws an exception if the internal function threw an exception.
        bool check_result();

        /// \brief Waits until the task completes, with a specified timeout.
        ///
        /// \returns future which resolves after the task stops running, containing
        ///          its result (which can be an exception).
        future<> wait_for_result(clock_type::time_point timeout);

        /// \brief Cancels this task now.
        ///
        /// Triggers task's abort source. The task will asynchronously wait
        /// until it stops running and then will remove itself from the map.
        ///
        /// It is safe to call this function multiple times; only the first
        /// cancellation has an effect.
        void cancel();

        /// \brief Cancels this task now and waits until it finishes.
        ///
        /// Triggers task's abort source and waits synchronously until
        /// the task stops running and removes itself from the map.
        ///
        /// \returns A future which resolves after the task removes itself
        ///          from the parent map and is destroyed.
        future<> cancel_and_finish();

        /// \brief (Re)sets internal timer, scheduling cancellation of the task.
        ///
        /// By default, the internal timer is not running. This function
        /// can be used to implement TTL for the task.
        ///
        /// Calling this function for the second time resets the timeout.
        /// If the task was cancelled, it has no effect.
        void schedule_expiry(clock_type::duration timeout);
    };

private:
    /// \brief Collection of coordinator-side tasks.
    ///
    /// A coordinator-side task creates follower-sync points and periodically
    /// queries the followers in order to check if hints were already replayed,
    /// or an error has occurred.
    ///
    /// Coordinator tasks are scheduled for expiry after the internal function stops
    /// (see `completed_coordinator_task_ttl`).
    ///
    /// Tasks should be created on task 0 only.
    task_map _coordinator_tasks;

    /// \brief Collection of follower-side tasks.
    ///
    /// A follower-side task keeps track of the progress of hint replay on the
    /// local node.
    ///
    /// Follower tasks have a TTL set right after they are created. The TTL
    /// is refreshed every time the status of the task is queried.
    ///
    /// Tasks should be created on task 0 only.
    task_map _follower_tasks;

    std::vector<std::reference_wrapper<manager>> _managers;
    netw::messaging_service& _messaging;

    /// \brief Scheduling group used for running tasks.
    seastar::scheduling_group _task_scheduling_group;

    bool _running = false;

public:
    sync_point_service(netw::messaging_service& ms, seastar::scheduling_group sg);
    ~sync_point_service();

    /// \brief Starts the sync point service.
    future<> start();

    /// \brief Stops the sync point service.
    future<> stop();

    /// \brief Registers a hints manager.
    ///
    /// After the call, newly created sync points will wait for hints
    /// from this manager.
    ///
    /// This function can be called even if the service was not started.
    /// This function is idempotent - you can call it multiple times
    /// and the manager will be registered only once.
    void register_manager(manager& mgr);

    /// \brief Unregisters a hints manager
    ///
    /// After the call, nely created sync points will NOT wait for hints
    /// from this manager.
    /// This function can be called even if the service was not started.
    /// This function is idempotent.
    void unregister_manager(manager& mgr);

    /// \brief Creates a new coordinator-side sync point.
    ///
    /// This function only creates the sync point but does not wait on it;
    /// in order to wait for the waiting operation to complete see `wait_for_sync_point`.
    ///
    /// \param source_nodes The sync point will wait until hints _from_ that nodes
    ///                     are replayed.
    /// \param target_nodes The sync point will wait until hints _towards_ that nodes
    ///                     are replayed.
    /// \returns ID of the created sync point.
    future<sync_point_id> create_sync_point(
            std::vector<gms::inet_address> source_nodes,
            std::vector<gms::inet_address> target_nodes);
    
    /// \brief Waits for a given coordinator-side sync point.
    ///
    /// \param id ID of the sync point to wait for
    /// \param timeout Timeout after which waiting should be aborted
    /// \return A future which resolves after the sync point resolves (reports success or failure)
    /// or given timeout is reached (timed_out_exception is thrown).
    future<> wait_for_sync_point(sync_point_id id, clock_type::time_point timeout);

    /// \brief Deletes given sync point.
    ///
    /// \param id ID of the sync point to delete
    /// \return A future which resolves after the sync point is deleted.
    future<> delete_sync_point(sync_point_id id);

    /// \brief Lists IDs of currently existing sync points.
    std::vector<sync_point_id> list_sync_points() const;

private:
    /// \brief Coordinates a hint waiting operation.
    ///
    /// The coordinator-side task runs this function.
    future<> coordinate_hint_waiting(
            abort_source& as,
            sync_point_id id,
            std::vector<gms::inet_address> source_nodes,
            std::vector<gms::inet_address> target_nodes);
    
    /// \brief Waits for local hints towards `target_nodes` to be replayed.
    ///
    /// The follower-side task runs this function.
    future<> wait_for_local_hints(
            abort_source& as,
            sync_point_id id,
            std::vector<gms::inet_address> target_nodes);

    void register_rpc_verbs();
    future<> unregister_rpc_verbs();

    /// \brief Checks if the service is running, and throws if it is not.
    void check_if_is_running() const;
};

}
}
