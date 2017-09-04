/*
 * Copyright (C) 2017 ScyllaDB
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
#include <seastar/core/scheduling.hh>
#include <seastar/core/timer.hh>
#include <seastar/core/gate.hh>
#include <chrono>

// Simple proportional controller to adjust shares for processes for which a backlog can be clearly
// defined.
//
// Goal is to consume the backlog as fast as we can, but not so fast that we steal all the CPU from
// incoming requests, and at the same time minimize user-visible fluctuations in the quota.
//
// What that translates to is we'll try to keep the backlog's firt derivative at 0 (IOW, we keep
// backlog constant). As the backlog grows we increase CPU usage, decreasing CPU usage as the
// backlog diminishes.
//
// The exact point at which the controller stops determines the desired CPU usage. As the backlog
// grows and approach a maximum desired, we need to be more aggressive. We will therefore define two
// thresholds, and increase the constant as we cross them.
//
// Doing that divides the range in three (before the first, between first and second, and after
// second threshold), and we'll be slow to grow in the first region, grow normally in the second
// region, and aggressively in the third region.
//
// The constants q1 and q2 are used to determine the proportional factor at each stage.
class backlog_controller {
protected:
    struct control_point {
        float input;
        float output;
    };

    std::chrono::milliseconds _interval;
    timer<> _update_timer;

    std::vector<control_point> _control_points;

    std::function<float()> _current_backlog;

    virtual void update_controller(float quota) = 0;

    void adjust();

    backlog_controller(std::chrono::milliseconds interval, std::vector<control_point> control_points, std::function<float()> backlog)
        : _interval(interval)
        , _update_timer([this] { adjust(); })
        , _control_points({{0,0}})
        , _current_backlog(std::move(backlog))
    {
        _control_points.insert(_control_points.end(), control_points.begin(), control_points.end());
         _update_timer.arm_periodic(_interval);
    }

    // Used when the controllers are disabled. When we deprecate the --auto-adjust-flush-quota
    // parameter we can delete this constructor.
    backlog_controller() = default;
    virtual ~backlog_controller() {}
};


class backlog_cpu_controller : public backlog_controller {
public:
    struct disabled {
        seastar::scheduling_group backup;
    };

    seastar::scheduling_group scheduling_group() {
        return _current_scheduling_group;
    }

    float current_shares() const {
        return _current_shares;
    }
protected:
    unsigned _current_shares = 1;

    void update_controller(float quota) override;

    seastar::scheduling_group _scheduling_group;
    seastar::scheduling_group _current_scheduling_group;

    backlog_cpu_controller(seastar::scheduling_group sg, std::chrono::milliseconds interval, std::vector<backlog_controller::control_point> control_points, std::function<float()> backlog)
        : backlog_controller(interval, std::move(control_points), backlog)
        , _scheduling_group(sg)
        , _current_scheduling_group(_scheduling_group)
    {}

    backlog_cpu_controller(disabled d)
        : backlog_controller()
        , _scheduling_group(d.backup)
        , _current_scheduling_group(d.backup) {}
};

// Right now: the CPU controller deals with quotas, the I/O controller deals with shares.
// The I/O Controllers will be always-enabled, the CPU controllers, conditionally. So it simplifies
// things to keep them separate. When the work on the CPU controller is fully done, we can unify
// them.
class backlog_io_controller : public backlog_controller {
    const ::io_priority_class& _io_priority;
    // updating shares for an I/O class may contact another shard and returns a future.
    future<> _inflight_update;

public:
    backlog_io_controller(const ::io_priority_class& iop, std::chrono::milliseconds interval, std::vector<backlog_controller::control_point> control_points, std::function<float()> backlog)
        : backlog_controller(interval, std::move(control_points), backlog)
        , _io_priority(iop)
        , _inflight_update(make_ready_future<>())
    {}

    void update_controller(float shares) override;

    future<> shutdown() {
        return std::move(_inflight_update);
    }
};

// memtable flush CPU controller.
//
// - First threshold is the soft limit line,
// - Maximum is the point in which we'd stop consuming request,
// - Second threshold is halfway between them.
//
// Below the soft limit, we are in no particular hurry to flush, since it means we're set to
// complete flushing before we a new memtable is ready. The quota is dirty * q1, and q1 is set to a
// low number.
//
// The first half of the virtual dirty region is where we expect to be usually, so we have a low
// slope corresponding to a sluggish response between q1 * soft_limit and q2.
//
// In the second half, we're getting close to the hard dirty limit so we increase the slope and
// become more responsive, up to a maximum quota of qmax.
class flush_cpu_controller : public backlog_cpu_controller {
    static constexpr float hard_dirty_limit = 1.0f;
public:
    flush_cpu_controller(backlog_cpu_controller::disabled d) : backlog_cpu_controller(std::move(d)) {}
    flush_cpu_controller(flush_cpu_controller&&) = default;
    flush_cpu_controller(seastar::scheduling_group sg, std::chrono::milliseconds interval, float soft_limit, std::function<float()> current_dirty)
        : backlog_cpu_controller(sg, std::move(interval),
          std::vector<backlog_controller::control_point>({{soft_limit, 100}, {soft_limit + (hard_dirty_limit - soft_limit) / 2, 200} , {hard_dirty_limit, 1000}}),
          std::move(current_dirty)
        )
    {}
};

class flush_io_controller : public backlog_io_controller {
    static constexpr float hard_dirty_limit = 1.0f;
public:
    flush_io_controller(const ::io_priority_class& iop, std::chrono::milliseconds interval, float soft_limit, std::function<float()> current_backlog)
        : backlog_io_controller(iop, std::move(interval),
          std::vector<backlog_controller::control_point>({{soft_limit, 10}, {soft_limit + (hard_dirty_limit - soft_limit) / 2, 100}, {hard_dirty_limit, 1000}}),
          std::move(current_backlog)
        )
    {}
};

class compaction_io_controller : public backlog_io_controller {
public:
    static constexpr unsigned normalization_factor = 10;
    compaction_io_controller(const ::io_priority_class& iop, std::chrono::milliseconds interval, std::function<float()> current_backlog)
        : backlog_io_controller(iop, std::move(interval),
          std::vector<backlog_controller::control_point>({{0.5, 10}, {1.5, 100} , {normalization_factor, 1000}}),
          std::move(current_backlog)
        )
    {}
};
