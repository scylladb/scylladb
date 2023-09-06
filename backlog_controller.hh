/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once
#include <seastar/core/scheduling.hh>
#include <seastar/core/timer.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/file.hh>
#include <chrono>
#include <cmath>

#include "seastarx.hh"

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
public:
    using scheduling_group = seastar::scheduling_group;

    future<> shutdown() {
        _update_timer.cancel();
        return std::move(_inflight_update);
    }

    future<> update_static_shares(float static_shares) {
        _static_shares = static_shares;
        return make_ready_future<>();
    }

protected:
    struct control_point {
        float input;
        float output;
    };

    scheduling_group _scheduling_group;

    std::vector<control_point> _control_points;

    std::function<float()> _current_backlog;
    timer<> _update_timer;
    // updating shares for an I/O class may contact another shard and returns a future.
    future<> _inflight_update;

    // Used when the controllers are disabled and a static share is used
    // When that option is deprecated we should remove this.
    float _static_shares;

    virtual void update_controller(float quota);

    bool controller_disabled() const noexcept {
        return _static_shares > 0;
    }

    static float slope(const control_point cp1, const control_point& cp2);

    void adjust();

    backlog_controller(scheduling_group sg, std::chrono::milliseconds interval,
                       std::vector<control_point> control_points, std::function<float()> backlog,
                       float static_shares = 0)
        : _scheduling_group(std::move(sg))
        , _control_points()
        , _current_backlog(std::move(backlog))
        , _update_timer([this] { adjust(); })
        , _inflight_update(make_ready_future<>())
        , _static_shares(static_shares)
    {
        _control_points.insert(_control_points.end(), control_points.begin(), control_points.end());
        _update_timer.arm_periodic(interval);
    }

    virtual ~backlog_controller() {}
public:
    backlog_controller(backlog_controller&&) = default;
    float backlog_of_shares(float shares) const;
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
class flush_controller : public backlog_controller {
    static constexpr float hard_dirty_limit = 1.0f;
public:
    flush_controller(backlog_controller::scheduling_group sg, float static_shares, std::chrono::milliseconds interval, float soft_limit, std::function<float()> current_dirty)
        : backlog_controller(std::move(sg), std::move(interval),
          std::vector<backlog_controller::control_point>({{0.0, 0.0}, {soft_limit, 10}, {soft_limit + (hard_dirty_limit - soft_limit) / 2, 200} , {hard_dirty_limit, 1000}}),
          std::move(current_dirty),
          static_shares
        )
    {}
};

class compaction_controller : public backlog_controller {
    // Normalization factor (NF) controls the trade-off between writes and reads.
    // With larger disk:ram ratio setups, the write amplification is higher, due to:
    //  write_amp = log(disk size / memory size)
    //
    // The optimal NF depends on many variables like the write bandwidth available,
    // the additional write amp caused by the compaction strategies, and the
    // write rate (the workload).
    // A suboptimal NF can lead to either compaction falling behind or running more
    // aggressive than needed.
    static constexpr float base_normalization_factor = 30.0f;
    static constexpr control_point middle_point();
    static constexpr control_point base_last_point();
    static const float min_normalization_factor();
    // Calculates the max backlog sensitivity, given max shares, that yield the minimum NF.
    static const float max_backlog_sensitivity(float max_shares);
    // Calculates the last control point for a given max shares and backlog sensitivity.
    static control_point last_control_point(float max_shares, float backlog_sensitivity);

    float max_shares() const noexcept {
        return _control_points.back().output;
    }

    // The backlog sensitivity has inverse proportionality to NF.
    //
    // The factor is applied to the slope in linear function for mapping backlog
    // into shares. Increasing the default of 1.0 means that the slope is steeper
    // (due to decreased NF), and therefore the system's sensitivity to compaction
    // backlog is increased.
    float _backlog_sensitivity;
public:
    static const float minimum_effective_max_shares();
    static const float default_max_shares();
    static constexpr float disable_backlog = std::numeric_limits<double>::infinity();
    static constexpr float backlog_disabled(float backlog) { return std::isinf(backlog); }
    static constexpr float min_backlog_sensitivity = 0.1f;
    static constexpr float default_backlog_sensitivity = 1.0f;

    struct config {
        backlog_controller::scheduling_group sg;
        float static_shares;
        float max_shares;
        float backlog_sensitivity;
        std::chrono::milliseconds interval;
    };

    void update_max_shares(float max_shares) {
        assert(!_control_points.empty());
        _control_points.back() = last_control_point(max_shares, _backlog_sensitivity);
    }

    void update_backlog_sensitivity(float sensitivity) {
        assert(!_control_points.empty());
        _backlog_sensitivity = sensitivity;
        _control_points.back() = last_control_point(max_shares(), _backlog_sensitivity);
    }

    float normalization_factor() const noexcept {
        return _control_points.size() ? _control_points.back().input : base_normalization_factor;
    }

    compaction_controller(config cfg, std::function<float()> current_backlog);
};
