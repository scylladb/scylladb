/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once
#include <seastar/core/scheduling.hh>
#include <seastar/core/timer.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/file.hh>
#include <seastar/util/bool_class.hh>
#include <seastar/util/log.hh>
#include <chrono>
#include <cmath>

#include "seastarx.hh"
#include "backlog_controller_fwd.hh"
#include "compaction/compaction_backlog_manager.hh"

// Simple proportional controller to adjust shares for processes for which a backlog can be clearly
// defined.
//
// Goal is to consume the backlog as fast as we can, but not so fast that we steal all the CPU from
// incoming requests, and at the same time minimize user-visible fluctuations in the quota.
//
// What that translates to is we'll try to keep the backlog's first derivative at 0 (IOW, we keep
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
    float current_backlog() const { return _current_backlog(); }
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

// Sets the shares of the compaction scheduling group.
class compaction_controller {
public:
    // The backlog a tracker reports when it can't compute one, e.g. for a strategy
    // without a backlog model. A controller then gives compaction its maximum shares.
    static constexpr float disable_backlog = std::numeric_limits<double>::infinity();
    static constexpr float backlog_disabled(float backlog) { return std::isinf(backlog); }

    virtual ~compaction_controller() = default;
    virtual future<> shutdown() = 0;
    // Static shares, when positive, replace the controller's own.
    virtual future<> update_static_shares(float static_shares) = 0;
    // Caps the shares the controller gives compaction.
    virtual void set_max_shares(float max_shares) = 0;
};

// Sets the shares from the backlog, normalized by the available memory, through a
// piecewise-linear curve.
class compaction_controller_v1 final : public compaction_controller, public backlog_controller {
public:
    static constexpr unsigned normalization_factor = 30;
    static inline const std::vector<backlog_controller::control_point> default_control_points = {
            backlog_controller::control_point{0.0, 50}, {1.5, 100}, {normalization_factor, default_compaction_maximum_shares}};
    compaction_controller_v1(backlog_controller::scheduling_group sg, float static_shares, std::optional<float> max_shares,
        std::chrono::milliseconds interval, std::function<float()> current_backlog)
        : backlog_controller(std::move(sg), std::move(interval),
          default_control_points,
          std::move(current_backlog),
          static_shares
        )
    {
        if (max_shares) {
            set_max_shares(*max_shares);
        }
    }

    future<> shutdown() override {
        return backlog_controller::shutdown();
    }
    future<> update_static_shares(float static_shares) override {
        return backlog_controller::update_static_shares(static_shares);
    }
    // Updates the maximum output value for control points.
    void set_max_shares(float max_shares) override;
};

// Sets the shares so that compaction keeps up with the work ingestion produces, by
// comparing the rate compaction has to run at, the target, with the rate it achieves,
// both summed over all compaction groups:
//
//   shares = shares * (target / achieved)^alpha
//
// alpha and the step limit keep each update to a part of the way, as the rates are
// averaged over minutes and respond slowly to the shares.
class compaction_controller_v2 final : public compaction_controller {
public:
    using scheduling_group = seastar::scheduling_group;
    // In a dry run, the controller computes the shares, and logs them and what led to
    // them, but doesn't set them: another controller does.
    using dry_run = seastar::bool_class<struct dry_run_tag>;

    static constexpr float min_shares = 50;
    static constexpr double alpha = 0.3;
    // An update changes the shares by up to this factor, either way.
    static constexpr double max_step = 2;
private:
    scheduling_group _scheduling_group;
    std::function<compaction::compaction_rates()> _current_rates;
    // Whether regular compaction ran since the last update. The shares only matter for
    // compaction that competes for CPU; while there is none, comparing the rates tells
    // nothing about them.
    // FIXME: use the time the compaction scheduling group waited for CPU instead.
    std::function<bool()> _compacting;
    float _static_shares;
    // FIXME: the maximum is v1's by default, but v2 doesn't have to stop there. Lift
    // the default once tested; compaction_max_shares can raise it meanwhile.
    float _max_shares = default_compaction_maximum_shares;
    // Start at the minimum and rise as the rates show compaction falls behind, rather than
    // from the scheduling group's shares: until a controller sets them, they are the
    // group's default, the maximum, e.g. when the node switches to this controller, or
    // starts it in shadow mode, early at startup.
    float _shares = min_shares;
    dry_run _dry_run;
    // A dry run logs its updates at most this often, so as not to flood the logs.
    seastar::logger::rate_limit _dry_run_log_limit{std::chrono::seconds(30)};
    timer<> _update_timer;

    void adjust();
public:
    compaction_controller_v2(scheduling_group sg, float static_shares, std::optional<float> max_shares, std::chrono::milliseconds interval,
            std::function<compaction::compaction_rates()> current_rates, std::function<bool()> compacting, dry_run dry_run = dry_run::no);

    future<> shutdown() override;
    future<> update_static_shares(float static_shares) override;
    void set_max_shares(float max_shares) override;

    // The shares after an update from `shares`, given the rates. A target above what
    // compaction achieves raises them, a target below lowers them. With nothing to
    // compare, e.g. unknown rates, they are left as they are.
    static float next_shares(float shares, const compaction::compaction_rates& rates, float max_shares);
};
