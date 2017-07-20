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
#include <seastar/core/thread.hh>
#include <seastar/core/timer.hh>
#include <chrono>

// Simple proportional controller to adjust shares of memtable/streaming flushes.
//
// Goal is to flush as fast as we can, but not so fast that we steal all the CPU from incoming
// requests, and at the same time minimize user-visible fluctuations in the flush quota.
//
// What that translates to is we'll try to keep virtual dirty's firt derivative at 0 (IOW, we keep
// virtual dirty constant), which means that the rate of incoming writes is equal to the rate of
// flushed bytes.
//
// The exact point at which the controller stops determines the desired flush CPU usage. As we
// approach the hard dirty limit, we need to be more aggressive. We will therefore define two
// thresholds, and increase the constant as we cross them.
//
//  1) the soft limit line
//  2) halfway between soft limit and dirty limit
//
// The constants q1 and q2 are used to determine the proportional factor at each stage.
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
//
// For now we'll just set them in the structure not to complicate the constructor. But q1, q2 and
// qmax can easily become parameters if we find another user.
class flush_cpu_controller {
    static constexpr float hard_dirty_limit = 0.50;
    static constexpr float q1 = 0.01;
    static constexpr float q2 = 0.2;
    static constexpr float qmax = 1;

    float _current_quota = 0.0f;
    float _goal;
    std::function<float()> _current_dirty;
    std::chrono::milliseconds _interval;
    timer<> _update_timer;

    seastar::thread_scheduling_group _scheduling_group;
    seastar::thread_scheduling_group *_current_scheduling_group = nullptr;

    void adjust();
public:
    seastar::thread_scheduling_group* scheduling_group() {
        return _current_scheduling_group;
    }
    float current_quota() const {
        return _current_quota;
    }

    struct disabled {
        seastar::thread_scheduling_group *backup;
    };
    flush_cpu_controller(disabled d) : _scheduling_group(std::chrono::nanoseconds(0), 0), _current_scheduling_group(d.backup) {}
    flush_cpu_controller(std::chrono::milliseconds interval, float soft_limit, std::function<float()> current_dirty);
    flush_cpu_controller(flush_cpu_controller&&) = default;
};


