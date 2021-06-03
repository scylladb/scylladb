/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
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

#include <seastar/core/sstring.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/distributed.hh>
#include "utils/bounded_stats_deque.hh"
#include <iosfwd>
#include <cmath>
#include <list>
#include <map>
#include <optional>

#include "gms/inet_address.hh"


namespace gms {
class i_failure_detection_event_listener;
class endpoint_state;

class arrival_window {
public:
    using clk = seastar::lowres_system_clock;
private:
    clk::time_point _tlast{clk::time_point::min()};
    utils::bounded_stats_deque _arrival_intervals;
    std::chrono::milliseconds _initial;
    std::chrono::milliseconds _max_interval;
    std::chrono::milliseconds _min_interval;

    // this is useless except to provide backwards compatibility in phi_convict_threshold,
    // because everyone seems pretty accustomed to the default of 8, and users who have
    // already tuned their phi_convict_threshold for their own environments won't need to
    // change.
    static constexpr double PHI_FACTOR{M_LOG10El};

public:
    arrival_window(int size, std::chrono::milliseconds initial,
            std::chrono::milliseconds max_interval, std::chrono::milliseconds min_interval)
        : _arrival_intervals(size)
        , _initial(initial)
        , _max_interval(max_interval)
        , _min_interval(min_interval) {
    }

    void add(clk::time_point value, const gms::inet_address& ep);

    double mean() const;

    // see CASSANDRA-2597 for an explanation of the math at work here.
    double phi(clk::time_point tnow);

    size_t size() { return _arrival_intervals.size(); }

    clk::time_point last_update() const { return _tlast; }

    friend std::ostream& operator<<(std::ostream& os, const arrival_window& w);

};


/**
 * This FailureDetector is an implementation of the paper titled
 * "The Phi Accrual Failure Detector" by Hayashibara.
 * Check the paper and the <i>IFailureDetector</i> interface for details.
 */
class failure_detector {
private:
    static constexpr int SAMPLE_SIZE = 1000;
    // this is useless except to provide backwards compatibility in phi_convict_threshold,
    // because everyone seems pretty accustomed to the default of 8, and users who have
    // already tuned their phi_convict_threshold for their own environments won't need to
    // change.
    static constexpr double PHI_FACTOR{M_LOG10El};

    std::map<inet_address, arrival_window> _arrival_samples;
    std::list<i_failure_detection_event_listener*> _fd_evnt_listeners;
    double _phi = 8;
    std::chrono::milliseconds _initial;
    std::chrono::milliseconds _max_interval;

    static constexpr std::chrono::milliseconds DEFAULT_MAX_PAUSE{5000};

    std::chrono::milliseconds get_max_local_pause() {
        // FIXME: cassandra.max_local_pause_in_ms
#if 0
        if (System.getProperty("cassandra.max_local_pause_in_ms") != null) {
            long pause = Long.parseLong(System.getProperty("cassandra.max_local_pause_in_ms"));
            logger.warn("Overriding max local pause time to {}ms", pause);
            return pause * 1000000L;
        } else {
            return DEFAULT_MAX_PAUSE;
        }
#endif
        return DEFAULT_MAX_PAUSE;
    }

    std::optional<arrival_window::clk::time_point> _last_interpret;
    arrival_window::clk::time_point _last_paused;

public:
    failure_detector(double phi, std::chrono::milliseconds initial, std::chrono::milliseconds max_interval)
            : _phi(phi), _initial(initial), _max_interval(max_interval) {
    }

    const std::map<inet_address, arrival_window>& arrival_samples() const {
        return _arrival_samples;
    }

public:
    /**
     * Dump the inter arrival times for examination if necessary.
     */
#if 0
    void dumpInterArrivalTimes() {
        File file = FileUtils.createTempFile("failuredetector-", ".dat");

        OutputStream os = null;
        try
        {
            os = new BufferedOutputStream(new FileOutputStream(file, true));
            os.write(toString().getBytes());
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, file);
        }
        finally
        {
            FileUtils.closeQuietly(os);
        }
    }
#endif

    void set_phi_convict_threshold(double phi);

    double get_phi_convict_threshold();

    void report(inet_address ep);

    void interpret(inet_address ep);

    void force_conviction(inet_address ep);

    void remove(inet_address ep);

    void register_failure_detection_event_listener(i_failure_detection_event_listener* listener);

    void unregister_failure_detection_event_listener(i_failure_detection_event_listener* listener);

    friend std::ostream& operator<<(std::ostream& os, const failure_detector& x);
};

} // namespace gms
