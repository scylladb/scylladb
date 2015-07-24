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
 * Modified by Cloudius Systems.
 * Copyright 2015 Cloudius Systems.
 */

#pragma once

#include "unimplemented.hh"
#include "core/sstring.hh"
#include "core/shared_ptr.hh"
#include "core/distributed.hh"
#include "utils/bounded_stats_deque.hh"
#include "gms/i_failure_detector.hh"
#include <iostream>
#include <cmath>
#include <list>
#include <map>

namespace gms {
class inet_address;
class i_failure_detection_event_listener;
class endpoint_state;

class arrival_window {
public:
    using clk = std::chrono::steady_clock;
private:
    clk::time_point _tlast{clk::time_point::min()};
    utils::bounded_stats_deque _arrival_intervals;

    // this is useless except to provide backwards compatibility in phi_convict_threshold,
    // because everyone seems pretty accustomed to the default of 8, and users who have
    // already tuned their phi_convict_threshold for their own environments won't need to
    // change.
    static constexpr double PHI_FACTOR{1.0 / std::log(10.0)};

public:
    arrival_window(int size)
        : _arrival_intervals(size) {
    }

    // in the event of a long partition, never record an interval longer than the rpc timeout,
    // since if a host is regularly experiencing connectivity problems lasting this long we'd
    // rather mark it down quickly instead of adapting
    // this value defaults to the same initial value the FD is seeded with
    static clk::duration get_max_interval();

    void add(clk::time_point value);

    double mean();

    // see CASSANDRA-2597 for an explanation of the math at work here.
    double phi(clk::time_point tnow);

    friend std::ostream& operator<<(std::ostream& os, const arrival_window& w);

};


/**
 * This FailureDetector is an implementation of the paper titled
 * "The Phi Accrual Failure Detector" by Hayashibara.
 * Check the paper and the <i>IFailureDetector</i> interface for details.
 */
class failure_detector : public i_failure_detector {
private:
    static constexpr int SAMPLE_SIZE = 1000;
    // this is useless except to provide backwards compatibility in phi_convict_threshold,
    // because everyone seems pretty accustomed to the default of 8, and users who have
    // already tuned their phi_convict_threshold for their own environments won't need to
    // change.
    static constexpr double PHI_FACTOR{1.0 / std::log(10.0)}; // 0.434...
    std::map<inet_address, arrival_window> _arrival_samples;
    std::list<i_failure_detection_event_listener*> _fd_evnt_listeners;

public:
    failure_detector() {
    }

    future<> stop() {
        return make_ready_future<>();
    }

    sstring get_all_endpoint_states();

    std::map<sstring, sstring> get_simple_states();

    int get_down_endpoint_count();

    int get_up_endpoint_count();

    sstring get_endpoint_state(sstring address);

private:
    void append_endpoint_state(std::stringstream& ss, endpoint_state& state);

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


    bool is_alive(inet_address ep);

    void report(inet_address ep);

    void interpret(inet_address ep);

    void force_conviction(inet_address ep);

    void remove(inet_address ep);

    void register_failure_detection_event_listener(i_failure_detection_event_listener* listener);

    void unregister_failure_detection_event_listener(i_failure_detection_event_listener* listener);

    friend std::ostream& operator<<(std::ostream& os, const failure_detector& x);
};

extern distributed<failure_detector> _the_failure_detector;
inline failure_detector& get_local_failure_detector() {
    return _the_failure_detector.local();
}
inline distributed<failure_detector>& get_failure_detector() {
    return _the_failure_detector;
}

inline future<> set_phi_convict_threshold(double phi) {
    return smp::submit_to(0, [phi] {
        get_local_failure_detector().set_phi_convict_threshold(phi);
    });
}

inline  future<double> get_phi_convict_threshold() {
    return smp::submit_to(0, [] {
        return get_local_failure_detector().get_phi_convict_threshold();
    });
}

inline future<sstring> get_all_endpoint_states() {
    return smp::submit_to(0, [] {
        return get_local_failure_detector().get_all_endpoint_states();
    });
}

inline future<sstring> get_endpoint_state(sstring address) {
    return smp::submit_to(0, [address] {
        return get_local_failure_detector().get_endpoint_state(address);
    });
}

inline future<std::map<sstring, sstring>> get_simple_states() {
    return smp::submit_to(0, [] {
        return get_local_failure_detector().get_simple_states();
    });
}

inline future<int> get_down_endpoint_count() {
    return smp::submit_to(0, [] {
        return get_local_failure_detector().get_down_endpoint_count();
    });
}


inline future<int> get_up_endpoint_count() {
    return smp::submit_to(0, [] {
        return get_local_failure_detector().get_up_endpoint_count();
    });
}

} // namespace gms
