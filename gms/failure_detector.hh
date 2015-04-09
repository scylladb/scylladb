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
#include "db_clock.hh"
#include "gms/inet_address.hh"
#include "gms/i_failure_detector.hh"
#include "core/sstring.hh"
#include "core/shared_ptr.hh"
#include "core/distributed.hh"
#include "gms/gossiper.hh"
#include "utils/bounded_stats_deque.hh"
#include <cmath>
#include <list>
#include <map>

namespace gms {

class failure_detector_helper {
public:
    static long get_initial_value() {
#if 0
        String newvalue = System.getProperty("cassandra.fd_initial_value_ms");
        if (newvalue == null)
        {
            return Gossiper.intervalInMillis * 2;
        }
        else
        {
            logger.info("Overriding FD INITIAL_VALUE to {}ms", newvalue);
            return Integer.parseInt(newvalue);
        }
#endif
        warn(unimplemented::cause::GOSSIP);
        return 1000 * 2;
    }

    static long INITIAL_VALUE_NANOS() {
        // Convert from milliseconds to nanoseconds
        return get_initial_value() * 1000;
    }
};

class arrival_window {
private:
    long _tlast = 0;
    utils::bounded_stats_deque _arrival_intervals;

    // this is useless except to provide backwards compatibility in phi_convict_threshold,
    // because everyone seems pretty accustomed to the default of 8, and users who have
    // already tuned their phi_convict_threshold for their own environments won't need to
    // change.
    static constexpr const double PHI_FACTOR{1.0 / std::log(10.0)};

    // in the event of a long partition, never record an interval longer than the rpc timeout,
    // since if a host is regularly experiencing connectivity problems lasting this long we'd
    // rather mark it down quickly instead of adapting
    // this value defaults to the same initial value the FD is seeded with
    long MAX_INTERVAL_IN_NANO = get_max_interval();

public:
    arrival_window(int size)
        : _arrival_intervals(size) {
    }

    static long get_max_interval() {
#if 0
        sstring newvalue = System.getProperty("cassandra.fd_max_interval_ms");
        if (newvalue == null)
        {
            return failure_detector.INITIAL_VALUE_NANOS;
        }
        else
        {
            logger.info("Overriding FD MAX_INTERVAL to {}ms", newvalue);
            return TimeUnit.NANOSECONDS.convert(Integer.parseInt(newvalue), TimeUnit.MILLISECONDS);
        }
#endif
        warn(unimplemented::cause::GOSSIP);
        return failure_detector_helper::INITIAL_VALUE_NANOS();
    }

    void add(long value) {
        assert(_tlast >= 0);
        if (_tlast > 0L) {
            long inter_arrival_time = value - _tlast;
            if (inter_arrival_time <= MAX_INTERVAL_IN_NANO) {
                _arrival_intervals.add(inter_arrival_time);
            } else  {
                //logger.debug("Ignoring interval time of {}", interArrivalTime);
            }
        } else {
            // We use a very large initial interval since the "right" average depends on the cluster size
            // and it's better to err high (false negatives, which will be corrected by waiting a bit longer)
            // than low (false positives, which cause "flapping").
            _arrival_intervals.add(failure_detector_helper::INITIAL_VALUE_NANOS());
        }
        _tlast = value;
    }

    double mean() {
        return _arrival_intervals.mean();
    }

    // see CASSANDRA-2597 for an explanation of the math at work here.
    double phi(long tnow) {
        assert(_arrival_intervals.size() > 0 && _tlast > 0); // should not be called before any samples arrive
        long t = tnow - _tlast;
        return t / mean();
    }

    friend inline std::ostream& operator<<(std::ostream& os, const arrival_window& w) {
        for (auto& x : w._arrival_intervals.deque()) {
            os << x << " ";
        }
        return os;
    }

};


/**
 * This FailureDetector is an implementation of the paper titled
 * "The Phi Accrual Failure Detector" by Hayashibara.
 * Check the paper and the <i>IFailureDetector</i> interface for details.
 */
class failure_detector : public i_failure_detector {
private:
    static constexpr const int SAMPLE_SIZE = 1000;
    // this is useless except to provide backwards compatibility in phi_convict_threshold,
    // because everyone seems pretty accustomed to the default of 8, and users who have
    // already tuned their phi_convict_threshold for their own environments won't need to
    // change.
    static constexpr const double PHI_FACTOR{1.0 / std::log(10.0)}; // 0.434...
    std::map<inet_address, arrival_window> _arrival_samples;
    std::list<shared_ptr<i_failure_detection_event_listener>> _fd_evnt_listeners;

public:
    failure_detector() {
    }

    sstring get_all_endpoint_states() {
        std::stringstream ss;
        for (auto& entry : get_local_gossiper().endpoint_state_map) {
            auto& ep = entry.first;
            auto& state = entry.second;
            ss << ep << "\n";
            append_endpoint_state(ss, state);
        }
        return sstring(ss.str());
    }

    std::map<sstring, sstring> get_simple_states() {
        std::map<sstring, sstring> nodes_status;
        for (auto& entry : get_local_gossiper().endpoint_state_map) {
            auto& ep = entry.first;
            auto& state = entry.second;
            std::stringstream ss;
            ss << ep;
            if (state.is_alive())
                nodes_status.emplace(sstring(ss.str()), "UP");
            else
                nodes_status.emplace(sstring(ss.str()), "DOWN");
        }
        return nodes_status;
    }

    int get_down_endpoint_count() {
        int count = 0;
        for (auto& entry : get_local_gossiper().endpoint_state_map) {
            auto& state = entry.second;
            if (!state.is_alive()) {
                count++;
            }
        }
        return count;
    }

    int get_up_endpoint_count() {
        int count = 0;
        for (auto& entry : get_local_gossiper().endpoint_state_map) {
            auto& state = entry.second;
            if (state.is_alive()) {
                count++;
            }
        }
        return count;
    }

    sstring get_endpoint_state(sstring address) {
        std::stringstream ss;
        auto eps = get_local_gossiper().get_endpoint_state_for_endpoint(inet_address(address));
        if (eps) {
            append_endpoint_state(ss, *eps);
            return sstring(ss.str());
        } else {
            return sstring("unknown endpoint ") + address;
        }
    }

private:
    void append_endpoint_state(std::stringstream& ss, endpoint_state& state) {
        ss << "  generation:" << state.get_heart_beat_state().get_generation() << "\n";
        ss << "  heartbeat:" << state.get_heart_beat_state().get_heart_beat_version() << "\n";
        for (auto& entry : state.get_application_state_map()) {
            auto& app_state = entry.first;
            auto& value = entry.second;
            if (app_state == application_state::TOKENS) {
                continue;
            }
            // FIXME: Add operator<< for application_state
            ss << "  " << int32_t(app_state) << ":" << value.value << "\n";
        }
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

    void set_phi_convict_threshold(double phi) {
        // FIXME
        // DatabaseDescriptor.setPhiConvictThreshold(phi);
    }

    double get_phi_convict_threshold() {
        // FIXME
        // return DatabaseDescriptor.getPhiConvictThreshold();
        warn(unimplemented::cause::GOSSIP);
        return 0;
    }


    bool is_alive(inet_address ep) {
        if (ep.is_broadcast_address()) {
            return true;
        }

        auto eps = get_local_gossiper().get_endpoint_state_for_endpoint(ep);
        // we could assert not-null, but having isAlive fail screws a node over so badly that
        // it's worth being defensive here so minor bugs don't cause disproportionate
        // badness.  (See CASSANDRA-1463 for an example).
        if (eps) {
            return eps->is_alive();
        } else {
            // logger.error("unknown endpoint {}", ep);
            return false;
        }
    }

    void report(inet_address ep) {
        // if (logger.isTraceEnabled())
        //     logger.trace("reporting {}", ep);
        long now = db_clock::now().time_since_epoch().count();
        auto it = _arrival_samples.find(ep);
        if (it == _arrival_samples.end()) {
            // avoid adding an empty ArrivalWindow to the Map
            auto heartbeat_window = arrival_window(SAMPLE_SIZE);
            heartbeat_window.add(now);
            _arrival_samples.emplace(ep, heartbeat_window);
        } else {
            it->second.add(now);
        }
    }

    void interpret(inet_address ep) {
        auto it = _arrival_samples.find(ep);
        if (it == _arrival_samples.end()) {
            return;
        }
        arrival_window& hb_wnd = it->second;
        long now = db_clock::now().time_since_epoch().count();
        double phi = hb_wnd.phi(now);
        // if (logger.isTraceEnabled())
        //     logger.trace("PHI for {} : {}", ep, phi);

        if (PHI_FACTOR * phi > get_phi_convict_threshold()) {
            // logger.trace("notifying listeners that {} is down", ep);
            // logger.trace("intervals: {} mean: {}", hb_wnd, hb_wnd.mean());
            for (auto& listener : _fd_evnt_listeners) {
                listener->convict(ep, phi);
            }
        }
    }

    void force_conviction(inet_address ep) {
        //logger.debug("Forcing conviction of {}", ep);
        for (auto& listener : _fd_evnt_listeners) {
            listener->convict(ep, get_phi_convict_threshold());
        }
    }

    void remove(inet_address ep) {
        _arrival_samples.erase(ep);
    }

    void register_failure_detection_event_listener(shared_ptr<i_failure_detection_event_listener> listener) {
        _fd_evnt_listeners.push_back(std::move(listener));
    }

    void unregister_failure_detection_event_listener(shared_ptr<i_failure_detection_event_listener> listener) {
        _fd_evnt_listeners.remove(listener);
    }

    friend inline std::ostream& operator<<(std::ostream& os, const failure_detector& x) {
        os << "-----------------------------------------------------------------------";
        for (auto& entry : x._arrival_samples) {
            const inet_address& ep = entry.first;
            const arrival_window& win = entry.second;
            os << ep << " : "  << win << "\n";
        }
        os << "-----------------------------------------------------------------------";
        return os;
    }
};

extern distributed<failure_detector> _the_failure_detector;
inline failure_detector& get_local_failure_detector() {
    assert(engine().cpu_id() == 0);
    return _the_failure_detector.local();
}
inline distributed<failure_detector>& get_failure_detector() {
    return _the_failure_detector;
}

} // namespace gms
