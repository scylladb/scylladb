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

#include "gms/failure_detector.hh"
#include "gms/gossiper.hh"
#include "gms/i_failure_detector.hh"
#include "gms/i_failure_detection_event_listener.hh"
#include "gms/endpoint_state.hh"
#include "gms/application_state.hh"
#include "gms/inet_address.hh"
#include <iostream>
#include <chrono>

namespace gms {

extern logging::logger logger;

using clk = arrival_window::clk;

static clk::duration get_initial_value() {
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
    return std::chrono::seconds(2);
}

clk::duration arrival_window::get_max_interval() {
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
    return get_initial_value();
}

void arrival_window::add(clk::time_point value) {
    if (_tlast > clk::time_point::min()) {
        auto inter_arrival_time = value - _tlast;
        if (inter_arrival_time <= get_max_interval()) {
            _arrival_intervals.add(inter_arrival_time.count());
        } else  {
            logger.debug("failure_detector: Ignoring interval time of {}", inter_arrival_time.count());
        }
    } else {
        // We use a very large initial interval since the "right" average depends on the cluster size
        // and it's better to err high (false negatives, which will be corrected by waiting a bit longer)
        // than low (false positives, which cause "flapping").
        _arrival_intervals.add(get_initial_value().count());
    }
    _tlast = value;
}

double arrival_window::mean() {
    return _arrival_intervals.mean();
}

double arrival_window::phi(clk::time_point tnow) {
    assert(_arrival_intervals.size() > 0 && _tlast > clk::time_point::min()); // should not be called before any samples arrive
    auto t = (tnow - _tlast).count();
    auto m = mean();
    double phi = t / m;
    logger.debug("failure_detector: now={}, tlast={}, t={}, mean={}, phi={}",
        tnow.time_since_epoch().count(), _tlast.time_since_epoch().count(), t, m, phi);
    return phi;
}

std::ostream& operator<<(std::ostream& os, const arrival_window& w) {
    for (auto& x : w._arrival_intervals.deque()) {
        os << x << " ";
    }
    return os;
}

sstring failure_detector::get_all_endpoint_states() {
    std::stringstream ss;
    for (auto& entry : get_local_gossiper().endpoint_state_map) {
        auto& ep = entry.first;
        auto& state = entry.second;
        ss << ep << "\n";
        append_endpoint_state(ss, state);
    }
    return sstring(ss.str());
}

std::map<sstring, sstring> failure_detector::get_simple_states() {
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

int failure_detector::get_down_endpoint_count() {
    int count = 0;
    for (auto& entry : get_local_gossiper().endpoint_state_map) {
        auto& state = entry.second;
        if (!state.is_alive()) {
            count++;
        }
    }
    return count;
}

int failure_detector::get_up_endpoint_count() {
    int count = 0;
    for (auto& entry : get_local_gossiper().endpoint_state_map) {
        auto& state = entry.second;
        if (state.is_alive()) {
            count++;
        }
    }
    return count;
}

sstring failure_detector::get_endpoint_state(sstring address) {
    std::stringstream ss;
    auto eps = get_local_gossiper().get_endpoint_state_for_endpoint(inet_address(address));
    if (eps) {
        append_endpoint_state(ss, *eps);
        return sstring(ss.str());
    } else {
        return sstring("unknown endpoint ") + address;
    }
}

void failure_detector::append_endpoint_state(std::stringstream& ss, endpoint_state& state) {
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

void failure_detector::set_phi_convict_threshold(double phi) {
    // FIXME
    // DatabaseDescriptor.setPhiConvictThreshold(phi);
}

double failure_detector::get_phi_convict_threshold() {
    // FIXME: phi_convict_threshold must be between 5 and 16"
    // return DatabaseDescriptor.getPhiConvictThreshold();
    warn(unimplemented::cause::GOSSIP);
    return 8;
}

bool failure_detector::is_alive(inet_address ep) {
    return get_local_gossiper().is_alive(ep);
}

void failure_detector::report(inet_address ep) {
    logger.trace("failure_detector: reporting {}", ep);
    auto now = clk::now();
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

// Runs inside seastar::async context
void failure_detector::interpret(inet_address ep) {
    auto it = _arrival_samples.find(ep);
    if (it == _arrival_samples.end()) {
        return;
    }
    arrival_window& hb_wnd = it->second;
    auto now = clk::now();
    double phi = hb_wnd.phi(now);
    logger.trace("failure_detector: PHI for {} : {}", ep, phi);

    if (PHI_FACTOR * phi > get_phi_convict_threshold()) {
        logger.trace("failure_detector: notifying listeners that {} is down", ep);
        logger.trace("failure_detector: intervals: {} mean: {}", hb_wnd, hb_wnd.mean());
        for (auto& listener : _fd_evnt_listeners) {
            logger.debug("failure_detector: convict ep={} phi={}", ep, phi);
            listener->convict(ep, phi);
        }
    }
}

// Runs inside seastar::async context
void failure_detector::force_conviction(inet_address ep) {
    logger.debug("failure_detector: Forcing conviction of {}", ep);
    for (auto& listener : _fd_evnt_listeners) {
        listener->convict(ep, get_phi_convict_threshold());
    }
}

void failure_detector::remove(inet_address ep) {
    _arrival_samples.erase(ep);
}

void failure_detector::register_failure_detection_event_listener(i_failure_detection_event_listener* listener) {
    _fd_evnt_listeners.push_back(std::move(listener));
}

void failure_detector::unregister_failure_detection_event_listener(i_failure_detection_event_listener* listener) {
    _fd_evnt_listeners.remove(listener);
}

std::ostream& operator<<(std::ostream& os, const failure_detector& x) {
    os << "----------- failure_detector:    -----------\n";
    for (auto& entry : x._arrival_samples) {
        const inet_address& ep = entry.first;
        const arrival_window& win = entry.second;
        os << ep << " : "  << win << "\n";
    }
    return os;
}

distributed<failure_detector> _the_failure_detector;

} // namespace gms
