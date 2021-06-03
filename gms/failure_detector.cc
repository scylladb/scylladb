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

#include <boost/range/adaptor/map.hpp>
#include "gms/failure_detector.hh"
#include "gms/gossiper.hh"
#include "gms/i_failure_detection_event_listener.hh"
#include "gms/endpoint_state.hh"
#include "gms/application_state.hh"
#include "gms/inet_address.hh"
#include "log.hh"
#include "db/config.hh"
#include <iostream>
#include <chrono>
#include "database.hh"

namespace gms {

static logging::logger logger("failure_detector");

constexpr std::chrono::milliseconds failure_detector::DEFAULT_MAX_PAUSE;

using clk = arrival_window::clk;

void arrival_window::add(clk::time_point value, const gms::inet_address& ep) {
    if (_tlast > clk::time_point::min()) {
        auto inter_arrival_time = value - _tlast;
        if (inter_arrival_time <= _max_interval && inter_arrival_time >= _min_interval) {
            _arrival_intervals.add(inter_arrival_time.count());
        } else  {
            logger.debug("failure_detector: Ignoring interval time of {} for {}, mean={}, size={}", inter_arrival_time.count(), ep, mean(), size());
        }
    } else {
        // We use a very large initial interval since the "right" average depends on the cluster size
        // and it's better to err high (false negatives, which will be corrected by waiting a bit longer)
        // than low (false positives, which cause "flapping").
        _arrival_intervals.add(_initial.count());
    }
    _tlast = value;
}

double arrival_window::mean() const {
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

void failure_detector::set_phi_convict_threshold(double phi) {
    _phi = phi;
}

double failure_detector::get_phi_convict_threshold() {
    return _phi;
}

void failure_detector::report(inet_address ep) {
    logger.trace("failure_detector: reporting {}", ep);
    auto now = clk::now();
    auto it = _arrival_samples.find(ep);
    if (it == _arrival_samples.end()) {
        // avoid adding an empty ArrivalWindow to the Map
        auto heartbeat_window = arrival_window(SAMPLE_SIZE, _initial, _max_interval, gossiper::INTERVAL);
        heartbeat_window.add(now, ep);
        _arrival_samples.emplace(ep, heartbeat_window);
    } else {
        it->second.add(now, ep);
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
    if (!_last_interpret) {
        *_last_interpret = now;
    }
    auto diff = std::chrono::duration_cast<std::chrono::milliseconds>(now - *_last_interpret);
    *_last_interpret = now;
    if (diff > get_max_local_pause()) {
        logger.warn("Not marking nodes down due to local pause of {} > {} (milliseconds)", diff.count(), get_max_local_pause().count());
        _last_paused = now;
        return;
    }
    if (clk::now() - _last_paused < get_max_local_pause()) {
        logger.debug("Still not marking nodes down due to local pause");
        return;
    }
    double phi = hb_wnd.phi(now);
    logger.trace("failure_detector: PHI for {} : {}", ep, phi);
    logger.trace("failure_detector: phi_convict_threshold={}", _phi);

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
    for (auto& entry : x._arrival_samples) {
        const inet_address& ep = entry.first;
        const arrival_window& win = entry.second;
        os << ep << " : "  << win << "\n";
    }
    return os;
}

} // namespace gms
