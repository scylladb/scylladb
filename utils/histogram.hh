/*
 * Copyright 2015 Cloudius Systems
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

#include <boost/circular_buffer.hpp>
#include "latency.hh"

namespace utils {

class ihistogram {
public:
    // count holds all the events
    int64_t count;
    // total holds only the events we sample
    int64_t total;
    int64_t min;
    int64_t max;
    int64_t sum;
    int64_t started;
    double mean;
    double variance;
    int64_t sample_mask;
    boost::circular_buffer<int64_t> sample;
    ihistogram(size_t size = 1024, int64_t _sample_mask = 0x80)
            : count(0), total(0), min(0), max(0), sum(0), started(0), mean(0), variance(0),
              sample_mask(_sample_mask), sample(
                    size) {
    }
    void mark(int64_t value) {
        if (total == 0 || value < min) {
            min = value;
        }
        if (total == 0 || value > max) {
            max = value;
        }
        if (total == 0) {
            mean = value;
            variance = 0;
        } else {
            double old_m = mean;
            double old_s = variance;

            mean = ((double)(sum + value)) / (total + 1);
            variance = old_s + ((value - old_m) * (value - mean));
        }
        sum += value;
        total++;
        count++;
        sample.push_back(value);
    }

    void mark(latency_counter& lc) {
        if (lc.is_start()) {
            mark(lc.stop().latency_in_nano());
        } else {
            count++;
        }
    }

    /**
     * Return true if the current event should be sample.
     * In the typical case, there is no need to use this method
     * Call set_latency, that would start a latency object if needed.
     */
    bool should_sample() const {
        return total == 0 || (started & sample_mask);
    }
    /**
     * Set the latency according to the sample rate.
     */
    ihistogram& set_latency(latency_counter& lc) {
        if (should_sample()) {
            lc.start();
        }
        started++;
        return *this;
    }

    /**
     * Allow to use the histogram as a counter
     * Increment the total number of events without
     * sampling the value.
     */
    ihistogram& inc() {
        count++;
        return *this;
    }

    int64_t pending() const {
        return started - count;
    }
};

}
