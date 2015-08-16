/*
 * Copyright 2015 Cloudius Systems
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
    double mean;
    double variance;
    int64_t sample_mask;
    boost::circular_buffer<int64_t> sample;
    ihistogram(size_t size = 1024, int64_t _sample_mask = 0x80)
            : count(0), total(0), min(0), max(0), sum(0), mean(0), variance(0),
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

            mean = old_m + ((value - old_m) / (total + 1));
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
        return total & sample_mask;
    }
    /**
     * Set the latency according to the sample rate.
     */
    ihistogram& set_latency(latency_counter& lc) {
        if (should_sample()) {
            lc.start();
        }
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
};

}
