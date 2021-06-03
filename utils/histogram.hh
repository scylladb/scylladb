/*
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

#include <boost/circular_buffer.hpp>
#include "latency.hh"
#include <cmath>
#include <seastar/core/timer.hh>
#include <iosfwd>
#include "seastarx.hh"

namespace utils {
/**
 * An exponentially-weighted moving average.
 */
class moving_average {
    double _alpha = 0;
    bool _initialized = false;
    latency_counter::duration _tick_interval;
    uint64_t _count = 0;
    double _rate = 0;
public:
    moving_average(latency_counter::duration interval, latency_counter::duration tick_interval) :
        _tick_interval(tick_interval) {
        _alpha = 1 - std::exp(-std::chrono::duration_cast<std::chrono::seconds>(tick_interval).count()/
                static_cast<double>(std::chrono::duration_cast<std::chrono::seconds>(interval).count()));
    }

    void add(uint64_t val = 1) {
        _count += val;
    }

    void update() {
        double instant_rate = _count / static_cast<double>(std::chrono::duration_cast<std::chrono::seconds>(_tick_interval).count());
        if (_initialized) {
            _rate += (_alpha * (instant_rate - _rate));
        } else {
            _rate = instant_rate;
            _initialized = true;
        }
        _count = 0;
    }

    bool is_initilized() const {
        return _initialized;
    }

    double rate() const {
        if (is_initilized()) {
            return _rate;
        }
        return 0;
    }
};

template <typename Unit>
class basic_ihistogram {
public:
    using duration_unit = Unit;
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
    basic_ihistogram(size_t size = 1024, int64_t _sample_mask = 0x80)
            : count(0), total(0), min(0), max(0), sum(0), started(0), mean(0), variance(0),
              sample_mask(_sample_mask), sample(
                    size) {
    }

    template <typename Rep, typename Ratio>
    void mark(std::chrono::duration<Rep, Ratio> dur) {
        auto value = std::chrono::duration_cast<Unit>(dur).count();
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
            mark(lc.stop().latency());
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
    basic_ihistogram& set_latency(latency_counter& lc) {
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
    basic_ihistogram& inc() {
        count++;
        return *this;
    }

    int64_t pending() const {
        return started - count;
    }

    inline double pow2(double a) {
        return a * a;
    }

    basic_ihistogram& operator +=(const basic_ihistogram& o) {
        if (count == 0) {
            *this = o;
        } else if (o.count > 0) {
            if (min > o.min) {
                min = o.min;
            }
            if (max < o.max) {
                max = o.max;
            }
            double ncount = count + o.count;
            sum += o.sum;
            double a = count / ncount;
            double b = o.count / ncount;

            double m = a * mean + b * o.mean;

            variance = (variance + pow2(m - mean)) * a
                    + (o.variance + pow2(o.mean - mean)) * b;
            mean = m;
            count += o.count;
            total += o.total;
            for (auto i : o.sample) {
                sample.push_back(i);
            }
        }
        return *this;
    }

    int64_t estimated_sum() const {
        return mean * count;
    }

    template <typename U>
    friend basic_ihistogram<U> operator +(basic_ihistogram<U> a, const basic_ihistogram<U>& b);
};

template <typename Unit>
inline basic_ihistogram<Unit> operator +(basic_ihistogram<Unit> a, const basic_ihistogram<Unit>& b) {
    a += b;
    return a;
}

using ihistogram = basic_ihistogram<std::chrono::microseconds>;

struct rate_moving_average {
    uint64_t count = 0;
    double rates[3] = {0};
    double mean_rate = 0;
    rate_moving_average& operator +=(const rate_moving_average& o) {
        count += o.count;
        mean_rate += o.mean_rate;
        for (int i=0; i<3; i++) {
            rates[i] += o.rates[i];
        }
        return *this;
    }
    friend rate_moving_average operator+ (rate_moving_average a, const rate_moving_average& b);
};

inline rate_moving_average operator+ (rate_moving_average a, const rate_moving_average& b) {
    a += b;
    return a;
}

class timed_rate_moving_average {
    static constexpr latency_counter::duration tick_interval() {
        return std::chrono::seconds(10);
    }
    moving_average rates[3] = {{std::chrono::minutes(1), tick_interval()}, {std::chrono::minutes(5), tick_interval()}, {std::chrono::minutes(15), tick_interval()}};
    latency_counter::time_point start_time;
    timer<> _timer;

public:
    // _count is public so the collectd will be able to use it.
    // for all other cases use the count() method
    uint64_t _count = 0;
    timed_rate_moving_average() : start_time(latency_counter::now()), _timer([this] {
        update();
    }) {
        _timer.arm_periodic(tick_interval());
    }

    void mark(uint64_t n = 1) {
        _count += n;
        for (int i = 0; i < 3; i++) {
            rates[i].add(n);
        }
    }

    rate_moving_average rate() const {
        rate_moving_average res;
        double elapsed = std::chrono::duration_cast<std::chrono::seconds>(latency_counter::now() - start_time).count();
        // We condition also in elapsed because it can happen that the call
        // for the rate calculation was performed too early and will not yield
        // meaningful results (i.e mean_rate is infinity) so the best thing is
        // to return 0 as it best reflects the state.
        if ((_count > 0) && (elapsed >= 1.0)) [[likely]] {
            res.mean_rate = (_count / elapsed);
        } else {
            res.mean_rate = 0;
        }
        res.count = _count;
        for (int i = 0; i < 3; i++) {
            res.rates[i] = rates[i].rate();
        }
        return res;
    }

    void update() {
        for (int i = 0; i < 3; i++) {
            rates[i].update();
        }
    }

    uint64_t count() const {
        return _count;
    }
};


struct rate_moving_average_and_histogram {
    ihistogram hist;
    rate_moving_average rate;

    rate_moving_average_and_histogram& operator +=(const rate_moving_average_and_histogram& o) {
        hist += o.hist;
        rate += o.rate;
        return *this;
    }
    friend rate_moving_average_and_histogram operator +(rate_moving_average_and_histogram a, const rate_moving_average_and_histogram& b);
};

inline rate_moving_average_and_histogram operator +(rate_moving_average_and_histogram a, const rate_moving_average_and_histogram& b) {
    a += b;
    return a;
}

/**
 * A timer metric which aggregates timing durations and provides duration statistics, plus
 * throughput statistics via meter
 */
class timed_rate_moving_average_and_histogram {
public:
    ihistogram hist;
    timed_rate_moving_average met;
    timed_rate_moving_average_and_histogram() = default;
    timed_rate_moving_average_and_histogram(timed_rate_moving_average_and_histogram&&) = default;
    timed_rate_moving_average_and_histogram(const timed_rate_moving_average_and_histogram&) = default;
    timed_rate_moving_average_and_histogram(size_t size, int64_t _sample_mask = 0x80) : hist(size, _sample_mask) {}
    timed_rate_moving_average_and_histogram& operator=(const timed_rate_moving_average_and_histogram&) = default;

    template <typename Rep, typename Ratio>
    void mark(std::chrono::duration<Rep, Ratio> dur) {
        if (std::chrono::duration_cast<ihistogram::duration_unit>(dur).count() >= 0) {
            hist.mark(dur);
            met.mark();
        }
    }

    void mark(latency_counter& lc) {
        hist.mark(lc);
        met.mark();
    }

    void set_latency(latency_counter& lc) {
        hist.set_latency(lc);
    }

    rate_moving_average_and_histogram rate() const {
        rate_moving_average_and_histogram res;
        res.hist = hist;
        res.rate = met.rate();
        return res;
    }
};

}
