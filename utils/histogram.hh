/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "latency.hh"
#include <cmath>
#include <seastar/core/timer.hh>
#include "seastarx.hh"
#include "estimated_histogram.hh"

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

/*!
 * \brief a helper timer class for the metering functionality
 *
 * To make an object use a timer, include an instance of this
 * class and set a handler at its constructor.
 */
class meter_timer {
    timer<> _timer;
public:
    static constexpr latency_counter::duration tick_interval() {
        return std::chrono::seconds(10);
    }

    meter_timer(noncopyable_function<void()>&& fun, latency_counter::duration period = tick_interval()) : _timer(std::move(fun)) {
        _timer.arm_periodic(period);
    }
};

struct rate_moving_average {
    uint64_t count = 0;
    // The one-minute rate
    double rate = 0;
    rate_moving_average& operator +=(const rate_moving_average& o) {
        count += o.count;
        rate += o.rate;
        return *this;
    }
    friend rate_moving_average operator+ (rate_moving_average a, const rate_moving_average& b);
};

inline rate_moving_average operator+ (rate_moving_average a, const rate_moving_average& b) {
    a += b;
    return a;
}

class rates_moving_average {
    // Only the one-minute rate is kept; it is the only one with a user.
    moving_average _rate{std::chrono::minutes(1), meter_timer::tick_interval()};
    uint64_t _count = 0;
public:
    void mark(uint64_t n = 1) {
        _count += n;
        _rate.add(n);
    }

    rate_moving_average rate() const {
        rate_moving_average res;
        res.count = _count;
        res.rate = _rate.rate();
        return res;
    }

    void update() noexcept {
        _rate.update();
    }

    uint64_t count() const {
        return _count;
    }
};

/*!
 * \brief A timed wrapper for the rates moving average.
 *
 * This is a wrapper for the rates_moving_average class. It uses a meter_timer
 * to update the rates_moving_average periodically.
 */
class timed_rate_moving_average {
    rates_moving_average _rates;
    meter_timer _timer;
public:
    timed_rate_moving_average() : _timer([this]{_rates.update();}) {

    }
    void mark(uint64_t n = 1) noexcept {
        _rates.mark(n);
    }

    uint64_t count() const noexcept {
        return _rates.count();
    }

    rate_moving_average rate() const noexcept {
        return _rates.rate();
    }
};

/*!
 * \brief A class for a histogram-based summary calculation.
 *
 * A summary is a histogram where each bucket holds some quantile.
 * While a histogram typically holds values from the system start,
 * a summary is defined over some duration (i.e., latencies in the last 30 seconds).
 * To calculate a summary, we use two estimated-histograms, calculate their delta, and get the
 * summary from that delta histogram.
 *
 */
class summary_calculator {
    std::vector<double> _quantiles = { 0.5, 0.95, 0.99};
    std::vector<double> _summary = { 0, 0, 0};
    time_estimated_histogram _previous_histogram;
    time_estimated_histogram _current_histogram;
public:
    /*!
     * \brief update the summary and histograms
     *
     * The update method is called once in every summary interval.
     * When done, _previous_histogram would equal _current_histogram
     * and the _summary would contain the current _summary calculation
     *
     * The calculation is done in two stages. first, we determine what is
     * the cutoff for each quantile, for example, assume that there are new 1000
     * entries and the quantiles are 0.5, 0.95 and 0.99
     * The cutoffs will be 500, 950, and 990. We reuse the _summary array
     * to hold these values.
     *
     * Second, while coping the _current_histogram to the _previous_histogram,
     * we collect the diffs. Each time we cross a cutoff value, we update the
     * _summary with the bucket limit (i.e., the latency value).
     *
     * To continue the previous example, if the first 3 diffs had the values:
     * 10, 300, 200. When reaching the third one, the total diff will be 510,
     * and we set the summary[0] as the third bucket limit.
     *
     */
    void update() {
        auto new_entries = _current_histogram.count() - _previous_histogram.count();
        if (new_entries == 0) {
            clear();
            return;
        }
        for (size_t i = 0; i < _quantiles.size(); i++ ) {
            _summary[i] = _quantiles[i] * new_entries;
        }
        size_t pos = 0;
        size_t total_diff = 0;

        for (size_t i = 0; i < _current_histogram.size(); i++) {
            total_diff += _current_histogram[i] - _previous_histogram[i];
            while (pos < _summary.size() && total_diff >= _summary[pos]) {
                _summary[pos] = (i + 1 < _current_histogram.size()) ? _current_histogram.get_bucket_upper_limit(i):
                        _current_histogram.get_bucket_lower_limit(i);
                pos++;
            }
            _previous_histogram[i] = _current_histogram[i];
        }
    }

    const std::vector<double>& quantiles() const noexcept {
        return _quantiles;
    }

    void clear() {
        for (size_t i =0; i< _summary.size(); i++) {
            _summary[i] = 0;
        }
    }
    void set_quantiles(const std::vector<double>& quantiles) {
        _quantiles = quantiles;
        _summary.resize(quantiles.size());
        clear();
    }

    const std::vector<double>& summary() const noexcept {
        return _summary;
    }

    template <typename Rep, typename Ratio>
    void mark(std::chrono::duration<Rep, Ratio> dur) {
        if (std::chrono::duration_cast<std::chrono::microseconds>(dur).count() >= 0) {
            _current_histogram.add(dur);
        }
    }

    const time_estimated_histogram& histogram() const noexcept {
        return _current_histogram;
    }
};

/**
 * \brief A unified timer-based histogram and summary collector.
 *
 * This timer metric handles all latencies histogram options for the API and the metrics layer.
 *
 * The metrics layer requires a histogram of the values from the system start and a quantile
 * summary from the last time tick.
 *
 * The API uses the same histogram, which also serves as its counter.
 *
 * The summary calculation is per some interval, that interval should be reasonable, by default
 * it is set to 30s, but can be set to something else.
 *
 */
class summary_and_histogram {
    meter_timer _timer;
    summary_calculator _summary;
public:
    summary_and_histogram(latency_counter::duration d = std::chrono::seconds(30)) : _timer([this]{
        _summary.update();}, d) {
    }

    summary_and_histogram(summary_and_histogram&&) = default;

    template <typename Rep, typename Ratio>
    void mark(std::chrono::duration<Rep, Ratio> dur) noexcept {
        if (std::chrono::duration_cast<std::chrono::microseconds>(dur).count() >= 0) {
            _summary.mark(dur);
        }
    }

    void mark(latency_counter& lc) noexcept {
        if (lc.is_start()) {
            lc.stop();
        }
        _summary.mark(lc.latency());
    }

    const time_estimated_histogram& histogram() const noexcept {
        return _summary.histogram();
    }

    const summary_calculator& summary() const noexcept {
        return _summary;
    }
};

// The class used to keep a moving average of the rate as well.
// The old name is kept for the code that uses it.
using timed_rate_moving_average_summary_and_histogram = summary_and_histogram;

}
