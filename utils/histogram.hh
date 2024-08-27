/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <boost/circular_buffer.hpp>
#include "latency.hh"
#include <cmath>
#include <seastar/core/timer.hh>
#include <iosfwd>
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
     *
     * Typically, sample_mask is of the form of 2^n-1 which would
     * mean that we sample one of 2^n, but setting sample_mask to zero
     * would mean we would always sample.
     */
    bool should_sample() const noexcept {
        return total == 0 || ((started & sample_mask) == sample_mask);
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

/*!
 * \brief a helper timer class for the metering functionality
 *
 * To make an object use a timer, include an instance of this
 * class and set a handler at its constructor.
 */
class meter_timer {
    std::function<void()> _fun;
    timer<> _timer;
public:
    static constexpr latency_counter::duration tick_interval() {
        return std::chrono::seconds(10);
    }

    meter_timer(std::function<void()>&& fun) : _fun(std::move(fun)), _timer(_fun) {
        _timer.arm_periodic(tick_interval());
    }
};

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

class rates_moving_average {
    latency_counter::time_point start_time;
    moving_average rates[3] = {{std::chrono::minutes(1), meter_timer::tick_interval()}, {std::chrono::minutes(5), meter_timer::tick_interval()}, {std::chrono::minutes(15), meter_timer::tick_interval()}};
public:
    // _count is public so the collectd will be able to use it.
    // for all other cases use the count() method
    uint64_t _count = 0;
    rates_moving_average() : start_time(latency_counter::now()) {
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

    void update() noexcept {
        for (int i = 0; i < 3; i++) {
            rates[i].update();
        }
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
    rates_moving_average& operator()() noexcept {
        return _rates;
    }
    const rates_moving_average& operator()() const noexcept {
        return _rates;
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
 * a summary is defined over some duration (i.e., latencies in the last 10 seconds).
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
     * The update method is called every time tick.
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
        if (std::chrono::duration_cast<ihistogram::duration_unit>(dur).count() >= 0) {
            _current_histogram.add(dur);
        }
    }

    const time_estimated_histogram& histogram() const noexcept {
        return _current_histogram;
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
    timed_rate_moving_average_and_histogram(size_t size, int64_t _sample_mask = 0x80) : hist(size, _sample_mask) {}

    template <typename Rep, typename Ratio>
    void mark(std::chrono::duration<Rep, Ratio> dur) {
        if (std::chrono::duration_cast<ihistogram::duration_unit>(dur).count() >= 0) {
            hist.mark(dur);
            met().mark();
        }
    }

    void mark(latency_counter& lc) {
        hist.mark(lc);
        met().mark();
    }

    void set_latency(latency_counter& lc) {
        hist.set_latency(lc);
    }

    rate_moving_average_and_histogram rate() const {
        rate_moving_average_and_histogram res;
        res.hist = hist;
        res.rate = met().rate();
        return res;
    }
};

/**
 * \brief A unified timer-based histogram rate and summary collector.
 *
 * This timer metric handles all latencies histogram options for the API and the metrics layer.
 *
 * The metrics layer requires a histogram of the values from the system start and a quantile
 * summary from the last time tick.
 *
 * The API requires a moving average and its kind of histogram (ihistogram)
 *
 * This class will replace timed_rate_moving_average_and_histogram and share the same API.
 *
 * The summary calculation is per some interval, that interval should be reasonable, by default
 * it is set to 30s, but can be set to something else.
 * Because it is different than the tick_interval _match_duration holds once in every how
 * many times the summary should be updated.
 *
 */
class timed_rate_moving_average_summary_and_histogram {
    meter_timer _timer;
    summary_calculator _summary;
    rates_moving_average _rates;
    size_t _match_duration = 0;
    size_t _last_update = 0;
public:
    ihistogram hist;
    timed_rate_moving_average_summary_and_histogram(latency_counter::duration d = std::chrono::seconds(30)) : _timer([this]{
        _rates.update();
        _summary.update();}) {
        _match_duration = d/meter_timer::tick_interval();
    }
    rates_moving_average& operator()() noexcept {
        return _rates;
    }
    const rates_moving_average& operator()() const noexcept {
        return _rates;
    }

    timed_rate_moving_average_summary_and_histogram(timed_rate_moving_average_summary_and_histogram&&) = default;
    timed_rate_moving_average_summary_and_histogram(size_t size) : _timer([this]{
        _rates.update();
        _last_update++;
        if (_last_update < _match_duration) {
            return;
        }
        _last_update = 0;
        _summary.update();}), hist(size, 0) {
    }

    template <typename Rep, typename Ratio>
    void mark(std::chrono::duration<Rep, Ratio> dur) noexcept {
        if (std::chrono::duration_cast<ihistogram::duration_unit>(dur).count() >= 0) {
            hist.mark(dur);
            _summary.mark(dur);
            _rates.mark();
        }
    }

    void mark(latency_counter& lc) noexcept {
        hist.mark(lc);
        _summary.mark(lc.latency());
        _rates.mark();
    }

    void set_latency(latency_counter& lc) noexcept {
        hist.set_latency(lc);
    }

    rate_moving_average_and_histogram rate() const noexcept {
        rate_moving_average_and_histogram res;
        res.hist = hist;
        res.rate = _rates.rate();
        return res;
    }
    const time_estimated_histogram& histogram() const noexcept {
        return _summary.histogram();
    }

    const summary_calculator& summary() const noexcept {
        return _summary;
    }
};

}
