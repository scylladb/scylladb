/*
 * Copyright 2015 Cloudius Systems
 */

#pragma once

#include <boost/circular_buffer.hpp>

namespace utils {

template<typename T>
class histogram {
public:
    int64_t count;
    T min;
    T max;
    T sum;
    double mean;
    double variance;
    boost::circular_buffer<T> sample;
    histogram(size_t size = 1024)
            : count(0), min(0), max(0), sum(0), mean(0), variance(0), sample(
                    size) {
    }
    void mark(T value) {
        if (count == 0 || value < min) {
            min = value;
        }
        if (count == 0 || value > max) {
            max = value;
        }
        if (count == 0) {
            mean = value;
            variance = 0;
        } else {
            double old_m = mean;
            double old_s = variance;

            mean = old_m + ((value - old_m) / (count + 1));
            variance = old_s + ((value - old_m) * (value - mean));
        }
        sum += value;
        count++;
        sample.push_back(value);
    }
};

using ihistogram = histogram<int64_t>;

}
