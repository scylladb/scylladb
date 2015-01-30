/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include <mutex>
#include <condition_variable>
#include <experimental/optional>

// Single-element blocking queue
template <typename T>
class exchanger {
private:
    std::mutex _mutex;
    std::condition_variable _cv;
    std::experimental::optional<T> _element;
public:
    void give(T value) {
        std::unique_lock<std::mutex> lock(_mutex);
        _cv.wait(lock, [this] { return !_element; });
        _element = value;
        _cv.notify_one();
    }
    T take() {
        std::unique_lock<std::mutex> lock(_mutex);
        _cv.wait(lock, [this] { return bool(_element); });
        auto v = *_element;
        _element = {};
        _cv.notify_one();
        return v;
    }
};
