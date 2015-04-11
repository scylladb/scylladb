/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
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
    std::exception_ptr _exception;
private:
    void interrupt_ptr(std::exception_ptr e) {
        std::unique_lock<std::mutex> lock(_mutex);
        if (!_exception) {
            _exception = e;
            _cv.notify_all();
        }
        // FIXME: log if already interrupted
    }
public:
    template <typename Exception>
    void interrupt(Exception e) {
        try {
            throw e;
        } catch (...) {
            interrupt_ptr(std::current_exception());
        }
    }
    void give(T value) {
        std::unique_lock<std::mutex> lock(_mutex);
        _cv.wait(lock, [this] { return !_element || _exception; });
        if (_exception) {
            std::rethrow_exception(_exception);
        }
        _element = value;
        _cv.notify_one();
    }
    T take() {
        std::unique_lock<std::mutex> lock(_mutex);
        _cv.wait(lock, [this] { return bool(_element) || _exception; });
        if (_exception) {
            std::rethrow_exception(_exception);
        }
        auto v = *_element;
        _element = {};
        _cv.notify_one();
        return v;
    }
};
