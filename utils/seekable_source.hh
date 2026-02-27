/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <chrono>
#include <seastar/core/iostream.hh>

class seekable_data_source_impl : public seastar::data_source_impl {
public:
    virtual seastar::future<seastar::temporary_buffer<char>> get(size_t limit) = 0;
    virtual seastar::future<> seek(uint64_t pos) = 0;
    virtual seastar::future<uint64_t> size() {
        return seastar::make_ready_future<uint64_t>(0);
    }
    virtual seastar::future<std::chrono::system_clock::time_point> timestamp() {
        return seastar::make_ready_future<std::chrono::system_clock::time_point>();
    }
};

class seekable_data_source : public seastar::data_source {
public:
    seekable_data_source(std::unique_ptr<seekable_data_source_impl> src)
        : seastar::data_source(std::move(src))
    {}
    seastar::future<seastar::temporary_buffer<char>> get(size_t limit) noexcept {
        try {
            return static_cast<seekable_data_source_impl*>(impl())->get(limit);
        } catch (...) {
            return seastar::current_exception_as_future<seastar::temporary_buffer<char>>();
        }
    }
    seastar::future<> seek(uint64_t pos) noexcept {
        try {
            return static_cast<seekable_data_source_impl*>(impl())->seek(pos);
        } catch (...) {
            return seastar::current_exception_as_future<>();
        }
    }
    seastar::future<uint64_t> size() noexcept {
        try {
            return static_cast<seekable_data_source_impl*>(impl())->size();
        } catch (...) {
            return seastar::current_exception_as_future<uint64_t>();
        }
    }
    seastar::future<std::chrono::system_clock::time_point> timestamp() noexcept {
        try {
            return static_cast<seekable_data_source_impl*>(impl())->timestamp();
        } catch (...) {
            return seastar::current_exception_as_future<std::chrono::system_clock::time_point>();
        }
    }
};
