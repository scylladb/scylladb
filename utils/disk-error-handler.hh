/*
 * Copyright 2016-present ScyllaDB
 **/

/* SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <boost/signals2/dummy_mutex.hpp>
#include <boost/signals2/signal_type.hpp>
#include <type_traits>
#include <concepts>

#include <seastar/core/future.hh>

#include "seastarx.hh"

namespace bs2 = boost::signals2;

using disk_error_signal_type = bs2::signal_type<void (), bs2::keywords::mutex_type<bs2::dummy_mutex>>::type;

extern thread_local disk_error_signal_type commit_error;
extern thread_local disk_error_signal_type sstable_read_error;
extern thread_local disk_error_signal_type sstable_write_error;
extern thread_local disk_error_signal_type general_disk_error;
extern thread_local disk_error_signal_type logstor_error;

bool should_stop_on_system_error(const std::system_error& e);

using io_error_handler = std::function<void (std::exception_ptr)>;

// An sstable's read and write error handlers. Checked files keep references
// to them, so they must outlive every sstable and file of their shard.
struct io_error_handlers {
    io_error_handler read;
    io_error_handler write;
};
// Returns the calling shard's handlers: a plain function, so it is safe to pass across shards.
using io_error_handler_gen = const io_error_handlers& (*)();

io_error_handler default_io_error_handler(disk_error_signal_type& signal);
// handlers that signal sstable_read_error / sstable_write_error
io_error_handler_gen default_io_error_handler_gen();

extern thread_local io_error_handler commit_error_handler;
extern thread_local io_error_handler sstable_write_error_handler;
extern thread_local io_error_handler general_disk_error_handler;
extern thread_local io_error_handler logstor_error_handler;

template<typename Func, typename... Args>
requires std::invocable<Func, Args&&...>
        && (!is_future<std::invoke_result_t<Func, Args&&...>>::value)
std::invoke_result_t<Func, Args&&...>
do_io_check(const io_error_handler& error_handler, Func&& func, Args&&... args) {
    try {
        // calling function
        return func(std::forward<Args>(args)...);
    } catch (...) {
        error_handler(std::current_exception());
        throw;
    }
}

template<typename Func, typename... Args>
requires std::invocable<Func, Args&&...>
        && is_future<std::invoke_result_t<Func, Args&&...>>::value
auto do_io_check(const io_error_handler& error_handler, Func&& func, Args&&... args) noexcept {
    return futurize_invoke(func, std::forward<Args>(args)...).handle_exception([&error_handler] (auto ep) {
        error_handler(ep);
        return futurize<std::invoke_result_t<Func, Args&&...>>::make_exception_future(ep);
    });
}

template<typename Func, typename... Args>
auto commit_io_check(Func&& func, Args&&... args) noexcept(is_future<std::invoke_result_t<Func, Args&&...>>::value) {
    return do_io_check(commit_error_handler, std::forward<Func>(func), std::forward<Args>(args)...);
}

template<typename Func, typename... Args>
auto sstable_io_check(const io_error_handler& error_handler, Func&& func, Args&&... args) noexcept(is_future<std::invoke_result_t<Func, Args&&...>>::value) {
    return do_io_check(error_handler, std::forward<Func>(func), std::forward<Args>(args)...);
}

template<typename Func, typename... Args>
auto io_check(const io_error_handler& error_handler, Func&& func, Args&&... args) noexcept(is_future<std::invoke_result_t<Func, Args&&...>>::value) {
    return do_io_check(error_handler, general_disk_error, std::forward<Func>(func), std::forward<Args>(args)...);
}

template<typename Func, typename... Args>
auto io_check(Func&& func, Args&&... args) noexcept(is_future<std::invoke_result_t<Func, Args&&...>>::value) {
    return do_io_check(general_disk_error_handler, std::forward<Func>(func), std::forward<Args>(args)...);
}
