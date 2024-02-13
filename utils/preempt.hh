/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/util/bool_class.hh>
#include <seastar/util/noncopyable_function.hh>
#include <seastar/core/preempt.hh>
#include <seastar/core/thread.hh>

#include "seastarx.hh"

class is_preemptible_tag;
using is_preemptible = bool_class<is_preemptible_tag>;

/// A function which decides when to preempt.
/// If it returns true then the algorithm will be interrupted.
using preemption_check = noncopyable_function<bool() noexcept>;

inline
preemption_check default_preemption_check() {
    return [] () noexcept {
        return seastar::need_preempt();
    };
}

inline
preemption_check never_preempt() {
    return [] () noexcept {
        return false;
    };
}

inline
preemption_check always_preempt() {
    return [] () noexcept {
        return true;
    };
}

struct basic_preemption_source {
    bool should_preempt() {
        return seastar::need_preempt();
    }
    void thread_yield() {
        seastar::thread::yield();
    }
};

struct custom_preemption_source {
    struct impl {
        virtual bool should_preempt() = 0;
        virtual void thread_yield() = 0;
        virtual ~impl() = default;
    };
    std::unique_ptr<impl> _impl;
    bool should_preempt() {
        return _impl ? _impl->should_preempt() : seastar::need_preempt();
    }
    void thread_yield() {
        _impl ? _impl->thread_yield() : seastar::thread::yield();
    }
    custom_preemption_source() = default;
    custom_preemption_source(std::unique_ptr<impl> i) : _impl(std::move(i)) {}
};

#ifdef SCYLLA_ENABLE_PREEMPTION_SOURCE
using preemption_source = custom_preemption_source;
#else
using preemption_source = basic_preemption_source;
#endif

