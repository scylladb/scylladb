/*
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
 */
#pragma once
#include "core/shared_ptr.hh"
#include "core/distributed.hh"

class filter_tracker {

    lw_shared_ptr<distributed<filter_tracker>> _ptr;
    uint64_t false_positive;
    uint64_t true_positive;

    uint64_t last_false_positive = 0;
    uint64_t last_true_positive = 0;

    uint64_t local_false_positive() {
        return false_positive;
    }
    uint64_t local_true_positive() {
        return true_positive;
    }
    uint64_t local_recent_false_positive() {
        auto t = false_positive - last_false_positive;
        last_false_positive = false_positive;
        return t;
    }
    uint64_t local_recent_true_positive() {
        auto t = true_positive - last_true_positive;
        last_true_positive = true_positive;
        return t;
    }

public:
    filter_tracker(lw_shared_ptr<distributed<filter_tracker>>&& ptr) : _ptr(std::move(ptr)) {}

    future<> stop() { return make_ready_future<>(); }

    void add_false_positive() {
        false_positive++;
    }

    void add_true_positive() {
        true_positive++;
    }

    future<uint64_t> get_false_positive() {
        return _ptr->map_reduce(adder<uint64_t>(), [] (auto& t) { return t.local_false_positive(); });
    }

    future<uint64_t> get_true_positive() {
        return _ptr->map_reduce(adder<uint64_t>(), [] (auto& t) { return t.local_true_positive(); });
    }

    future<uint64_t> get_recent_false_positive() {
        return _ptr->map_reduce(adder<uint64_t>(), [] (auto& t) { return t.local_recent_false_positive(); });
    }

    future<uint64_t> get_recent_true_positive() {
        return _ptr->map_reduce(adder<uint64_t>(), [] (auto& t) { return t.local_recent_true_positive(); });
    }
};
