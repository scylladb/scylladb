/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <vector>
#include <random>
#include <string>
#include <seastar/core/thread.hh>
#include "utils/assert.hh"
#include "utils/logalloc.hh"

struct stress_config {
    int count;
    int iters;
    std::string keys;
    bool verb;
};

enum class stress_step { before_insert, before_erase, iteration_finished };

template <typename Insert, typename Erase, typename Validate, typename Step>
void stress_collection(const stress_config& conf, Insert&& insert, Erase&& erase, Validate&& validate, Step&& step) {
    std::vector<int> keys;

    fmt::print("Inserting {:d} k:v pairs {:d} times\n", conf.count, conf.iters);

    for (int i = 0; i < conf.count; i++) {
        keys.push_back(i + 1);
    }

    std::random_device rd;
    std::mt19937 g(rd());

    if (conf.keys == "desc") {
        fmt::print("Reversing keys vector\n");
        std::reverse(keys.begin(), keys.end());
    }

    bool shuffle = conf.keys == "rand";
    if (shuffle) {
        fmt::print("Will shuffle keys each iteration\n");
    }


    for (auto rep = 0; rep < conf.iters; rep++) {
        if (conf.verb) {
            fmt::print("Iteration {:d}\n", rep);
        }

        if (shuffle) {
            std::shuffle(keys.begin(), keys.end(), g);
        }

        step(stress_step::before_insert);
        for (int i = 0; i < conf.count; i++) {
            if (conf.verb) {
                fmt::print("+++ {}\n", keys[i]);
            }

            insert(keys[i]);
            if (i % (i/1000 + 1) == 0) {
                validate();
            }

            seastar::thread::maybe_yield();
        }

        if (shuffle) {
            std::shuffle(keys.begin(), keys.end(), g);
        }

        step(stress_step::before_erase);
        for (int i = 0; i < conf.count; i++) {
            if (conf.verb) {
                fmt::print("--- {}\n", keys[i]);
            }

            erase(keys[i]);
            if ((conf.count-i) % ((conf.count-i)/1000 + 1) == 0) {
                validate();
            }

            seastar::thread::maybe_yield();
        }

        step(stress_step::iteration_finished);
    }
}

class reference {
    reference* _ref = nullptr;
public:
    reference() = default;
    reference(const reference& other) = delete;

    reference(reference&& other) noexcept : _ref(other._ref) {
        if (_ref != nullptr) {
            _ref->_ref = this;
        }
        other._ref = nullptr;
    }

    ~reference() {
        if (_ref != nullptr) {
            _ref->_ref = nullptr;
        }
    }

    void link(reference& other) {
        SCYLLA_ASSERT(_ref == nullptr);
        _ref = &other;
        other._ref = this;
    }

    reference* get() {
        SCYLLA_ASSERT(_ref != nullptr);
        return _ref;
    }
};

template <typename Tree>
class tree_pointer {
    reference _ref;

    class tree_wrapper {
        friend class tree_pointer;
        Tree _tree;
        reference _ref;
    public:
        template <typename... Args>
        tree_wrapper(Args&&... args) : _tree(std::forward<Args>(args)...) {}
    };

    tree_wrapper* get_wrapper() {
        return boost::intrusive::get_parent_from_member(_ref.get(), &tree_wrapper::_ref);
    }

public:

    tree_pointer(const tree_pointer& other) = delete;
    tree_pointer(tree_pointer&& other) = delete;

    template <typename... Args>
    tree_pointer(Args&&... args) {
        tree_wrapper *t = current_allocator().construct<tree_wrapper>(std::forward<Args>(args)...);
        _ref.link(t->_ref);
    }

    Tree* operator->() {
        tree_wrapper *tw = get_wrapper();
        return &tw->_tree;
    }

    Tree& operator*() {
        tree_wrapper *tw = get_wrapper();
        return tw->_tree;
    }

    ~tree_pointer() {
        tree_wrapper *tw = get_wrapper();
        current_allocator().destroy(tw);
    }
};

template <typename Insert, typename Erase, typename Validate, typename Clear>
void stress_compact_collection(const stress_config& conf, Insert&& insert, Erase&& erase, Validate&& validate, Clear&& clear) {
    fmt::print("Compacting {:d} k:v pairs {:d} times\n", conf.count, conf.iters);

    std::vector<int> keys;
    for (int i = 0; i < conf.count; i++) {
        keys.push_back(i + 1);
    }

    std::random_device rd;
    std::mt19937 g(rd());

    logalloc::region mem;

    with_allocator(mem.allocator(), [&] {
        for (auto rep = 0; rep < conf.iters; rep++) {
            std::shuffle(keys.begin(), keys.end(), g);
            {
                logalloc::reclaim_lock rl(mem);

                for (int i = 0; i < conf.count; i++) {
                    insert(keys[i]);
                }
            }

            mem.full_compaction();
            validate();

            std::shuffle(keys.begin(), keys.end(), g);
            {
                logalloc::reclaim_lock rl(mem);

                for (int i = 0; i < conf.count; i++) {
                    erase(keys[i]);
                }
            }

            mem.full_compaction();
            validate();

            seastar::thread::maybe_yield();
        }

        clear();
    });
}
