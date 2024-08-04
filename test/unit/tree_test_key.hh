/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "utils/assert.hh"
#include <fmt/core.h>
#include <cassert>

/*
 * Helper class that helps to check that tree
 * - works with keys without default contstuctor
 * - moves the keys around properly
 */
class tree_test_key_base {
    int _val;
    int* _cookie;
    int* _p_cookie;

public:
    int cookie() const noexcept { return *_cookie; }

    bool is_alive() const {
        if (_val == -1) {
            fmt::print("key value is reset\n");
            return false;
        }

        if (_cookie == nullptr) {
            fmt::print("key cookie is reset\n");
            return false;
        }

        if (*_cookie != 0) {
            fmt::print("key cookie value is corrupted {}\n", *_cookie);
            return false;
        }

        return true;
    }

    bool less(const tree_test_key_base& o) const noexcept {
        return _val < o._val;
    }

    int compare(const int o) const noexcept {
        if (_val > o) {
            return 1;
        } else if (_val < o) {
            return -1;
        } else {
            return 0;
        }
    }

    int compare(const tree_test_key_base& o) const noexcept {
        return compare(o._val);
    }

    explicit tree_test_key_base(int nr, int cookie = 0) : _val(nr) {
        _cookie = new int(cookie);
        _p_cookie = new int(1);
    }

    operator int() const noexcept { return _val; }

    tree_test_key_base& operator=(const tree_test_key_base& other) = delete;
    tree_test_key_base& operator=(tree_test_key_base&& other) = delete;

private:
    /*
     * Keep this private to make bptree.hh explicitly call the
     * copy_key in the places where the key is copied
     */
    tree_test_key_base(const tree_test_key_base& other) : _val(other._val) {
        _cookie = new int(*other._cookie);
        _p_cookie = new int(*other._p_cookie);
    }

    friend tree_test_key_base copy_key(const tree_test_key_base&);

public:
    struct force_copy_tag {};
    tree_test_key_base(const tree_test_key_base& other, force_copy_tag) : tree_test_key_base(other) {}

    tree_test_key_base(tree_test_key_base&& other) noexcept : _val(other._val) {
        other._val = -1;
        _cookie = other._cookie;
        other._cookie = nullptr;
        _p_cookie = new int(*other._p_cookie);
    }

    ~tree_test_key_base() {
        if (_cookie != nullptr) {
            delete _cookie;
        }
        SCYLLA_ASSERT(_p_cookie != nullptr);
        delete _p_cookie;
    }
};

tree_test_key_base copy_key(const tree_test_key_base& other) { return tree_test_key_base(other); }

struct test_key_compare {
    bool operator()(const tree_test_key_base& a, const tree_test_key_base& b) const noexcept { return a.less(b); }
};

struct test_key_tri_compare {
    std::strong_ordering operator()(const tree_test_key_base& a, const tree_test_key_base& b) const noexcept { return a.compare(b) <=> 0; }
    std::strong_ordering operator()(const int a, const tree_test_key_base& b) const noexcept { return -b.compare(a) <=> 0; }
};

template <std::derived_from<tree_test_key_base> T> struct fmt::formatter<T> : fmt::formatter<int> {};
