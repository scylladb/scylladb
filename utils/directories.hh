/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <filesystem>
#include <set>
#include <string_view>
#include <vector>

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/bool_class.hh>

#include "utils/file_lock.hh"

namespace fs = std::filesystem;

namespace db {
class config;
}

namespace utils {

class directories {
public:
    class set;
    using recursive = seastar::bool_class<struct recursive_tag>;

    directories(bool developer_mode);
    seastar::future<> create_and_verify(set dir_set);
    static seastar::future<> verify_owner_and_mode(fs::path path, recursive r = recursive::yes);

private:
    static seastar::future<> do_verify_owner_and_mode(fs::path path, recursive, int level);

    bool _developer_mode;
    std::vector<file_lock> _locks;
};

class directories::set {
public:
    void add(fs::path path);
    void add(seastar::sstring path);
    void add(std::vector<seastar::sstring> path);
    void add_sharded(seastar::sstring path);

    const std::set<fs::path> get_paths() const {
        return _paths;
    }

private:
    std::set<fs::path> _paths;
};

} // namespace utils
