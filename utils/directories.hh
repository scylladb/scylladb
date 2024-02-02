/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <set>
#include <vector>
#include <seastar/core/future.hh>
#include <seastar/core/smp.hh>
#include "utils/file_lock.hh"

using namespace seastar;

namespace db {
class config;
}

namespace utils {

class directories {
public:
    class set {
    public:
        void add(fs::path path);
        void add(sstring path);
        void add(std::vector<sstring> path);
        void add_sharded(sstring path);

        const std::set<fs::path> get_paths() const {
            return _paths;
        }

    private:
        std::set<fs::path> _paths;
    };

    using recursive = bool_class<struct recursive_tag>;

    directories(bool developer_mode);
    future<> create_and_verify(set dir_set, recursive recursive = recursive::yes);
    static future<> verify_owner_and_mode(std::filesystem::path path, recursive recursive = recursive::yes);
    static future<> verify_owner_and_mode_of_data_dir(set dir_set);
private:
    bool _developer_mode;
    std::vector<file_lock> _locks;

    static future<> do_verify_owner_and_mode(std::filesystem::path path, recursive, int level, std::function<bool(const fs::path&)> do_verify_subpath = {});
};

} // namespace utils
