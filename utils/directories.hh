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

    directories(const ::db::config& cfg);
    seastar::future<> create_and_verify(set dir_set);
    static seastar::future<> verify_owner_and_mode(fs::path path, recursive r = recursive::yes);

    seastar::sstring get_work_dir() const;
    seastar::sstring get_commitlog_dir() const;
    seastar::sstring get_schema_commitlog_dir() const;
    seastar::sstring get_hints_dir() const;
    seastar::sstring get_view_hints_dir() const;
    seastar::sstring get_saved_caches_dir() const;
    std::vector<seastar::sstring> get_data_file_dirs() const;

private:
    static seastar::future<> do_verify_owner_and_mode(fs::path path, recursive, int level);

    void override_empty_paths();
    void override_if_empty(fs::path& p, const fs::path& dest_parent, std::string_view subdir);
    void override_if_empty(std::vector<fs::path>& v, const fs::path& dest_parent, std::string_view subdir);

    bool _developer_mode;
    std::vector<file_lock> _locks;
    fs::path _work_dir{};
    fs::path _commitlog_dir{};
    fs::path _schema_commitlog_dir{};
    fs::path _hints_dir{};
    fs::path _view_hints_dir{};
    fs::path _saved_caches_dir{};
    std::vector<fs::path> _data_file_dirs{};
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
