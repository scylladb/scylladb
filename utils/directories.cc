/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <fmt/std.h>
#include <seastar/core/seastar.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include "init.hh"
#include "supervisor.hh"
#include "directories.hh"
#include "sstables/exceptions.hh"
#include "sstables/open_info.hh"
#include "utils/disk-error-handler.hh"
#include "utils/fmt-compat.hh"
#include "utils/lister.hh"

namespace utils {

static future<> disk_sanity(fs::path path, bool developer_mode) {
    try {
        co_await check_direct_io_support(path.native());
    } catch (...) {
        startlog.error("Coould not access {}: {}", path, std::current_exception());
        throw;
    }
};

static future<file_lock> touch_and_lock(fs::path path) {
    return io_check([path] { return recursive_touch_directory(path.native()); }).then_wrapped([path] (future<> f) {
        try {
            f.get();
            return file_lock::acquire(path / ".lock").then_wrapped([path](future<file_lock> f) {
                // only do this because "normal" unhandled exception exit in seastar
                // _drops_ system_error message ("what()") and thus does not quite deliver
                // the relevant info to the user
                try {
                    return make_ready_future<file_lock>(f.get());
                } catch (std::exception& e) {
                    startlog.error("Could not initialize {}: {}", path, e.what());
                    throw;
                } catch (...) {
                    throw;
                }
            });
        } catch (...) {
            startlog.error("Directory '{}' cannot be initialized. Tried to do it but failed with: {}", path, std::current_exception());
            throw;
        }
    });
}

void directories::set::add(fs::path path) {
    _paths.insert(path);
}

void directories::set::add(sstring path) {
    add(fs::path(path));
}

void directories::set::add(std::vector<sstring> paths) {
    for (auto& path : paths) {
        add(path);
    }
}

void directories::set::add_sharded(sstring p) {
    fs::path path(p);

    for (unsigned i = 0; i < smp::count; i++) {
        add(path / seastar::to_sstring(i).c_str());
    }
}

directories::directories(bool developer_mode)
        : _developer_mode(developer_mode)
{ }

future<> directories::create_and_verify(directories::set dir_set, recursive recursive) {
    std::vector<file_lock> locks;
    locks.reserve(dir_set.get_paths().size());
    co_await coroutine::parallel_for_each(dir_set.get_paths(), [this, &locks, recursive] (fs::path path) -> future<> {
        file_lock lock = co_await touch_and_lock(path);
        locks.emplace_back(std::move(lock));
        co_await disk_sanity(path, _developer_mode);
        try {
            co_await directories::verify_owner_and_mode(path, recursive);
        } catch (...) {
            std::exception_ptr ep = std::current_exception();
            startlog.error("Failed owner and mode verification: {}", ep);
            throw ep;
        }
    });
    std::move(locks.begin(), locks.end(), std::back_inserter(_locks));
}

template <typename... Args>
static inline
void verification_error(fs::path path, const char* fstr, Args&&... args) {
    auto emsg = fmt::format(fmt::runtime(fstr), std::forward<Args>(args)...);
    startlog.error("{}: {}", path.string(), emsg);
    throw std::runtime_error(emsg);
}

// Verify that all files and directories are owned by current uid
// and that files can be read and directories can be read, written, and looked up (execute)
// No other file types may exist.
// If a 'do_verify_subpath' function is provided, only the subpaths
// that return true when called with that function will be verified.
future<> directories::do_verify_owner_and_mode(fs::path path, recursive recurse, int level, std::function<bool(const fs::path&)> do_verify_subpath) {
    auto sd = co_await file_stat(path.string(), follow_symlink::no);
    // Under docker, we run with euid 0 and there is no reasonable way to enforce that the
    // in-container uid will have the same uid as files mounted from outside the container. So
    // just allow euid 0 as a special case. It should survive the file_accessible() checks below.
    // See #4823.
    if (geteuid() != 0 && sd.uid != geteuid()) {
        verification_error(std::move(path), "File not owned by current euid: {}. Owner is: {}", geteuid(), sd.uid);
    }
    switch (sd.type) {
    case directory_entry_type::regular: {
        bool can_access = co_await file_accessible(path.string(), access_flags::read);
        if (!can_access) {
            verification_error(std::move(path), "File cannot be accessed for read");
        }
        break;
    }
    case directory_entry_type::directory: {
        bool can_access = co_await file_accessible(path.string(), access_flags::read | access_flags::write | access_flags::execute);
        if (!can_access) {
            verification_error(std::move(path), "Directory cannot be accessed for read, write, and execute");
        }
        if (level && !recurse) {
            co_return;
        }
        co_await lister::scan_dir(path, {}, [recurse, level = level + 1, &do_verify_subpath] (fs::path dir, directory_entry de) -> future<> {
            auto subpath = dir / de.name;
            if (!do_verify_subpath || do_verify_subpath(subpath)) {
                co_await do_verify_owner_and_mode(std::move(subpath), recurse, level, do_verify_subpath);
            }
        });
        break;
    }
    default:
        verification_error(std::move(path), "Must be either a regular file or a directory (type={})", static_cast<int>(sd.type));
    }
};

future<> directories::verify_owner_and_mode(fs::path path, recursive recursive) {
    return do_verify_owner_and_mode(std::move(path), recursive, 0);
}

// Verify the data directory contents in a specific order so as to prevent
// memory fragmentation in the inode/dentry cache. All the data and index files,
// which will be held open for a longer duration are verified first and the
// other files that will be closed immediately are verified later to ensure
// their separation in the dentry/inode cache.
future<> directories::verify_owner_and_mode_of_data_dir(directories::set dir_set) {
    // verify data and index files in the first iteration and the other files in the second iteration.
    for (auto verify_data_and_index_files : { true, false }) {
        co_await coroutine::parallel_for_each(dir_set.get_paths(), [verify_data_and_index_files] (const auto &path) {
            return do_verify_owner_and_mode(std::move(path), recursive::yes, 0, [verify_data_and_index_files] (const fs::path &path) {
                component_type path_component_type;
                try {
                    // use parse_path to deduce the component type as using system calls
                    // like stat/lstat will load the inode/dentry into the cache.
                    auto descriptor_tuple = sstables::parse_path(path);
                    path_component_type = std::get<0>(descriptor_tuple).component;
                } catch (sstables::malformed_sstable_exception) {
                    // path is not a SSTable component - do not filter it out
                    return true;
                }
                auto data_or_index_file = (path_component_type == component_type::Data || path_component_type == component_type::Index);
                return verify_data_and_index_files ? data_or_index_file : !data_or_index_file;
            });
        });
    }
}

} // namespace utils
