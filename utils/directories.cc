/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/core/coroutine.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/smp.hh>

#include "db/config.hh"
#include "init.hh"
#include "supervisor.hh"
#include "directories.hh"
#include "utils/disk-error-handler.hh"
#include "utils/fmt-compat.hh"
#include "utils/lister.hh"

using namespace seastar;

namespace {

std::vector<fs::path> as_paths(const std::vector<sstring>& dirs) {
    std::vector<fs::path> paths;
    paths.reserve(dirs.size());

    for (const auto& dir_str : dirs) {
        paths.emplace_back(dir_str);
    }

    return paths;
}

sstring to_sstring(const fs::path& p) {
    return sstring{p.c_str()};
}

} // namespace

namespace utils {

static future<> disk_sanity(fs::path path, bool developer_mode) {
    return check_direct_io_support(path.native()).then([] {
        return make_ready_future<>();
    }).handle_exception([path](auto ep) {
        startlog.error("Could not access {}: {}", path, ep);
        return make_exception_future<>(ep);
    });
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

directories::directories(const ::db::config& cfg)
        : _developer_mode{cfg.developer_mode()}
        , _work_dir{cfg.work_directory()}
        , _commitlog_dir{cfg.commitlog_directory()}
        , _schema_commitlog_dir{cfg.schema_commitlog_directory()}
        , _hints_dir{cfg.hints_directory()}
        , _view_hints_dir{cfg.view_hints_directory()}
        , _saved_caches_dir{cfg.saved_caches_directory()}
        , _data_file_dirs{as_paths(cfg.data_file_directories())}
{
    override_empty_paths();
}

void directories::override_empty_paths() {
    // if path is empty override it to be a subdirectory of the second argument
    override_if_empty(_commitlog_dir, _work_dir, "commitlog");
    override_if_empty(_schema_commitlog_dir, _commitlog_dir, "schema");
    override_if_empty(_data_file_dirs, _work_dir, "data");
    override_if_empty(_hints_dir, _work_dir, "hints");
    override_if_empty(_view_hints_dir, _work_dir, "view_hints");
    override_if_empty(_saved_caches_dir, _work_dir, "saved_caches");
}

void directories::override_if_empty(fs::path& p, const fs::path& dest_parent, std::string_view subdir) {
    if (p.empty()) {
        p = (dest_parent / fs::path{subdir});
    }
}

void directories::override_if_empty(std::vector<fs::path>& v, const fs::path& dest_parent, std::string_view subdir) {
    if (v.empty()) {
        v.push_back(dest_parent / fs::path{subdir});
    }
}

future<> directories::create_and_verify(directories::set dir_set) {
    return do_with(std::vector<file_lock>(), [this, dir_set = std::move(dir_set)] (std::vector<file_lock>& locks) {
        return parallel_for_each(dir_set.get_paths(), [this, &locks] (fs::path path) {
            return touch_and_lock(path).then([path = std::move(path), developer_mode = _developer_mode, &locks] (file_lock lock) {
                locks.emplace_back(std::move(lock));
                return disk_sanity(path, developer_mode).then([path = std::move(path)] {
                    return directories::verify_owner_and_mode(path).handle_exception([](auto ep) {
                        startlog.error("Failed owner and mode verification: {}", ep);
                        return make_exception_future<>(ep);
                    });
                });
            });
        }).then([this, &locks] {
            std::move(locks.begin(), locks.end(), std::back_inserter(_locks));
        });
    });
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
future<> directories::do_verify_owner_and_mode(fs::path path, recursive recurse, int level) {
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
        co_await lister::scan_dir(path, {}, [recurse, level = level + 1] (fs::path dir, directory_entry de) -> future<> {
            co_await do_verify_owner_and_mode(dir / de.name, recurse, level);
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

future<> directories::create_and_verify_directories() {
    // Note: creation of hints_dir and view_hints_dir is
    //       responsibility of db::hints::directory_initializer.
    utils::directories::set dir_set;
    dir_set.add(get_data_file_dirs());
    dir_set.add(get_commitlog_dir());
    dir_set.add(get_schema_commitlog_dir());

    return create_and_verify(std::move(dir_set));
}

sstring directories::get_work_dir() const {
    return to_sstring(_work_dir);
}

sstring directories::get_commitlog_dir() const {
    return to_sstring(_commitlog_dir);
}

sstring directories::get_schema_commitlog_dir() const {
    return to_sstring(_schema_commitlog_dir);
}

sstring directories::get_hints_dir() const {
    return to_sstring(_hints_dir);
}

sstring directories::get_view_hints_dir() const {
    return to_sstring(_view_hints_dir);
}

sstring directories::get_saved_caches_dir() const {
    return to_sstring(_saved_caches_dir);
}

std::vector<sstring> directories::get_data_file_dirs() const {
    std::vector<sstring> dirs;
    dirs.reserve(_data_file_dirs.size());

    for (const auto & dir_path : _data_file_dirs) {
        dirs.emplace_back(to_sstring(dir_path));
    }

    return dirs;
}

} // namespace utils
