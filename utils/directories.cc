/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <seastar/core/seastar.hh>
#include "init.hh"
#include "supervisor.hh"
#include "directories.hh"
#include "utils/disk-error-handler.hh"
#include "db/config.hh"
#include "lister.hh"

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

directories::directories(bool developer_mode)
        : _developer_mode(developer_mode)
{ }

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
future<> verification_error(fs::path path, const char* fstr, Args&&... args) {
    auto emsg = fmt::format(fstr, std::forward<Args>(args)...);
    startlog.error("{}: {}", path.string(), emsg);
    return make_exception_future<>(std::runtime_error(emsg));
}

// Verify that all files and directories are owned by current uid
// and that files can be read and directories can be read, written, and looked up (execute)
// No other file types may exist.
future<> directories::verify_owner_and_mode(fs::path path) {
    return file_stat(path.string(), follow_symlink::no).then([path = std::move(path)] (stat_data sd) {
        // Under docker, we run with euid 0 and there is no reasonable way to enforce that the
        // in-container uid will have the same uid as files mounted from outside the container. So
        // just allow euid 0 as a special case. It should survive the file_accessible() checks below.
        // See #4823.
        if (geteuid() != 0 && sd.uid != geteuid()) {
            return verification_error(std::move(path), "File not owned by current euid: {}. Owner is: {}", geteuid(), sd.uid);
        }
        switch (sd.type) {
        case directory_entry_type::regular: {
            auto f = file_accessible(path.string(), access_flags::read);
            return f.then([path = std::move(path)] (bool can_access) {
                if (!can_access) {
                    return verification_error(std::move(path), "File cannot be accessed for read");
                }
                return make_ready_future<>();
            });
            break;
        }
        case directory_entry_type::directory: {
            auto f = file_accessible(path.string(), access_flags::read | access_flags::write | access_flags::execute);
            return f.then([path = std::move(path)] (bool can_access) {
                if (!can_access) {
                    return verification_error(std::move(path), "Directory cannot be accessed for read, write, and execute");
                }
                return lister::scan_dir(path, {}, [] (fs::path dir, directory_entry de) {
                    return verify_owner_and_mode(dir / de.name);
                });
            });
            break;
        }
        default:
            return verification_error(std::move(path), "Must be either a regular file or a directory (type={})", static_cast<int>(sd.type));
        }
    });
};

} // namespace utils
