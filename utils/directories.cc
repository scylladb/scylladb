/*
 * Copyright (C) 2019 ScyllaDB
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

#include "init.hh"
#include "supervisor.hh"
#include "directories.hh"
#include "distributed_loader.hh"
#include "disk-error-handler.hh"

namespace utils {

static future<> disk_sanity(sstring path, bool developer_mode) {
    return check_direct_io_support(path).then([] {
        return make_ready_future<>();
    }).handle_exception([path](auto ep) {
        startlog.error("Could not access {}: {}", path, ep);
        return make_exception_future<>(ep);
    });
};

future<> directories::touch_and_lock(sstring path) {
    return io_check([path] { return recursive_touch_directory(path); }).then_wrapped([this, path] (future<> f) {
        try {
            f.get();
            return file_lock::acquire(fs::path(path) / ".lock").then([this](file_lock lock) {
               _locks.emplace_back(std::move(lock));
            }).handle_exception([path](auto ep) {
                // only do this because "normal" unhandled exception exit in seastar
                // _drops_ system_error message ("what()") and thus does not quite deliver
                // the relevant info to the user
                try {
                    std::rethrow_exception(ep);
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

future<> directories::init(db::config& cfg, bool hinted_handoff_enabled) {
  // XXX -- this indentation is temporary, wil go away with next patches
  return seastar::async([&] {
    std::unordered_set<sstring> directories;
    directories.insert(cfg.data_file_directories().cbegin(),
            cfg.data_file_directories().cend());
    directories.insert(cfg.commitlog_directory());

    if (hinted_handoff_enabled) {
        fs::path hints_base_dir(cfg.hints_directory());
        directories.insert(cfg.hints_directory());
        for (unsigned i = 0; i < smp::count; ++i) {
            sstring shard_dir((hints_base_dir / seastar::to_sstring(i).c_str()).native());
            directories.insert(std::move(shard_dir));
        }
    }
    fs::path view_pending_updates_base_dir = fs::path(cfg.view_hints_directory());
    sstring view_pending_updates_base_dir_str = view_pending_updates_base_dir.native();
    directories.insert(view_pending_updates_base_dir_str);
    for (unsigned i = 0; i < smp::count; ++i) {
        sstring shard_dir((view_pending_updates_base_dir / seastar::to_sstring(i).c_str()).native());
        directories.insert(std::move(shard_dir));
    }

    supervisor::notify("creating and verifying directories");
    parallel_for_each(directories, [this, &cfg] (sstring path) {
        return touch_and_lock(path).then([path = std::move(path), &cfg] {
            return disk_sanity(path, cfg.developer_mode()).then([path = std::move(path)] {
                return distributed_loader::verify_owner_and_mode(fs::path(path)).handle_exception([](auto ep) {
                    startlog.error("Failed owner and mode verification: {}", ep);
                    return make_exception_future<>(ep);
                });
            });
        });
    }).get();
  });
}

} // namespace utils
