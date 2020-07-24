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

#include <seastar/core/seastar.hh>
#include "init.hh"
#include "supervisor.hh"
#include "directories.hh"
#include "distributed_loader.hh"
#include "utils/disk-error-handler.hh"
#include "db/config.hh"

namespace utils {

static future<> disk_sanity(fs::path path, bool developer_mode) {
    return check_direct_io_support(path.native()).then([] {
        return make_ready_future<>();
    }).handle_exception([path](auto ep) {
        startlog.error("Could not access {}: {}", path, ep);
        return make_exception_future<>(ep);
    });
};

future<> directories::touch_and_lock(fs::path path) {
    return io_check([path] { return recursive_touch_directory(path.native()); }).then_wrapped([this, path] (future<> f) {
        try {
            f.get();
            return file_lock::acquire(path / ".lock").then([this](file_lock lock) {
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

static void add(fs::path path, std::set<fs::path>& to) {
    to.insert(path);
}

static void add(sstring path, std::set<fs::path>& to) {
    add(fs::path(path), to);
}

static void add(std::vector<sstring> paths, std::set<fs::path>& to) {
    for (auto& path : paths) {
        add(path, to);
    }
}

static void add_sharded(sstring p, std::set<fs::path>& to) {
    fs::path path(p);

    for (unsigned i = 0; i < smp::count; i++) {
         add(path / seastar::to_sstring(i).c_str(), to);
    }
}

future<> directories::init(db::config& cfg, bool hinted_handoff_enabled) {
    std::set<fs::path> paths;

    add(cfg.data_file_directories(), paths);
    add(cfg.commitlog_directory(), paths);
    if (hinted_handoff_enabled) {
        add_sharded(cfg.hints_directory(), paths);
    }
    add_sharded(cfg.view_hints_directory(), paths);

    supervisor::notify("creating and verifying directories");
    return parallel_for_each(paths, [this, &cfg] (fs::path path) {
        return touch_and_lock(path).then([path = std::move(path), &cfg] {
            return disk_sanity(path, cfg.developer_mode()).then([path = std::move(path)] {
                return distributed_loader::verify_owner_and_mode(path).handle_exception([](auto ep) {
                    startlog.error("Failed owner and mode verification: {}", ep);
                    return make_exception_future<>(ep);
                });
            });
        });
    });
}

} // namespace utils
