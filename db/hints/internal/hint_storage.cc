/*
 * Modified by ScyllaDB
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "db/hints/internal/hint_storage.hh"

#include <fmt/ranges.h>
#include <fmt/std.h>

// Seastar features.
#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/file-types.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/file.hh>

// Scylla includes.
#include "db/hints/internal/hint_logger.hh"
#include "db/hints/manager.hh"
#include <seastar/core/future.hh>
#include "utils/disk-error-handler.hh"
#include "utils/lister.hh"

// STD.
#include <concepts>
#include <filesystem>
#include <functional>
#include <list>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <vector>

namespace fs = std::filesystem;

namespace db::hints {
namespace internal {

namespace {

using segment_list = std::list<fs::path>;
// map: shard -> segments
using hints_ep_segments_map = std::unordered_map<unsigned, segment_list>;
// map: IP -> (map: shard -> segments)
using hints_segments_map = std::unordered_map<sstring, hints_ep_segments_map>;

future<> scan_shard_hint_directories(const fs::path& hint_directory,
        std::function<future<>(fs::path /* hint dir */, directory_entry, unsigned /* shard_id */)> func)
{
    return lister::scan_dir(hint_directory, lister::dir_entry_types::of<directory_entry_type::directory>(),
            [func = std::move(func)] (fs::path dir, directory_entry de) mutable {
        unsigned shard_id;

        try {
            shard_id = std::stoi(de.name);
        } catch (std::invalid_argument& ex) {
            manager_logger.debug("Ignore invalid directory {}", de.name);
            return make_ready_future<>();
        }

        return func(std::move(dir), std::move(de), shard_id);
    });
}

/// \brief Scan the given hint directory and build the map of all present hint segments.
///
/// Complexity: O(N+K), where N is a total number of present hint segments and
///                     K = <number of shards during the previous boot> * <number of endpoints
///                             for which hints where ever created>
///
/// \param hint_directory directory to scan
/// \return a map: ep -> map: shard -> segments (full paths)
future<hints_segments_map> get_current_hints_segments(const fs::path& hint_directory) {
    hints_segments_map current_hint_segments{};

    // Shard level.
    co_await scan_shard_hint_directories(hint_directory,
            [&current_hint_segments] (fs::path dir, directory_entry de, unsigned shard_id) {
        manager_logger.trace("shard_id = {}", shard_id);

        // IP level.
        return lister::scan_dir(dir / de.name, lister::dir_entry_types::of<directory_entry_type::directory>(),
                [&current_hint_segments, shard_id] (fs::path dir, directory_entry de) {
            manager_logger.trace("\tIP: {}", de.name);

            // Hint files.
            return lister::scan_dir(dir / de.name, lister::dir_entry_types::of<directory_entry_type::regular>(),
                    [&current_hint_segments, shard_id, ep = de.name] (fs::path dir, directory_entry de) {
                manager_logger.trace("\t\tfile: {}", de.name);

                current_hint_segments[ep][shard_id].emplace_back(dir / de.name);

                return make_ready_future<>();
            });
        });
    });

    co_return current_hint_segments;
}

/// \brief Rebalance hint segments for a given (destination) end point
///
/// This method is going to consume files from the \ref segments_to_move and distribute
/// them between the present shards (taking into an account the \ref ep_segments state - there may
/// be zero or more segments that belong to a particular shard in it) until we either achieve
/// the requested \ref segments_per_shard level on each shard or until we are out of files to move.
///
/// As a result (in addition to the actual state on the disk) both \ref ep_segments
/// and \ref segments_to_move are going to be modified.
///
/// Complexity: O(N), where N is a total number of present hint segments for
///             the \ref ep end point (as a destination).
///
/// \param ep destination end point ID (a string with its IP address)
/// \param segments_per_shard number of hint segments per-shard we want to achieve
/// \param hint_directory a root hint directory
/// \param ep_segments a map that was originally built by get_current_hints_segments() for this endpoint
/// \param segments_to_move a list of segments we are allowed to move
/// \param extensions the extensions whose commitlog file extensions must follow moved segments
future<> rebalance_segments_for(const sstring& ep, size_t segments_per_shard,
        const fs::path& hint_directory, hints_ep_segments_map& ep_segments,
        segment_list& segments_to_move, const db::extensions* extensions)
{
    manager_logger.trace("{}: segments_per_shard: {}, total number of segments to move: {}",
            ep, segments_per_shard, segments_to_move.size());

    // Sanity check.
    if (segments_to_move.empty() || !segments_per_shard) {
        co_return;
    }

    for (unsigned i = 0; i < this_smp_shard_count() && !segments_to_move.empty(); ++i) {
        const fs::path endpoint_dir_path = hint_directory / fmt::to_string(i) / ep;
        segment_list& current_shard_segments = ep_segments[i];

        // Make sure that the endpoint_dir_path exists. If not, create it.
        co_await io_check([name = endpoint_dir_path.c_str()] {
            return recursive_touch_directory(name);
        });

        while (current_shard_segments.size() < segments_per_shard && !segments_to_move.empty()) {
            auto seg_path_it = segments_to_move.begin();
            const fs::path seg_new_path = endpoint_dir_path / seg_path_it->filename();

            // Don't move the file to the same location. It's pointless.
            if (*seg_path_it != seg_new_path) {
                manager_logger.trace("going to move: {} -> {}", *seg_path_it, seg_new_path);
                // Let commitlog do it, so that the files its extensions keep for the segment
                // (e.g. the information needed to decrypt it) follow the segment.
                co_await io_check([&] {
                    return commitlog::rename_segment(seg_path_it->native(), seg_new_path.native(), extensions);
                });
            } else {
                manager_logger.trace("skipping: {}", *seg_path_it);
            }

            current_shard_segments.splice(current_shard_segments.end(), segments_to_move,
                    seg_path_it, std::next(seg_path_it));
        }
    }
}

/// \brief Rebalance all present hint segments.
///
/// The difference between the number of segments on any two shards will not be
/// greater than 1 after the rebalancing.
///
/// Complexity: O(N), where N is a total number of present hint segments.
///
/// \param hint_directory a root hint directory
/// \param segments_map a map that was built by get_current_hints_segments()
/// \param extensions the extensions whose commitlog file extensions must follow moved segments
future<> rebalance_segments(const fs::path& hint_directory, hints_segments_map& segments_map, const db::extensions* extensions) {
    // Count how many hint segments we have for each destination.
    std::unordered_map<sstring, size_t> per_ep_hints;

    for (const auto& [ep, ep_hint_segments] : segments_map) {
        per_ep_hints[ep] = std::ranges::fold_left(ep_hint_segments
                | std::views::values
                | std::views::transform(std::mem_fn(&segment_list::size)),
                size_t(0), std::plus());
        manager_logger.trace("{}: total files: {}", ep, per_ep_hints[ep]);
    }

    // Create a map of lists of segments that we will move (for each destination endpoint):
    //   if a shard has segments, then we will NOT move q = int(N/S) segments out of them,
    //   where N is a total number of segments to the current destination
    //   and S is the current number of shards.
    std::unordered_map<sstring, segment_list> segments_to_move;

    for (auto& [ep, ep_segments] : segments_map) {
        const size_t q = per_ep_hints[ep] / this_smp_shard_count();
        auto& current_segments_to_move = segments_to_move[ep];

        for (auto& [shard_id, shard_segments] : ep_segments) {
            // Move all segments from the shards that are no longer relevant
            // (re-sharding to the lower number of shards).
            if (shard_id >= this_smp_shard_count()) {
                current_segments_to_move.splice(current_segments_to_move.end(), shard_segments);
            } else if (shard_segments.size() > q) {
                current_segments_to_move.splice(current_segments_to_move.end(), shard_segments,
                        std::next(shard_segments.begin(), q), shard_segments.end());
            }
        }
    }

    // Since N (a total number of segments to a specific destination) may be not a multiple
    // of S (the current number of shards) we will distribute files in two passes:
    //    * if N = S * q + r, then
    //       * one pass for segments_per_shard = q
    //       * another one for segments_per_shard = q + 1.
    //
    // This way we will ensure as close to the perfect distribution as possible.
    //
    // Right till this point we haven't moved any segments. However we have created a logical
    // separation of segments into two groups:
    //    * Segments that are not going to be moved: segments in the segments_map.
    //    * Segments that are going to be moved: segments in the segments_to_move.
    //
    // rebalance_segments_for() is going to consume segments from segments_to_move
    // and move them to corresponding lists in the segments_map AND actually move segments
    // to the corresponding shard's sub-directory till the requested segments_per_shard level
    // is reached (see more details in the description of rebalance_segments_for()).
    for (const auto& [ep, N] : per_ep_hints) {
        const size_t q = N / this_smp_shard_count();
        const size_t r = N - q * this_smp_shard_count();
        auto& current_segments_to_move = segments_to_move[ep];
        auto& current_segments_map = segments_map[ep];

        if (q) {
            co_await rebalance_segments_for(ep, q, hint_directory, current_segments_map, current_segments_to_move, extensions);
        }

        if (r) {
            co_await rebalance_segments_for(ep, q + 1, hint_directory, current_segments_map, current_segments_to_move, extensions);
        }
    }
}

bool is_hint_segment(const std::string& filename) {
    try {
        (void)db::commitlog::descriptor(filename, manager::FILENAME_PREFIX);
        return true;
    } catch (const std::domain_error&) {
        return false;
    }
}

/// \brief Find the hint segments stored in a directory.
///
/// \param dir a directory storing hint segments. Its subdirectories are not searched.
future<std::vector<fs::path>> find_hint_segments(fs::path dir) {
    std::vector<fs::path> segments;
    co_await lister::scan_dir(dir, lister::dir_entry_types::of<directory_entry_type::regular>(),
            [&segments] (fs::path dir, directory_entry de) {
        if (is_hint_segment(de.name)) {
            segments.push_back(dir / de.name);
        }
        return make_ready_future<>();
    });
    co_return segments;
}

/// \brief Remove subdirectories of shards that are not relevant anymore (re-sharding to a lower number of shards case).
///
/// The endpoint directories that still contain hint segments are kept, and so are their shard directories.
///
/// Complexity: O(S*E), where S is the number of shards during the previous boot and
///                           E is the number of endpoints for which hints were ever created.
///
/// \param hint_directory a root hint directory
future<> remove_irrelevant_shards_directories(const fs::path& hint_directory) {
    // Shard level.
    co_await scan_shard_hint_directories(hint_directory,
            [] (fs::path dir, directory_entry de, unsigned shard_id) -> future<> {
        if (shard_id < this_smp_shard_count()) {
            co_return;
        }

        const fs::path shard_dir = dir / de.name;
        bool keep = false;

        // IP level.
        co_await lister::scan_dir(shard_dir, lister::dir_entry_types::full(), lister::show_hidden::yes,
                [&keep] (fs::path dir, directory_entry de) -> future<> {
            const fs::path path = dir / de.name;
            if (de.type == directory_entry_type::directory) {
                if (!co_await remove_hint_directory(path)) {
                    keep = true;
                }
            } else if (is_hint_segment(de.name)) {
                manager_logger.error("Not removing hint segment {} found outside of any endpoint directory", path);
                keep = true;
            } else {
                co_await io_check(remove_file, path.native());
            }
        });

        if (!keep) {
            co_await io_check(remove_file, shard_dir.native());
        }
    });
}

} // anonymous namespace

future<bool> remove_hint_directory(fs::path dir) {
    // The directory is expected to be empty, so try the cheap way first. Don't use io_check() for it:
    // a directory that isn't empty isn't a disk error, and io_check() would treat it like one.
    std::exception_ptr ex;
    try {
        co_await remove_file(dir.native());
        co_return true;
    } catch (const std::system_error& e) {
        // POSIX allows both errors for a directory that isn't empty.
        if (e.code() != std::errc::directory_not_empty && e.code() != std::errc::file_exists) {
            ex = std::current_exception();
        }
    }
    if (ex) {
        general_disk_error_handler(ex);
        std::rethrow_exception(std::move(ex));
    }

    const std::vector<fs::path> segments = co_await find_hint_segments(dir);
    if (!segments.empty()) {
        manager_logger.error("Not removing hint directory {} because it still contains hint segments: {}", dir, segments);
        co_return false;
    }

    // The directory contains other files only, e.g. files left behind by commitlog file extensions.
    co_await io_check([&dir] { return recursive_remove_directory(dir); });
    co_return true;
}

future<> rebalance_hints(fs::path hint_directory, const db::extensions* extensions) {
    // Scan currently present hint segments.
    hints_segments_map current_hints_segments = co_await get_current_hints_segments(hint_directory);

    // Move segments to achieve an even distribution of files among all present shards.
    co_await rebalance_segments(hint_directory, current_hints_segments, extensions);

    // Remove the directories of shards that are not present anymore.
    // They should not have any segments by now.
    co_await remove_irrelevant_shards_directories(hint_directory);
}

} // namespace internal
} // namespace db::hints
