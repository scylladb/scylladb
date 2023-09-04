/*
 * Modified by ScyllaDB
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "db/hints/internal/hint_storage.hh"

// Seastar features.
#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/file-types.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/thread.hh>

// Boost features.
#include <boost/range/adaptors.hpp>

// Scylla includes.
#include "db/hints/internal/hint_logger.hh"
#include "seastar/core/future.hh"
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

namespace fs = std::filesystem;

namespace db::hints {
namespace internal {

namespace {

// map: shard -> segments
using hint_host_segments_map = std::unordered_map<shard_id, std::list<fs::path>>;
// map: IP -> map: shard -> segments
using hint_segments_map = std::unordered_map<sstring, hint_host_segments_map>;

/// The caller needs to ensure that the captures of the passed functor will remain valid
/// at least until the returned future resolves.
template <typename Func>
    requires std::invocable<Func, fs::path, directory_entry, shard_id>
future<> scan_for_hint_dirs(const fs::path& hints_directory, Func&& func) {
    return lister::scan_dir(fs::path{hints_directory},
            lister::dir_entry_types::of<directory_entry_type::directory>(),
            [func = std::forward<Func>(func)] (fs::path dir, directory_entry de) mutable {
        shard_id sid;

        try {
            sid = std::stoi(de.name.c_str());
        } catch (std::invalid_argument& ex) {
            manager_logger.debug("Ignore invalid directory {}", de.name);
            return make_ready_future<>();
        }
        
        return func(std::move(dir), std::move(de), sid);
    });
}

/// \brief Scan the given hints directory and build the map of all present hints segments.
///
/// Complexity: O(N+K), where N is a total number of present hints' segments and
///                           K = <number of shards during the previous boot> * <number of end points for which hints where ever created>
///
/// \note Should be called from a seastar::thread context.
///
/// \param hint_directory directory to scan
/// \return a map: ep -> map: shard -> segments (full paths)
hint_segments_map get_current_hint_segments(const fs::path& hint_directory) {
    hint_segments_map current_hint_segments;

    // Shard level.
    scan_for_hint_dirs(hint_directory, [&current_hint_segments] (fs::path dir, directory_entry de, shard_id sid) {
        manager_logger.trace("shard_id = {}", sid);

        // IP level.
        return lister::scan_dir(dir / de.name, lister::dir_entry_types::of<directory_entry_type::directory>(),
                [&current_hint_segments, sid] (fs::path dir, directory_entry de) {
            manager_logger.trace("\tIP: {}", de.name);

            // Hint files.
            return lister::scan_dir(dir / de.name, lister::dir_entry_types::of<directory_entry_type::regular>(),
                    [&current_hint_segments, sid, ep_addr = de.name] (fs::path dir, directory_entry de) {
                manager_logger.trace("\t\tfile: {}", de.name);
                current_hint_segments[ep_addr][sid].emplace_back(dir / de.name);
                return make_ready_future<>();
            });
        });
    }).get();

    return current_hint_segments;
}

/// \brief Rebalance hints segments for a given (destination) end point
///
/// This method is going to consume files from the \ref segments_to_move and distribute them
/// between the present shards (taking into an account the \ref ep_segments state -- there may be
/// zero or more segments that belong to a particular shard in it) until we either achieve
/// the requested \ref segments_per_shard level on each shard or until we are out of files to move.
///
/// As a result (in addition to the actual state on the disk) both \ref ep_segments
/// and \ref segments_to_move are going to be modified.
///
/// Complexity: O(N), where N is a total number of present hints' segments
///             for the \ref ep end point (as a destination).
///
/// \note Should be called from a seastar::thread context.
///
/// \param ep destination end point ID (a string with its IP address)
/// \param segments_per_shard number of hints segments per-shard we want to achieve
/// \param hint_directory a root hints directory
/// \param host_segments a map that was originally built by get_current_hints_segments() for this end point
/// \param segments_to_move a list of segments we are allowed to move
void rebalance_segments_for(const std::string_view ep, size_t segments_per_shard,
        const fs::path& hint_directory, hint_host_segments_map& host_segments,
        std::list<fs::path>& segments_to_move)
{
    manager_logger.trace("{}: segments_per_shard: {}, total number of segments to move: {}",
            ep, segments_per_shard, segments_to_move.size());

    // Sanity check.
    if (segments_to_move.empty() || !segments_per_shard) {
        return;
    }

    const fs::path hint_directory_path{hint_directory};

    for (shard_id i = 0; i < smp::count && !segments_to_move.empty(); ++i) {
        fs::path shard_path_dir{hint_directory_path / seastar::format("{:d}", i) / ep};
        std::list<fs::path>& current_shard_segments = host_segments[i];

        // Make sure that the shard_path_dir exists. If not, create it.
        io_check([name = shard_path_dir.c_str()] {
            return recursive_touch_directory(name);
        }).get();

        while (current_shard_segments.size() < segments_per_shard && !segments_to_move.empty()) {
            auto seg_path_it = segments_to_move.begin();
            fs::path new_path{shard_path_dir / seg_path_it->filename()};

            // Don't move the file to the same location. It's pointless.
            if (*seg_path_it != new_path) {
                manager_logger.trace("going to move: {} -> {}", *seg_path_it, new_path);
                io_check(rename_file, seg_path_it->native(), new_path.native()).get();
            } else {
                manager_logger.trace("skipping: {}", *seg_path_it);
            }

            current_shard_segments.splice(current_shard_segments.end(), segments_to_move,
                    seg_path_it, std::next(seg_path_it));
        }
    }
}

/// \brief Rebalance all present hints segments.
///
/// Ensure that the difference in the number of segments between any two shards won't be greater
/// than 1 after the rebalancing.
///
/// Complexity: O(N), where N is a total number of present hints' segments.
///
/// \note Should be called from a seastar::thread context.
///
/// \param hint_directory a root hint directory
/// \param segments_map a map that was built by get_current_hints_segments()
void rebalance_segments(const fs::path& hint_directory, hint_segments_map& segments_map) {
    // Count how many hints segments to each destination we have.
    std::unordered_map<sstring, size_t> per_ep_hints;

    for (const auto& [ip, hint_host_map] : segments_map) {
        per_ep_hints[ip] = boost::accumulate(
                hint_host_map |
                boost::adaptors::map_values |
                boost::adaptors::transformed(std::mem_fn(&std::list<fs::path>::size)),
                size_t(0));
        manager_logger.trace("{}: total files: {}", ip, per_ep_hints[ip]);
    }

    // Create a map of lists of segments that we will move (for each destination end point):
    // if a shard has segments, then we will NOT move q = int(N/S) segments out of them,
    // where N is the total number of segments to the current destination and S is the current
    // number of shards.
    std::unordered_map<sstring, std::list<fs::path>> segments_to_move;

    for (auto& [ep, ep_segments] : segments_map) {
        const size_t q = per_ep_hints[ep] / smp::count;
        auto& current_segments_to_move = segments_to_move[ep];

        for (auto& [shard_id, shard_segments] : ep_segments) {
            // Move all segments from the shards that are no longer relevant (re-sharding to the lower number of shards).
            if (shard_id >= smp::count) {
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
    // This way we will achieve a distribution as close to the perfect one as possible.
    //
    // Right till this point, we haven't moved any segments. However, we have created a logical
    // separation of segments into two groups:
    //    * Segments that are not going to be moved: segments in the segments_map.
    //    * Segments that are going to be moved: segments in the segments_to_move.
    //
    // rebalance_segments_for() is going to consume segments from segments_to_move and move
    // them to the corresponding lists in the segments_map AND actually move segments
    // to the corresponding shard's sub-directory till the requested segments_per_shard level
    // is reached (see more details in the description of rebalance_segments_for()).
    for (const auto& [ep, N] : per_ep_hints) {
        const size_t q = N / smp::count;
        const size_t r = N - q * smp::count;
        auto& current_segments_to_move = segments_to_move[ep];
        auto& current_segments_map = segments_map[ep];

        if (q) {
            rebalance_segments_for(ep, q, hint_directory, current_segments_map, current_segments_to_move);
        }

        if (r) {
            rebalance_segments_for(ep, q + 1, hint_directory, current_segments_map, current_segments_to_move);
        }
    }
}

/// \brief Remove sub-directories of shards that are not relevant any more
/// (re-sharding to a lower number of shards case).
///
/// Complexity: O(S*E), where S is a number of shards during the previous boot and
///                           E is a number of end points for which hints where ever created.
///
/// \param hint_directory a root hint directory
void remove_irrelevant_shards_directories(const fs::path& hint_directory) {
    // shards level
    scan_for_hint_dirs(hint_directory, [] (fs::path dir, directory_entry de, shard_id sid) {
        if (sid >= smp::count) {
            // IP level.
            return lister::scan_dir(dir / de.name, lister::dir_entry_types::full(),
                    lister::show_hidden::yes, [] (fs::path dir, directory_entry de) {
                return io_check(remove_file, (dir / de.name).native());
            }).then([shard_base_dir = dir, shard_entry = de] {
                return io_check(remove_file, (shard_base_dir / shard_entry.name).native());
            });
        }

        return make_ready_future<>();
    }).get();
}

} // anonymous namespace

future<> rebalance_hints(fs::path hints_directory) {
    return seastar::async([hints_directory = std::move(hints_directory)] {
        // Scan currently present hints segments.
        hint_segments_map current_hints_segments = get_current_hint_segments(hints_directory);

        // Move segments to achieve an even distribution of files among all present shards.
        rebalance_segments(hints_directory, current_hints_segments);

        // Remove the directories of shards that are not present anymore - they should not have any segments by now
        remove_irrelevant_shards_directories(hints_directory);
    });
}

} // namespace internal
} // namespace db::hints
