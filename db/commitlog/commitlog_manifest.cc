/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "commitlog.hh"
#include "commitlog_manifest.hh"
#include "replay_position.hh"

#include <algorithm>
#include <charconv>
#include <filesystem>
#include <limits>
#include <map>
#include <optional>

#include <fmt/ranges.h>

#include <seastar/core/coroutine.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/core/seastar.hh>
#include <seastar/util/file.hh>

#include "utils/checked-file-impl.hh"
#include "utils/disk-error-handler.hh"
#include "utils/error_injection.hh"
#include "utils/log.hh"
#include "utils/rjson.hh"

static logging::logger cmlogger("commitlog_manifest");

namespace db {

static constexpr uint64_t manifest_format_version = 1;
// A segment id carries the shard in max_cpu_bits, so no commitlog has more shards.
static constexpr uint64_t max_shard_count = uint64_t(1) << replay_position::max_cpu_bits;
static const sstring record_name = "record";

commitlog_manifest::commitlog_manifest(sstring dir, std::string fname_prefix, unsigned shard)
    : _dir(std::move(dir))
    , _fname_prefix(std::move(fname_prefix))
    , _shard(shard)
{}

sstring commitlog_manifest::generation_dir(const sstring& dir, const std::string& fname_prefix, uint64_t gen) {
    return fmt::format("{}/{}manifest-{}", dir, fname_prefix, gen);
}

static std::optional<uint64_t> parse_number(std::string_view s) {
    uint64_t n;
    auto [p, ec] = std::from_chars(s.data(), s.data() + s.size(), n);
    if (s.empty() || ec != std::errc() || p != s.data() + s.size()) {
        return std::nullopt;
    }
    return n;
}

// Without wanted, lists entries of every type. Opens the directory through
// commit_error_handler like every other commitlog I/O, so a disk error isolates
// the node the same way; lister::scan_dir() would bypass the handler.
static future<std::vector<sstring>> list_entries(sstring dir, std::optional<directory_entry_type> wanted, bool include_hidden) {
    auto d = co_await open_checked_directory(commit_error_handler, dir);
    std::vector<sstring> names;
    std::exception_ptr ep;
    try {
        auto lister = d.list_directory([&] (directory_entry de) -> future<> {
            if (!include_hidden && de.name[0] == '.') {
                co_return;
            }
            if (wanted) {
                auto type = de.type;
                if (!type) {
                    auto path = dir + "/" + de.name;
                    type = co_await commit_io_check([&] { return file_type(path); });
                }
                if (type != *wanted) {
                    co_return;
                }
            }
            names.push_back(de.name);
        });
        co_await lister.done();
    } catch (...) {
        ep = std::current_exception();
    }
    co_await d.close();
    if (ep) {
        std::rethrow_exception(ep);
    }
    co_return names;
}

static future<std::vector<uint64_t>> list_generations(sstring dir, std::string fname_prefix) {
    auto prefix = fname_prefix + "manifest-";
    std::vector<uint64_t> gens;
    for (const auto& name : co_await list_entries(dir, directory_entry_type::directory, false)) {
        if (!name.starts_with(prefix)) {
            continue;
        }
        // Only the canonical spelling: "07" would otherwise be read and dropped
        // through the path of generation 7. The maximum is skipped too, since
        // the next generation is one above the newest.
        auto suffix = std::string_view(name).substr(prefix.size());
        auto gen = parse_number(suffix);
        if (!gen || *gen == std::numeric_limits<uint64_t>::max() || std::to_string(*gen) != suffix) {
            cmlogger.warn("Ignoring {}/{}: not a commitlog manifest generation", dir, name);
            continue;
        }
        gens.push_back(*gen);
    }
    std::ranges::sort(gens);
    co_return gens;
}

// tmp, flush, close, rename, sync: a reader sees the old file or the new one, never a torn one.
static future<> write_atomically(sstring dir, sstring path, std::string content) {
    auto tmp = path + ".tmp";
    // A manifest is a few KB; the default 1 MB extent hint would have XFS
    // allocate that much for every write.
    file_open_options opt;
    opt.extent_allocation_size_hint = 0;
    auto f = co_await open_checked_file_dma(commit_error_handler, tmp, open_flags::wo | open_flags::create | open_flags::truncate, opt);
    auto out = co_await make_file_output_stream(std::move(f));
    std::exception_ptr ep;
    try {
        co_await out.write(content.data(), content.size());
        co_await out.flush();
    } catch (...) {
        ep = std::current_exception();
    }
    try {
        co_await out.close();
    } catch (...) {
        if (!ep) {
            ep = std::current_exception();
        }
    }
    if (ep) {
        std::rethrow_exception(ep);
    }
    co_await commit_io_check([&] { return rename_file(tmp, path); });
    co_await commit_io_check([&] { return sync_directory(dir); });
}

future<> commitlog_manifest::write() {
    auto units = co_await get_units(_write_sem, 1);
    utils::get_local_injector().inject("commitlog_manifest_write_fail", [] {
        throw std::runtime_error("commitlog_manifest_write_fail: injected error");
    });
    // Built under _write_sem from the live set, so a write that completes
    // after remove() returned cannot list a removed name.
    auto root = rjson::empty_object();
    rjson::add(root, "version", manifest_format_version);
    rjson::add(root, "shard", _shard);
    auto segments = rjson::empty_array();
    for (const auto& name : _segments) {
        rjson::push_back(segments, rjson::from_string(name));
    }
    rjson::add(root, "segments", std::move(segments));
    auto dir = generation_dir(_dir, _fname_prefix, _generation);
    // Removed at runtime, the directory would fail every write with ENOENT, which
    // commit_error_handler tolerates, and stall allocation. Recreate the
    // directory; the generation then reads as unsealed on the next start.
    if (!co_await commit_io_check([&] { return file_exists(dir); })) {
        cmlogger.warn("Commitlog manifest directory {} is missing, recreating it", dir);
        co_await commit_io_check([&] { return touch_directory(dir); });
        co_await commit_io_check([&] { return sync_directory(_dir); });
    }
    co_await write_atomically(dir, fmt::format("{}/{}", dir, _shard), rjson::print(root));
}

future<> commitlog_manifest::add(sstring basename) {
    auto inserted = _segments.insert(std::move(basename)).second;
    if (!inserted || !_persist) {
        co_return;
    }
    // A failed write may still have landed (rename done, sync failed), so the
    // name stays in the set until remove() rewrites the file without it.
    co_await write();
}

future<> commitlog_manifest::remove(std::vector<sstring> basenames) {
    std::vector<sstring> erased;
    for (const auto& name : basenames) {
        if (_segments.erase(name)) {
            erased.push_back(name);
        }
    }
    if (erased.empty() || !_persist) {
        co_return;
    }
    try {
        co_await write();
    } catch (...) {
        _segments.insert(erased.begin(), erased.end());
        throw;
    }
}

future<> commitlog_manifest::enable(uint64_t generation) {
    if (_persist) {
        on_internal_error(cmlogger, fmt::format("Commitlog manifest in {} enabled twice: generation {} is in use, {} requested", _dir, _generation, generation));
    }
    auto dir = generation_dir(_dir, _fname_prefix, generation);
    co_await commit_io_check([&] { return touch_directory(dir); });
    co_await commit_io_check([&] { return sync_directory(_dir); });
    _generation = generation;
    // Set before the write: an add() racing activation then queues on
    // _write_sem and writes again with its own name.
    _persist = true;
    co_await write();
    cmlogger.info("Commitlog manifest enabled: generation {} in {}", generation, dir);
}

future<> commitlog_manifest::seal(unsigned shard_count) {
    if (!_persist) {
        on_internal_error(cmlogger, fmt::format("Commitlog manifest in {} sealed before it was enabled", _dir));
    }
    auto root = rjson::empty_object();
    rjson::add(root, "version", manifest_format_version);
    rjson::add(root, "shard_count", shard_count);
    auto dir = generation_dir(_dir, _fname_prefix, _generation);
    co_await write_atomically(dir, fmt::format("{}/{}", dir, record_name), rjson::print(root));
    _sealed = true;
}

static rjson::value parse_manifest_file(std::string_view content) {
    auto doc = rjson::parse(content);
    if (!doc.IsObject()) {
        throw std::runtime_error("not a JSON object");
    }
    auto& version = rjson::get(doc, "version");
    if (!version.IsUint64()) {
        throw std::runtime_error("\"version\" is not a number");
    }
    // Versions start at 1; 0 is a corrupt or hand-edited file, not an older format.
    if (version.GetUint64() == 0 || version.GetUint64() > manifest_format_version) {
        throw std::runtime_error(fmt::format("unsupported version {}", version.GetUint64()));
    }
    return doc;
}

future<commitlog_manifest::verify_result> commitlog_manifest::verify(sstring dir, std::string fname_prefix, std::vector<sstring> on_disk_basenames, bool ignore_errors) {
    verify_result res;
    auto gens = co_await list_generations(dir, fname_prefix);
    if (gens.empty()) {
        co_return res;
    }

    auto fail = [ignore_errors] (const sstring& msg) {
        if (!ignore_errors) {
            throw commitlog_manifest_error(msg);
        }
        cmlogger.error("{}", msg);
    };

    // segment basename -> the manifest file that lists it
    std::map<sstring, sstring> expected;
    // `find commitlog -type f -delete` leaves the generation directories behind.
    // Only a manifest file that was read counts as a manifest found.
    bool read_any_file = false;
    for (auto gen : gens) {
        auto gdir = generation_dir(dir, fname_prefix, gen);
        std::optional<uint64_t> shard_count;
        std::set<uint64_t> shards;
        bool read_file = false;
        for (const auto& name : co_await list_entries(gdir, directory_entry_type::regular, false)) {
            auto shard = parse_number(name);
            if (shard && std::to_string(*shard) != std::string_view(name)) {
                // Only the canonical spelling, as for the generation directories.
                cmlogger.warn("Ignoring {}/{}: not a commitlog manifest shard file", gdir, name);
                continue;
            }
            if (name != record_name && !shard) {
                continue; // *.tmp and anything else drop_generations removes
            }
            if (shard) {
                // A corrupt shard file counts as present; the parse error below reports it.
                shards.insert(*shard);
            }
            read_file = true;
            auto path = gdir + "/" + name;
            auto content = co_await commit_io_check([&] { return util::read_entire_file_contiguous(std::filesystem::path(path)); });
            try {
                auto doc = parse_manifest_file(content);
                if (!shard) {
                    auto& n = rjson::get(doc, "shard_count");
                    if (!n.IsUint64()) {
                        throw std::runtime_error("\"shard_count\" is not a number");
                    }
                    if (n.GetUint64() == 0 || n.GetUint64() > max_shard_count) {
                        throw std::runtime_error(fmt::format("\"shard_count\" {} is not in [1, {}]", n.GetUint64(), max_shard_count));
                    }
                    shard_count = n.GetUint64();
                    continue;
                }
                auto& segments = rjson::get(doc, "segments");
                if (!segments.IsArray()) {
                    throw std::runtime_error("\"segments\" is not an array");
                }
                for (const auto& s : segments.GetArray()) {
                    expected.emplace(sstring(rjson::to_string_view(s)), path);
                }
            } catch (...) {
                fail(fmt::format("Commitlog manifest {} cannot be parsed: {}", path, std::current_exception()));
            }
        }
        std::vector<uint64_t> missing_shards;
        for (uint64_t k = 0; shard_count && k < *shard_count; ++k) {
            if (!shards.contains(k)) {
                missing_shards.push_back(k);
            }
        }
        if (!missing_shards.empty()) {
            fail(fmt::format("Commitlog manifest generation {} in {} is sealed for {} shards but the manifest for shard {} is missing", gen, dir, *shard_count, fmt::join(missing_shards, ", ")));
        }
        if (!read_file) {
            cmlogger.warn("Commitlog manifest generation {} in {} has no manifest files", gen, dir);
        }
        read_any_file |= read_file;
    }

    std::set<std::string_view> on_disk(on_disk_basenames.begin(), on_disk_basenames.end());
    std::vector<sstring> missing;
    for (const auto& [name, path] : expected) {
        if (!on_disk.contains(name)) {
            missing.push_back(fmt::format("Commitlog manifest {} lists segment {} which is missing from {}", path, name, dir));
        }
    }
    if (!missing.empty()) {
        fail(fmt::format("{}. {}", fmt::join(missing, "; "), ignore_errors
                ? "Starting anyway because the manifest errors are ignored; the data in the missing segments is lost."
                : "Refusing to start: the data in the missing segments is lost. Set unsafe_ignore_commitlog_manifest: true to start anyway; "
                  "a node serving strongly consistent tables should be replaced instead."));
    }
    for (const auto& name : on_disk_basenames) {
        if (!expected.contains(name) && !name.starts_with(commitlog::descriptor::RECYCLED_PREFIX)) {
            cmlogger.warn("Segment {} is not listed in any commitlog manifest, replaying anyway", name);
        }
    }

    res.found = read_any_file;
    res.old_generations = std::move(gens);
    res.next_generation = res.old_generations.back() + 1;
    co_return res;
}

future<> commitlog_manifest::drop_generations(sstring dir, std::string fname_prefix, std::vector<uint64_t> gens) {
    for (auto gen : gens) {
        auto gdir = generation_dir(dir, fname_prefix, gen);
        if (!co_await commit_io_check([&] { return file_exists(gdir); })) {
            continue;
        }
        // Every entry, or the rmdir below fails with ENOTEMPTY.
        auto names = co_await list_entries(gdir, std::nullopt, true);
        auto record = std::ranges::find(names, record_name);
        if (record != names.end()) {
            auto path = gdir + "/" + record_name;
            co_await commit_io_check([&] { return remove_file(path); });
            co_await commit_io_check([&] { return sync_directory(gdir); });
            names.erase(record);
        }
        for (const auto& name : names) {
            auto path = gdir + "/" + name;
            co_await commit_io_check([&] { return remove_file(path); });
        }
        co_await commit_io_check([&] { return sync_directory(gdir); });
        co_await commit_io_check([&] { return remove_file(gdir); });
        co_await commit_io_check([&] { return sync_directory(dir); });
    }
}

}
