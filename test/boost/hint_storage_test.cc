/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <boost/test/unit_test.hpp>
#include "test/lib/scylla_test_case.hh"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <set>
#include <string>
#include <string_view>

#include <boost/program_options.hpp>
#include <fmt/ranges.h>
#include <fmt/std.h>
#include <seastar/core/smp.hh>

#include "db/commitlog/commitlog.hh"
#include "db/config.hh"
#include "db/extensions.hh"
#include "db/hints/internal/hint_storage.hh"
#include "db/hints/manager.hh"
#include "init.hh"
#include "test/lib/tmpdir.hh"
#include "utils/UUID_gen.hh"

namespace fs = std::filesystem;

namespace {

constexpr std::string_view test_ep = "a50433ed-94bc-4cbb-8491-4f73ebb5f82f";

// The name of the encryption info of a segment that doesn't exist.
const std::string orphaned_sidecar = fmt::format(".{}4-1.log", db::hints::manager::FILENAME_PREFIX);

fs::path get_host_hint_dir(const fs::path& hints_dir, unsigned shard) {
    return hints_dir / fmt::to_string(shard) / test_ep;
}

// The extensions of a node with encryption of system info, which includes the commitlog, enabled.
class encryption_extensions {
    std::shared_ptr<db::extensions> _exts = std::make_shared<db::extensions>();
    seastar::shared_ptr<db::config> _cfg = seastar::make_shared<db::config>(_exts);
    configurable::notify_set _notify_set;

public:
    explicit encryption_extensions(const fs::path& key_directory) {
        boost::program_options::options_description desc;
        boost::program_options::options_description_easy_init init(&desc);
        configurable::append_all(*_cfg, init);

        _cfg->read_from_yaml(fmt::format(
                "system_key_directory: {}\n"
                "system_info_encryption:\n"
                "    enabled: true\n"
                "    key_provider: LocalFileSystemKeyProviderFactory\n",
                key_directory.native()));
        _notify_set = configurable::init_all(*_cfg, *_exts).get();

        BOOST_REQUIRE_MESSAGE(!_exts->commitlog_file_extensions().empty(),
                "Commitlog encryption isn't enabled");
    }

    ~encryption_extensions() {
        _notify_set.notify_all(configurable::system_state::stopped).get();
    }

    const db::extensions* get() const {
        return _exts.get();
    }
};

std::set<std::string> list_segments(const fs::path& dir) {
    std::set<std::string> segments;
    for (const auto& entry : fs::directory_iterator(dir)) {
        if (entry.path().filename().native().starts_with(db::hints::manager::FILENAME_PREFIX)) {
            segments.insert(entry.path().filename().native());
        }
    }
    return segments;
}

// Writes `count` hint segments to `dir` with a commitlog and returns their names.
// Each segment stores a single hint.
std::set<std::string> write_segments(const fs::path& dir, unsigned count, const db::extensions* exts) {
    fs::create_directories(dir);
    const auto existing = list_segments(dir);

    db::commitlog::config cfg;
    cfg.commit_log_location = dir.native();
    cfg.fname_prefix = db::hints::manager::FILENAME_PREFIX;
    cfg.commitlog_segment_size_in_mb = 1;
    cfg.max_reserve_segments = 0;
    cfg.allow_fragmented_entries = false;
    cfg.extensions = exts;
    cfg.warn_about_segments_left_on_disk_after_shutdown = false;

    auto cl = db::commitlog::create_commitlog(std::move(cfg)).get();

    const auto id = table_id(utils::UUID_gen::get_time_UUID());
    const sstring hint = "hint";
    for (unsigned i = 0; i < count; ++i) {
        if (i > 0) {
            cl.force_new_active_segment().get();
        }
        // Like hints, release the handle without marking the data as flushed,
        // so that the segment is kept on disk after shutdown.
        cl.add_mutation(id, hint.size(), db::commitlog::force_sync::yes, [&hint] (db::commitlog::output& out) {
            out.write(hint.data(), hint.size());
        }).get().release();
    }
    cl.shutdown().get();
    cl.release().get();

    std::set<std::string> written;
    std::ranges::set_difference(list_segments(dir), existing, std::inserter(written, written.end()));
    BOOST_REQUIRE_EQUAL(written.size(), count);
    return written;
}

size_t count_hints(const fs::path& segment, const db::extensions* exts) {
    size_t count = 0;
    db::commitlog::read_log_file(segment.native(), db::hints::manager::FILENAME_PREFIX, [&count] (db::commitlog::buffer_and_replay_position) {
        ++count;
        return make_ready_future<>();
    }, 0, exts).get();
    return count;
}

// Verify that exactly `segments` are present in the hint directory and that the hints stored
// in them can be read. For encrypted segments, that requires their encryption info (a hidden
// file next to the segment) to be in place.
void check_segments(const fs::path& hints_dir, const std::set<std::string>& segments, const db::extensions* exts) {
    std::set<std::string> found;
    for (const auto& entry : fs::recursive_directory_iterator(hints_dir)) {
        if (!entry.is_regular_file()) {
            continue;
        }
        const std::string name = entry.path().filename().native();
        if (name.starts_with(".")) {
            BOOST_REQUIRE_MESSAGE(fs::exists(entry.path().parent_path() / name.substr(1)),
                    fmt::format("Leftover file: {}", entry.path()));
            continue;
        }
        BOOST_REQUIRE_MESSAGE(found.insert(name).second,
                fmt::format("Duplicate segment: {}", entry.path()));
        BOOST_REQUIRE_MESSAGE(count_hints(entry.path(), exts) == 1,
                fmt::format("Can't read the hint stored in {}", entry.path()));
    }
    BOOST_REQUIRE_MESSAGE(found == segments,
            fmt::format("Segments: {}, expected: {}", found, segments));
}

// Creates a file that isn't a hint segment.
void add_leftover(const fs::path& dir, std::string_view file_name) {
    fs::create_directories(dir);
    std::ofstream{dir / file_name};
}

} // anonymous namespace

// Hints are rebalanced away from a shard that doesn't exist anymore. The moved
// segments must stay readable, including the encrypted ones, which requires
// their encryption info to follow them.
// See also: SCYLLADB-4295.
SEASTAR_THREAD_TEST_CASE(test_rebalance_hints_keeps_segments_readable) {
    tmpdir tmp;
    const fs::path key_dir = tmp.path() / "keys";
    const fs::path hints_dir = tmp.path() / "hints";
    const fs::path stale_dir = get_host_hint_dir(hints_dir, this_smp_shard_count());
    const encryption_extensions exts(key_dir);

    const auto encrypted = write_segments(stale_dir, 2 * this_smp_shard_count(), exts.get());
    for (const auto& segment : encrypted) {
        BOOST_REQUIRE_MESSAGE(count_hints(stale_dir / segment, nullptr) == 0,
                fmt::format("Segment {} isn't encrypted", segment));
    }
    auto segments = encrypted;
    // Segments written before encryption was enabled are unencrypted.
    // They must stay readable too.
    segments.merge(write_segments(stale_dir, 1, nullptr));
    check_segments(hints_dir, segments, exts.get());

    db::hints::internal::rebalance_hints(hints_dir, exts.get()).get();

    BOOST_REQUIRE(!fs::exists(stale_dir.parent_path()));
    check_segments(hints_dir, segments, exts.get());
}

// Files that got separated from their segments, e.g. because an older version of
// Scylla moved the segments without them, must not prevent removing the directory
// of a shard that doesn't exist anymore.
// See also: SCYLLADB-4295.
SEASTAR_THREAD_TEST_CASE(test_rebalance_hints_removes_stale_shard_directory_with_leftovers) {
    tmpdir tmp;
    const fs::path hints_dir = tmp.path() / "hints";
    const fs::path stale_dir = get_host_hint_dir(hints_dir, this_smp_shard_count());

    const auto segments = write_segments(stale_dir, 1, nullptr);
    add_leftover(stale_dir, orphaned_sidecar);

    db::hints::internal::rebalance_hints(hints_dir, nullptr).get();

    BOOST_REQUIRE(!fs::exists(stale_dir.parent_path()));
    check_segments(hints_dir, segments, nullptr);
}

// Files that aren't hint segments don't prevent removing a hint directory.
SEASTAR_THREAD_TEST_CASE(test_remove_hint_directory_removes_leftovers) {
    tmpdir tmp;
    const fs::path hints_dir = tmp.path() / "hints";
    const fs::path dir = get_host_hint_dir(hints_dir, 0);

    add_leftover(dir, orphaned_sidecar);
    add_leftover(dir, "not_a_segment");

    BOOST_REQUIRE(db::hints::internal::remove_hint_directory(dir).get());
    BOOST_REQUIRE(!fs::exists(dir));
}

// A hint directory that still contains hint segments must be kept intact:
// the segments may still contain hints, and the other files may be needed to
// read them.
SEASTAR_THREAD_TEST_CASE(test_remove_hint_directory_keeps_directory_with_segments) {
    tmpdir tmp;
    const fs::path hints_dir = tmp.path() / "hints";
    const fs::path dir = get_host_hint_dir(hints_dir, 0);

    const auto segments = write_segments(dir, 1, nullptr);
    const auto orphan = dir / orphaned_sidecar;
    const auto not_a_segment = dir / "not_a_segment";
    add_leftover(dir, orphan.filename().native());
    add_leftover(dir, not_a_segment.filename().native());

    BOOST_REQUIRE(!db::hints::internal::remove_hint_directory(dir).get());

    BOOST_REQUIRE(fs::exists(orphan));
    BOOST_REQUIRE(fs::exists(not_a_segment));
    fs::remove(orphan);

    BOOST_REQUIRE(!db::hints::internal::remove_hint_directory(dir).get());

    BOOST_REQUIRE(fs::exists(not_a_segment));
    fs::remove(not_a_segment);

    check_segments(hints_dir, segments, nullptr);
    BOOST_REQUIRE(!db::hints::internal::remove_hint_directory(dir).get());

    for (const auto& segment : segments) {
        fs::remove(dir / segment);
    }

    BOOST_REQUIRE(db::hints::internal::remove_hint_directory(dir).get());
}
