/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <boost/test/unit_test.hpp>

#include <filesystem>
#include <fstream>
#include <set>
#include <sstream>

#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/testing/on_internal_error.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/util/defer.hh>

#include "db/commitlog/commitlog.hh"
#include "db/commitlog/commitlog_extensions.hh"
#include "db/commitlog/commitlog_manifest.hh"
#include "db/commitlog/rp_set.hh"
#include "db/extensions.hh"
#include "test/lib/tmpdir.hh"
#include "utils/UUID_gen.hh"
#include "utils/error_injection.hh"
#include "utils/log.hh"
#include "utils/rjson.hh"

BOOST_AUTO_TEST_SUITE(commitlog_manifest_test)

using namespace db;
namespace fs = std::filesystem;

namespace {

// metrics_category_name stays empty: the restart and refusal tests create a
// second commitlog in the same directory while the first one's metrics may exist.
commitlog::config make_config(const tmpdir& tmp) {
    commitlog::config cfg;
    cfg.commit_log_location = tmp.path().string();
    cfg.commitlog_segment_size_in_mb = 1;
    cfg.commitlog_total_space_in_mb = 64;
    cfg.use_manifest = true;
    return cfg;
}

fs::path gen_dir(const tmpdir& tmp, uint64_t gen) {
    return fs::path(commitlog_manifest::generation_dir(tmp.path().string(), commitlog::descriptor::FILENAME_PREFIX, gen));
}

void write_text(const fs::path& path, const std::string& text) {
    fs::create_directories(path.parent_path());
    std::ofstream(path) << text;
}

void write_shard_file(const tmpdir& tmp, uint64_t gen, unsigned shard, const std::vector<std::string>& segments) {
    auto root = rjson::empty_object();
    rjson::add(root, "version", 1);
    rjson::add(root, "shard", shard);
    auto arr = rjson::empty_array();
    for (const auto& s : segments) {
        rjson::push_back(arr, rjson::from_string(s));
    }
    rjson::add(root, "segments", std::move(arr));
    write_text(gen_dir(tmp, gen) / fmt::to_string(shard), rjson::print(root));
}

void write_record(const tmpdir& tmp, uint64_t gen, unsigned shard_count) {
    write_text(gen_dir(tmp, gen) / "record", fmt::format(R"({{"version":1,"shard_count":{}}})", shard_count));
}

std::set<std::string> read_shard_file(const tmpdir& tmp, uint64_t gen, unsigned shard) {
    std::ifstream in(gen_dir(tmp, gen) / fmt::to_string(shard));
    std::stringstream ss;
    ss << in.rdbuf();
    auto doc = rjson::parse(ss.str());
    std::set<std::string> res;
    for (const auto& s : doc["segments"].GetArray()) {
        res.emplace(rjson::to_string_view(s));
    }
    return res;
}

// Segment basenames on disk, without the recycled ones.
std::set<std::string> segments_on_disk(const tmpdir& tmp) {
    std::set<std::string> res;
    for (const auto& e : fs::directory_iterator(tmp.path())) {
        auto name = e.path().filename().string();
        if (e.is_regular_file() && name.starts_with(commitlog::descriptor::FILENAME_PREFIX)) {
            res.insert(name);
        }
    }
    return res;
}

size_t recycled_on_disk(const tmpdir& tmp) {
    return std::ranges::count_if(fs::directory_iterator(tmp.path()), [] (const fs::directory_entry& e) {
        return e.is_regular_file() && e.path().filename().string().starts_with("Recycled-");
    });
}

std::set<std::string> basenames(const std::vector<sstring>& paths) {
    std::set<std::string> res;
    for (const auto& p : paths) {
        res.insert(fs::path(p).filename().string());
    }
    return res;
}

// Writes until at least n segments are in use, the last one still allocating.
// Without rps the handles are released, so the segments stay dirty.
void fill_segments(commitlog& log, table_id uuid, rp_set* rps, size_t n) {
    constexpr size_t size = 64 * 1024;
    while (log.get_active_segment_names().size() < n) {
        auto h = log.add_mutation(uuid, size, commitlog::force_sync::no, [&] (commitlog::output& dst) {
            dst.fill('1', size);
        }).get();
        if (rps) {
            rps->put(std::move(h));
        } else {
            h.release();
        }
    }
}

// The handles hold the segments, so the last ones are queued for disposal
// only when rps is destroyed. delete_segments() runs a disposal pass.
void discard_all(commitlog& log, table_id uuid, rp_set& rps) {
    log.discard_completed_segments(uuid, std::exchange(rps, {}));
    log.delete_segments({}).get();
    log.wait_for_pending_deletes().get();
}

// As if the node crashed: the dirty segments stay on disk.
void crash_stop(commitlog& log) {
    log.sync_all_segments().get();
    log.release().get();
    log.shutdown().get();
}

void clean_stop(commitlog& log) {
    log.shutdown().get();
    log.clear().get();
}

bool message_contains(const commitlog_manifest_error& e, std::initializer_list<std::string_view> parts) {
    std::string_view what = e.what();
    BOOST_TEST_MESSAGE(what);
    return std::ranges::all_of(parts, [&] (auto p) { return what.find(p) != std::string_view::npos; });
}

template <typename Func>
std::string capture_log(Func&& func) {
    std::ostringstream captured;
    seastar::logger::set_ostream(captured);
    try {
        func();
    } catch (...) {
        seastar::logger::set_ostream(std::cerr);
        throw;
    }
    seastar::logger::set_ostream(std::cerr);
    return captured.str();
}

table_id make_table_id() {
    return table_id(utils::UUID_gen::get_time_UUID());
}

const std::string unlisted_segment = "CommitLog-4-100.log";
const std::string missing_segment = "CommitLog-4-101.log";

}

SEASTAR_THREAD_TEST_CASE(test_manifest_lists_allocated_segments) {
    tmpdir tmp;
    auto log = commitlog::create_commitlog(make_config(tmp)).get();
    log.enable_manifest(1).get();
    log.seal_manifest(1).get();
    BOOST_REQUIRE(fs::exists(gen_dir(tmp, 1) / "record"));

    auto uuid = make_table_id();
    rp_set rps;
    fill_segments(log, uuid, &rps, 4);
    log.sync_all_segments().get();

    // A reserve segment can be between file creation and add(), so the
    // listing sits between the active segments and the files on disk.
    auto active = basenames(log.get_active_segment_names());
    auto listed = read_shard_file(tmp, 1, 0);
    BOOST_REQUIRE(std::ranges::includes(listed, active));
    BOOST_REQUIRE(std::ranges::includes(segments_on_disk(tmp), listed));

    discard_all(log, uuid, rps);

    listed = read_shard_file(tmp, 1, 0);
    BOOST_REQUIRE(std::ranges::includes(segments_on_disk(tmp), listed));
    auto removed = std::ranges::count_if(active, [&] (const std::string& name) { return !listed.contains(name); });
    BOOST_REQUIRE_GE(removed, 3);
    for (const auto& name : listed) {
        BOOST_REQUIRE(!name.starts_with("Recycled-"));
    }

    clean_stop(log);
}

// The write path recreates the directory, so allocation does not stall on
// ENOENT. Without the record the generation reads as unsealed next time.
SEASTAR_THREAD_TEST_CASE(test_removed_generation_dir_is_recreated) {
    tmpdir tmp;
    auto log = commitlog::create_commitlog(make_config(tmp)).get();
    log.enable_manifest(1).get();
    log.seal_manifest(1).get();
    fs::remove_all(gen_dir(tmp, 1));

    auto uuid = make_table_id();
    rp_set rps;
    auto out = capture_log([&] {
        fill_segments(log, uuid, &rps, 3);
    });
    log.sync_all_segments().get();

    BOOST_REQUIRE_NE(out.find("is missing, recreating it"), std::string::npos);
    BOOST_REQUIRE(fs::exists(gen_dir(tmp, 1) / "0"));
    BOOST_REQUIRE(!fs::exists(gen_dir(tmp, 1) / "record"));
    auto listed = read_shard_file(tmp, 1, 0);
    BOOST_REQUIRE(std::ranges::includes(listed, basenames(log.get_active_segment_names())));
    BOOST_REQUIRE(std::ranges::includes(segments_on_disk(tmp), listed));

    discard_all(log, uuid, rps);
    clean_stop(log);
}

SEASTAR_THREAD_TEST_CASE(test_missing_listed_segment_refuses) {
    tmpdir tmp;
    auto cfg = make_config(tmp);
    std::string victim;
    {
        auto log = commitlog::create_commitlog(cfg).get();
        log.enable_manifest(1).get();
        log.seal_manifest(1).get();
        fill_segments(log, make_table_id(), nullptr, 2);
        victim = *basenames(log.get_active_segment_names()).begin();
        crash_stop(log);
    }
    BOOST_REQUIRE(read_shard_file(tmp, 1, 0).contains(victim));
    fs::remove(tmp.path() / victim);

    BOOST_REQUIRE_EXCEPTION(commitlog::create_commitlog(cfg).get(), commitlog_manifest_error, [&] (const commitlog_manifest_error& e) {
        return message_contains(e, {victim, "which is missing", "Refusing to start"});
    });
}

SEASTAR_THREAD_TEST_CASE(test_incomplete_or_corrupt_generation_refuses) {
    {
        tmpdir tmp;
        write_record(tmp, 3, 2);
        write_shard_file(tmp, 3, 0, {});
        BOOST_REQUIRE_EXCEPTION(commitlog::create_commitlog(make_config(tmp)).get(), commitlog_manifest_error, [] (const commitlog_manifest_error& e) {
            return message_contains(e, {"generation 3", "sealed for 2 shards but the manifest for shard 1 is missing"});
        });
    }
    {
        tmpdir tmp;
        write_record(tmp, 3, 3);
        write_shard_file(tmp, 3, 0, {});
        BOOST_REQUIRE_EXCEPTION(commitlog::create_commitlog(make_config(tmp)).get(), commitlog_manifest_error, [] (const commitlog_manifest_error& e) {
            return message_contains(e, {"sealed for 3 shards but the manifest for shard 1, 2 is missing"});
        });
    }
    {
        tmpdir tmp;
        write_text(gen_dir(tmp, 3) / "0", "{\"version\":1,\"segm");
        BOOST_REQUIRE_EXCEPTION(commitlog::create_commitlog(make_config(tmp)).get(), commitlog_manifest_error, [] (const commitlog_manifest_error& e) {
            return message_contains(e, {"CommitLog-manifest-3/0", "cannot be parsed"});
        });
    }
    // A newer format version may carry fields this binary cannot check.
    {
        tmpdir tmp;
        write_text(gen_dir(tmp, 3) / "0", R"({"version":2,"shard":0,"segments":[]})");
        BOOST_REQUIRE_EXCEPTION(commitlog::create_commitlog(make_config(tmp)).get(), commitlog_manifest_error, [] (const commitlog_manifest_error& e) {
            return message_contains(e, {"CommitLog-manifest-3/0", "cannot be parsed", "unsupported version 2"});
        });
    }
    // Versions start at 1, so 0 is corruption, not an older format.
    {
        tmpdir tmp;
        write_text(gen_dir(tmp, 3) / "0", R"({"version":0,"shard":0,"segments":[]})");
        BOOST_REQUIRE_EXCEPTION(commitlog::create_commitlog(make_config(tmp)).get(), commitlog_manifest_error, [] (const commitlog_manifest_error& e) {
            return message_contains(e, {"CommitLog-manifest-3/0", "cannot be parsed", "unsupported version 0"});
        });
    }
    // Only the canonical spelling names a shard file: "00" does not stand in for "0".
    {
        tmpdir tmp;
        write_record(tmp, 3, 1);
        write_text(gen_dir(tmp, 3) / "00", R"({"version":1,"shard":0,"segments":[]})");
        BOOST_REQUIRE_EXCEPTION(commitlog::create_commitlog(make_config(tmp)).get(), commitlog_manifest_error, [] (const commitlog_manifest_error& e) {
            return message_contains(e, {"sealed for 1 shards but the manifest for shard 0 is missing"});
        });
    }
    // A corrupt shard_count must not drive the completeness loop.
    for (auto count : {"0", "18446744073709551615"}) {
        tmpdir tmp;
        write_text(gen_dir(tmp, 3) / "record", fmt::format(R"({{"version":1,"shard_count":{}}})", count));
        write_shard_file(tmp, 3, 0, {});
        BOOST_REQUIRE_EXCEPTION(commitlog::create_commitlog(make_config(tmp)).get(), commitlog_manifest_error, [] (const commitlog_manifest_error& e) {
            return message_contains(e, {"CommitLog-manifest-3/record", "cannot be parsed", "shard_count"});
        });
    }
}

SEASTAR_THREAD_TEST_CASE(test_unsealed_generation_is_unioned_not_completed) {
    {
        tmpdir tmp;
        write_text(tmp.path() / unlisted_segment, "");
        write_shard_file(tmp, 1, 1, {unlisted_segment});
        auto log = commitlog::create_commitlog(make_config(tmp)).get();
        BOOST_REQUIRE(log.manifest_found_on_startup());
        clean_stop(log);
    }
    {
        tmpdir tmp;
        write_text(tmp.path() / unlisted_segment, "");
        write_shard_file(tmp, 1, 1, {unlisted_segment, missing_segment});
        BOOST_REQUIRE_EXCEPTION(commitlog::create_commitlog(make_config(tmp)).get(), commitlog_manifest_error, [] (const commitlog_manifest_error& e) {
            return message_contains(e, {"CommitLog-manifest-1/1 lists segment " + missing_segment});
        });
    }
}

SEASTAR_THREAD_TEST_CASE(test_unlisted_segment_is_replayed) {
    tmpdir tmp;
    write_text(tmp.path() / unlisted_segment, "");
    write_record(tmp, 1, 1);
    write_shard_file(tmp, 1, 0, {});

    std::optional<commitlog> log;
    auto out = capture_log([&] {
        log.emplace(commitlog::create_commitlog(make_config(tmp)).get());
    });
    BOOST_REQUIRE_NE(out.find("Segment " + unlisted_segment + " is not listed in any commitlog manifest, replaying anyway"), std::string::npos);
    BOOST_REQUIRE(basenames(log->get_segments_to_replay().get()).contains(unlisted_segment));
    clean_stop(*log);
}

SEASTAR_THREAD_TEST_CASE(test_next_generation_above_leftovers) {
    tmpdir tmp;
    write_record(tmp, 5, 1);
    write_shard_file(tmp, 5, 0, {});
    write_shard_file(tmp, 7, 0, {});
    write_text(gen_dir(tmp, 7) / "0.tmp", "garbage");
    // Anything in a generation directory must go with it, or rmdir fails.
    write_text(gen_dir(tmp, 7) / ".hidden", "garbage");

    auto log = commitlog::create_commitlog(make_config(tmp)).get();
    BOOST_REQUIRE(log.manifest_found_on_startup());
    BOOST_REQUIRE_EQUAL(log.next_manifest_generation(), 8);

    // Generation 7 exists; enabling it would overwrite its shard file.
    {
        seastar::testing::scoped_no_abort_on_internal_error no_abort;
        BOOST_REQUIRE_THROW(log.enable_manifest(7).get(), std::runtime_error);
    }
    log.enable_manifest(8).get();
    log.seal_manifest(1).get();
    log.drop_old_manifest_generations().get();
    BOOST_REQUIRE(!fs::exists(gen_dir(tmp, 5)));
    BOOST_REQUIRE(!fs::exists(gen_dir(tmp, 7)));
    BOOST_REQUIRE(fs::exists(gen_dir(tmp, 8) / "record"));
    BOOST_REQUIRE(fs::exists(gen_dir(tmp, 8) / "0"));
    clean_stop(log);
}

// `find commitlog -type f -delete` leaves the generation directories. No
// manifest file was read, so the marker check refuses; the empty directories
// are still dropped and never reused.
SEASTAR_THREAD_TEST_CASE(test_empty_generation_dir_is_not_a_manifest) {
    tmpdir tmp;
    fs::create_directories(gen_dir(tmp, 3));

    std::optional<commitlog> log;
    auto out = capture_log([&] {
        log.emplace(commitlog::create_commitlog(make_config(tmp)).get());
    });
    BOOST_REQUIRE_NE(out.find("generation 3 in " + tmp.path().string() + " has no manifest files"), std::string::npos);
    BOOST_REQUIRE(!log->manifest_found_on_startup());
    BOOST_REQUIRE(log->has_old_manifest_generations());
    BOOST_REQUIRE_EQUAL(log->next_manifest_generation(), 4);

    log->enable_manifest(4).get();
    log->seal_manifest(1).get();
    log->drop_old_manifest_generations().get();
    BOOST_REQUIRE(!fs::exists(gen_dir(tmp, 3)));
    BOOST_REQUIRE(fs::exists(gen_dir(tmp, 4) / "record"));
    clean_stop(*log);
}

// Dropping the old generations before the new one is sealed would leave no
// sealed manifest after a crash, and enabling twice would switch generations
// behind the caller's back.
SEASTAR_THREAD_TEST_CASE(test_drop_before_seal_and_double_enable_are_refused) {
    tmpdir tmp;
    write_record(tmp, 5, 1);
    write_shard_file(tmp, 5, 0, {});

    auto log = commitlog::create_commitlog(make_config(tmp)).get();
    seastar::testing::scoped_no_abort_on_internal_error no_abort;
    BOOST_REQUIRE_THROW(log.drop_old_manifest_generations().get(), std::runtime_error);
    BOOST_REQUIRE(fs::exists(gen_dir(tmp, 5) / "record"));

    log.enable_manifest(6).get();
    BOOST_REQUIRE_THROW(log.enable_manifest(7).get(), std::runtime_error);
    BOOST_REQUIRE(!fs::exists(gen_dir(tmp, 7)));
    // Enabled but not sealed.
    BOOST_REQUIRE_THROW(log.drop_old_manifest_generations().get(), std::runtime_error);
    BOOST_REQUIRE(fs::exists(gen_dir(tmp, 5) / "record"));
    log.seal_manifest(1).get();
    log.drop_old_manifest_generations().get();
    BOOST_REQUIRE(!fs::exists(gen_dir(tmp, 5)));
    clean_stop(log);
}

// Only the canonical spelling names a generation: "07" would be verified and
// dropped through the path of generation 7, which does not exist. The maximum
// value is ignored too, or the next generation would wrap to 0.
SEASTAR_THREAD_TEST_CASE(test_non_canonical_generation_dir_is_ignored) {
    tmpdir tmp;
    write_record(tmp, 5, 1);
    write_shard_file(tmp, 5, 0, {});
    auto odd_dir = tmp.path() / (commitlog::descriptor::FILENAME_PREFIX + "manifest-07");
    write_text(odd_dir / "0", R"({"version":1,"shard":0,"segments":[]})");
    auto max_dir = tmp.path() / (commitlog::descriptor::FILENAME_PREFIX + "manifest-18446744073709551615");
    write_text(max_dir / "0", R"({"version":1,"shard":0,"segments":[]})");

    std::optional<commitlog> log;
    auto out = capture_log([&] {
        log.emplace(commitlog::create_commitlog(make_config(tmp)).get());
    });
    BOOST_REQUIRE_NE(out.find("Ignoring " + odd_dir.string()), std::string::npos);
    BOOST_REQUIRE_NE(out.find("Ignoring " + max_dir.string()), std::string::npos);
    BOOST_REQUIRE_EQUAL(log->next_manifest_generation(), 6);

    log->enable_manifest(6).get();
    log->seal_manifest(1).get();
    log->drop_old_manifest_generations().get();
    BOOST_REQUIRE(!fs::exists(gen_dir(tmp, 5)));
    BOOST_REQUIRE(fs::exists(odd_dir / "0"));
    BOOST_REQUIRE(fs::exists(max_dir / "0"));
    clean_stop(*log);
}

SEASTAR_THREAD_TEST_CASE(test_ignore_option_logs_and_starts) {
    tmpdir tmp;
    write_record(tmp, 1, 1);
    write_shard_file(tmp, 1, 0, {missing_segment});
    auto cfg = make_config(tmp);
    cfg.ignore_manifest_errors = true;

    std::optional<commitlog> log;
    auto out = capture_log([&] {
        log.emplace(commitlog::create_commitlog(cfg).get());
    });
    BOOST_REQUIRE_NE(out.find("ERROR"), std::string::npos);
    BOOST_REQUIRE_NE(out.find("lists segment " + missing_segment + " which is missing"), std::string::npos);
    BOOST_REQUIRE(log->manifest_found_on_startup());
    BOOST_REQUIRE_EQUAL(log->next_manifest_generation(), 2);
    clean_stop(*log);
}

// A corrupt shard file is present, so the completeness check does not report it again.
SEASTAR_THREAD_TEST_CASE(test_ignore_option_reports_corrupt_shard_file_once) {
    tmpdir tmp;
    write_record(tmp, 1, 2);
    write_shard_file(tmp, 1, 0, {});
    write_text(gen_dir(tmp, 1) / "1", "garbage");
    auto cfg = make_config(tmp);
    cfg.ignore_manifest_errors = true;

    std::optional<commitlog> log;
    auto out = capture_log([&] {
        log.emplace(commitlog::create_commitlog(cfg).get());
    });
    BOOST_REQUIRE_NE(out.find("CommitLog-manifest-1/1 cannot be parsed"), std::string::npos);
    BOOST_REQUIRE_EQUAL(out.find("manifest for shard 1 is missing"), std::string::npos);
    clean_stop(*log);
}

#ifdef SCYLLA_ENABLE_ERROR_INJECTION
SEASTAR_THREAD_TEST_CASE(test_manifest_write_failure_keeps_files) {
    tmpdir tmp;
    auto log = commitlog::create_commitlog(make_config(tmp)).get();
    log.enable_manifest(1).get();
    log.seal_manifest(1).get();

    auto uuid = make_table_id();
    rp_set rps;
    fill_segments(log, uuid, &rps, 3);
    log.sync_all_segments().get();
    auto active = log.get_active_segment_names();
    // The last segment is still allocating and is not disposed.
    auto old = basenames(std::vector<sstring>(active.begin(), active.end() - 1));

    utils::get_local_injector().enable("commitlog_manifest_write_fail");
    // A failed assertion below must not leave the injection on for the next tests.
    auto disable_injection = defer([] noexcept { utils::get_local_injector().disable("commitlog_manifest_write_fail"); });
    // delete_segments() returns the disposal pass's future, so it shows that nothing is thrown.
    discard_all(log, uuid, rps);

    auto listed = read_shard_file(tmp, 1, 0);
    auto on_disk = segments_on_disk(tmp);
    for (const auto& name : old) {
        BOOST_REQUIRE(on_disk.contains(name));
        BOOST_REQUIRE(listed.contains(name));
    }

    utils::get_local_injector().disable("commitlog_manifest_write_fail");
    // Runs another disposal pass.
    log.delete_segments({}).get();

    listed = read_shard_file(tmp, 1, 0);
    on_disk = segments_on_disk(tmp);
    for (const auto& name : old) {
        BOOST_REQUIRE(!on_disk.contains(name));
        BOOST_REQUIRE(!listed.contains(name));
    }
    clean_stop(log);
}

// add() fails for the segment that refills the reserve after the first write
// takes the current one. allocate_segment() disposes that segment and the
// replenisher retries, so the write completes and the file ends up recycled.
SEASTAR_THREAD_TEST_CASE(test_manifest_add_failure_disposes_segment) {
    tmpdir tmp;
    auto log = commitlog::create_commitlog(make_config(tmp)).get();
    log.enable_manifest(1).get();
    log.seal_manifest(1).get();

    utils::get_local_injector().enable("commitlog_manifest_write_fail", true);
    // A failed assertion before the shot fires must not leave it armed for the next tests.
    auto disable_injection = defer([] noexcept { utils::get_local_injector().disable("commitlog_manifest_write_fail"); });
    auto uuid = make_table_id();
    rp_set rps;
    auto out = capture_log([&] {
        fill_segments(log, uuid, &rps, 2);
    });
    BOOST_REQUIRE_NE(out.find("Exception in segment reservation"), std::string::npos);
    BOOST_REQUIRE_NE(out.find("commitlog_manifest_write_fail: injected error"), std::string::npos);

    // Runs the disposal pass for the segment whose add() failed.
    log.delete_segments({}).get();
    log.wait_for_pending_deletes().get();

    auto listed = read_shard_file(tmp, 1, 0);
    auto on_disk = segments_on_disk(tmp);
    BOOST_REQUIRE(std::ranges::includes(on_disk, listed));
    BOOST_REQUIRE(std::ranges::includes(listed, basenames(log.get_active_segment_names())));
    // Two active segments and at most one reserve segment; the disposed one is recycled.
    BOOST_REQUIRE_LE(on_disk.size(), 3);
    BOOST_REQUIRE_EQUAL(recycled_on_disk(tmp), 1);

    discard_all(log, uuid, rps);
    clean_stop(log);
}
#endif

// The documented restart sequence: verify in init(), enable the next generation,
// seal, drop the old generations, then delete the replayed segments. The next
// start finds one sealed generation and nothing unlisted.
SEASTAR_THREAD_TEST_CASE(test_restart_sequence_keeps_manifest_consistent) {
    tmpdir tmp;
    auto cfg = make_config(tmp);
    std::set<std::string> dirty;
    {
        auto log = commitlog::create_commitlog(cfg).get();
        log.enable_manifest(1).get();
        log.seal_manifest(1).get();
        fill_segments(log, make_table_id(), nullptr, 2);
        dirty = basenames(log.get_active_segment_names());
        crash_stop(log);
    }
    {
        auto log = commitlog::create_commitlog(cfg).get();
        BOOST_REQUIRE(log.manifest_found_on_startup());
        BOOST_REQUIRE_EQUAL(log.next_manifest_generation(), 2);
        BOOST_REQUIRE(std::ranges::includes(read_shard_file(tmp, 1, 0), dirty));
        BOOST_REQUIRE(std::ranges::includes(segments_on_disk(tmp), dirty));

        log.enable_manifest(log.next_manifest_generation()).get();
        log.seal_manifest(1).get();
        log.drop_old_manifest_generations().get();
        BOOST_REQUIRE(!fs::exists(gen_dir(tmp, 1)));
        auto replay = log.get_segments_to_replay().get();
        BOOST_REQUIRE(std::ranges::includes(basenames(replay), dirty));
        log.delete_segments(std::move(replay)).get();
        log.wait_for_pending_deletes().get();
        auto on_disk = segments_on_disk(tmp);
        for (const auto& name : dirty) {
            BOOST_REQUIRE(!on_disk.contains(name));
        }
        clean_stop(log);
    }
    {
        std::optional<commitlog> log;
        auto out = capture_log([&] {
            log.emplace(commitlog::create_commitlog(cfg).get());
        });
        BOOST_REQUIRE_EQUAL(out.find("is not listed in any commitlog manifest"), std::string::npos);
        BOOST_REQUIRE(log->manifest_found_on_startup());
        BOOST_REQUIRE_EQUAL(log->next_manifest_generation(), 3);
        BOOST_REQUIRE(fs::exists(gen_dir(tmp, 2) / "record"));
        clean_stop(*log);
    }
}

// delete_segments() before drop_old_manifest_generations() unlinks segments the
// old generation still lists, so a crash between the two refuses the next start.
SEASTAR_THREAD_TEST_CASE(test_delete_before_drop_refuses_next_start) {
    tmpdir tmp;
    auto cfg = make_config(tmp);
    {
        auto log = commitlog::create_commitlog(cfg).get();
        log.enable_manifest(1).get();
        log.seal_manifest(1).get();
        fill_segments(log, make_table_id(), nullptr, 2);
        crash_stop(log);
    }
    {
        auto log = commitlog::create_commitlog(cfg).get();
        log.enable_manifest(log.next_manifest_generation()).get();
        log.seal_manifest(1).get();
        log.delete_segments(log.get_segments_to_replay().get()).get();
        log.wait_for_pending_deletes().get();
        crash_stop(log);
    }
    BOOST_REQUIRE_EXCEPTION(commitlog::create_commitlog(cfg).get(), commitlog_manifest_error, [] (const commitlog_manifest_error& e) {
        return message_contains(e, {"CommitLog-manifest-1/0 lists segment", "which is missing"});
    });
}

SEASTAR_THREAD_TEST_CASE(test_activation_races_allocation) {
    struct hold_first_allocation : public commitlog_file_extension {
        bool held = false;
        sstring name;
        promise<> reached;
        promise<> resume;

        future<file> wrap_file(const sstring& filename, file f, open_flags) override {
            if (!held) {
                held = true;
                name = fs::path(filename).filename().string();
                reached.set_value();
                co_await resume.get_future();
            }
            co_return f;
        }
        future<> before_delete(const sstring&) override {
            co_return;
        }
    };

    auto ext = std::make_unique<hold_first_allocation>();
    auto& hold = *ext;
    db::extensions exts;
    exts.add_commitlog_file_extension("hold_first_allocation", std::move(ext));

    tmpdir tmp;
    auto cfg = make_config(tmp);
    cfg.extensions = &exts;
    auto log = commitlog::create_commitlog(cfg).get();

    // The replenisher's first segment is created and not yet listed. Non-fatal checks:
    // a fatal one would destroy the commitlog under the held allocation instead of
    // reaching clean_stop().
    hold.reached.get_future().get();
    log.enable_manifest(1).get();
    BOOST_CHECK(!read_shard_file(tmp, 1, 0).contains(hold.name));
    hold.resume.set_value();

    // The write takes the held segment from the reserve, which happens after add().
    {
        auto h = log.add_mutation(make_table_id(), 16, commitlog::force_sync::no, [] (commitlog::output& dst) {
            dst.fill('1', 16);
        }).get();
        BOOST_CHECK(basenames(log.get_active_segment_names()).contains(hold.name));
        BOOST_CHECK(read_shard_file(tmp, 1, 0).contains(hold.name));
    }
    clean_stop(log);
}

BOOST_AUTO_TEST_SUITE_END()
