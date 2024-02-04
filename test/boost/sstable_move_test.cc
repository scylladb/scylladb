/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <filesystem>
#include "test/lib/scylla_test_case.hh"
#include <seastar/testing/thread_test_case.hh>

#include "utils/lister.hh"
#include "test/lib/tmpdir.hh"
#include "test/lib/sstable_test_env.hh"
#include "sstable_test.hh"

using namespace sstables;
namespace fs = std::filesystem;

// Must be called from a seastar thread
static auto copy_sst_to_tmpdir(fs::path tmp_path, test_env& env, sstables::schema_ptr schema_ptr, fs::path src_path, sstables::generation_type gen) {
    auto sst = env.reusable_sst(schema_ptr, src_path.native(), gen).get();
    auto dst_path = tmp_path / src_path.filename() / format("gen-{}", gen);
    recursive_touch_directory(dst_path.native()).get();
    for (auto p : sst->all_components()) {
        auto src_path = test(sst).filename(p.first);
        copy_file(src_path, dst_path / src_path.filename());
    }
    return std::make_pair(env.reusable_sst(schema_ptr, dst_path.native(), gen).get(), dst_path.native());
}

SEASTAR_THREAD_TEST_CASE(test_sstable_move) {
    tmpdir tmp;
    auto env = test_env();
    auto stop_env = defer([&env] { env.stop().get(); });

    sstables::sstable_generation_generator gen_generator{0};
    auto [ sst, cur_dir ] = copy_sst_to_tmpdir(tmp.path(), env, uncompressed_schema(), fs::path(uncompressed_dir()), gen_generator());

    generation_type gen{0};
    for (auto i = 0; i < 2; i++) {
        gen = gen_generator();
        auto new_dir = format("{}/gen-{}", fs::path(cur_dir).parent_path().native(), gen);
        touch_directory(new_dir).get();
        test(sst).move_to_new_dir(new_dir, gen).get();
        // the source directory must be empty now
        remove_file(cur_dir).get();
        cur_dir = new_dir;
    }

    // close  the sst and make we can load it from the new directory.
    sst->close_files().get();
    sst = env.reusable_sst(uncompressed_schema(), cur_dir, gen).get();
}

SEASTAR_THREAD_TEST_CASE(test_sstable_move_idempotent) {
    tmpdir tmp;
    auto env = test_env();
    auto stop_env = defer([&env] { env.stop().get(); });
    sstables::sstable_generation_generator gen_generator{0};

    auto [ sst, cur_dir ] = copy_sst_to_tmpdir(tmp.path(), env, uncompressed_schema(), fs::path(uncompressed_dir()), gen_generator());
    sstring old_path = sst->get_storage().prefix();
    touch_directory(format("{}/{}", old_path, sstables::staging_dir)).get();
    sst->change_state(sstables::sstable_state::staging).get();
    sst->change_state(sstables::sstable_state::normal).get();
    BOOST_REQUIRE(sstable_directory::compare_sstable_storage_prefix(old_path, sst->get_storage().prefix()));

    sst->close_files().get();
}

// Simulate a crashed create_links.
// This implementation simulates create_links;
// ideally, we should have error injection points in create_links
// to cause it to return mid-way.
//
// Returns true when done
//
// Must be called from a seastar thread
static bool partial_create_links(sstable_ptr sst, fs::path dst_path, sstables::generation_type gen, int count) {
    auto schema = sst->get_schema();
    auto tmp_toc = sstable::filename(dst_path.native(), schema->ks_name(), schema->cf_name(), sst->get_version(), gen, sstable_format_types::big, component_type::TemporaryTOC);
    link_file(test(sst).filename(component_type::TOC).native(), tmp_toc).get();
    for (auto& [c, s] : sst->all_components()) {
        if (count-- <= 0) {
            return false;
        }
        auto src = test(sst).filename(c);
        auto dst = sstable::filename(dst_path.native(), schema->ks_name(), schema->cf_name(), sst->get_version(), gen, sstable_format_types::big, c);
        link_file(src.native(), dst).get();
    }
    if (count-- <= 0) {
        return false;
    }
    auto dst = sstable::filename(dst_path.native(), schema->ks_name(), schema->cf_name(), sst->get_version(), gen, sstable_format_types::big, component_type::TOC);
    remove_file(tmp_toc).get();
    return true;
}

SEASTAR_THREAD_TEST_CASE(test_sstable_move_replay) {
    tmpdir tmp;
    auto env = test_env();
    auto stop_env = defer([&env] { env.stop().get(); });

    sstables::sstable_generation_generator gen_generator{0};
    auto [ sst, cur_dir ] = copy_sst_to_tmpdir(tmp.path(), env, uncompressed_schema(), fs::path(uncompressed_dir()), gen_generator());

    bool done;
    int count = 0;
    do {
        auto gen = gen_generator();
        auto new_dir = format("{}/gen-{}", fs::path(cur_dir).parent_path().native(), gen);
        touch_directory(new_dir).get();
        done = partial_create_links(sst, fs::path(new_dir), gen, count++);
        test(sst).move_to_new_dir(new_dir, gen).get();
        remove_file(cur_dir).get();
        cur_dir = new_dir;
    } while (!done);
}

SEASTAR_THREAD_TEST_CASE(test_sstable_move_exists_failure) {
    tmpdir tmp;
    auto env = test_env();
    auto stop_env = defer([&env] { env.stop().get(); });

    // please note, the SSTables used by this test are stored under
    // get_test_dir("uncompressed", "ks", fs::path(uncompressed_dir())), so the
    // gen_1 and gen_2 have to match with the these SSTables under that directory.
    sstables::generation_type gen_1{1};
    auto [ src_sst, cur_dir ] = copy_sst_to_tmpdir(tmp.path(), env, uncompressed_schema(), fs::path(uncompressed_dir()), gen_1);
    sstables::generation_type gen_2{2};
    auto [ dst_sst, new_dir ] = copy_sst_to_tmpdir(tmp.path(), env, uncompressed_schema(), fs::path(uncompressed_dir()), gen_2);
    dst_sst->close_files().get();
    BOOST_REQUIRE_THROW(test(src_sst).move_to_new_dir(new_dir, gen_2).get(), malformed_sstable_exception);
}
