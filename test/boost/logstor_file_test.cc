/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <stdexcept>
#include <type_traits>

#include <fmt/format.h>

#include <seastar/core/aligned_buffer.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/core/seastar.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include "replica/logstor/segment_file.hh"
#include "test/lib/eventually.hh"
#include "test/lib/tmpdir.hh"
#include "utils/error_injection.hh"

using namespace replica::logstor;

namespace {

constexpr uint64_t test_file_size = 256 * 1024;

file_manager_config make_config(const tmpdir& dir, uint64_t file_count, bool format_on_startup, bool sparse_files = true,
        uint64_t file_size = test_file_size) {
    return file_manager_config{
        .base_dir = dir.path(),
        .file_size = file_size,
        .file_count = file_count,
        .format_sg = default_scheduling_group(),
        .format_on_startup = format_on_startup,
        .sparse_files = sparse_files,
        // The next allocation of a file starts a failed format again.
        .format_retry_interval = std::chrono::milliseconds(0),
    };
}

// Runs the three steps of the recovery, and returns the count of the found files.
file_id_t recover(file_manager& files, file_id_t used_file_count = 0) {
    auto recovery = files.start_recovery();
    const auto found_file_count = recovery.open_found_files().get();
    recovery.retire_unused_files(used_file_count).get();
    recovery.finish().get();
    return found_file_count;
}

bool on_disk(const std::filesystem::path& path) {
    return seastar::file_exists(path.string()).get();
}

void make_file(const std::filesystem::path& path) {
    auto f = seastar::open_file_dma(path.string(), open_flags::rw | open_flags::create | open_flags::truncate).get();
    f.truncate(test_file_size).get();
    f.close().get();
}

} // anonymous namespace

// Checks that a file is allocated by its first segment, and not when its background format ends. A
// file manager that takes a formatted file as allocated starts no format of the next file, and the
// first segment of the next file then fails.
SEASTAR_THREAD_TEST_CASE(test_file_manager_allocates_a_file_at_its_first_segment) {
    tmpdir dir;
    file_manager files(make_config(dir, 3, false));
    auto stop = defer([&files] noexcept { files.stop().get(); });

    BOOST_REQUIRE_EQUAL(recover(files), 0);
    BOOST_REQUIRE_EQUAL(files.allocated_file_count(), 0);

    // The end of the recovery starts the format of file 0.
    files.get_file_for_new_segment(0).get();
    BOOST_REQUIRE_EQUAL(files.allocated_file_count(), 1);

    // The allocation of file 0 starts the format of file 1. File 1 is then formatted, but it is not
    // allocated.
    BOOST_REQUIRE(eventually_true([&] { return on_disk(files.file_path(1)); }));
    BOOST_REQUIRE_EQUAL(files.allocated_file_count(), 1);

    // A later segment of an allocated file does not allocate a file.
    files.get_file_for_new_segment(0).get();
    BOOST_REQUIRE_EQUAL(files.allocated_file_count(), 1);

    // The format of file 1 ended before its first segment. This segment allocates file 1, and the
    // allocation starts the format of file 2.
    files.get_file_for_new_segment(1).get();
    BOOST_REQUIRE_EQUAL(files.allocated_file_count(), 2);
    BOOST_REQUIRE(eventually_true([&] { return on_disk(files.file_path(2)); }));
    files.get_file_for_new_segment(2).get();
    BOOST_REQUIRE_EQUAL(files.allocated_file_count(), 3);

    // File 2 is the last configured file, thus its allocation starts no format. stop() waits for a
    // format that runs.
    files.stop().get();
    BOOST_REQUIRE(!on_disk(files.file_path(3)));
}

// Checks the recovery of a prepared directory: the found files, the temporary file of a format that
// did not finish, the files of other shards, the retired files, and a missing file.
SEASTAR_THREAD_TEST_CASE(test_file_manager_recovers_a_prepared_directory) {
    tmpdir dir;
    auto path = [&] (std::string_view name) { return dir.path() / name; };
    auto name = [] (unsigned shard, file_id_t file_id) { return fmt::format("ls_{}-{}-Data.db", shard, file_id); };

    for (file_id_t file_id = 0; file_id < 4; ++file_id) {
        make_file(path(name(this_shard_id(), file_id)));
    }
    const auto temporary_file = path(name(this_shard_id(), 4) + ".tmp");
    const auto file_of_other_shard = path(name(this_shard_id() + 1, 0));
    const auto file_with_bad_id = path(fmt::format("ls_{}-1x-Data.db", this_shard_id()));
    make_file(temporary_file);
    make_file(file_of_other_shard);
    make_file(file_with_bad_id);

    // Four files on a disk of two files. Files 0 to 2 hold used segments, thus only file 3 is
    // retired.
    {
        file_manager files(make_config(dir, 2, false));
        auto stop = defer([&files] noexcept { files.stop().get(); });
        auto recovery = files.start_recovery();
        BOOST_REQUIRE_EQUAL(recovery.open_found_files().get(), 4);
        BOOST_REQUIRE_EQUAL(files.allocated_file_count(), 4);
        recovery.retire_unused_files(3).get();
        BOOST_REQUIRE_EQUAL(files.allocated_file_count(), 3);
        recovery.finish().get();

        for (file_id_t file_id = 0; file_id < 3; ++file_id) {
            BOOST_REQUIRE(on_disk(files.file_path(file_id)));
            files.get_open_file(file_id);
        }
        BOOST_REQUIRE(!on_disk(files.file_path(3)));
    }
    BOOST_REQUIRE(!on_disk(temporary_file));
    BOOST_REQUIRE(on_disk(file_of_other_shard));
    BOOST_REQUIRE(on_disk(file_with_bad_id));

    // Three files on a disk of five files. The end of the recovery starts the format of file 3, and
    // the first segment of file 3 allocates it.
    {
        file_manager files(make_config(dir, 5, false));
        auto stop = defer([&files] noexcept { files.stop().get(); });
        BOOST_REQUIRE_EQUAL(recover(files, 3), 3);
        BOOST_REQUIRE_EQUAL(files.allocated_file_count(), 3);
        files.get_file_for_new_segment(3).get();
        BOOST_REQUIRE_EQUAL(files.allocated_file_count(), 4);
        BOOST_REQUIRE(on_disk(files.file_path(3)));
    }

    // A missing file fails the recovery.
    seastar::remove_file(path(name(this_shard_id(), 1)).string()).get();
    {
        file_manager files(make_config(dir, 5, false));
        auto stop = defer([&files] noexcept { files.stop().get(); });
        auto recovery = files.start_recovery();
        BOOST_REQUIRE_THROW(recovery.open_found_files().get(), std::runtime_error);
    }
}

// Checks the format on startup: the end of the recovery formats the files that the recovery did not
// find, and allocates them. Thus no file is formatted later.
SEASTAR_THREAD_TEST_CASE(test_file_manager_formats_the_missing_files_on_startup) {
    tmpdir dir;
    file_manager files(make_config(dir, 3, true));
    auto stop = defer([&files] noexcept { files.stop().get(); });
    make_file(files.file_path(0));

    BOOST_REQUIRE_EQUAL(recover(files, 1), 1);
    BOOST_REQUIRE_EQUAL(files.allocated_file_count(), 3);
    for (file_id_t file_id = 0; file_id < 3; ++file_id) {
        BOOST_REQUIRE(on_disk(files.file_path(file_id)));
        files.get_open_file(file_id);
    }
}

// Checks that a file that the recovery opens can be written. The read and the write paths share one
// handle per file, and the recovery opens the found files before it reads them, so a read-only
// handle makes the first segment write to a found file fail.
SEASTAR_THREAD_TEST_CASE(test_file_manager_recovered_files_are_writable) {
    tmpdir dir;
    file_manager files(make_config(dir, 2, false));
    auto stop = defer([&files] noexcept { files.stop().get(); });
    make_file(files.file_path(0));
    BOOST_REQUIRE_EQUAL(recover(files, 1), 1);

    constexpr uint64_t block = 4096;
    auto data = allocate_aligned_buffer<char>(block, block);
    std::memset(data.get(), 0xab, block);
    auto written = files.get_file_for_new_segment(0).get().dma_write(block, data.get(), block).get();
    BOOST_REQUIRE_EQUAL(written, block);

    auto buf = files.get_open_file(0).dma_read_bulk<char>(block, block).get();
    BOOST_REQUIRE_EQUAL(buf.size(), block);
    BOOST_REQUIRE(std::equal(buf.begin(), buf.end(), data.get()));
}

// Checks that no file is allocated at or above the configured count after the configured disk size
// became smaller. The recovery keeps the files above the configured count that hold used
// segments, and removes the others down to the configured count.
SEASTAR_THREAD_TEST_CASE(test_file_manager_allocates_no_file_above_a_smaller_configuration) {
    const auto do_abort = set_abort_on_internal_error(false);
    auto reset_abort = defer([do_abort] noexcept {
        set_abort_on_internal_error(do_abort);
    });

    // Four files on a disk of two files.
    struct shrink {
        file_id_t used_file_count;
        file_id_t kept_file_count;
    };
    for (auto [used_file_count, kept_file_count] : {shrink{3, 3}, shrink{0, 2}}) {
        BOOST_TEST_MESSAGE(fmt::format("{} files hold used segments", used_file_count));
        tmpdir dir;
        file_manager files(make_config(dir, 2, false));
        auto stop = defer([&files] noexcept { files.stop().get(); });
        for (file_id_t file_id = 0; file_id < 4; ++file_id) {
            make_file(files.file_path(file_id));
        }

        BOOST_REQUIRE_EQUAL(recover(files, used_file_count), 4);
        BOOST_REQUIRE_EQUAL(files.allocated_file_count(), kept_file_count);
        BOOST_REQUIRE_THROW(files.get_file_for_new_segment(kept_file_count).get(), std::runtime_error);
        BOOST_REQUIRE_EQUAL(files.allocated_file_count(), kept_file_count);
        files.get_file_for_new_segment(kept_file_count - 1).get();

        // The recovery started no format, and stop() waits for a format that runs.
        files.stop().get();
        for (file_id_t file_id = kept_file_count; file_id < 4; ++file_id) {
            BOOST_REQUIRE(!on_disk(files.file_path(file_id)));
        }
    }
}

// Checks that stop() waits for the background format of the next file, so that the format does not
// leave a temporary file. The format of a full file is slow enough to run when stop() is called. The
// next recovery finds the formatted file, and allocates it.
SEASTAR_THREAD_TEST_CASE(test_file_manager_stop_waits_for_the_background_format) {
    constexpr uint64_t file_size = 16 * 1024 * 1024;
    tmpdir dir;
    {
        file_manager files(make_config(dir, 2, false, false, file_size));
        auto stop = defer([&files] noexcept { files.stop().get(); });

        // The end of the recovery starts the format of file 0.
        BOOST_REQUIRE_EQUAL(recover(files), 0);
        files.stop().get();
        BOOST_REQUIRE_EQUAL(files.allocated_file_count(), 0);
        BOOST_REQUIRE(on_disk(files.file_path(0)));
        BOOST_REQUIRE(!on_disk(files.file_path(0).string() + ".tmp"));
        BOOST_REQUIRE_EQUAL(seastar::file_size(files.file_path(0).string()).get(), file_size);
    }

    file_manager files(make_config(dir, 2, false, false, file_size));
    auto stop = defer([&files] noexcept { files.stop().get(); });
    BOOST_REQUIRE_EQUAL(recover(files), 1);
    BOOST_REQUIRE_EQUAL(files.allocated_file_count(), 1);
    files.get_open_file(0);
}

// Checks that a failed open of a new file leaves the file not allocated, and that the next segment
// of the file opens it again. The open fails while the format of the file runs. A format that opens
// the file would fail, and then the file could not be allocated again.
SEASTAR_THREAD_TEST_CASE(test_file_manager_tries_a_failed_open_again) {
    if constexpr (!std::is_same_v<utils::error_injection_type, utils::error_injection<true>>) {
        return;
    }

    // The end of the recovery starts the format of file 0, thus the injection is enabled before the
    // recovery.
    utils::get_local_injector().enable("logstor_open_file_fail", true);
    auto disable_injection = defer([] noexcept {
        utils::get_local_injector().disable("logstor_open_file_fail");
    });

    tmpdir dir;
    file_manager files(make_config(dir, 2, false));
    auto stop = defer([&files] noexcept { files.stop().get(); });
    recover(files);

    BOOST_REQUIRE_THROW(files.get_file_for_new_segment(0).get(), std::runtime_error);
    BOOST_REQUIRE_EQUAL(files.allocated_file_count(), 0);
    // The open failed one time. A one shot injection is disabled after it fails a call.
    BOOST_REQUIRE(!utils::get_local_injector().is_enabled("logstor_open_file_fail"));

    // The next segment of file 0 opens it, and the allocation starts the format of file 1.
    files.get_file_for_new_segment(0).get();
    BOOST_REQUIRE_EQUAL(files.allocated_file_count(), 1);
    files.get_open_file(0);
    BOOST_REQUIRE(eventually_true([&] { return on_disk(files.file_path(1)); }));
}

// Checks that the allocation of a file fails when its format failed, and that an allocation after
// the retry interval starts the format again, but an allocation before it does not.
SEASTAR_THREAD_TEST_CASE(test_file_manager_tries_a_failed_format_again) {
    if constexpr (!std::is_same_v<utils::error_injection_type, utils::error_injection<true>>) {
        return;
    }

    // The end of the recovery starts the format of file 0, and the injection makes it fail. The
    // injection fails only one format, thus a retry succeeds.
    auto with_failed_format = [] (std::chrono::milliseconds retry_interval, auto test) {
        tmpdir dir;
        auto config = make_config(dir, 2, false);
        config.format_retry_interval = retry_interval;
        file_manager files(std::move(config));
        auto stop = defer([&files] noexcept { files.stop().get(); });
        utils::get_local_injector().enable("logstor_format_file_fail", true /* one shot */);
        auto disable_injection = defer([] noexcept {
            utils::get_local_injector().disable("logstor_format_file_fail");
        });
        recover(files);
        // A one shot injection is disabled after it fails a call.
        BOOST_REQUIRE(eventually_true([] { return !utils::get_local_injector().is_enabled("logstor_format_file_fail"); }));
        test(files);
    };

    // Before the retry interval passes, each allocation fails, and the format is not tried again.
    with_failed_format(std::chrono::hours(1), [] (file_manager& files) {
        BOOST_REQUIRE_THROW(files.get_file_for_new_segment(0).get(), std::runtime_error);
        BOOST_REQUIRE_THROW(files.get_file_for_new_segment(0).get(), std::runtime_error);
        BOOST_REQUIRE_EQUAL(files.allocated_file_count(), 0);
        BOOST_REQUIRE(!on_disk(files.file_path(0)));
    });

    // After the retry interval, an allocation starts the format again. If the failed format has not
    // ended yet, the allocation waits for it and fails, so the test tries again.
    with_failed_format(std::chrono::milliseconds(0), [] (file_manager& files) {
        BOOST_REQUIRE(eventually_true([&files] {
            try {
                files.get_file_for_new_segment(0).get();
                return true;
            } catch (const std::runtime_error&) {
                return false;
            }
        }));
        BOOST_REQUIRE_EQUAL(files.allocated_file_count(), 1);
        BOOST_REQUIRE(on_disk(files.file_path(0)));
    });

    // An allocation that must not wait also fails with the error of the failed format.
    with_failed_format(std::chrono::hours(1), [] (file_manager& files) {
        BOOST_REQUIRE_THROW(files.get_file_for_new_segment(0, wait_for_format::no).get(), std::runtime_error);
        BOOST_REQUIRE_EQUAL(files.allocated_file_count(), 0);
    });

    // An allocation that must not wait also starts the format again, and fails while it runs. The
    // first call fails in all cases: the failed format did not end yet, or the call started the
    // retry, which does I/O and is not done when the call checks it.
    with_failed_format(std::chrono::milliseconds(0), [] (file_manager& files) {
        BOOST_REQUIRE_THROW(files.get_file_for_new_segment(0, wait_for_format::no).get(), std::runtime_error);
        BOOST_REQUIRE(eventually_true([&files] {
            try {
                files.get_file_for_new_segment(0, wait_for_format::no).get();
                return true;
            } catch (const std::runtime_error&) {
                return false;
            }
        }));
        BOOST_REQUIRE_EQUAL(files.allocated_file_count(), 1);
    });
}

// Checks that an allocation that must not wait fails while the format of its file runs, and that
// the format continues, so that a later allocation gets the file.
SEASTAR_THREAD_TEST_CASE(test_file_manager_allocation_without_wait_fails_during_a_format) {
    tmpdir dir;
    file_manager files(make_config(dir, 2, false));
    auto stop = defer([&files] noexcept { files.stop().get(); });

    // The end of the recovery starts the format of file 0. Nothing yields after it, and a format
    // does I/O, so the format is not done at the first allocation.
    recover(files);
    BOOST_REQUIRE_THROW(files.get_file_for_new_segment(0, wait_for_format::no).get(), std::runtime_error);
    BOOST_REQUIRE_EQUAL(files.allocated_file_count(), 0);

    files.get_file_for_new_segment(0).get();
    BOOST_REQUIRE_EQUAL(files.allocated_file_count(), 1);
}

// Checks that each misuse of the file manager is an internal error: the recovery steps out of order
// or repeated, an allocation before the recovery finished, a read of a file that is not allocated,
// an allocation that skips a file, and a zero of a region that is not in an allocated file.
SEASTAR_THREAD_TEST_CASE(test_file_manager_misuse_is_an_internal_error) {
    const auto do_abort = set_abort_on_internal_error(false);
    auto reset_abort = defer([do_abort] noexcept {
        set_abort_on_internal_error(do_abort);
    });

    // Each case has its own directory and file manager, because a failed call can leave the file
    // manager in a state that no later call expects.
    auto with_file_manager = [] (auto test) {
        tmpdir dir;
        file_manager files(make_config(dir, 3, false));
        auto stop = defer([&files] noexcept { files.stop().get(); });
        test(files);
    };

    // Recovery steps out of order, and a second recovery.
    with_file_manager([] (file_manager& files) {
        auto recovery = files.start_recovery();
        BOOST_REQUIRE_THROW(files.start_recovery(), std::runtime_error);
        BOOST_REQUIRE_THROW(recovery.retire_unused_files(0).get(), std::runtime_error);
        BOOST_REQUIRE_THROW(recovery.finish().get(), std::runtime_error);
        recovery.open_found_files().get();
        BOOST_REQUIRE_THROW(recovery.open_found_files().get(), std::runtime_error);
        BOOST_REQUIRE_THROW(recovery.finish().get(), std::runtime_error);
        recovery.retire_unused_files(0).get();
        recovery.finish().get();
        BOOST_REQUIRE_THROW(recovery.finish().get(), std::runtime_error);
    });

    // An allocation before the recovery finished.
    with_file_manager([] (file_manager& files) {
        BOOST_REQUIRE_THROW(files.get_file_for_new_segment(0).get(), std::runtime_error);
        auto recovery = files.start_recovery();
        recovery.open_found_files().get();
        recovery.retire_unused_files(0).get();
        BOOST_REQUIRE_THROW(files.get_file_for_new_segment(0).get(), std::runtime_error);
    });

    // A read of a file that is not allocated, and an allocation that skips file 0.
    with_file_manager([] (file_manager& files) {
        recover(files);
        BOOST_REQUIRE_THROW(files.get_open_file(0), std::runtime_error);
        BOOST_REQUIRE_THROW(files.get_file_for_new_segment(1).get(), std::runtime_error);
        BOOST_REQUIRE_EQUAL(files.allocated_file_count(), 0);

        files.get_file_for_new_segment(0).get();
        files.get_open_file(0);
        BOOST_REQUIRE_THROW(files.get_open_file(1), std::runtime_error);
    });

    // A zero of a file that is not allocated, and of a region that is not in the file.
    with_file_manager([] (file_manager& files) {
        constexpr uint64_t block = 4096;
        recover(files);
        BOOST_REQUIRE_THROW(files.zero_region(0, 0, block).get(), std::runtime_error);

        files.get_file_for_new_segment(0).get();
        files.zero_region(0, test_file_size - block, block).get();
        BOOST_REQUIRE_THROW(files.zero_region(0, test_file_size - block, 2 * block).get(), std::runtime_error);
        BOOST_REQUIRE_THROW(files.zero_region(0, test_file_size + block, block).get(), std::runtime_error);
        BOOST_REQUIRE_THROW(files.zero_region(1, 0, block).get(), std::runtime_error);
    });
}

// Checks that zero_region() writes zeros over the region and nothing else. The regions are one
// block, the size of the zero buffer, one block more than it, and more than the largest write of a
// format.
SEASTAR_THREAD_TEST_CASE(test_file_manager_zero_region_writes_only_the_region) {
    constexpr uint64_t block = 4096;
    constexpr uint64_t size = 4 * 1024 * 1024;
    tmpdir dir;
    file_manager files(make_config(dir, 1, false, true, size));
    auto stop = defer([&files] noexcept { files.stop().get(); });

    recover(files);
    auto f = files.get_file_for_new_segment(0).get();
    auto ones = allocate_aligned_buffer<char>(size, block);
    std::memset(ones.get(), 0xff, size);

    struct region {
        uint64_t offset;
        uint64_t size;
    };
    for (auto [offset, region_size] : {
            region{block, block},
            region{2 * block, 128 * 1024},
            region{3 * block, 128 * 1024 + block},
            region{5 * block, 1024 * 1024 + 128 * 1024 + block}}) {
        BOOST_TEST_MESSAGE(fmt::format("Region at {} of {} bytes", offset, region_size));
        for (uint64_t written = 0; written < size;) {
            written += f.dma_write(written, ones.get() + written, size - written).get();
        }

        files.zero_region(0, offset, region_size).get();

        auto buf = f.dma_read_bulk<char>(0, size).get();
        BOOST_REQUIRE_EQUAL(buf.size(), size);
        auto is = [&] (uint64_t begin, uint64_t end, char value) {
            return std::all_of(buf.get() + begin, buf.get() + end, [value] (char c) { return c == value; });
        };
        BOOST_REQUIRE(is(0, offset, char(0xff)));
        BOOST_REQUIRE(is(offset, offset + region_size, 0));
        BOOST_REQUIRE(is(offset + region_size, size, char(0xff)));
    }
}
