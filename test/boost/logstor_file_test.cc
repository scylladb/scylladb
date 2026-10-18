/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <optional>
#include <stdexcept>
#include <system_error>
#include <type_traits>
#include <vector>

#include <fmt/format.h>

#include <seastar/core/align.hh>
#include <seastar/core/aligned_buffer.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include "exceptions/exceptions.hh"
#include "replica/logstor/ondisk.hh"
#include "replica/logstor/segment_file.hh"
#include "test/lib/eventually.hh"
#include "test/lib/tmpdir.hh"
#include "utils/checked-file-impl.hh"
#include "utils/disk-error-handler.hh"
#include "utils/error_injection.hh"
#include "utils/exceptions.hh"

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

// Checks that the segment ids continue from one file to the next, also in the files above the
// configured count, and that segments_in_file() gives the segments that location_of() puts in the
// file.
BOOST_AUTO_TEST_CASE(test_segment_geometry_maps_a_segment_to_its_file) {
    constexpr uint64_t segment_size = 128 * 1024;
    const segment_geometry geometry(segment_size, 4 * segment_size, 17 * segment_size);
    BOOST_REQUIRE_EQUAL(geometry.segments_per_file(), 4);
    BOOST_REQUIRE_EQUAL(geometry.file_count(), 4);
    BOOST_REQUIRE_EQUAL(geometry.segment_count(), 16);

    auto location = geometry.location_of(log_segment_id(0));
    BOOST_REQUIRE_EQUAL(location.file_id, 0);
    BOOST_REQUIRE_EQUAL(location.file_offset, 0);
    location = geometry.location_of(log_segment_id(3));
    BOOST_REQUIRE_EQUAL(location.file_id, 0);
    BOOST_REQUIRE_EQUAL(location.file_offset, 3 * segment_size);
    location = geometry.location_of(log_segment_id(4));
    BOOST_REQUIRE_EQUAL(location.file_id, 1);
    BOOST_REQUIRE_EQUAL(location.file_offset, 0);
    location = geometry.location_of(log_segment_id(21));
    BOOST_REQUIRE_EQUAL(location.file_id, 5);
    BOOST_REQUIRE_EQUAL(location.file_offset, segment_size);

    for (file_id_t file_id = 0; file_id < 6; ++file_id) {
        uint64_t expected_offset = 0;
        for (auto segment_id : geometry.segments_in_file(file_id)) {
            BOOST_REQUIRE_EQUAL(segment_id.value, file_id * 4 + expected_offset / segment_size);
            location = geometry.location_of(segment_id);
            BOOST_REQUIRE_EQUAL(location.file_id, file_id);
            BOOST_REQUIRE_EQUAL(location.file_offset, expected_offset);
            expected_offset += segment_size;
        }
        BOOST_REQUIRE_EQUAL(expected_offset, 4 * segment_size);
    }
}

// Checks that a file holds only the segments that fit in it completely, and that the disk holds
// only complete files.
BOOST_AUTO_TEST_CASE(test_segment_geometry_counts_only_complete_segments_and_files) {
    constexpr uint64_t segment_size = 128 * 1024;
    const segment_geometry geometry(segment_size, 2 * segment_size + segment_size / 2, 8 * segment_size);
    BOOST_REQUIRE_EQUAL(geometry.segments_per_file(), 2);
    BOOST_REQUIRE_EQUAL(geometry.file_count(), 3);
    BOOST_REQUIRE_EQUAL(geometry.segment_count(), 6);

    const auto location = geometry.location_of(log_segment_id(2));
    BOOST_REQUIRE_EQUAL(location.file_id, 1);
    BOOST_REQUIRE_EQUAL(location.file_offset, 0);
}

// Checks that a configuration with an unaligned segment size, without one complete segment in a file,
// or without one complete file in the disk size, is refused, and that one segment in one file is
// accepted.
BOOST_AUTO_TEST_CASE(test_segment_geometry_refuses_an_invalid_configuration) {
    constexpr uint64_t segment_size = 128 * 1024;
    BOOST_REQUIRE_THROW(segment_geometry(0, segment_size, segment_size), exceptions::configuration_exception);
    BOOST_REQUIRE_THROW(segment_geometry(segment_size, 0, segment_size), exceptions::configuration_exception);
    BOOST_REQUIRE_THROW(segment_geometry(segment_size + 1024, 2 * segment_size, 2 * segment_size), exceptions::configuration_exception);
    BOOST_REQUIRE_THROW(segment_geometry(2 * segment_size, segment_size, 2 * segment_size), exceptions::configuration_exception);
    BOOST_REQUIRE_THROW(segment_geometry(segment_size, 2 * segment_size, segment_size), exceptions::configuration_exception);

    const segment_geometry geometry(segment_size, segment_size, segment_size);
    BOOST_REQUIRE_EQUAL(geometry.segment_count(), 1);
}

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

namespace {

constexpr uint64_t block = ondisk::block_alignment;
constexpr uint64_t test_segment_size = 16 * block;

// One allocated file of a file manager, and the segments of the test in it. It stops the segments
// before the file manager, which closes the file.
class segment_test_env {
    tmpdir _dir;
    file_manager _files;
    seastar::file _file;
    std::vector<lw_shared_ptr<segment>> _segments;

public:
    segment_test_env()
        : _files(make_config(_dir, 1, false))
    {
        // The destructor does not run when the constructor throws.
        auto stop_files = defer([this] noexcept { _files.stop().get(); });
        recover(_files);
        _file = _files.get_file_for_new_segment(0).get();
        stop_files.cancel();
    }

    ~segment_test_env() {
        for (auto& seg : _segments) {
            seg->stop().get();
        }
        _files.stop().get();
    }

    seastar::file& file() noexcept {
        return _file;
    }

    // Makes and starts segment `index` of the file. The segment writes through `file`, if given.
    segment& make_segment(uint32_t index, std::optional<seastar::file> file = std::nullopt) {
        auto seg = make_lw_shared<segment>(log_segment_id{index}, file.value_or(_file), index * test_segment_size, test_segment_size);
        seg->start(segment_ref(), segment_sequence{index});
        _segments.push_back(seg);
        return *seg;
    }

    // True if all the bytes of the file in [offset, offset + size) have this value.
    bool file_holds(uint64_t offset, uint64_t size, char value) {
        auto buf = _file.dma_read_bulk<char>(offset, size).get();
        return buf.size() == size && std::all_of(buf.get(), buf.get() + size, [value] (char c) { return c == value; });
    }
};

// Data that a direct write takes: aligned memory, filled with one value.
class test_data {
    temporary_buffer<char> _buf;

public:
    test_data(size_t size, char value)
        : _buf(temporary_buffer<char>::aligned(block, size))
    {
        std::memset(_buf.get_write(), value, size);
    }

    bytes_view view() const noexcept {
        return bytes_view(reinterpret_cast<const int8_t*>(_buf.get()), _buf.size());
    }
};

// A file that writes at most `max_write` bytes in each write, as a device can. Its disk write
// alignment is smaller than its memory alignment, and it refuses a write that is not aligned to them,
// as a device does. A direct write must have an aligned size, thus the file writes the block aligned
// part of the bytes it reports.
class short_write_file_impl : public checked_file_impl {
    size_t _max_write;

public:
    short_write_file_impl(const io_error_handler& error_handler, seastar::file file, size_t max_write)
        : checked_file_impl(error_handler, std::move(file))
        , _max_write(max_write)
    {
        _disk_write_dma_alignment = 512;
        _memory_dma_alignment = block;
    }

    future<size_t> write_dma(uint64_t pos, const void* buffer, size_t len, io_intent* intent) override {
        if (pos % _disk_write_dma_alignment != 0 || reinterpret_cast<uintptr_t>(buffer) % _memory_dma_alignment != 0) {
            co_await coroutine::return_exception(std::system_error(EINVAL, std::system_category(), "misaligned direct write"));
        }
        const auto reported = std::min(len, _max_write);
        const auto written = align_down(reported, size_t(block));
        if (written > 0) {
            co_await checked_file_impl::write_dma(pos, buffer, written, intent);
        }
        co_return reported;
    }
};

const io_error_handler rethrow_error_handler = [] (std::exception_ptr ep) {
    std::rethrow_exception(ep);
};

} // anonymous namespace

// Checks that each append goes to the next offset of the segment, and that the segment writes only
// its own region of the file.
SEASTAR_THREAD_TEST_CASE(test_segment_appends_to_its_region_of_the_file) {
    segment_test_env env;
    auto& seg0 = env.make_segment(0);
    auto& seg1 = env.make_segment(1);

    auto pos = seg0.append(test_data(block, 'a').view()).get();
    BOOST_REQUIRE_EQUAL(pos.segment.value, 0);
    BOOST_REQUIRE_EQUAL(pos.offset, 0);
    pos = seg1.append(test_data(block, 'c').view()).get();
    BOOST_REQUIRE_EQUAL(pos.segment.value, 1);
    BOOST_REQUIRE_EQUAL(pos.offset, 0);
    pos = seg0.append(test_data(2 * block, 'b').view()).get();
    BOOST_REQUIRE_EQUAL(pos.offset, block);

    BOOST_REQUIRE(env.file_holds(0, block, 'a'));
    BOOST_REQUIRE(env.file_holds(block, 2 * block, 'b'));
    BOOST_REQUIRE(env.file_holds(3 * block, test_segment_size - 3 * block, 0));
    BOOST_REQUIRE(env.file_holds(test_segment_size, block, 'c'));
    BOOST_REQUIRE(env.file_holds(test_segment_size + block, test_segment_size - block, 0));
}

// Checks that an append that does not fit is refused, writes nothing and does not move the append
// offset, and that an append of the exact remaining size fits.
SEASTAR_THREAD_TEST_CASE(test_segment_refuses_an_append_that_does_not_fit) {
    segment_test_env env;
    auto& seg = env.make_segment(0);

    seg.append(test_data(test_segment_size - 2 * block, 'a').view()).get();
    BOOST_REQUIRE(!seg.reserve(3 * block).get());
    BOOST_REQUIRE_THROW(seg.append(test_data(3 * block, 'b').view()).get(), std::runtime_error);
    BOOST_REQUIRE(env.file_holds(test_segment_size - 2 * block, 2 * block, 0));

    auto pos = seg.append(test_data(2 * block, 'c').view()).get();
    BOOST_REQUIRE_EQUAL(pos.offset, test_segment_size - 2 * block);
    BOOST_REQUIRE(env.file_holds(test_segment_size - 2 * block, 2 * block, 'c'));
    BOOST_REQUIRE(!seg.reserve(block).get());
}

// Checks that a reservation waits until the write of the reservation before it completes, and that
// it gets the offset after that reservation.
SEASTAR_THREAD_TEST_CASE(test_segment_writes_one_reservation_at_a_time) {
    segment_test_env env;
    auto& seg = env.make_segment(0);

    auto first = seg.reserve(2 * block).get();
    BOOST_REQUIRE(first);
    auto second_reserved = seg.reserve(block);
    BOOST_REQUIRE(!second_reserved.available());

    auto pos = seg.write_reserved(std::move(*first), test_data(2 * block, 'a').view()).get();
    BOOST_REQUIRE_EQUAL(pos.offset, 0);
    auto second = second_reserved.get();
    BOOST_REQUIRE(second);
    BOOST_REQUIRE_EQUAL(second->pos.offset, 2 * block);
    seg.write_reserved(std::move(*second), test_data(block, 'b').view()).get();

    BOOST_REQUIRE(env.file_holds(0, 2 * block, 'a'));
    BOOST_REQUIRE(env.file_holds(2 * block, block, 'b'));
}

// Checks that a reservation that is dropped without its write gives its region back, also to a
// reservation that waits for it.
SEASTAR_THREAD_TEST_CASE(test_segment_gives_back_a_dropped_reservation) {
    segment_test_env env;
    auto& seg = env.make_segment(0);

    auto first = seg.reserve(2 * block).get();
    BOOST_REQUIRE(first);
    auto second_reserved = seg.reserve(block);
    BOOST_REQUIRE(!second_reserved.available());

    first.reset();
    auto second = second_reserved.get();
    BOOST_REQUIRE(second);
    BOOST_REQUIRE_EQUAL(second->pos.offset, 0);
    second.reset();

    auto pos = seg.append(test_data(test_segment_size, 'a').view()).get();
    BOOST_REQUIRE_EQUAL(pos.offset, 0);
    BOOST_REQUIRE(env.file_holds(0, test_segment_size, 'a'));
}

// Checks that a failed write fails the segment: a reservation that waits for the failed write, and
// each later reservation and append, write nothing.
SEASTAR_THREAD_TEST_CASE(test_segment_fails_after_a_failed_write) {
    if constexpr (!std::is_same_v<utils::error_injection_type, utils::error_injection<true>>) {
        return;
    }

    segment_test_env env;
    auto& seg = env.make_segment(0);

    auto first = seg.reserve(block).get();
    BOOST_REQUIRE(first);
    auto second_reserved = seg.reserve(block);
    BOOST_REQUIRE(!second_reserved.available());

    utils::get_local_injector().enable("logstor_fail_segment_write", true /* one shot */);
    auto disable_injection = defer([] noexcept {
        utils::get_local_injector().disable("logstor_fail_segment_write");
    });
    BOOST_REQUIRE_THROW(seg.write_reserved(std::move(*first), test_data(block, 'a').view()).get(), std::runtime_error);

    BOOST_REQUIRE(!second_reserved.get());
    BOOST_REQUIRE(!seg.reserve(block).get());
    BOOST_REQUIRE_THROW(seg.append(test_data(block, 'b').view()).get(), std::runtime_error);
    BOOST_REQUIRE(env.file_holds(0, test_segment_size, 0));
}

// Checks that stop() waits for the writers that hold the segment, that a second stop() returns at
// once, and that no writer can hold the segment after stop().
SEASTAR_THREAD_TEST_CASE(test_segment_stop_waits_for_the_writers) {
    segment_test_env env;
    auto& seg = env.make_segment(0);

    std::optional<seastar::gate::holder> writer = seg.hold();
    auto stopped = seg.stop();
    BOOST_REQUIRE(!stopped.available());
    BOOST_REQUIRE_THROW(seg.hold(), seastar::gate_closed_exception);

    writer.reset();
    stopped.get();
    seg.stop().get();
}

// Checks that a short write goes on from the last block boundary that it wrote, which is aligned in
// the file and in memory, and that a write that cannot write one block is a disk error and fails the
// segment.
SEASTAR_THREAD_TEST_CASE(test_segment_continues_after_a_short_write) {
    segment_test_env env;
    auto short_write_file = [&env] (size_t max_write) {
        return seastar::file(seastar::make_shared<short_write_file_impl>(rethrow_error_handler, env.file(), max_write));
    };

    // Each write ends after a disk write alignment boundary that is not a block boundary.
    auto& seg0 = env.make_segment(0, short_write_file(3 * block + 512 + 100));
    seg0.append(test_data(test_segment_size, 'a').view()).get();
    BOOST_REQUIRE(env.file_holds(0, test_segment_size, 'a'));

    unsigned signalled = 0;
    boost::signals2::scoped_connection conn = logstor_error.connect([&signalled] { ++signalled; });
    auto& seg1 = env.make_segment(1, short_write_file(100));
    BOOST_REQUIRE_THROW(seg1.append(test_data(block, 'b').view()).get(), storage_io_error);
    BOOST_REQUIRE_EQUAL(signalled, 1u);
    BOOST_REQUIRE(!seg1.reserve(block).get());
    BOOST_REQUIRE(env.file_holds(test_segment_size, test_segment_size, 0));
}

// Checks that each misuse of a segment is an internal error: a reservation that is empty or not
// block aligned, a write of a reservation of another segment, and a write of another size than its
// reservation. The last also fails the segment.
SEASTAR_THREAD_TEST_CASE(test_segment_misuse_is_an_internal_error) {
    const auto do_abort = set_abort_on_internal_error(false);
    auto reset_abort = defer([do_abort] noexcept {
        set_abort_on_internal_error(do_abort);
    });

    segment_test_env env;
    auto& seg = env.make_segment(0);

    BOOST_REQUIRE_THROW(seg.reserve(0).get(), std::runtime_error);
    BOOST_REQUIRE_THROW(seg.reserve(block + 1).get(), std::runtime_error);
    BOOST_REQUIRE_THROW(seg.append(test_data(block + 1, 'a').view()).get(), std::runtime_error);

    auto& other = env.make_segment(1);
    auto foreign = other.reserve(block).get();
    BOOST_REQUIRE(foreign);
    BOOST_REQUIRE_THROW(seg.write_reserved(std::move(*foreign), test_data(block, 'c').view()).get(), std::runtime_error);

    auto reservation = seg.reserve(2 * block).get();
    BOOST_REQUIRE(reservation);
    BOOST_REQUIRE_THROW(seg.write_reserved(std::move(*reservation), test_data(block, 'b').view()).get(), std::runtime_error);
    BOOST_REQUIRE(!seg.reserve(block).get());
    BOOST_REQUIRE(env.file_holds(0, test_segment_size, 0));
}
