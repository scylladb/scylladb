/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <chrono>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <fmt/format.h>
#include <sys/statvfs.h>

#include <seastar/core/coroutine.hh>
#include <seastar/core/manual_clock.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/coroutine/exception.hh>
#include <seastar/util/closeable.hh>
#include <seastar/util/later.hh>
#include <seastar/util/short_streams.hh>

#include "test/lib/log.hh"
#include "test/lib/scylla_test_case.hh"
#include "test/lib/tmpdir.hh"

#include "utils/disk_space_monitor.hh"
#include "utils/UUID_gen.hh"

static future<> write_file(const fs::path& file_name, uint64_t length) {
    std::exception_ptr ex;
    auto buf = temporary_buffer<char>(131072);
    std::memset(buf.get_write(), 0, buf.size());
    auto f = co_await open_file_dma(file_name.native(), open_flags::create | open_flags::truncate | open_flags::rw);
    auto os = co_await make_file_output_stream(std::move(f), 131072);
    try {
        size_t sz = 0;
        for (size_t pos = 0; pos < length; pos += sz) {
            sz = std::min(buf.size(), length - pos);
            co_await os.write(buf.get(), sz);
        }
        co_await os.flush();
    } catch (...) {
        ex = std::current_exception();
    }
    co_await os.close();
    if (ex) {
        co_await coroutine::return_exception_ptr(std::move(ex));
    }
}

static future<sstring> read_file_contiguous(const fs::path& path) {
    auto f = co_await open_file_dma(path.native(), open_flags::ro);
    sstring res = co_await with_closeable(make_file_input_stream(f), [] (input_stream<char>& in) {
        return util::read_entire_stream_contiguous(in);
    });
    // Strip trailing white space
    while (!res.empty() && std::isspace(res[res.size() - 1])) {
        res.resize(res.size() - 1);
    }
    co_return res;
}

static future<> remove_file_nothrow(const fs::path& path) {
    try {
        co_await remove_file(path.native());
    } catch (...) {
        testlog.error("Could not remove {}: {}", path.native(), std::current_exception());
    }
}

// return cmd standard output on success,
// or throws an error containing the cmd standard error.
static future<sstring> run_system_command(tmpdir& dir, sstring cmd) {
    fs::path out_path = dir.path() / fmt::to_string(utils::make_random_uuid());
    fs::path err_path = dir.path() / fmt::to_string(utils::make_random_uuid());
    sstring err;
    sstring out;
    auto redirect_cmd = fmt::format("{} >{} 2>{}", cmd, out_path.native(), err_path.native());
    auto status = std::system(redirect_cmd.c_str());
    if (status) {
        err = fmt::format("{} failed: {}", cmd, read_file_contiguous(err_path).get());
    } else {
        out = read_file_contiguous(out_path).get();
        testlog.debug("{}: {}", cmd, out);
    }
    remove_file_nothrow(out_path).get();
    remove_file_nothrow(err_path).get();
    if (!err.empty()) {
        throw std::runtime_error(err);
    }
    co_return out;
}

SEASTAR_THREAD_TEST_CASE(test_disk_space_monitor_metrics) {
    using namespace std::chrono;

    tmpdir dir;

    // Prepare test filesystem
    auto image_file = dir.path() / "image";
    uint64_t image_size = 16 << 20; // Minimum of 4096 blocks
    write_file(image_file, image_size).get();

    // new mkfs.xfs does not support <300MB file systems except for its own unit tests.
    // temp workaround, add same env vars as said tests to force create this small fs.
    // See: https://lkml.kernel.org/linux-xfs/Yv2A9Ggkv%2FNBrTd4@magnolia/
    size_t block_size = 1024;
    sstring mkfs_cmd = fmt::format("TEST_DIR=1 TEST_DEV=1 QA_CHECK_FS=1 mkfs.xfs -b size={} -f '{}' -m crc=0,finobt=0", block_size, image_file.native());
    run_system_command(dir, mkfs_cmd).discard_result().get();

    // setup a new loop device
    sstring losetup_setup_cmd = fmt::format("sudo losetup --find --show '{}'", image_file.native());
    auto loop_dev = run_system_command(dir, losetup_setup_cmd).get();
    auto detach_loop_dev = defer([&] () noexcept {
        sstring losetup_detach_cmd = fmt::format("sudo losetup --detach '{}'", loop_dev);
        try {
            run_system_command(dir, losetup_detach_cmd).discard_result().get();
        } catch (...) {
            on_fatal_internal_error(testlog, fmt::format("{} failed: {}", losetup_detach_cmd, std::current_exception()));
        }
    });

    // Mount the loop device
    auto mnt_path = dir.path() / "mnt";
    touch_directory(mnt_path.native()).get();

    sstring mount_cmd = fmt::format("sudo mount -o loop -t xfs '{}' '{}'", loop_dev, mnt_path.native());
    run_system_command(dir, mount_cmd).discard_result().get();
    auto umount = defer([&] () noexcept {
        sstring umount_cmd = fmt::format("sudo umount '{}'", mnt_path.native());
        try {
            run_system_command(dir, umount_cmd).discard_result().get();
        } catch (...) {
            on_fatal_internal_error(testlog, fmt::format("{} failed: {}", umount_cmd, std::current_exception()));
        }
    });

    sstring chmod_cmd = fmt::format("sudo chmod 0777 '{}'", mnt_path.native());
    run_system_command(dir, chmod_cmd).discard_result().get();

    auto space = fs::space(mnt_path);
    testlog.debug("std::space: capacity={} free={} available={}", space.capacity, space.free, space.available);

    abort_source as;
    utils::disk_space_monitor::config dsm_config{
        .sched_group = default_scheduling_group(),
        .normal_polling_interval = utils::updateable_value<int>(1),
        .high_polling_interval = utils::updateable_value<int>(1),
        .polling_interval_threshold = utils::updateable_value<float>(0.5),
    };
    utils::disk_space_monitor dsm(as, mnt_path, dsm_config);
    dsm.start().get();
    auto stop_dsm = deferred_stop(dsm);

    auto dsm_space = dsm.space();
    testlog.debug("disk_space_monitor initial space: capacity={} free={} available={} utilization={}", dsm_space.capacity, dsm_space.free, dsm_space.available, dsm.disk_utilization());

    BOOST_REQUIRE_EQUAL(dsm_space.capacity, space.capacity);
    BOOST_REQUIRE_EQUAL(dsm_space.free, space.free);
    BOOST_REQUIRE_EQUAL(dsm_space.available, space.available);

    auto foo_name = mnt_path / "foo";
    write_file(foo_name, image_size * 3 / 4).get();

    sstring syncfs_cmd = fmt::format("sudo sync -f '{}'", foo_name.native());
    run_system_command(dir, syncfs_cmd).discard_result().get();

    auto st = file_stat(foo_name.native()).get();
    testlog.debug("foo: allocated_size={} block_size={} blocks={}", st.allocated_size, st.block_size, st.allocated_size / st.block_size);

    std::filesystem::space_info sampled_space;
    float sampled_disk_utilization = 0;
    auto sub = dsm.listen([&] (const utils::disk_space_monitor& dsm) -> future<> {
        sampled_disk_utilization = dsm.disk_utilization();
        sampled_space = dsm.space();
        testlog.debug("disk_space_monitor with file: capacity={} free={} available={} utilization={}", sampled_space.capacity, sampled_space.free, sampled_space.available, sampled_disk_utilization);
        return make_ready_future();
    });

    auto start_time = lowres_clock::now();
    auto deadline = start_time + 30s;
    while (lowres_clock::now() < deadline && sampled_disk_utilization <= 0.5) {
        sleep(1s).get();
    }

    BOOST_REQUIRE_GT(sampled_disk_utilization, 0.5);
    BOOST_REQUIRE_EQUAL(sampled_space.available + st.allocated_size, dsm_space.available);
}
