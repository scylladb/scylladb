/*
 * Copyright (C) 2016 ScyllaDB
 */



#include <boost/test/unit_test.hpp>
#include <stdint.h>
#include <random>

#include <seastar/core/future-util.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/fstream.hh>

#include <seastar/testing/test_case.hh>

#include "ent/encryption/encryption.hh"
#include "ent/encryption/symmetric_key.hh"
#include "ent/encryption/encrypted_file_impl.hh"
#include "test/lib/log.hh"
#include "test/lib/tmpdir.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/exception_utils.hh"

using namespace encryption;

static tmpdir dir;

static future<std::tuple<file, ::shared_ptr<symmetric_key>>> make_file(const sstring& name, open_flags mode, ::shared_ptr<symmetric_key> k = nullptr) {
    file f = co_await open_file_dma(sstring(dir.path() / std::string(name)), mode);
    if (k == nullptr) {
        key_info info{"AES/CBC", 256};
        k = ::make_shared<symmetric_key>(info);
    }
    co_return std::tuple(file(make_encrypted_file(f, k)), k);
}

static temporary_buffer<uint8_t> generate_random(size_t n, size_t align) {
    auto tmp = temporary_buffer<uint8_t>::aligned(align, align_up(n, align));
    auto data = tests::random::get_sstring(n);
    std::copy(data.begin(), data.end(), tmp.get_write());
    return tmp;
}

static future<> test_random_data_disk(size_t n) {
    auto name = "test_rand_" + std::to_string(n);
    auto t = co_await make_file(name, open_flags::rw|open_flags::create);
    auto f = std::get<0>(t);
    std::exception_ptr ex = nullptr;

    try {
        auto k = std::get<1>(t);
        auto a = f.memory_dma_alignment();
        auto buf = generate_random(n, a);
        auto w = co_await f.dma_write(0, buf.get(), buf.size());

        co_await f.flush();
        if (n != buf.size()) {
            co_await f.truncate(n);
        }

        BOOST_REQUIRE_EQUAL(w, buf.size());

        auto k2 = ::make_shared<symmetric_key>(k->info(), k->key());
        auto f2 = std::get<0>(co_await make_file(name, open_flags::ro, k2));

        auto tmp = temporary_buffer<uint8_t>::aligned(a, buf.size());
        auto n2 = co_await f2.dma_read(0, tmp.get_write(), tmp.size());

        BOOST_REQUIRE_EQUAL(n2, n);
        BOOST_REQUIRE_EQUAL_COLLECTIONS(tmp.get(), tmp.get() + n2, buf.get(), buf.get() + n2);
    } catch (...) {
        ex = std::current_exception();
    }

    co_await f.close();
    if (ex) {
        std::rethrow_exception(ex);
    }
}

static void test_random_data(size_t n) {
    auto buf = generate_random(n, 8);


    // first, verify padded.
    {
        key_info info{"AES/CBC/PKCSPadding", 256};
        auto k = ::make_shared<symmetric_key>(info);

        bytes b(bytes::initialized_later(), k->iv_len());
        k->generate_iv(b.data(), k->iv_len());

        temporary_buffer<uint8_t> tmp(n + k->block_size());
        k->encrypt(buf.get(), buf.size(), tmp.get_write(), tmp.size(), b.data());

        auto bytes = k->key();
        auto k2 = ::make_shared<symmetric_key>(info, bytes);

        temporary_buffer<uint8_t> tmp2(n + k->block_size());
        k2->decrypt(tmp.get(), tmp.size(), tmp2.get_write(), tmp2.size(), b.data());

        BOOST_REQUIRE_EQUAL_COLLECTIONS(tmp2.get(), tmp2.get() + n, buf.get(), buf.get() + n);
    }

    // unpadded
    {
        key_info info{"AES/CBC", 256};
        auto k = ::make_shared<symmetric_key>(info);

        bytes b(bytes::initialized_later(), k->iv_len());
        k->generate_iv(b.data(), k->iv_len());

        temporary_buffer<uint8_t> tmp(n);
        k->encrypt_unpadded(buf.get(), buf.size(), tmp.get_write(), b.data());

        auto bytes = k->key();
        auto k2 = ::make_shared<symmetric_key>(info, bytes);

        temporary_buffer<uint8_t> tmp2(buf.size());
        k2->decrypt_unpadded(tmp.get(), tmp.size(), tmp2.get_write(), b.data());

        BOOST_REQUIRE_EQUAL_COLLECTIONS(tmp2.get(), tmp2.get() + n, buf.get(), buf.get() + n);
    }
}


BOOST_AUTO_TEST_CASE(test_encrypting_data_128) {
    test_random_data(128);
}

BOOST_AUTO_TEST_CASE(test_encrypting_data_4k) {
    test_random_data(4*1024);
}


SEASTAR_TEST_CASE(test_encrypted_file_data_4k) {
    return test_random_data_disk(4*1024);
}

SEASTAR_TEST_CASE(test_encrypted_file_data_16k) {
    return test_random_data_disk(16*1024);
}

SEASTAR_TEST_CASE(test_encrypted_file_data_unaligned) {
    return test_random_data_disk(16*1024 - 3);
}

SEASTAR_TEST_CASE(test_encrypted_file_data_unaligned2) {
    return test_random_data_disk(16*1024 - 4092);
}

SEASTAR_TEST_CASE(test_short) {
    auto name = "test_short";
    file f = co_await open_file_dma(sstring(dir.path() / name), open_flags::rw|open_flags::create);
    co_await f.truncate(1);
    co_await f.close();

    auto t = co_await make_file(name, open_flags::ro);
    f = std::get<0>(t);
    std::exception_ptr ex = nullptr;

    try {
        temporary_buffer<char> buf(f.memory_dma_alignment());

        BOOST_REQUIRE_EXCEPTION(
            co_await f.dma_read(0, buf.get_write(), buf.size()),
            std::domain_error,
            exception_predicate::message_contains("file size 1, expected 0 or at least 16")
        );
    } catch (...) {
        ex = std::current_exception();
    }

    co_await f.close();
    if (ex) {
        std::rethrow_exception(ex);
    }
}

SEASTAR_TEST_CASE(test_read_across_size_boundary) {
    auto name = "test_read_across_size_boundary";

    auto [dst, k] = co_await make_file(name, open_flags::rw|open_flags::create);
    auto size = dst.disk_write_dma_alignment() - 1;
    co_await dst.truncate(size);
    co_await dst.close();

    auto [f, _] = co_await make_file(name, open_flags::ro, k);
    auto a = f.disk_write_dma_alignment();
    auto m = f.memory_dma_alignment();

    auto buf = temporary_buffer<char>::aligned(m, a);
    auto n = co_await f.dma_read(0, buf.get_write(), buf.size());

    auto buf2 = temporary_buffer<char>::aligned(m, a);
    auto n2 = co_await f.dma_read(a, buf2.get_write(), buf2.size());

    auto buf3 = temporary_buffer<char>::aligned(m, a);
    std::vector<iovec> iov({{buf3.get_write(), buf3.size()}});
    auto n3 = co_await f.dma_read(a, std::move(iov));

    auto buf4 = co_await f.dma_read_bulk<char>(a, size_t(a));

    co_await f.close();

    BOOST_REQUIRE_EQUAL(size, n);
    buf.trim(n);
    for (auto c : buf) {
        BOOST_REQUIRE_EQUAL(c, 0);
    }

    BOOST_REQUIRE_EQUAL(0, n2);
    BOOST_REQUIRE_EQUAL(0, n3);
    BOOST_REQUIRE_EQUAL(0, buf4.size());
}

static future<> test_read_across_size_boundary_unaligned_helper(int64_t size_off, int64_t read_off) {
    auto name = "test_read_across_size_boundary_unaligned";
    auto [dst, k] = co_await make_file(name, open_flags::rw|open_flags::create);
    auto size = dst.disk_write_dma_alignment() + size_off;
    co_await dst.truncate(size);
    co_await dst.close();

    auto [f, k2] = co_await make_file(name, open_flags::ro, k);
    auto buf = co_await f.dma_read_bulk<char>(f.disk_write_dma_alignment() + read_off, size_t(f.disk_write_dma_alignment()));

    co_await f.close();

    BOOST_REQUIRE_EQUAL(0, buf.size());
}

SEASTAR_TEST_CASE(test_read_across_size_boundary_unaligned) {
    co_await test_read_across_size_boundary_unaligned_helper(-1, 1);
}

SEASTAR_TEST_CASE(test_read_across_size_boundary_unaligned2) {
    co_await test_read_across_size_boundary_unaligned_helper(-2, -1);
}

SEASTAR_TEST_CASE(test_truncating_empty) {
    auto name = "test_truncating_empty";
    auto t = co_await make_file(name, open_flags::rw|open_flags::create);
    auto f = std::get<0>(t);
    auto k = std::get<1>(t);
    auto s = 64 * f.memory_dma_alignment();

    co_await f.truncate(s);

    temporary_buffer<char> buf(s);
    auto n = co_await f.dma_read(0, buf.get_write(), buf.size());

    co_await f.close();

    BOOST_REQUIRE_EQUAL(s, n);

    for (auto c : buf) {
        BOOST_REQUIRE_EQUAL(c, 0);
    }
}

SEASTAR_TEST_CASE(test_truncating_extend) {
    auto name = "test_truncating_extend";
    auto t = co_await make_file(name, open_flags::rw|open_flags::create);
    auto f = std::get<0>(t);
    auto k = std::get<1>(t);
    auto a = f.memory_dma_alignment();
    auto s = 32 * a;
    auto buf = generate_random(s, a);
    auto w = co_await f.dma_write(0, buf.get(), buf.size());

    co_await f.flush();
    BOOST_REQUIRE_EQUAL(s, w);

    for (size_t i = 1; i < 64; ++i) {
        // truncate smaller, unaligned
        auto l = w - i;
        auto r = w + 8 * a;
        co_await f.truncate(l);
        BOOST_REQUIRE_EQUAL(l, (co_await f.stat()).st_size);

        {
            auto tmp = temporary_buffer<uint8_t>::aligned(a, align_up(l, a));
            auto n = co_await f.dma_read(0, tmp.get_write(), tmp.size());

            BOOST_REQUIRE_EQUAL(l, n);
            BOOST_REQUIRE_EQUAL_COLLECTIONS(tmp.get(), tmp.get() + l, buf.get(), buf.get() + l);

            auto k = align_down(l, a);

            while (k > 0) {
                n = co_await f.dma_read(0, tmp.get_write(), k);

                BOOST_REQUIRE_EQUAL(k, n);
                BOOST_REQUIRE_EQUAL_COLLECTIONS(tmp.get(), tmp.get() + k, buf.get(), buf.get() + k);

                n = co_await f.dma_read(k, tmp.get_write(), tmp.size());
                BOOST_REQUIRE_EQUAL(l - k, n);
                BOOST_REQUIRE_EQUAL_COLLECTIONS(tmp.get(), tmp.get() + n, buf.get() + k, buf.get() + k + n);

                k -= a;
            }
        }

        co_await f.truncate(r);
        BOOST_REQUIRE_EQUAL(r, (co_await f.stat()).st_size);

        auto tmp = temporary_buffer<uint8_t>::aligned(a, align_up(r, a));
        auto n = co_await f.dma_read(0, tmp.get_write(), tmp.size());

        BOOST_REQUIRE_EQUAL(r, n);
        BOOST_REQUIRE_EQUAL_COLLECTIONS(tmp.get(), tmp.get() + l, buf.get(), buf.get() + l);

        while (l < r) {
            BOOST_REQUIRE_EQUAL(tmp[l], 0);
            ++l;
        }
    }

    co_await f.close();
}

// Reproducer for https://github.com/scylladb/scylladb/issues/22236
SEASTAR_TEST_CASE(test_read_from_padding) {
    key_info kinfo {"AES/CBC/PKCSPadding", 128};
    shared_ptr<symmetric_key> k = make_shared<symmetric_key>(kinfo);
    testlog.info("Created symmetric key: info={} key={} ", k->info(), k->key());

    size_t block_size;
    size_t buf_size;

    constexpr auto& filename = "encrypted_file";
    const auto& filepath = dir.path() / filename;

    testlog.info("Creating encrypted file {}", filepath.string());
    {
        auto [file, _] = co_await make_file(filename, open_flags::create | open_flags::wo, k);
        auto ostream = co_await make_file_output_stream(file);

        block_size = file.disk_write_dma_alignment();
        buf_size = block_size - 1;

        auto wbuf = seastar::temporary_buffer<char>::aligned(file.memory_dma_alignment(), buf_size);
        co_await ostream.write(wbuf.get(), wbuf.size());
        testlog.info("Wrote {} bytes to encrypted file {}", wbuf.size(), filepath.string());

        co_await ostream.close();
        testlog.info("Length of {}: {} bytes", filename, co_await file.size());
    }

    testlog.info("Testing DMA reads from padding area of file {}", filepath.string());
    {
        auto [file, _] = co_await make_file(filename, open_flags::ro, k);

        // Triggering the bug requires reading from the padding area:
        // `buf_size < read_pos < file.size()`
        //
        // For `dma_read()`, we have the additional requirement that `read_pos` must be aligned.
        // For `dma_read_bulk()`, it doesn't have to.
        uint64_t read_pos = block_size;
        size_t read_len = block_size;
        auto rbuf = seastar::temporary_buffer<char>::aligned(file.memory_dma_alignment(), read_len);
        std::vector<iovec> iov {{static_cast<void*>(rbuf.get_write()), rbuf.size()}};

        auto res = co_await file.dma_read_bulk<char>(read_pos, read_len);
        BOOST_CHECK_MESSAGE(res.size() == 0, seastar::format(
                "Bulk DMA read on pos {}, len {}: returned {} bytes instead of zero", read_pos, read_len, res.size()));

        auto res_len = co_await file.dma_read(read_pos, iov);
        BOOST_CHECK_MESSAGE(res_len == 0, seastar::format(
                "IOV DMA read on pos {}, len {}: returned {} bytes instead of zero", read_pos, read_len, res_len));

        res_len = co_await file.dma_read<char>(read_pos, rbuf.get_write(), read_len);
        BOOST_CHECK_MESSAGE(res_len == 0, seastar::format(
                "DMA read on pos {}, len {}: returned {} bytes instead of zero", read_pos, read_len, res_len));

        co_await file.close();
    }
}
