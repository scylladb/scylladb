/*
 * Copyright (C) 2016 ScyllaDB
 */



#include <boost/range/irange.hpp>
#include <boost/range/adaptors.hpp>
#include <boost/range/algorithm.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/lexical_cast.hpp>
#include <stdint.h>
#include <random>

#include <seastar/core/future-util.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/align.hh>

#include <seastar/testing/test_case.hh>

#include "ent/encryption/encryption.hh"
#include "ent/encryption/symmetric_key.hh"

using namespace encryption;

static temporary_buffer<uint8_t> generate_random(size_t n, size_t align) {
    std::random_device r;
    std::default_random_engine e1(r());
    std::uniform_int_distribution<uint8_t> dist('0', 'z');

    auto tmp = temporary_buffer<uint8_t>::aligned(align, align_up(n, align));
    std::generate(tmp.get_write(), tmp.get_write() + tmp.size(), std::bind(dist, std::ref(e1)));
    return tmp;
}

static void test_random_data(const sstring& desc, unsigned int bits) {
    auto buf = generate_random(128, 8);
    auto n = buf.size();

    // first, verify padded.
    {
        key_info info{desc, bits};
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
        auto desc2 = desc;
        desc2.resize(desc.find_last_of('/'));
        key_info info{desc2, bits};
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


SEASTAR_TEST_CASE(test_cipher_types) {
    static const std::unordered_map<sstring, std::vector<unsigned int>> ciphers = {
            { "AES/CBC/PKCS5Padding", { 128, 192, 256 } },
            { "AES/ECB/PKCS5Padding", { 128, 192, 256 } },
            { "DES/CBC/PKCS5Padding", { 56 } },
            { "DESede/CBC/PKCS5Padding", { 112, 168 } },
            { "Blowfish/CBC/PKCS5Padding", { 32, 64, 448 } },
            { "RC2/CBC/PKCS5Padding", { 40, 41, 64, 67, 120, 128 } },
    };

    for (auto & p : ciphers) {
        for (auto s : p.second) {
            test_random_data(p.first, s);
        }
    }
    return make_ready_future<>();
}

// OpenSSL only supports one form of padding. We used to just allow
// non-empty string -> pkcs5/pcks7. We now instead verify this to be 
// within the "sane" limits, i.e. pkcs, pkcs5 or pkcs7. 
// Check an non-exhaustive set of invalid padding options and verify 
// we get an exception as expected.
// See below for test for valid strings.
SEASTAR_TEST_CASE(test_invalid_padding_options) {
    static const std::unordered_map<sstring, unsigned int> ciphers = {
            { "AES/CBC/PKCSU", 128 },
            { "AES/ECB/Gris", 128 },
            { "DES/CBC/PKCS12Padding", 56 },
            { "DES/CBC/KorvPadding", 56 },
            { "DES/CBC/MUPadding", 56 },
    };
    for (auto& p : ciphers) {
        try {
            key_info info{p.first, p.second};
            symmetric_key k(info);
            BOOST_ERROR("should not reach");
        } catch (...) {
            // ok.
        }
    }
    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_valid_padding_options) {
    static const std::unordered_map<sstring, unsigned int> ciphers = {
            { "AES/CBC/PKCS", 128 },
            { "AES/CBC/PKCSPadding", 128 },
            { "AES/ECB/PKCS7Padding", 128 },
            { "AES/ECB/PKCS7", 128 },
            { "DES/CBC/PKCS5Padding", 56 },
            { "DES/CBC/PKCS5", 56 },
            { "AES/CBC/NoPadding", 128 },
            { "AES/ECB/NoPadding", 128 },
            { "DES/CBC/NoPadding", 56 },
            { "AES/CBC/No", 128 },
            { "AES/ECB/No", 128 },
            { "DES/CBC/No", 56 },
    };
    for (auto& p : ciphers) {
        key_info info{p.first, p.second};
        symmetric_key k(info);

        auto errors = k.validate_exact_info_result();
        BOOST_REQUIRE_EQUAL(errors, std::string{});
    }
    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_warn_adjusted_options) {
    static const std::unordered_map<sstring, std::vector<unsigned int>> ciphers = {
            // blowfish only supports CBC and will become CBC whatever you say
            { "Blowfish/CFB/PKCS5Padding", { 32, 64, 448 } },
            { "Blowfish/XTS/PKCS5Padding", { 32, 64, 448 } },
    };
    for (auto& p : ciphers) {
        for (auto s : p.second) {
            auto alg = p.first;
            key_info info{alg, s};
            symmetric_key k(info);

            auto errors = k.validate_exact_info_result();
            BOOST_REQUIRE_NE(errors, std::string{});
        }
    }
    return make_ready_future<>();
}

/**
 * Verifies that when using defaults in a key, the key info returned is still
 * equal to the input one (by bit and textually)
 */
SEASTAR_TEST_CASE(test_cipher_defaults) {
    static const std::unordered_map<sstring, std::vector<unsigned int>> ciphers = {
            { "AES/CBC/PKCS5Padding", { 128, 192, 256 } },
            { "AES/ECB/PKCS5Padding", { 128, 192, 256 } },
            { "DES/CBC/PKCS5Padding", { 56 } },
            { "DESede/CBC/PKCS5Padding", { 112, 168 } },
            { "Blowfish/CBC/PKCS5Padding", { 32, 64, 448 } },
            { "RC2/CBC/PKCS5Padding", { 40, 41, 64, 67, 120, 128 } },
    };

    for (auto& p : ciphers) {
        for (auto s : p.second) {
            auto alg = p.first;
            for (;;) {
                key_info info{alg, s};
                symmetric_key k(info);

                BOOST_REQUIRE_EQUAL(info, k.info());
                BOOST_REQUIRE_EQUAL(boost::lexical_cast<std::string>(info), boost::lexical_cast<std::string>(k.info()));

                auto i = alg.find_last_of('/');
                if (i != sstring::npos) {
                    alg.resize(i);
                    continue;
                }
                // also verify that whatever we say (or don't say), we get a blockmode
                // -> iv len > 0
                BOOST_CHECK_GT(k.iv_len(), 0);

                auto errors = k.validate_exact_info_result();
                if (i != sstring::npos) {
                    BOOST_REQUIRE_EQUAL(errors, std::string{});
                } else {
                    // Again, if we cut out block mode (i.e. only cipher name left)
                    // we will still force a block mode. Thus this should warn.
                    BOOST_REQUIRE_NE(errors, std::string{});
                }

                break;
            }
        }
    }
    return make_ready_future<>();
}
