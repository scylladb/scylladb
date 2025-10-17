/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "auth/passwords.hh"
#include "utils/hashers.hh"   // added for sha512_hasher / cryptopp_hasher

#include <cerrno>
#include <cryptopp/config_int.h>
#include <openssl/evp.h>
#include <seastar/core/sstring.hh>
#include <seastar/util/log.hh>
#include <stdexcept>
#include <vector>
#include <cstring>
#include <algorithm>

extern "C" {
#include <crypt.h>
#include <unistd.h>
}

namespace auth::passwords {

static thread_local crypt_data tlcrypt = {};

namespace detail {

void verify_scheme(scheme scheme) {
    const sstring random_part_of_salt = "aaaabbbbccccdddd";

    const sstring salt = sstring(prefix_for_scheme(scheme)) + random_part_of_salt;
    const char* e = crypt_r("fisk", salt.c_str(), &tlcrypt);

    if (e && (e[0] != '*')) {
        return;
    }

    throw no_supported_schemes();
}

static seastar::logger logger("pass");


sstring hash_with_salt(const sstring& pass, const sstring& salt) {
    auto res = crypt_r(pass.c_str(), salt.c_str(), &tlcrypt);
    if (!res || (res[0] == '*')) {
        throw std::system_error(errno, std::system_category());
    }
    return res;
}


static const char b64t[] =
    "./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

// Added: thread_local reusable contexts for fast cloning
static thread_local EVP_MD_CTX* tl_sha512_base = nullptr;
static thread_local EVP_MD_CTX* tl_sha512_work = nullptr;

// Append 'n' chars to sstring starting at position 'pos' safely
static void b64_from_24bit(unsigned char B2, unsigned char B1, unsigned char B0,
                           int n, sstring &out, size_t &pos) {
    unsigned int w = (B2 << 16) | (B1 << 8) | B0;

    // Resize if needed to hold new characters
    if (pos + n > out.size()) {
        out.resize(pos + n);  // safe: sstring resizes dynamically
    }

    for (int i = 0; i < n; ++i) {
        out[pos++] = b64t[w & 0x3f];
        w >>= 6;
    }
}

// SHA-crypt (SHA-512) compatible with glibc using OpenSSL primitives; exact output matches crypt_r.
// Optimized: use direct SHA512_* API (no EVP alloc), reuse stack contexts, avoid std::string copies, pre-reserve output.
sstring hash_with_salt_v2(const sstring &pass, const sstring &salt_or_full_hash) {
        const sstring& full = salt_or_full_hash;

        size_t rounds = 5000;
        size_t salt_start = 3;
        if (full.compare(salt_start, 7, "rounds=") == 0) {
            size_t num_start = salt_start + 7;
            const char* p = full.data() + num_start;
            const char* endp = full.data() + full.size();
            unsigned long v = 0;
            while (p < endp && *p >= '0' && *p <= '9') { v = v * 10 + (*p - '0'); ++p; }
            rounds = v;
            salt_start = (p - full.data()) + 1;
        }
        size_t salt_end_dollar = full.find('$', salt_start);
        sstring salt = full.substr(salt_start, salt_end_dollar - salt_start);
        if (rounds < 1000) rounds = 1000;
        else if (rounds > 999999999) rounds = 999999999;

        const size_t pw_len = pass.size();
        const size_t salt_len = salt.size();

        // --- Optimized digest management using copy_ex ---
        const EVP_MD* md = EVP_sha512();
        if (!tl_sha512_base) {
            tl_sha512_base = EVP_MD_CTX_new();
            tl_sha512_work = EVP_MD_CTX_new();
            if (!tl_sha512_base || !tl_sha512_work) {
                throw std::runtime_error("EVP_MD_CTX_new failed");
            }
            if (EVP_DigestInit_ex(tl_sha512_base, md, nullptr) != 1) {
                throw std::runtime_error("EVP_DigestInit_ex (base) failed");
            }
        }
        auto digest_new = [&] {
            if (EVP_MD_CTX_copy_ex(tl_sha512_work, tl_sha512_base) != 1) {
                throw std::runtime_error("EVP_MD_CTX_copy_ex failed");
            }
        };
        auto digest_update = [&](const void* d, size_t l) {
            if (l && EVP_DigestUpdate(tl_sha512_work, d, l) != 1) {
                throw std::runtime_error("EVP_DigestUpdate failed");
            }
        };
        auto digest_final = [&](unsigned char* out) {
            unsigned int l = 0;
            if (EVP_DigestFinal_ex(tl_sha512_work, out, &l) != 1 || l != 64) {
                throw std::runtime_error("EVP_DigestFinal_ex failed");
            }
        };
        // --------------------------------------------------

        unsigned char alt[64];
        digest_new();
        digest_update(pass.data(), pw_len);
        digest_update(salt.data(), salt_len);
        digest_update(pass.data(), pw_len);
        digest_final(alt);

        unsigned char alt_result[64];
        digest_new();
        digest_update(pass.data(), pw_len);
        digest_update(salt.data(), salt_len);

        size_t cnt = pw_len;
        while (cnt >= 64) { digest_update(alt, 64); cnt -= 64; }
        if (cnt) { digest_update(alt, cnt); }

        for (size_t n = pw_len; n > 0; n >>= 1) {
            if (n & 1) digest_update(alt, 64);
            else digest_update(pass.data(), pw_len);
        }
        digest_final(alt_result);

        std::vector<unsigned char> P_bytes;
        if (pw_len) {
            P_bytes.resize(pw_len);
            unsigned char DP[64];
            digest_new();
            for (size_t i = 0; i < pw_len; ++i) {
                digest_update(pass.data(), pw_len);
            }
            digest_final(DP);
            size_t off = 0;
            while (off + 64 <= pw_len) { std::memcpy(P_bytes.data() + off, DP, 64); off += 64; }
            if (off < pw_len) { std::memcpy(P_bytes.data() + off, DP, pw_len - off); }
        }

        std::vector<unsigned char> S_bytes;
        if (salt_len) {
            S_bytes.resize(salt_len);
            unsigned char DS[64];
            digest_new();
            size_t repeat = 16 + alt_result[0];
            for (size_t i = 0; i < repeat; ++i) {
                digest_update(salt.data(), salt_len);
            }
            digest_final(DS);
            size_t off = 0;
            while (off + 64 <= salt_len) { std::memcpy(S_bytes.data() + off, DS, 64); off += 64; }
            if (off < salt_len) { std::memcpy(S_bytes.data() + off, DS, salt_len - off); }
        }

        for (size_t i = 0; i < rounds; ++i) {
            digest_new();
            if ((i & 1) == 0) {
                digest_update(alt_result, 64);
            } else if (!P_bytes.empty()) {
                digest_update(P_bytes.data(), P_bytes.size());
            }
            if (i % 3 != 0 && !S_bytes.empty()) {
                digest_update(S_bytes.data(), S_bytes.size());
            }
            if (i % 7 != 0 && !P_bytes.empty()) {
                digest_update(P_bytes.data(), P_bytes.size());
            }
            if ((i & 1) == 0) {
                if (!P_bytes.empty()) {
                    digest_update(P_bytes.data(), P_bytes.size());
                }
            } else {
                digest_update(alt_result, 64);
            }
            digest_final(alt_result);
        }

        size_t header_end = salt_end_dollar;
        sstring out;
        // sstring has no reserve(); we rely on b64_from_24bit to resize incrementally.
        out.append(full.data(), header_end + 1);

        size_t pos = out.size();
        auto enc = [&](unsigned char b2, unsigned char b1, unsigned char b0, int n) {
            b64_from_24bit(b2, b1, b0, n, out, pos);
        };
        enc(alt_result[0],  alt_result[21], alt_result[42], 4);
        enc(alt_result[22], alt_result[43], alt_result[1],  4);
        enc(alt_result[44], alt_result[2],  alt_result[23], 4);
        enc(alt_result[3],  alt_result[24], alt_result[45], 4);
        enc(alt_result[25], alt_result[46], alt_result[4],  4);
        enc(alt_result[47], alt_result[5],  alt_result[26], 4);
        enc(alt_result[6],  alt_result[27], alt_result[48], 4);
        enc(alt_result[28], alt_result[49], alt_result[7],  4);
        enc(alt_result[50], alt_result[8],  alt_result[29], 4);
        enc(alt_result[9],  alt_result[30], alt_result[51], 4);
        enc(alt_result[31], alt_result[52], alt_result[10], 4);
        enc(alt_result[53], alt_result[11], alt_result[32], 4);
        enc(alt_result[12], alt_result[33], alt_result[54], 4);
        enc(alt_result[34], alt_result[55], alt_result[13], 4);
        enc(alt_result[56], alt_result[14], alt_result[35], 4);
        enc(alt_result[15], alt_result[36], alt_result[57], 4);
        enc(alt_result[37], alt_result[58], alt_result[16], 4);
        enc(alt_result[59], alt_result[17], alt_result[38], 4);
        enc(alt_result[18], alt_result[39], alt_result[60], 4);
        enc(alt_result[40], alt_result[61], alt_result[19], 4);
        enc(alt_result[62], alt_result[20], alt_result[41], 4);
        enc(0, 0, alt_result[63], 2);

        // Removed obsolete EVP_MD_CTX_free(ctx); contexts are thread_local reused.
        return out;
}


// SHA-crypt (SHA-512) using cryptopp_hasher<sha512_hasher,64> from hashers.cc (no OpenSSL).
sstring hash_with_salt_v3(const sstring &pass, const sstring &salt_or_full_hash) {
    const sstring& full = salt_or_full_hash;
    size_t rounds = 5000;
    size_t salt_start = 3;
    if (full.compare(salt_start, 7, "rounds=") == 0) {
        size_t num_start = salt_start + 7;
        const char* p = full.data() + num_start;
        const char* endp = full.data() + full.size();
        unsigned long v = 0;
        while (p < endp && *p >= '0' && *p <= '9') {
            v = v * 10 + (*p - '0');
            ++p;
        }
        rounds = v;
        salt_start = (p - full.data()) + 1;
    }
    size_t salt_end_dollar = full.find('$', salt_start);
    sstring salt = full.substr(salt_start, salt_end_dollar - salt_start);
    if (rounds < 1000) rounds = 1000;
    else if (rounds > 999999999) rounds = 999999999;

    const size_t pw_len = pass.size();
    const size_t salt_len = salt.size();

    auto digest_pw_salt_pw = [&]() {
        cryptopp_hasher<sha512_hasher, 64> h;
        h.update(pass.data(), pw_len);
        h.update(salt.data(), salt_len);
        h.update(pass.data(), pw_len);
        return h.finalize_array();
    };

    auto alt = digest_pw_salt_pw(); // A

    // Initial alt_result
    std::array<uint8_t, 64> alt_result;
    {
        cryptopp_hasher<sha512_hasher, 64> h;
        h.update(pass.data(), pw_len);
        h.update(salt.data(), salt_len);
        size_t cnt = pw_len;
        while (cnt >= 64) {
            h.update(reinterpret_cast<const char*>(alt.data()), 64);
            cnt -= 64;
        }
        if (cnt) {
            h.update(reinterpret_cast<const char*>(alt.data()), cnt);
        }
        for (size_t n = pw_len; n > 0; n >>= 1) {
            if (n & 1) {
                h.update(reinterpret_cast<const char*>(alt.data()), 64);
            } else {
                h.update(pass.data(), pw_len);
            }
        }
        alt_result = h.finalize_array();
    }

    // P_bytes
    std::vector<unsigned char> P_bytes;
    if (pw_len) {
        P_bytes.resize(pw_len);
        unsigned char DP[64];
        {
            cryptopp_hasher<sha512_hasher, 64> h;
            for (size_t i = 0; i < pw_len; ++i) {
                h.update(pass.data(), pw_len);
            }
            auto arr = h.finalize_array();
            std::memcpy(DP, arr.data(), 64);
        }
        size_t off = 0;
        while (off + 64 <= pw_len) {
            std::memcpy(P_bytes.data() + off, DP, 64);
            off += 64;
        }
        if (off < pw_len) {
            std::memcpy(P_bytes.data() + off, DP, pw_len - off);
        }
    }

    // S_bytes
    std::vector<unsigned char> S_bytes;
    if (salt_len) {
        S_bytes.resize(salt_len);
        unsigned char DS[64];
        {
            cryptopp_hasher<sha512_hasher, 64> h;
            size_t repeat = 16 + alt_result[0];
            for (size_t i = 0; i < repeat; ++i) {
                h.update(salt.data(), salt_len);
            }
            auto arr = h.finalize_array();
            std::memcpy(DS, arr.data(), 64);
        }
        size_t off = 0;
        while (off + 64 <= salt_len) {
            std::memcpy(S_bytes.data() + off, DS, 64);
            off += 64;
        }
        if (off < salt_len) {
            std::memcpy(S_bytes.data() + off, DS, salt_len - off);
        }
    }

    const bool have_P = !P_bytes.empty();
    const bool have_S = !S_bytes.empty();

    for (size_t i = 0; i < rounds; ++i) {
        cryptopp_hasher<sha512_hasher, 64> h;
        if ((i & 1) == 0) {
            h.update(reinterpret_cast<const char*>(alt_result.data()), 64);
        } else if (have_P) {
            h.update(reinterpret_cast<const char*>(P_bytes.data()), P_bytes.size());
        }
        if (i % 3 != 0 && have_S) {
            h.update(reinterpret_cast<const char*>(S_bytes.data()), S_bytes.size());
        }
        if (i % 7 != 0 && have_P) {
            h.update(reinterpret_cast<const char*>(P_bytes.data()), P_bytes.size());
        }
        if ((i & 1) == 0) {
            if (have_P) h.update(reinterpret_cast<const char*>(P_bytes.data()), P_bytes.size());
        } else {
            h.update(reinterpret_cast<const char*>(alt_result.data()), 64);
        }
        alt_result = h.finalize_array();
    }

    // Assemble output (reuse dynamic b64)
    size_t header_end = salt_end_dollar;
    sstring out;
    out.append(full.data(), header_end + 1);
    size_t pos = out.size();
    auto enc = [&](unsigned char b2, unsigned char b1, unsigned char b0, int n) {
        b64_from_24bit(b2, b1, b0, n, out, pos);
    };
    enc(alt_result[0],  alt_result[21], alt_result[42], 4);
    enc(alt_result[22], alt_result[43], alt_result[1],  4);
    enc(alt_result[44], alt_result[2],  alt_result[23], 4);
    enc(alt_result[3],  alt_result[24], alt_result[45], 4);
    enc(alt_result[25], alt_result[46], alt_result[4],  4);
    enc(alt_result[47], alt_result[5],  alt_result[26], 4);
    enc(alt_result[6],  alt_result[27], alt_result[48], 4);
    enc(alt_result[28], alt_result[49], alt_result[7],  4);
    enc(alt_result[50], alt_result[8],  alt_result[29], 4);
    enc(alt_result[9],  alt_result[30], alt_result[51], 4);
    enc(alt_result[31], alt_result[52], alt_result[10], 4);
    enc(alt_result[53], alt_result[11], alt_result[32], 4);
    enc(alt_result[12], alt_result[33], alt_result[54], 4);
    enc(alt_result[34], alt_result[55], alt_result[13], 4);
    enc(alt_result[56], alt_result[14], alt_result[35], 4);
    enc(alt_result[15], alt_result[36], alt_result[57], 4);
    enc(alt_result[37], alt_result[58], alt_result[16], 4);
    enc(alt_result[59], alt_result[17], alt_result[38], 4);
    enc(alt_result[18], alt_result[39], alt_result[60], 4);
    enc(alt_result[40], alt_result[61], alt_result[19], 4);
    enc(alt_result[62], alt_result[20], alt_result[41], 4);
    enc(0, 0, alt_result[63], 2);
    return out;
}

std::string_view prefix_for_scheme(scheme c) noexcept {
    switch (c) {
    case scheme::bcrypt_y: return "$2y$";
    case scheme::bcrypt_a: return "$2a$";
    case scheme::sha_512: return "$6$";
    case scheme::sha_256: return "$5$";
    case scheme::md5: return "$1$";
    }
}

} // namespace detail

no_supported_schemes::no_supported_schemes()
        : std::runtime_error("No allowed hashing schemes are supported on this system") {
}

static seastar::logger logger("pass2");

bool check(const sstring& pass, const sstring& salted_hash) {
    auto got = detail::hash_with_salt_v2(pass, salted_hash);
    if (got != salted_hash) {
        logger.error("expected hash {} got {}", salted_hash, got);
    }
    return got == salted_hash;
}

} // namespace auth::passwords
