/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "encryption.hh"
#include "thrift_compact.hh"
#include "thrift_compact_writer.hh"

#include <openssl/evp.h>
#include <openssl/rand.h>
#include <openssl/err.h>

#include <cstring>
#include <stdexcept>

namespace sstables::parquet::format {

namespace {

// OpenSSL rather than gnutls, which this tree also links. gnutls does not expose AES-CTR at all,
// and AES_GCM_CTR_V1 needs it to *read* files other writers produced -- so using gnutls would have
// meant two crypto libraries for one feature. OpenSSL covers both modes.
const EVP_CIPHER* gcm_for(size_t key_len) {
    switch (key_len) {
    case 16: return EVP_aes_128_gcm();
    case 24: return EVP_aes_192_gcm();
    case 32: return EVP_aes_256_gcm();
    default: throw std::invalid_argument("parquet encryption: key must be 16, 24 or 32 bytes");
    }
}

const EVP_CIPHER* ctr_for(size_t key_len) {
    switch (key_len) {
    case 16: return EVP_aes_128_ctr();
    case 24: return EVP_aes_192_ctr();
    case 32: return EVP_aes_256_ctr();
    default: throw std::invalid_argument("parquet encryption: key must be 16, 24 or 32 bytes");
    }
}

[[noreturn]] void ssl_throw(const char* what) {
    char buf[256] = {};
    ERR_error_string_n(ERR_get_error(), buf, sizeof(buf));
    throw std::runtime_error(std::string("parquet encryption: ") + what + ": " + buf);
}

struct evp_ctx {
    EVP_CIPHER_CTX* p = EVP_CIPHER_CTX_new();
    evp_ctx() { if (!p) { ssl_throw("EVP_CIPHER_CTX_new"); } }
    ~evp_ctx() { EVP_CIPHER_CTX_free(p); }
    evp_ctx(const evp_ctx&) = delete;
    evp_ctx& operator=(const evp_ctx&) = delete;
};

void put_u32le(std::vector<uint8_t>& out, uint32_t v) {
    out.push_back(uint8_t(v));
    out.push_back(uint8_t(v >> 8));
    out.push_back(uint8_t(v >> 16));
    out.push_back(uint8_t(v >> 24));
}

uint32_t get_u32le(std::span<const uint8_t> b) {
    return uint32_t(b[0]) | (uint32_t(b[1]) << 8) | (uint32_t(b[2]) << 16) | (uint32_t(b[3]) << 24);
}

void append_i16le(std::vector<uint8_t>& v, int16_t x) {
    v.push_back(uint8_t(uint16_t(x) & 0xff));
    v.push_back(uint8_t((uint16_t(x) >> 8) & 0xff));
}

// AES-CTR for a page body under AES_GCM_CTR_V1. The spec fixes the counter block as the 12-byte
// nonce followed by a 4-byte big-endian counter starting at 1 -- the same construction GCM uses
// for its keystream, so the initial value is 1 rather than 0.
void aes_ctr_xor(const encryption_key& key, std::span<const uint8_t> nonce,
                 std::span<const uint8_t> in, std::vector<uint8_t>& out) {
    uint8_t iv[16] = {};
    std::memcpy(iv, nonce.data(), nonce.size());
    iv[15] = 1;
    evp_ctx ctx;
    if (EVP_EncryptInit_ex(ctx.p, ctr_for(key.bytes.size()), nullptr,
                           key.bytes.data(), iv) != 1) {
        ssl_throw("EVP_EncryptInit_ex (ctr)");
    }
    const size_t base = out.size();
    out.resize(base + in.size());
    int outl = 0;
    if (EVP_EncryptUpdate(ctx.p, out.data() + base, &outl, in.data(), int(in.size())) != 1) {
        ssl_throw("EVP_EncryptUpdate (ctr)");
    }
    out.resize(base + size_t(outl));
}

} // namespace

void random_bytes(std::span<uint8_t> out) {
    if (RAND_bytes(out.data(), int(out.size())) != 1) {
        ssl_throw("RAND_bytes");
    }
}

std::vector<uint8_t> build_aad(std::string_view aad_prefix, std::string_view aad_file_unique,
                               module_type mt, int row_group, int column, int page) {
    std::vector<uint8_t> aad;
    aad.reserve(aad_prefix.size() + aad_file_unique.size() + 7);
    aad.insert(aad.end(), aad_prefix.begin(), aad_prefix.end());
    aad.insert(aad.end(), aad_file_unique.begin(), aad_file_unique.end());
    aad.push_back(uint8_t(mt));
    // The ordinals are positional: a module type that has them always has all the preceding
    // ones, so a single ordered append is enough and a gap is a programming error.
    if (row_group >= 0) { append_i16le(aad, int16_t(row_group)); }
    if (column >= 0) {
        if (row_group < 0) {
            throw std::invalid_argument("parquet encryption: column ordinal without a row group");
        }
        append_i16le(aad, int16_t(column));
    }
    if (page >= 0) {
        if (column < 0) {
            throw std::invalid_argument("parquet encryption: page ordinal without a column");
        }
        append_i16le(aad, int16_t(page));
    }
    return aad;
}

size_t encrypt_module(std::vector<uint8_t>& out, std::span<const uint8_t> plain,
                      const encryption_key& key, std::span<const uint8_t> aad,
                      cipher algo, bool ctr_body) {
    if (!key.valid()) { throw std::invalid_argument("parquet encryption: invalid key length"); }
    const bool use_ctr = (algo == cipher::aes_gcm_ctr_v1) && ctr_body;
    const size_t body = nonce_len + plain.size() + (use_ctr ? 0 : tag_len);
    const size_t start = out.size();
    put_u32le(out, uint32_t(body));
    uint8_t nonce[nonce_len];
    random_bytes(nonce);
    out.insert(out.end(), nonce, nonce + nonce_len);
    if (use_ctr) {
        // No tag: AES_GCM_CTR_V1 deliberately trades page-body integrity for speed. The page
        // *headers* are still GCM, so a corrupted body is detected only by the data itself.
        aes_ctr_xor(key, nonce, plain, out);
    } else {
        evp_ctx ctx;
        if (EVP_EncryptInit_ex(ctx.p, gcm_for(key.bytes.size()), nullptr, nullptr, nullptr) != 1) {
            ssl_throw("EVP_EncryptInit_ex (gcm)");
        }
        if (EVP_CIPHER_CTX_ctrl(ctx.p, EVP_CTRL_AEAD_SET_IVLEN, int(nonce_len), nullptr) != 1) {
            ssl_throw("set ivlen");
        }
        if (EVP_EncryptInit_ex(ctx.p, nullptr, nullptr, key.bytes.data(), nonce) != 1) {
            ssl_throw("EVP_EncryptInit_ex (key)");
        }
        int outl = 0;
        if (!aad.empty()
            && EVP_EncryptUpdate(ctx.p, nullptr, &outl, aad.data(), int(aad.size())) != 1) {
            ssl_throw("EVP_EncryptUpdate (aad)");
        }
        const size_t base = out.size();
        out.resize(base + plain.size() + tag_len);
        outl = 0;
        if (!plain.empty()
            && EVP_EncryptUpdate(ctx.p, out.data() + base, &outl, plain.data(),
                                 int(plain.size())) != 1) {
            ssl_throw("EVP_EncryptUpdate");
        }
        int finl = 0;
        if (EVP_EncryptFinal_ex(ctx.p, out.data() + base + outl, &finl) != 1) {
            ssl_throw("EVP_EncryptFinal_ex");
        }
        const size_t ct = size_t(outl) + size_t(finl);
        if (EVP_CIPHER_CTX_ctrl(ctx.p, EVP_CTRL_AEAD_GET_TAG, int(tag_len),
                                out.data() + base + ct) != 1) {
            ssl_throw("get tag");
        }
        out.resize(base + ct + tag_len);
    }
    return out.size() - start;
}

std::vector<uint8_t> decrypt_module(std::span<const uint8_t> buf, const encryption_key& key,
                                    std::span<const uint8_t> aad, size_t* consumed,
                                    cipher algo, bool ctr_body) {
    if (!key.valid()) { throw std::invalid_argument("parquet encryption: invalid key length"); }
    if (buf.size() < length_prefix_len + nonce_len) {
        throw std::runtime_error("parquet encryption: truncated module envelope");
    }
    const uint32_t body = get_u32le(buf);
    const bool use_ctr = (algo == cipher::aes_gcm_ctr_v1) && ctr_body;
    const size_t min_body = nonce_len + (use_ctr ? 0 : tag_len);
    if (body < min_body || length_prefix_len + body > buf.size()) {
        throw std::runtime_error("parquet encryption: module length out of range");
    }
    if (consumed) { *consumed = length_prefix_len + body; }
    auto nonce = buf.subspan(length_prefix_len, nonce_len);
    auto rest  = buf.subspan(length_prefix_len + nonce_len, body - nonce_len);
    std::vector<uint8_t> plain;
    if (use_ctr) {
        aes_ctr_xor(key, nonce, rest, plain);
        return plain;
    }
    evp_ctx ctx;
    if (EVP_DecryptInit_ex(ctx.p, gcm_for(key.bytes.size()), nullptr, nullptr, nullptr) != 1) {
        ssl_throw("EVP_DecryptInit_ex (gcm)");
    }
    if (EVP_CIPHER_CTX_ctrl(ctx.p, EVP_CTRL_AEAD_SET_IVLEN, int(nonce_len), nullptr) != 1) {
        ssl_throw("set ivlen");
    }
    if (EVP_DecryptInit_ex(ctx.p, nullptr, nullptr, key.bytes.data(), nonce.data()) != 1) {
        ssl_throw("EVP_DecryptInit_ex (key)");
    }
    int outl = 0;
    if (!aad.empty()
        && EVP_DecryptUpdate(ctx.p, nullptr, &outl, aad.data(), int(aad.size())) != 1) {
        ssl_throw("EVP_DecryptUpdate (aad)");
    }
    plain.resize(rest.size() - tag_len);
    outl = 0;
    if (!plain.empty()
        && EVP_DecryptUpdate(ctx.p, plain.data(), &outl, rest.data(),
                             int(rest.size() - tag_len)) != 1) {
        ssl_throw("EVP_DecryptUpdate");
    }
    if (EVP_CIPHER_CTX_ctrl(ctx.p, EVP_CTRL_AEAD_SET_TAG, int(tag_len),
                            const_cast<uint8_t*>(rest.data() + rest.size() - tag_len)) != 1) {
        ssl_throw("set tag");
    }
    int finl = 0;
    if (EVP_DecryptFinal_ex(ctx.p, plain.data() + outl, &finl) != 1) {
        // A failed tag is the feature, not an error to paper over: it means this ciphertext is
        // not the one that belongs at this position in this file, under this key.
        throw std::runtime_error("parquet encryption: authentication failed");
    }
    size_t outlen = size_t(outl) + size_t(finl);
    plain.resize(outlen);
    return plain;
}

std::vector<uint8_t> write_file_crypto_metadata(const file_crypto_metadata& m) {
    std::vector<uint8_t> out;
    compact_writer w(out);
    compact_writer::struct_scope s(w);
    // 1: EncryptionAlgorithm, a union of AesGcmV1 (1) and AesGcmCtrV1 (2).
    w.field_struct(1);
    {
        compact_writer::elem_scope alg(w);
        w.field_struct(m.algo == cipher::aes_gcm_v1 ? 1 : 2);
        compact_writer::elem_scope inner(w);
        if (!m.aad_prefix.empty())  { w.field_binary(1, m.aad_prefix); }
        if (!m.aad_file_unique.empty()) { w.field_binary(2, m.aad_file_unique); }
        w.field_bool(3, m.supply_aad_prefix);
    }
    if (m.key_metadata) { w.field_binary(2, *m.key_metadata); }
    return out;
}

file_crypto_metadata parse_file_crypto_metadata(std::span<const uint8_t> blob, size_t* consumed) {
    file_crypto_metadata m;
    compact_reader r(blob);
    {
        compact_reader::struct_scope sc(r);
        while (true) {
            auto f = r.field_begin();
            if (f.stop) { break; }
            if (f.id == 1 && f.type == ctype::strct) {
                compact_reader::struct_scope alg(r);
                while (true) {
                    auto a = r.field_begin();
                    if (a.stop) { break; }
                    if ((a.id == 1 || a.id == 2) && a.type == ctype::strct) {
                        m.algo = (a.id == 1) ? cipher::aes_gcm_v1 : cipher::aes_gcm_ctr_v1;
                        compact_reader::struct_scope in(r);
                        while (true) {
                            auto g = r.field_begin();
                            if (g.stop) { break; }
                            if (g.id == 1 && g.type == ctype::binary) {
                                m.aad_prefix = std::string(r.binary_v());
                            } else if (g.id == 2 && g.type == ctype::binary) {
                                m.aad_file_unique = std::string(r.binary_v());
                            } else if (g.id == 3 && (g.type == ctype::boolean_true
                                                     || g.type == ctype::boolean_false)) {
                                m.supply_aad_prefix = f.bool_value || g.bool_value;
                            } else {
                                r.skip(g.type);
                            }
                        }
                    } else {
                        r.skip(a.type);
                    }
                }
            } else if (f.id == 2 && f.type == ctype::binary) {
                m.key_metadata = std::string(r.binary_v());
            } else {
                r.skip(f.type);
            }
        }
    }
    if (consumed) { *consumed = r.position(); }
    return m;
}

bool has_encrypted_footer(std::span<const uint8_t> file_image) {
    return file_image.size() >= 8
           && std::memcmp(file_image.data() + file_image.size() - 4, magic_encrypted, 4) == 0;
}

} // namespace sstables::parquet::format
