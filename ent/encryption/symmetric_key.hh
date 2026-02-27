/*
 * Copyright (C) 2018 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <memory>
#include <tuple>
#include <iosfwd>
#include <fmt/core.h>
#include <fmt/ostream.h>

#include "../../bytes.hh"

// forward declare openssl evp.
extern "C" {
struct evp_cipher_ctx_st;
}

namespace encryption {

struct key_info {
    sstring alg;
    unsigned len;

    bool compatible(const key_info&) const;
};

bool operator==(const key_info& k1, const key_info& k2);
bool operator!=(const key_info& k1, const key_info& k2);
std::ostream& operator<<(std::ostream&, const key_info&);

struct key_info_hash {
    size_t operator()(const key_info& e) const;
};

std::tuple<sstring, sstring, sstring> parse_key_spec(const sstring&);

// shared between key & kmip
std::tuple<sstring, sstring, sstring> parse_key_spec_and_validate_defaults(const sstring&);

class symmetric_key {
    std::unique_ptr<evp_cipher_ctx_st, void (*)(evp_cipher_ctx_st*)> _ctxt;
    key_info _info;
    bytes _key;
    unsigned _iv_len = 0;
    unsigned _block_size = 0;
    bool _padding = true;

    operator evp_cipher_ctx_st *() const {
        return _ctxt.get();
    }

    void generate_iv_impl(uint8_t* dst, size_t) const;
    size_t decrypt_impl(const uint8_t* input, size_t input_len, uint8_t* output,
                    size_t output_len, const uint8_t* iv) const;
    size_t encrypt_impl(const uint8_t* input, size_t input_len, uint8_t* output,
                    size_t output_len, const uint8_t* iv) const;

public:
    symmetric_key(const key_info& info, const bytes& key = { });

    const key_info& info() const {
        return _info;
    }
    const bytes& key() const {
        return _key;
    }
    size_t iv_len() const {
        return _iv_len;
    }
    size_t block_size() const {
        return _block_size;
    }

    /**
     * Evaluates whether or not the key info provided resulted in
     * the exact same result from openssl, i.e. whether the combination
     * of alg/block mode/padding etc was actually fully valid (or our 
     * heuristics have issues)
    */
    std::string validate_exact_info_result() const;

    /**
     * Write a random IV to dst. Must be iv_len() sized or larger
     */
    template<typename T>
    void generate_iv(T* dst, size_t s) const {
        static_assert(sizeof(T) == sizeof(uint8_t) && std::is_integral_v<T>);
        generate_iv_impl(reinterpret_cast<uint8_t *>(dst), s);
    }

    // returns minimal buffer size required to encrypt n bytes. I.e.
    // block alignment
    size_t encrypted_size(size_t n) const;

    template<typename T, typename V, typename I = char>
    size_t decrypt(const T* input, size_t input_len, V* output,
                    size_t output_len, const I* iv = nullptr) const {
        static_assert(sizeof(T) == sizeof(uint8_t) && std::is_integral_v<T>);
        return decrypt_impl(reinterpret_cast<const uint8_t*>(input), input_len,
                        reinterpret_cast<uint8_t *>(output), output_len,
                        reinterpret_cast<const uint8_t*>(iv));
    }
    template<typename T, typename V, typename I = char>
    size_t encrypt(const T* input, size_t input_len, V* output,
                    size_t output_len, const I* iv = nullptr) const {
        static_assert(sizeof(T) == sizeof(uint8_t) && std::is_integral_v<T>);
        return encrypt_impl(reinterpret_cast<const uint8_t*>(input), input_len,
                        reinterpret_cast<uint8_t *>(output), output_len,
                        reinterpret_cast<const uint8_t*>(iv));
    }

    enum class mode {
        decrypt, encrypt,
    };
    template<typename T, typename V, typename I = char>
    void transform_unpadded(mode m, const T* input, size_t input_len, V* output,
                    const I* iv = nullptr) const {
        static_assert(sizeof(T) == sizeof(uint8_t) && std::is_integral_v<T>);
        return transform_unpadded_impl(reinterpret_cast<const uint8_t*>(input),
                        input_len, reinterpret_cast<uint8_t *>(output),
                        reinterpret_cast<const uint8_t*>(iv), m);
    }
    template<typename T, typename V, typename I = char>
    void encrypt_unpadded(const T* input, size_t input_len, V* output,
                    const I* iv = nullptr) const {
        static_assert(sizeof(T) == sizeof(uint8_t) && std::is_integral_v<T>);
        return transform_unpadded_impl(reinterpret_cast<const uint8_t*>(input),
                        input_len, reinterpret_cast<uint8_t *>(output),
                        reinterpret_cast<const uint8_t*>(iv), mode::encrypt);
    }
    template<typename T, typename V, typename I = char>
    void decrypt_unpadded(const T* input, size_t input_len, V* output,
                    const I* iv = nullptr) const {
        static_assert(sizeof(T) == sizeof(uint8_t) && std::is_integral_v<T>);
        return transform_unpadded_impl(reinterpret_cast<const uint8_t*>(input),
                        input_len, reinterpret_cast<uint8_t *>(output),
                        reinterpret_cast<const uint8_t*>(iv), mode::decrypt);
    }

private:
    void transform_unpadded_impl(const uint8_t* input, size_t input_len,
                    uint8_t* output, const uint8_t* iv, mode) const;
};

}

template <> struct fmt::formatter<encryption::key_info> : fmt::ostream_formatter {};
