/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Parquet Modular Encryption (parquet-format 2.7+).
//
// This is a *format* feature, not a Scylla one, and that distinction is the whole reason to
// implement it here rather than encrypting the file from outside. A storage-layer encryption of
// the Data component would make the file opaque to every external reader and forfeit the
// interoperability that motivates using Parquet at all. Modular encryption keeps the file a
// Parquet file: a reader holding the right key opens it with a stock library, and can be given
// keys for only the columns it is entitled to.
//
// What is encrypted is *modules*, each with its own AAD so a ciphertext cannot be moved to
// another position in the file (or to another file) without detection. The AAD is
//
//     [aad_prefix] || aad_file_unique || module_type || row_group_ordinal || column_ordinal || page_ordinal
//
// with the ordinals as little-endian int16 and present only for the module types that have them.
// Everything in this header was validated byte-for-byte against files written by parquet-cpp
// (via pyarrow 25.0.1) rather than read off the specification -- see test_encryption.cc, which
// decrypts those files with this code.
//
// One consequence worth stating up front: **an encrypted file is not byte-reproducible.** Every
// module carries a fresh random nonce, because reusing a nonce under one key destroys AES-GCM's
// guarantees. Tests that assert byte-identical output across runs must not enable encryption.

#pragma once

#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <vector>

namespace sstables::parquet::format {

// Module type constants are fixed by the format: they are part of the AAD, so a reader and a
// writer that disagree produce an authentication failure rather than a wrong answer.
enum class module_type : uint8_t {
    footer                 = 0,
    column_metadata        = 1,
    data_page              = 2,
    dictionary_page        = 3,
    data_page_header       = 4,
    dictionary_page_header = 5,
    column_index           = 6,
    offset_index           = 7,
    bloom_filter_header    = 8,
    bloom_filter_bitset    = 9,
};

enum class cipher {
    aes_gcm_v1,        // everything AES-GCM: authenticated, 16-byte tag per module
    aes_gcm_ctr_v1,    // metadata AES-GCM, page *bodies* AES-CTR: no tag, no integrity
};

// AES-128/192/256 are all legal; the width is the key length.
struct encryption_key {
    std::vector<uint8_t> bytes;
    bool valid() const {
        return bytes.size() == 16 || bytes.size() == 24 || bytes.size() == 32;
    }
};

// What a file needs to encrypt or decrypt its modules. `aad_file_unique` is 8 bytes chosen per
// file; `aad_prefix` is optional and, when the writer does not store it, has to be supplied by
// the reader out of band (that is what AesGcmV1.supply_aad_prefix records).
struct crypto_context {
    cipher               algo = cipher::aes_gcm_v1;
    encryption_key       footer_key;
    std::string          aad_file_unique;
    std::string          aad_prefix;              // empty when unused
    bool                 store_aad_prefix = false;
};

constexpr size_t nonce_len = 12;
constexpr size_t tag_len   = 16;
// A module's on-disk envelope is a 4-byte little-endian length followed by that many bytes.
constexpr size_t length_prefix_len = 4;

// AAD for one module. Pass -1 for an ordinal the module type does not carry; the footer has
// none, dictionary pages and column metadata have no page ordinal.
std::vector<uint8_t> build_aad(std::string_view aad_prefix, std::string_view aad_file_unique,
                               module_type, int row_group = -1, int column = -1, int page = -1);

// Random bytes from the platform CSPRNG. Used for nonces and for aad_file_unique.
void random_bytes(std::span<uint8_t> out);

// Encrypt `plain` and append the whole envelope (length, nonce, ciphertext, tag) to `out`.
// Returns the number of bytes appended.
size_t encrypt_module(std::vector<uint8_t>& out, std::span<const uint8_t> plain,
                      const encryption_key&, std::span<const uint8_t> aad,
                      cipher = cipher::aes_gcm_v1, bool ctr_body = false);

// Decrypt one envelope from the head of `buf`. `consumed` receives the envelope's total size,
// so a caller walking a page stream can advance. Throws on a bad tag, which is the point: a
// truncated or tampered module must not decode to anything.
std::vector<uint8_t> decrypt_module(std::span<const uint8_t> buf, const encryption_key&,
                                    std::span<const uint8_t> aad, size_t* consumed = nullptr,
                                    cipher = cipher::aes_gcm_v1, bool ctr_body = false);

// ---- FileCryptoMetaData, the plaintext structure that says how the rest is encrypted.
struct file_crypto_metadata {
    cipher                     algo = cipher::aes_gcm_v1;
    std::string                aad_file_unique;
    std::string                aad_prefix;
    bool                       supply_aad_prefix = false;
    std::optional<std::string> key_metadata;
};

// Serialise/parse FileCryptoMetaData as Thrift compact. Written in the clear immediately before
// the encrypted footer, which is how a reader learns which key and which algorithm to use.
std::vector<uint8_t> write_file_crypto_metadata(const file_crypto_metadata&);
file_crypto_metadata parse_file_crypto_metadata(std::span<const uint8_t> blob,
                                                size_t* consumed = nullptr);

// "PARE" marks a file whose footer is encrypted; "PAR1" a plaintext footer (which may still
// have encrypted columns) and an unencrypted file alike.
constexpr const char magic_plain[4]     = {'P', 'A', 'R', '1'};
constexpr const char magic_encrypted[4] = {'P', 'A', 'R', 'E'};

bool has_encrypted_footer(std::span<const uint8_t> file_image);

} // namespace sstables::parquet::format
