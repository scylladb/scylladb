/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Write encrypted Parquet files for an external reader to open.
//
// The conformance test reads parquet-cpp's files with our code. This is the other direction, and
// it is the one that decides whether the feature is worth anything: a file only we can read is a
// container, not a Parquet file. So this writes files and prints nothing but their paths -- the
// verdict comes from pyarrow, in test_encrypt_interop.py.
//
// Two configurations, because they exercise different module types: with and without a
// dictionary (the dictionary page is a separate pair of modules with no page ordinal, which is
// exactly the kind of detail that round-trips fine against ourselves and fails against anyone
// else).

#include "parquet_writer.hh"
#include "parquet_reader.hh"
#include "encryption.hh"

#include <cstring>
#include <fstream>
#include <map>
#include <iostream>
#include <string>
#include <vector>

using namespace sstables::parquet::format;

static int self_failures = 0;
static constexpr int N = 100;

static void write_file(const std::string& path, bool dict, cipher algo,
                       const std::string& key_bytes, const std::string& aad_prefix,
                       bool store_prefix) {
    std::vector<column_spec> schema;
    schema.push_back(column_spec{"id", phys_type::int64, repetition::required});
    schema.push_back(column_spec{"name", phys_type::byte_array, repetition::optional});

    writer_options opt;
    opt.compression = codec::zstd;
    opt.use_dictionary = dict;
    opt.write_page_index = true;
    opt.page_values = 40;            // several pages, so page ordinals actually vary
    opt.encryption.enabled = true;
    opt.encryption.algo = algo;
    opt.encryption.footer_key = encryption_key{
        std::vector<uint8_t>(key_bytes.begin(), key_bytes.end())};
    opt.encryption.aad_prefix = aad_prefix;
    opt.encryption.store_aad_prefix = store_prefix;
    // key_metadata is opaque to the format: the spec says only "whatever the reader needs to
    // find the key". pyarrow's *Python* API can only decrypt through a KMS, and its KMS layer
    // expects this particular JSON -- parquet-java's "key tools" key-material format. Emitting
    // it is what makes the file openable by a stock high-level reader rather than only by one
    // using the C++ explicit-key API. No key material is in it: wrappedDEK is a placeholder and
    // the test's KMS returns the key for masterKeyID.
    opt.encryption.key_metadata = std::string(
            "{\"keyMaterialType\":\"PKMT1\",\"internalStorage\":true,"
            "\"isFooterKey\":true,\"kmsInstanceID\":\"DEFAULT\","
            "\"kmsInstanceURL\":\"DEFAULT\",\"masterKeyID\":\"scylla-test-key\","
            "\"wrappedDEK\":\"AAAAAAAAAAAAAAAAAAAAAA==\",\"doubleWrapping\":false}");

    parquet_file_writer w(schema, opt);
    w.add_key_value("scylla.test", "encrypted");

    column_data ids, names;
    for (int i = 0; i < N; ++i) {
        ids.i64.push_back(int64_t(i));
        names.def_levels.push_back(1);
        // Low cardinality when a dictionary is wanted, so the dictionary path is really taken.
        names.str.push_back(dict ? ("g" + std::to_string(i % 4)) : ("v" + std::to_string(i)));
    }
    std::vector<column_data> cols{std::move(ids), std::move(names)};
    w.add_row_group(cols);
    auto img = w.finish();

    std::ofstream f(path, std::ios::binary);
    f.write(reinterpret_cast<const char*>(img.data()), std::streamsize(img.size()));
    std::cout << path << " " << img.size() << "\n";

    // Read it back with our own reader before handing it to pyarrow. This catches the errors
    // that are ours alone -- a page ordinal that advances on a dictionary page, an AAD built
    // with the wrong module type -- and it does so with a specific message instead of pyarrow's
    // generic decryption failure.
    auto ef = parse_encrypted_footer(img, opt.encryption.footer_key, aad_prefix);
    if (ef.md.num_rows != N) {
        std::cout << "  SELF-READ FAIL: num_rows " << ef.md.num_rows << " != " << N << "\n";
        ++self_failures;
        return;
    }
    auto cols_back = read_row_group(img, ef.md, 0, &ef.crypto);
    if (cols_back.size() != 2 || cols_back[0].i64.size() != size_t(N)) {
        std::cout << "  SELF-READ FAIL: shape " << cols_back.size() << " x "
                  << (cols_back.empty() ? 0 : cols_back[0].i64.size()) << "\n";
        ++self_failures;
        return;
    }
    for (int i = 0; i < N; ++i) {
        if (cols_back[0].i64[size_t(i)] != int64_t(i)) {
            std::cout << "  SELF-READ FAIL: id[" << i << "] = "
                      << cols_back[0].i64[size_t(i)] << "\n";
            ++self_failures;
            return;
        }
    }
    if (cols_back[1].str.size() != size_t(N)) {
        std::cout << "  SELF-READ FAIL: name count " << cols_back[1].str.size() << "\n";
        ++self_failures;
        return;
    }
    // And the wrong key must fail rather than return plausible bytes.
    bool refused = false;
    try {
        encryption_key wrong{std::vector<uint8_t>(16, 0x5a)};
        (void) parse_encrypted_footer(img, wrong, aad_prefix);
    } catch (const std::exception&) { refused = true; }
    if (!refused) {
        std::cout << "  SELF-READ FAIL: opened with the wrong key\n";
        ++self_failures;
    }
}

// Per-column keys: `name` gets its own key, `id` stays under the footer key.
//
// The assertion that matters is the *partial* one -- a reader with the footer key alone must open
// the file and read `id`, and must not be able to touch `name`. That is the shape a real
// deployment wants ("encrypt the PII columns"), and it is the shape that would silently degrade
// into "everything readable" if the column key were not actually being used.
static std::string key_material(const char* id, bool footer) {
    // parquet-java's key-material JSON, which is what pyarrow's KMS layer requires. See
    // encryption_keys.hh for why the convention beats the minimal encoding. wrappedDEK is a
    // placeholder: the KMS is asked for the key by masterKeyID.
    return std::string("{\"keyMaterialType\":\"PKMT1\",\"internalStorage\":true,"
                       "\"isFooterKey\":") + (footer ? "true" : "false")
           + ",\"kmsInstanceID\":\"DEFAULT\",\"kmsInstanceURL\":\"DEFAULT\","
             "\"masterKeyID\":\"" + id
           + "\",\"wrappedDEK\":\"AAAAAAAAAAAAAAAAAAAAAA==\",\"doubleWrapping\":false}";
}

static void write_percolumn(const std::string& path, const std::string& footer_key_bytes,
                            const std::string& col_key_bytes) {
    std::vector<column_spec> schema;
    schema.push_back(column_spec{"id", phys_type::int64, repetition::required});
    schema.push_back(column_spec{"name", phys_type::byte_array, repetition::optional});

    writer_options opt;
    opt.compression = codec::zstd;
    opt.use_dictionary = true;
    opt.write_page_index = true;
    opt.page_values = 40;
    opt.encryption.enabled = true;
    opt.encryption.footer_key = encryption_key{
        std::vector<uint8_t>(footer_key_bytes.begin(), footer_key_bytes.end())};
    opt.encryption.key_metadata = key_material("footerkey", true);
    writer_options::encryption_options::column_key ck;
    ck.key = encryption_key{std::vector<uint8_t>(col_key_bytes.begin(), col_key_bytes.end())};
    ck.key_metadata = key_material("namekey", false);
    opt.encryption.column_keys["name"] = ck;

    parquet_file_writer w(schema, opt);
    column_data ids, names;
    for (int i = 0; i < N; ++i) {
        ids.i64.push_back(int64_t(i));
        names.def_levels.push_back(1);
        names.str.push_back("g" + std::to_string(i % 4));
    }
    std::vector<column_data> cols{std::move(ids), std::move(names)};
    w.add_row_group(cols);
    auto img = w.finish();
    std::ofstream f(path, std::ios::binary);
    f.write(reinterpret_cast<const char*>(img.data()), std::streamsize(img.size()));
    std::cout << path << " " << img.size() << "\n";

    const encryption_key fkey{std::vector<uint8_t>(footer_key_bytes.begin(),
                                                   footer_key_bytes.end())};
    const encryption_key ckey{std::vector<uint8_t>(col_key_bytes.begin(), col_key_bytes.end())};

    // (a) footer key only: the file opens, `id` has metadata, `name` has none.
    {
        auto ef = parse_encrypted_footer(img, fkey);
        const auto& chunks = ef.md.row_groups.at(0).columns;
        const bool ok = chunks.size() == 2
                  && chunks[0].meta.has_value()
                  && !chunks[1].meta.has_value()
                  && chunks[1].metadata_is_encrypted()
                  && chunks[1].crypto_metadata
                  && !chunks[1].crypto_metadata->with_footer_key
                  && chunks[1].crypto_metadata->path_in_schema
                         == std::vector<std::string>{"name"};
        if (!ok) {
            std::cout << "  PERCOL FAIL: the footer-key-only view is wrong\n";
            ++self_failures;
            return;
        }
    }
    // (b) footer key plus the column key: both columns decode, values exact.
    {
        std::map<std::string, encryption_key> cks{{"name", ckey}};
        auto ef = parse_encrypted_footer(img, fkey, {}, cks);
        const auto& chunks = ef.md.row_groups.at(0).columns;
        if (!chunks[1].meta.has_value()) {
            std::cout << "  PERCOL FAIL: column metadata did not decrypt with the column key\n";
            ++self_failures;
            return;
        }
        auto back = read_row_group(img, ef.md, 0, &ef.crypto);
        if (back.size() != 2 || back[0].i64.size() != size_t(N)
            || back[1].str.size() != size_t(N)) {
            std::cout << "  PERCOL FAIL: shape after decrypting both columns\n";
            ++self_failures;
            return;
        }
        for (int i = 0; i < N; ++i) {
            if (back[0].i64[size_t(i)] != int64_t(i)
                || back[1].str[size_t(i)] != ("g" + std::to_string(i % 4))) {
                std::cout << "  PERCOL FAIL: value mismatch at " << i << "\n";
                ++self_failures;
                return;
            }
        }
    }
    // (c) a wrong column key must fail rather than return plausible bytes.
    {
        std::map<std::string, encryption_key> bad{
                {"name", encryption_key{std::vector<uint8_t>(16, 0x11)}}};
        bool refused = false;
        try {
            (void) parse_encrypted_footer(img, fkey, {}, bad);
        } catch (const std::exception&) { refused = true; }
        if (!refused) {
            std::cout << "  PERCOL FAIL: a wrong column key was accepted\n";
            ++self_failures;
        }
    }
}

int main(int argc, char** argv) {
    const std::string dir = argc > 1 ? argv[1] : ".";
    const std::string key = "0123456789abcdef";
    write_file(dir + "/scylla_gcm_dict.parquet",   true,  cipher::aes_gcm_v1, key, "", false);
    write_file(dir + "/scylla_gcm_plain.parquet",  false, cipher::aes_gcm_v1, key, "", false);
    write_file(dir + "/scylla_ctr_dict.parquet",   true,  cipher::aes_gcm_ctr_v1, key, "", false);
    // An AAD prefix, stored so a reader needs nothing out of band.
    write_file(dir + "/scylla_gcm_prefix.parquet", true,  cipher::aes_gcm_v1, key,
               "ks.tbl/generation-42", true);
    write_percolumn(dir + "/scylla_percolumn.parquet", key, "fedcba9876543210");
    if (self_failures) {
        std::cout << "ENCRYPTED SELF-READ FAIL (" << self_failures << ")\n";
        return 1;
    }
    std::cout << "ENCRYPTED SELF-READ PASS\n";
    return 0;
}
