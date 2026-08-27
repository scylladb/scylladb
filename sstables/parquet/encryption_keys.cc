/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "sstables/parquet/encryption_keys.hh"

namespace sstables::parquet {

namespace {
key_source* g_key_source = nullptr;
}

void set_key_source(key_source* ks) {
    g_key_source = ks;
}

key_source* key_source_ptr() {
    return g_key_source;
}

std::optional<key_metadata_format> parse_key_metadata_format(std::string_view v) {
    if (v == "provider")    { return key_metadata_format::provider; }
    if (v == "parquet_kms") { return key_metadata_format::parquet_kms; }
    return std::nullopt;
}

const char* to_string(key_metadata_format f) {
    return f == key_metadata_format::provider ? "provider" : "parquet_kms";
}

seastar::sstring make_key_metadata(const seastar::sstring& key_id, key_metadata_format fmt) {
    if (fmt == key_metadata_format::provider) {
        // Verbatim. The id is whatever the key provider issued, and we do not interpret it -- which
        // is what lets a BYOK provider define its own.
        return key_id;
    }
    // Field order matters to nobody, but the field *set* does: pyarrow rejects the material if
    // kmsInstanceID or kmsInstanceURL is missing, which is how the first attempt at this failed.
    //
    // masterKeyID carries the provider's id, which for some providers (the local-file one) is
    // empty. That is left empty rather than filled with an invented label: the string is round
    // tripped back to the provider on read, and a value we made up would be a value the provider
    // has to be told to ignore.
    return seastar::sstring("{\"keyMaterialType\":\"PKMT1\",\"internalStorage\":true,"
                            "\"isFooterKey\":true,\"kmsInstanceID\":\"DEFAULT\","
                            "\"kmsInstanceURL\":\"DEFAULT\",\"masterKeyID\":\"")
           + key_id
           + "\",\"wrappedDEK\":\"AAAAAAAAAAAAAAAAAAAAAA==\",\"doubleWrapping\":false}";
}

seastar::sstring key_id_from_metadata(const seastar::sstring& km) {
    const std::string s(km);
    const std::string tag = "\"masterKeyID\"";
    auto p = s.find(tag);
    if (p == std::string::npos) {
        return km;      // the bare-id form
    }
    p = s.find(':', p + tag.size());
    if (p == std::string::npos) { return {}; }
    auto a = s.find('"', p);
    if (a == std::string::npos) { return {}; }
    auto b = s.find('"', a + 1);
    if (b == std::string::npos) { return {}; }
    return seastar::sstring(s.substr(a + 1, b - a - 1));
}

} // namespace sstables::parquet
