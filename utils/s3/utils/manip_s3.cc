/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "manip_s3.hh"

namespace aws::s3 {
namespace fs = std::filesystem;
constexpr std::string_view s3_fqn_prefix = "s3://";

fs::path s3_canonicalize(const fs::path& path) {
    // Canonicalizing the original "s3://" changes it to "s3:/". Trim and re-add the "s3://" prefix.
    auto canonical = path.lexically_normal().string().substr(s3_fqn_prefix.size() - 1);
    return std::string(s3_fqn_prefix) + canonical;
}

bool is_s3_fqn(const fs::path& fqn) {
    return *(fqn.begin()) == s3_fqn_prefix.substr(0, 3);
}

bool s3fqn_to_parts(const fs::path& fqn, std::string& bucket_name, std::string& object_name) {
    if (!is_s3_fqn(fqn)) {
        return false;
    }

    auto name = s3_canonicalize(fqn);
    auto it = name.begin();
    ++it; // skip the `s3:/` part
    bucket_name = it->string();
    ++it; // skip the bucket name

    fs::path object_path;
    for (; it != name.end(); ++it) {
        object_path /= *it;
    }
    object_name = object_path.string();
    if (object_name.empty())
        object_name = "/";

    return true;
}
} // namespace aws::s3
