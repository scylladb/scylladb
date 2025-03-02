/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "manip_s3.hh"

#include <iostream>

namespace s3 {
namespace fs = std::filesystem;
constexpr std::string_view s3_fqn_prefix = "s3://";

fs::path s3_canonicalize(const fs::path& path) {
    if (!is_s3_fqn(path) || path.string().length() < s3_fqn_prefix.length() - 1) {
        return path;
    }
    // Canonicalizing the original "s3://" changes it to "s3:/". Trim and re-add the "s3://" prefix.
    auto canonical = path.lexically_normal().string().substr(s3_fqn_prefix.length() - 1);
    return std::string(s3_fqn_prefix) + canonical;
}

bool is_s3_fqn(const fs::path& fqn) {
    if (fqn.empty())
        return false;

    return *(fqn.begin()) == s3_fqn_prefix.substr(0, 3);
}

bool s3fqn_to_parts(const fs::path& fqn, std::string& bucket_name, std::string& object_name) {
    if (!is_s3_fqn(fqn)) {
        return false;
    }

    const auto canonical = s3_canonicalize(fqn);
    auto it = canonical.begin();

    // Expect at least two components: the scheme (e.g., "s3:") and the bucket name.
    if (std::distance(it, canonical.end()) < 2) {
        return false;
    }

    // Skip the scheme component.
    ++it;

    // The next component is the bucket name.
    bucket_name = it->string();

    // Advance to check for object parts.
    ++it;
    if (it == canonical.end()) {
        // No object parts – default to root.
        object_name = "/";
        return true;
    }

    // Combine remaining parts into the object path.
    fs::path obj;
    for (; it != canonical.end(); ++it) {
        obj /= *it;
    }

    object_name = obj.string().empty() ? "/" : obj.string();
    return true;
}

} // namespace s3
