/*
* Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once
#include <filesystem>

namespace s3 {

std::filesystem::path s3_canonicalize(const std::filesystem::path& path);
bool is_s3_fqn(const std::filesystem::path& fqn);
bool s3fqn_to_parts(const std::filesystem::path& fqn, std::string& bucket_name, std::string& object_name);

} // namespace s3
