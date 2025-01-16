/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once
#include <filesystem>

namespace aws::s3 {

std::filesystem::path s3_canonicalize(const std::filesystem::path& path);
bool is_s3_fqn(const std::filesystem::path& fqn);
bool s3fqn_to_parts(const std::filesystem::path& fqn, std::string& bucket_name, std::string& object_name);

} // namespace aws::s3
