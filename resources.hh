/*
 * Copyright (C) 2025-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/future.hh>

#include <filesystem>
#include <variant>

#include "bytes.hh"

namespace resources {

struct resource {
    std::string name;
    std::string content_type;
    bool compressed; // whether the content is gzip-compressed
    std::variant<bytes_view, std::filesystem::path> content; // either in-memory content or path to file on disk
};

future<std::vector<resource>> load_resource_manifest(const std::filesystem::path& manifest_path);

} // namespace resources
