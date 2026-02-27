/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "manip_s3.hh"
#include "data_dictionary/storage_options.hh"
#include <iostream>

namespace s3 {
namespace fs = std::filesystem;

bool is_s3_fqn(const fs::path& fqn) {
    return data_dictionary::is_object_storage_fqn(fqn, "s3");
}

bool s3fqn_to_parts(const fs::path& fqn, std::string& bucket_name, std::string& object_name) {
    return data_dictionary::object_storage_fqn_to_parts(fqn, "s3", bucket_name, object_name);
}

} // namespace s3
