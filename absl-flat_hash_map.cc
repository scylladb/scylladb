/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "absl-flat_hash_map.hh"

size_t sstring_hash::operator()(std::string_view v) const noexcept {
    return absl::Hash<std::string_view>{}(v);
}
