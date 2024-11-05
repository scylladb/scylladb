/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "schema/schema.hh" // IWYU pragma: keep
#include <span>
#include <memory>

extern seastar::logger trie_logger;

namespace trie {

using const_bytes = std::span<const std::byte>;

} // namespace trie
