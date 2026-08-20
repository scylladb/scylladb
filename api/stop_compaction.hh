/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "compaction/compaction_descriptor.hh"

namespace api {

std::vector<sstring> valid_compaction_types_for_stop();
std::expected<compaction::compaction_type_set, sstring> parse_compaction_types_to_stop(const sstring& type_name);

} // namespace api
