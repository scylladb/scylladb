/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <seastar/util/bool_class.hh>

using multishard_reader_buffer_hint = seastar::bool_class<struct multishard_reader_buffer_hint_tag>;
using read_ahead = seastar::bool_class<struct read_ahead_tag>;
