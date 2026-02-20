/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "utils/mutable_view.hh"

template <mutable_view is_mutable_view>
class managed_bytes_basic_view;
using managed_bytes_view = managed_bytes_basic_view<mutable_view::no>;
using managed_bytes_mutable_view = managed_bytes_basic_view<mutable_view::yes>;
