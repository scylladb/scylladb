// Copyright 2023-present ScyllaDB
// SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

#pragma once

#include "managed_bytes.hh"
#include "serializer.hh"
#include "bytes_ostream.hh"

namespace utils {

managed_bytes_view
buffer_view_to_managed_bytes_view(ser::buffer_view<bytes_ostream::fragment_iterator> bv);

managed_bytes_view_opt
buffer_view_to_managed_bytes_view(std::optional<ser::buffer_view<bytes_ostream::fragment_iterator>> bvo);

}
