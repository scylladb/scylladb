/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#pragma once

#include "schema/schema_fwd.hh"

namespace streaming {

struct stream_detail {
    table_id cf_id;
    stream_detail() = default;
    stream_detail(table_id cf_id_)
        : cf_id(std::move(cf_id_)) {
    }
};

} // namespace streaming
