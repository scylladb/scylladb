/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
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
