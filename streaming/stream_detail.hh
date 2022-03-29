/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "query-request.hh"
#include "utils/UUID.hh"
#include <vector>
#include "range.hh"
#include "dht/i_partitioner.hh"

namespace streaming {

struct stream_detail {
    using UUID = utils::UUID;
    UUID cf_id;
    stream_detail() = default;
    stream_detail(UUID cf_id_)
        : cf_id(std::move(cf_id_)) {
    }
};

} // namespace streaming
