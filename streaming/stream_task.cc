/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "streaming/stream_task.hh"
#include "streaming/stream_session.hh"

namespace streaming {

stream_task::stream_task(shared_ptr<stream_session> _session, table_id _cf_id)
    : session(_session)
    , cf_id(std::move(_cf_id)) {
}

stream_task::~stream_task() = default;

}
