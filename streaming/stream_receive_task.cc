/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "streaming/stream_session.hh"
#include "streaming/stream_receive_task.hh"

namespace streaming {

stream_receive_task::stream_receive_task(shared_ptr<stream_session> _session, table_id _cf_id, int _total_files, long _total_size)
        : stream_task(_session, _cf_id)
        , total_files(_total_files)
        , total_size(_total_size) {
}

stream_receive_task::~stream_receive_task() {
}

} // namespace streaming
