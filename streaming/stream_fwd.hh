/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "utils/UUID.hh"

namespace streaming {

class stream_event_handler;
class stream_manager;
class stream_result_future;
class stream_session;
class stream_state;

using plan_id = utils::tagged_uuid<struct plan_id_tag>;

} // namespace streaming

namespace service {

using session_id = utils::tagged_uuid<struct session_id_tag>;

}