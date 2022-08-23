/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
