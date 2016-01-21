/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

// Used to ensure that all .hh files build, as well as a place to put
// out-of-line implementations.

#include "streaming/messages/stream_message.hh"
#include "streaming/messages/stream_init_message.hh"
#include "streaming/messages/incoming_file_message.hh"
#include "streaming/messages/outgoing_file_message.hh"
#include "streaming/messages/file_message_header.hh"
#include "streaming/messages/prepare_message.hh"
#include "streaming/messages/received_message.hh"
#include "streaming/messages/retry_message.hh"
#include "streaming/messages/session_failed_message.hh"
#include "streaming/stream_session.hh"
#include "streaming/stream_task.hh"
#include "streaming/stream_event.hh"
#include "streaming/stream_event_handler.hh"
#include "streaming/compress/compression_info.hh"
#include "streaming/stream_state.hh"
#include "streaming/stream_result_future.hh"
#include "streaming/stream_manager.hh"
