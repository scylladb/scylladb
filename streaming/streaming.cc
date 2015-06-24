/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

// Used to ensure that all .hh files build, as well as a place to put
// out-of-line implementations.

#include "streaming/messages/stream_message.hh"
#include "streaming/messages/stream_init_message.hh"
#include "streaming/messages/complete_message.hh"
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
#include "streaming/connection_handler.hh"
#include "streaming/compress/compression_info.hh"
#include "streaming/stream_state.hh"
