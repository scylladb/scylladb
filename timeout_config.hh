/*
 * Copyright (C) 2018-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "db/timeout_clock.hh"
#include "db/write_type.hh"
#include "utils/updateable_value.hh"

namespace db { class config; }

class updateable_timeout_config;

/// timeout_config represents a snapshot of the options stored in it when
/// an instance of this class is created. so far this class is only used by
/// client_state. so either these classes are obliged to
/// update it by themselves, or they are fine with using the maybe-updated
/// options in the lifecycle of a client / connection even if some of these
/// options are changed whtn the client / connection is still alive.
struct timeout_config {
    using duration_t = db::timeout_clock::duration;

    duration_t read_timeout;
    duration_t write_timeout;
    duration_t range_read_timeout;
    duration_t counter_write_timeout;
    duration_t truncate_timeout;
    duration_t cas_timeout;
    duration_t other_timeout;
};

struct updateable_timeout_config {
    using timeout_option_t = utils::updateable_value<uint32_t>;

    timeout_option_t read_timeout_in_ms;
    timeout_option_t write_timeout_in_ms;
    timeout_option_t range_read_timeout_in_ms;
    timeout_option_t counter_write_timeout_in_ms;
    timeout_option_t truncate_timeout_in_ms;
    timeout_option_t cas_timeout_in_ms;
    timeout_option_t other_timeout_in_ms;

    explicit updateable_timeout_config(const db::config& cfg);

    timeout_config current_values() const;
};


using timeout_config_selector = db::timeout_clock::duration (timeout_config::*);

extern const timeout_config infinite_timeout_config;

struct timeout_info {
    timeout_config_selector selector;
    bool is_write;
    db::write_type write_type = db::write_type::SIMPLE;
    // Whether responses participate in send-queue timeout accounting.
    bool has_send_queue_deadline = true;
};

constexpr timeout_info read_timeout_info = {.selector = &timeout_config::read_timeout, .is_write = false};
constexpr timeout_info range_read_timeout_info = {.selector = &timeout_config::range_read_timeout, .is_write = false};
constexpr timeout_info write_timeout_info = {.selector = &timeout_config::write_timeout, .is_write = true};
constexpr timeout_info counter_write_timeout_info = {.selector = &timeout_config::counter_write_timeout, .is_write = true, .write_type = db::write_type::COUNTER};
constexpr timeout_info truncate_timeout_info = {.selector = &timeout_config::truncate_timeout, .is_write = true, .write_type = db::write_type::SIMPLE, .has_send_queue_deadline = false};
constexpr timeout_info other_timeout_info = {.selector = &timeout_config::other_timeout, .is_write = false, .write_type = db::write_type::SIMPLE, .has_send_queue_deadline = false};
constexpr timeout_info batch_write_timeout_info = {.selector = &timeout_config::write_timeout, .is_write = true, .write_type = db::write_type::BATCH};
constexpr timeout_info unlogged_batch_write_timeout_info = {.selector = &timeout_config::write_timeout, .is_write = true, .write_type = db::write_type::UNLOGGED_BATCH};
