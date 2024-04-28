/*
 * Copyright (C) 2018-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "db/timeout_clock.hh"
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
