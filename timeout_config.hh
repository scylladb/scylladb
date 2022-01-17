/*
 * Copyright (C) 2018-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "db/timeout_clock.hh"

struct timeout_config {
    db::timeout_clock::duration read_timeout;
    db::timeout_clock::duration write_timeout;
    db::timeout_clock::duration range_read_timeout;
    db::timeout_clock::duration counter_write_timeout;
    db::timeout_clock::duration truncate_timeout;
    db::timeout_clock::duration cas_timeout;
    db::timeout_clock::duration other_timeout;
};

using timeout_config_selector = db::timeout_clock::duration (timeout_config::*);

extern const timeout_config infinite_timeout_config;

namespace db { class config; }
timeout_config make_timeout_config(const db::config& cfg);
