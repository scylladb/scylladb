/*
 * Copyright (C) 2018-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <seastar/core/sstring.hh>
#include "consistency_level_type.hh"

#include "seastarx.hh"

class schema;

namespace db {

void validate_for_read(consistency_level cl);

void validate_for_write(consistency_level cl);

bool is_serial_consistency(consistency_level cl);

void validate_for_cas(consistency_level cl);

void validate_for_cas_learn(consistency_level cl, const sstring& keyspace);

void validate_counter_for_write(const schema& s, consistency_level cl);

}
