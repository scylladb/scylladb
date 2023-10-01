/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <iostream>
#include <fmt/ostream.h>

namespace db {

enum class read_repair_decision {
  NONE,
  GLOBAL,
  DC_LOCAL
};

inline std::ostream&  operator<<(std::ostream& out, db::read_repair_decision d) {
    switch (d) {
    case db::read_repair_decision::NONE: out << "NONE"; break;
    case db::read_repair_decision::GLOBAL: out << "GLOBAL"; break;
    case db::read_repair_decision::DC_LOCAL: out << "DC_LOCAL"; break;
    default: out << "ERR"; break;
    }
    return out;
}

}

template <> struct fmt::formatter<db::read_repair_decision> : fmt::ostream_formatter {};
