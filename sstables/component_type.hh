/*
 * Copyright (C) 2018-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <iosfwd>

namespace sstables {

enum class component_type {
    Index,
    CompressionInfo,
    Data,
    TOC,
    Summary,
    Digest,
    CRC,
    Filter,
    Statistics,
    TemporaryTOC,
    TemporaryStatistics,
    Scylla,
    Unknown,
};

std::ostream& operator<<(std::ostream&, const sstables::component_type&);

}

using component_type = ::sstables::component_type;
