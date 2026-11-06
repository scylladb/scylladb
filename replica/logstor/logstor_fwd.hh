/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

// Forward declarations of the logstor types that replica::database, replica::table and
// replica::compaction_group refer to. Those headers are included nearly everywhere, so they
// declare logstor rather than include it, and only the files that use logstor include it.

namespace replica::logstor {

class logstor;
class primary_index;
class logstor_group;
class segment_manager;
class compaction_manager;
class segment_set;
struct segment_snapshot;
class segment_stream_sink;

} // namespace replica::logstor
