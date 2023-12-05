/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


#include <seastar/core/metrics.hh>

/*
 * The following macro will be part of metrics.hh
 * It's here just for the RFC
 * Using a macro will allow future changes, such as centralizing
 * registration.
 */
#define METRICS_DESCRIPTION(m, d) static sm::description m(d)



namespace sm = seastar::metrics;

METRICS_DESCRIPTION(node_ops_finished_percentage, "Finished percentage of node operation on this shard");
